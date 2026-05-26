"""
247Sports Player TIMELINE Scraper - LONG FORMAT
================================================
Companion to scraper.py. Reuses the same player-list loading and the same
full-timeline page mechanics, but instead of keeping ONLY the commitment event,
it captures EVERY timeline event (official visits, unofficial visits, junior
days, camps, commitment, decommitment, signing, draft, etc.).

OUTPUT SHAPE: LONG FORMAT (one row per EVENT, not per player)
-------------------------------------------------------------
Because timelines vary wildly in length (one player may have 3 events, another
60+), a wide "Event 1..Event N" layout would be mostly empty and impossible to
analyze. Long format repeats the player identity/rating columns on every event
row, so the data pivots cleanly in Excel:
    - PivotTable: count of Event Type by Event Team by Class
    - Filter: all "Official Visit" rows, group by team
    - Funnel: camps/junior days -> commitment conversion

COLUMNS:
    247 ID | Player Name | High School Class | Position |
    247 Stars | 247 Rating | 247 National Rank | 247 Position Rank |
    Committed To |
    Event Date | Event Type | Event Team | Event Detail

Offers are intentionally EXCLUDED (per spec).

Reused, proven patterns from scraper.py (do not change these conventions):
    - domcontentloaded (never networkidle)
    - continue on exception, never break, with consecutive-failure counter
    - per-year checkpoint CSV with incremental saves
    - randomized delays + fixed user agent
"""

import asyncio
import csv
import os
import re
import random
import sys
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup

# =============================================================================
# CONFIGURATION
# =============================================================================

YEARS = [int(os.getenv('SCRAPE_YEAR', '2026'))]   # Set by workflow
OUTPUT_DIR = Path("output")
TEST_MODE = os.getenv('TEST_MODE', 'false').lower() == 'true'
MAX_CONCURRENT = 4
START_FROM_PLAYER = int(os.getenv('START_FROM', '0'))
MAX_TIMELINE_PAGES = 15   # safety cap on pagination per player

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# =============================================================================
# OUTPUT SCHEMA (LONG FORMAT - one row per event)
# =============================================================================

CSV_HEADERS = [
    "247 ID",
    "Player Name",
    "High School Class",
    "Position",
    "247 Stars",
    "247 Rating",
    "247 National Rank",
    "247 Position Rank",
    "Committed To",
    "Event Date",
    "Event Type",
    "Event Team",
    "Event Detail",
]

# =============================================================================
# EVENT TYPE CLASSIFICATION
# =============================================================================
# Maps keywords found in a timeline event's text to a normalized Event Type.
# Order matters: more specific phrases are checked before generic ones.

EVENT_TYPE_RULES = [
    ("Official Visit",      ["official visit", "officially visited", "ov to", "took an official"]),
    ("Unofficial Visit",    ["unofficial visit", "unofficially visited", "uv to", "took an unofficial"]),
    ("Junior Day",          ["junior day"]),
    ("Camp",                ["camp", "camped", "showcase", "combine", "elite camp"]),
    ("Decommitment",        ["decommit", "de-commit", "decommitted", "backs off", "reopened"]),
    ("Commitment",          ["commitment", "committed", "commits to", "pledge", "pledged"]),
    ("Signing",             ["signed", "signing", "loi", "letter of intent", "enrolled"]),
    ("Offer Visit/Contact", ["in-home", "in home visit", "home visit", "spring visit", "game day visit"]),
    ("Draft",               ["draft", "drafted", "selected by", "picked by"]),
    ("Crystal Ball",        ["crystal ball", "prediction", "predicts", "expert pick", "forecast"]),
]


def classify_event(text: str) -> str:
    """Return normalized Event Type from raw event text. 'Other' if unmatched."""
    low = text.lower()
    for label, keywords in EVENT_TYPE_RULES:
        for kw in keywords:
            if kw in low:
                return label
    return "Other"


# =============================================================================
# HELPER FUNCTIONS (mirrors scraper.py conventions)
# =============================================================================

def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()


def extract_player_id(url: str) -> str:
    match = re.search(r'-(\d+)/?$', url.rstrip('/'))
    if match:
        return match.group(1)
    match = re.search(r'/player/[^/]+-(\d+)', url)
    return match.group(1) if match else "NA"


def normalize_player_url(url: str) -> str:
    """Strip query params / fragments, force https, drop www, drop sub-profile path."""
    if not url:
        return url
    url = url.split('?')[0].split('#')[0]
    url = url.replace('http://', 'https://').replace('://www.', '://')
    # Reduce any institution-specific sub-path back to the base player URL
    m = re.match(r'(https://247sports\.com/player/[^/]+-\d+)', url)
    if m:
        return m.group(1) + '/'
    return url


def parse_rank(text: str) -> str:
    if not text:
        return "NA"
    match = re.search(r'#?(\d+)', text)
    return match.group(1) if match else "NA"


def normalize_date(date_str: str) -> str:
    if not date_str:
        return "NA"
    date_str = clean_text(date_str)
    for fmt in ("%m/%d/%Y", "%b %d, %Y", "%B %d, %Y", "%m/%d/%y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%m/%d/%Y")
        except ValueError:
            continue
    return date_str


def extract_date(text: str) -> str:
    """Find first date-like token in text and normalize it."""
    m = re.search(r'([A-Z][a-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4})', text)
    return normalize_date(m.group(1)) if m else "NA"


def extract_team(text: str, event_type: str) -> str:
    """
    Best-effort extraction of the team/school associated with an event.
    Timelines phrase this many ways: 'visited Arkansas', 'commits to LSU',
    'Oklahoma offered', 'camped at Texas'. We grab the proper-noun phrase
    after a linking verb/preposition.
    """
    patterns = [
        r'(?:commits?\s+to|committed\s+to|pledged?\s+to)\s+([A-Z][A-Za-z&\.\'\- ]+)',
        r'(?:visit(?:ed)?|camp(?:ed)?|signed\s+with|enrolled\s+at)\s+(?:to|at|with)?\s*([A-Z][A-Za-z&\.\'\- ]+)',
        r'(?:to|at|with)\s+([A-Z][A-Za-z&\.\'\- ]+)',
        r'([A-Z][A-Za-z&\.\'\- ]+?)\s+(?:offered|selected|drafted|picked)',
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            team = clean_text(m.group(1))
            # Trim trailing filler words
            team = re.sub(r'\b(on|in|for|during|the|a|an)\b.*$', '', team).strip()
            team = team.rstrip('.,;:')
            if 1 < len(team) <= 40:
                return team
    return "NA"


def append_to_csv(filename: Path, rows: list):
    file_exists = filename.exists()
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


async def rand_delay(lo=0.4, hi=1.1):
    await asyncio.sleep(random.uniform(lo, hi))


# =============================================================================
# PLAYER LIST LOADING  (same approach as scraper.py: Load More until complete)
# =============================================================================

async def load_player_list(browser, year: int) -> list:
    """Returns list of (player_url) for the year's composite rankings."""
    print(f"\n📋 Loading player list for {year}...")
    context = await browser.new_context(user_agent=USER_AGENT)
    page = await context.new_page()
    url = f"https://247sports.com/season/{year}-football/compositerecruitrankings/"

    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=60000)
        await page.wait_for_timeout(2000)
    except Exception as e:
        print(f"  ❌ Failed to load list page: {e}")
        await context.close()
        return []

    selectors = [
        "li.rankings-page__list-item",
        "li.recruit",
        ".rankings-page__container ul > li",
    ]
    valid_selector = None
    for sel in selectors:
        if await page.locator(sel).count() > 0:
            valid_selector = sel
            print(f"  ✓ Using selector '{sel}'")
            break
    if not valid_selector:
        print("  ⚠️ No players found")
        await context.close()
        return []

    click_count = 0
    max_clicks = 20 if TEST_MODE else 500
    consecutive_failures = 0
    no_button_checks = 0

    while click_count < max_clicks:
        load_more = page.locator(
            'a.load-more, button.load-more, a.rankings-page__showmore, a:has-text("Load More")'
        )
        try:
            if await load_more.count() > 0 and await load_more.first.is_visible():
                await load_more.first.click()
                await page.wait_for_timeout(random.randint(800, 1400))
                click_count += 1
                consecutive_failures = 0
                no_button_checks = 0
            else:
                # triple-check the button is truly gone (race-condition guard)
                no_button_checks += 1
                if no_button_checks >= 3:
                    break
                await page.wait_for_timeout(800)
        except Exception:
            consecutive_failures += 1
            if consecutive_failures >= 10:
                break
            await page.wait_for_timeout(800)
            continue  # never break on a single exception

    # Extract player profile links
    link_selectors = [".rankings-page__name-link", "a.rankings-page__name-link", "li a[href*='/player/']"]
    hrefs = []
    for sel in link_selectors:
        loc = page.locator(sel)
        n = await loc.count()
        if n > 0:
            for i in range(n):
                try:
                    href = await loc.nth(i).get_attribute('href')
                    if href and '/player/' in href:
                        hrefs.append(normalize_player_url(
                            href if href.startswith('http') else f"https://247sports.com{href}"
                        ))
                except Exception:
                    continue
            if hrefs:
                break

    # Dedup preserving order
    seen = set()
    unique = []
    for h in hrefs:
        if h not in seen:
            seen.add(h)
            unique.append(h)

    print(f"  ✓ Collected {len(unique)} unique player URLs")
    await context.close()

    if TEST_MODE:
        unique = unique[:50]
        print(f"  🧪 TEST_MODE: limited to {len(unique)} players")
    return unique


# =============================================================================
# PROFILE HEADER + RATINGS  (validated against live HTML, e.g. Colton Yarbrough)
# =============================================================================

async def navigate_to_hs_profile(page):
    """If landed on a JUCO/NCAA sub-profile, hop to the (HS) profile."""
    try:
        hs_href = await page.evaluate("""
            () => {
                const links = [...document.querySelectorAll('a')];
                const hs = links.find(a => a.textContent.includes('(HS)'));
                return hs ? hs.href : null;
            }
        """)
        if hs_href:
            await page.goto(hs_href, wait_until='domcontentloaded', timeout=30000)
            await page.wait_for_timeout(800)
    except Exception:
        pass


def parse_identity_and_ratings(soup, url, year) -> dict:
    """Extract the player identity + 247Sports (native) rating block."""
    rec = {h: "NA" for h in CSV_HEADERS}
    rec["247 ID"] = extract_player_id(url)
    rec["High School Class"] = str(year)

    name_elem = soup.select_one('.name') or soup.select_one('h1.name')
    if name_elem:
        rec["Player Name"] = clean_text(name_elem.get_text())

    # Position from header vitals
    for item in soup.select('.metrics-list li') + soup.select('.details li') + soup.select('ul.vitals li'):
        text = item.get_text(strip=True)
        if 'Pos' in text or 'Position' in text:
            m = re.search(r'(?:Pos|Position)[:\s]*(.*)', text, re.IGNORECASE)
            if m:
                rec["Position"] = clean_text(m.group(1))
                break

    # 247Sports (native) ranking section only — NOT composite
    for section in soup.select('section.rankings-section, div.ranking-section, section.rankings'):
        header = section.select_one('.rankings-header h3, h3.title, h3')
        if not header:
            continue
        htext = clean_text(header.get_text()).upper()
        if "JUCO" in htext:
            continue
        if "247SPORTS" in htext and "COMPOSITE" not in htext:
            stars = section.select('span.icon-starsolid.yellow, i.icon-starsolid.yellow')
            if stars:
                rec["247 Stars"] = str(min(len(stars), 5))
            rating_elem = section.select_one('.rank-block, .score, .rating')
            if rating_elem:
                rm = re.search(r'(\d+(?:\.\d+)?)', clean_text(rating_elem.get_text()))
                if rm:
                    rec["247 Rating"] = rm.group(1)
            ranks_list = section.select_one('ul.ranks-list')
            if ranks_list:
                for li in ranks_list.select('li'):
                    link = li.select_one('a')
                    if not link:
                        continue
                    href = link.get('href', '')
                    strong = link.select_one('strong')
                    if 'Position=' in href and strong:
                        rec["247 Position Rank"] = parse_rank(strong.get_text())
                    elif ('State=' in href) or ('state=' in href):
                        continue
                    elif 'InstitutionGroup=HighSchool' in href and strong:
                        rec["247 National Rank"] = parse_rank(strong.get_text())
            break
    return rec


# =============================================================================
# FULL TIMELINE EXTRACTION  (the new part — keep ALL events)
# =============================================================================

async def extract_all_timeline_events(page) -> list:
    """
    Navigate to the player's full timeline and return a list of event dicts:
        {date, type, team, detail}
    Reuses the discovered structure: a[href*='TimelineEvents'] -> full list,
    ul.timeline-event-index_lst li rows, li.next_itm a pagination.
    Falls back to the abbreviated on-profile timeline if no full link exists.
    """
    events = []
    seen = set()

    def harvest(soup):
        rows = soup.select('ul.timeline-event-index_lst li')
        if not rows:
            rows = soup.select('.timeline-item, .timeline li, ul.timeline > li, .vertical-timeline-element-content')
        for item in rows:
            text = clean_text(item.get_text())
            if not text:
                continue
            date = extract_date(text)
            etype = classify_event(text)
            # Skip pure offer rows (spec excludes offers) unless they carry a visit/commit too
            if etype == "Other" and 'offer' in text.lower():
                continue
            team = extract_team(text, etype)
            key = (date, etype, team, text[:60])
            if key in seen:
                continue
            seen.add(key)
            events.append({
                "date": date,
                "type": etype,
                "team": team,
                "detail": text[:240],
            })

    try:
        # Try to reach the full timeline page
        await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
        await page.wait_for_timeout(1200)
        see_all = page.locator('a[href*="TimelineEvents"]')
        if await see_all.count() > 0:
            href = await see_all.first.get_attribute('href')
            if href:
                full_url = f"https://247sports.com{href}" if href.startswith('/') else href
                try:
                    await page.goto(full_url, wait_until='domcontentloaded', timeout=20000)
                    await page.wait_for_timeout(800)
                    page_count = 0
                    while page_count < MAX_TIMELINE_PAGES:
                        soup = BeautifulSoup(await page.content(), 'html.parser')
                        harvest(soup)
                        nxt = page.locator('li.next_itm a')
                        try:
                            if await nxt.count() > 0 and await nxt.first.is_visible():
                                await nxt.first.click()
                                await page.wait_for_timeout(random.randint(700, 1200))
                                page_count += 1
                            else:
                                break
                        except Exception:
                            break  # pagination done/failed; keep what we have
                    return events
                except Exception:
                    pass  # fall through to abbreviated timeline

        # Fallback: abbreviated timeline already on the profile page
        soup = BeautifulSoup(await page.content(), 'html.parser')
        harvest(soup)
    except Exception:
        pass
    return events


# =============================================================================
# COMMITTED-TO  (single resolved destination, for the per-player identity cols)
# =============================================================================

def resolve_committed_to(soup, events) -> str:
    # Prefer the commit banner if present
    banner = soup.select_one('.commit-banner, .commitment')
    if banner:
        link = banner.select_one('a')
        if link:
            t = clean_text(link.get_text())
            if t and t.lower() not in ('committed', 'commitment', 'signed'):
                return t
    # Otherwise the most recent Commitment/Signing event team
    for e in events:
        if e["type"] in ("Commitment", "Signing") and e["team"] != "NA":
            return e["team"]
    return "NA"


# =============================================================================
# PER-PLAYER + BATCH
# =============================================================================

async def scrape_player_timeline(page, url, year, idx, total) -> list:
    """Return a list of LONG-format event rows for one player."""
    try:
        print(f"  [{idx}/{total}] {url.split('/player/')[-1].rstrip('/')}")
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(700)
        await navigate_to_hs_profile(page)

        soup = BeautifulSoup(await page.content(), 'html.parser')
        identity = parse_identity_and_ratings(soup, url, year)

        events = await extract_all_timeline_events(page)
        identity["Committed To"] = resolve_committed_to(soup, events)

        if not events:
            # Emit a single row so the player still appears (no events found)
            row = dict(identity)
            row.update({"Event Date": "NA", "Event Type": "No Events Found",
                        "Event Team": "NA", "Event Detail": "NA"})
            return [row]

        rows = []
        for e in events:
            row = dict(identity)
            row.update({
                "Event Date": e["date"],
                "Event Type": e["type"],
                "Event Team": e["team"],
                "Event Detail": e["detail"],
            })
            rows.append(row)
        print(f"        → {len(rows)} events")
        return rows

    except Exception as e:
        print(f"        ❌ Error: {e}")
        return []  # never break the batch


async def scrape_batch(browser, urls, year, total, start_idx) -> list:
    context = await browser.new_context(user_agent=USER_AGENT)
    out = []

    async def one(u, i):
        page = await context.new_page()
        try:
            return await scrape_player_timeline(page, u, year, start_idx + i + 1, total)
        finally:
            await page.close()
            await rand_delay()

    results = await asyncio.gather(*[one(u, i) for i, u in enumerate(urls)],
                                   return_exceptions=True)
    for r in results:
        if isinstance(r, list):
            out.extend(r)
    await context.close()
    return out


# =============================================================================
# YEAR DRIVER
# =============================================================================

async def scrape_year(browser, year: int) -> int:
    print(f"\n{'='*80}\n🗓️  TIMELINE SCRAPE — {year} CLASS\n{'='*80}")
    urls = await load_player_list(browser, year)
    if not urls:
        print(f"  ❌ No players for {year}")
        return 0

    if START_FROM_PLAYER > 0:
        print(f"  ⏩ Resuming from #{START_FROM_PLAYER}")
        urls = urls[START_FROM_PLAYER:]

    OUTPUT_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d')
    filename = OUTPUT_DIR / f"player_timeline_{year}_{timestamp}.csv"

    total = len(urls)
    total_rows = 0
    buffer = []

    for i in range(0, total, MAX_CONCURRENT):
        batch = urls[i:i + MAX_CONCURRENT]
        print(f"\n  📦 Batch {i // MAX_CONCURRENT + 1}/{(total + MAX_CONCURRENT - 1)//MAX_CONCURRENT}")
        rows = await scrape_batch(browser, batch, year, total, i)
        buffer.extend(rows)
        total_rows += len(rows)

        if len(buffer) >= 200:   # incremental save (event rows accumulate fast)
            append_to_csv(filename, buffer)
            print(f"    💾 Saved {len(buffer)} event rows")
            buffer = []
        print(f"    → Progress: {min(i+MAX_CONCURRENT, total)}/{total} players, {total_rows} event rows")

    if buffer:
        append_to_csv(filename, buffer)
        print(f"    💾 Saved final {len(buffer)} event rows")

    print(f"\n✅ {year}: {total_rows} event rows written to {filename.name}")
    return total_rows


async def main():
    print("="*80)
    print("247SPORTS PLAYER TIMELINE SCRAPER (LONG FORMAT)")
    print(f"Years: {YEARS} | TEST_MODE: {TEST_MODE} | START_FROM: {START_FROM_PLAYER}")
    print("="*80)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
        grand_total = 0
        for year in YEARS:
            grand_total += await scrape_year(browser, year)
        await browser.close()

    if grand_total == 0:
        print("\n❌ No event rows produced.")
        sys.exit(1)
    print(f"\n🎉 DONE — {grand_total} total event rows.")


if __name__ == "__main__":
    asyncio.run(main())
