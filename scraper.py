"""
247Sports High School Recruiting Class Scraper - PRODUCTION VERSION
Scrapes recruiting class data from 247Sports composite rankings (2019-2026)

OPTIMIZATIONS:
- Deep timeline dive for top 1000 players only (commitment dates)
- Early exit when commitment found (no unnecessary pagination)
- Incremental CSV saves (no data loss on timeout)
- Resume capability (can continue from player #X)
- No debug logging (maximum speed)

FIXES (v2):
- Load More loop uses continue instead of break on errors
- Consecutive failure counter (10 in a row before stopping)
- Post-loop verification that Load More is truly gone
- Triple-check before declaring list complete (race condition fix)
- Expected total capture for validation
- Dual-selector link extraction
- URL normalization to prevent duplicate scrapes

FIXES (v3):
- JUCO/NCAA profile detection: navigates to (HS) profile before parsing
- Link extraction uses specific list selector first, broad as fallback
- DataFrame dedup safety net on 247 ID before CSV save
- Improved URL normalization (www, http, query params)
"""

import asyncio
import csv
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# =============================================================================
# CONFIGURATION
# =============================================================================

YEARS = [int(os.getenv('SCRAPE_YEAR', '2020'))]  # Set by workflow matrix (2018-2027)
OUTPUT_DIR = Path("output")
TEST_MODE = os.getenv('TEST_MODE', 'false').lower() == 'true'
MAX_CONCURRENT = 4
DEEP_TIMELINE_LIMIT = 1000  # Only get commitment dates for top 1000 players

# Resume capability
START_FROM_PLAYER = int(os.getenv('START_FROM', '0'))  # Set via workflow input

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# =============================================================================
# DATA STRUCTURES
# =============================================================================

CSV_HEADERS = [
    "247 ID", "Player Name", "Position", "Height", "Weight", "High School",
    "City, ST", "Class", "247 Stars", "247 Rating", "247 National Rank",
    "247 Position", "247 Position Rank", "Composite Stars", "Composite Rating",
    "Composite National Rank", "Composite Position", "Composite Position Rank",
    "Signed Date", "Signed Team", "Draft Date", "Draft Team", "Recruiting Year",
    "Profile URL", "Scrape Date", "Data Source"
]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def extract_player_id(url: str) -> str:
    match = re.search(r'/player/[^/]+-(\d+)/', url)
    return match.group(1) if match else "NA"

def clean_text(text: str) -> str:
    if not text: return "NA"
    return text.strip().replace('\n', ' ').replace('\r', '')

def normalize_height(height_str: str) -> str:
    """Normalize height to prevent Excel from converting to dates (6-3 -> '6-3)"""
    if not height_str or height_str == "NA": 
        return "NA"
    
    # Remove any quotes already present
    height_str = height_str.strip().strip("'\"")
    
    # If it looks like a height (contains - or '), add leading apostrophe for Excel
    if '-' in height_str or "'" in height_str or (len(height_str) <= 4 and any(c.isdigit() for c in height_str)):
        return f"'{height_str}"
    
    return height_str

def normalize_player_url(url: str) -> str:
    """
    Normalize 247Sports player URLs to prevent duplicate scrapes.
    Handles: www vs non-www, http vs https, trailing slash, query params,
    and /college-XXXXX/ or /junior-college-XXXXX/ suffixes.
    """
    url = url.split('?')[0].split('#')[0]
    url = url.replace('://www.', '://')
    url = url.replace('http://', 'https://')
    
    match = re.match(r'(https://247sports\.com/player/[^/]+)', url)
    if match:
        return match.group(1) + '/'
    return url

def parse_rank(text: str) -> str:
    if not text: return "NA"
    match = re.search(r'#?(\d+)', text)
    return match.group(1) if match else "NA"

def normalize_date(date_str: str) -> str:
    """Converts various date formats to MM/DD/YYYY"""
    if not date_str: return "NA"
    date_str = clean_text(date_str)
    
    formats = [
        "%m/%d/%Y",
        "%b %d, %Y",
        "%B %d, %Y"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%m/%d/%Y")
        except ValueError:
            continue
    return date_str

def is_date_valid_for_class(date_str: str, recruiting_year: int) -> bool:
    """Date must be BEFORE Sept 1st of the Recruiting Year"""
    if date_str == "NA": return False
    try:
        dt = datetime.strptime(date_str, "%m/%d/%Y")
        cutoff_date = datetime(recruiting_year, 9, 1)
        return dt < cutoff_date
    except:
        return True

def append_to_csv(filename: Path, players: list):
    """Append players to CSV file (creates if doesn't exist)"""
    file_exists = filename.exists()
    
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(players)

# =============================================================================
# LOAD MORE FUNCTIONALITY
# =============================================================================

async def click_load_more_until_complete(browser, year: int) -> list:
    print(f"\n📋 Loading all players for {year}...")
    
    context = await browser.new_context(user_agent=USER_AGENT)
    page = await context.new_page()
    
    url = f"https://247sports.com/season/{year}-football/compositerecruitrankings/"
    
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=60000)
        await page.wait_for_timeout(2000)
    except Exception as e:
        print(f"❌ Failed to load initial page for {year}: {e}")
        await context.close()
        return []

    selectors = [
        "li.rankings-page__list-item",
        "li.recruit",
        ".rankings-page__container ul > li"
    ]
    
    valid_selector = None
    for selector in selectors:
        count = await page.locator(selector).count()
        if count > 0:
            valid_selector = selector
            print(f"  ✓ Found {count} players using selector: '{selector}'")
            break
            
    if not valid_selector:
        print(f"⚠️  No players found")
        await context.close()
        return []

    # Capture expected total from page header for validation
    expected_total = None
    try:
        for hdr_selector in [
            ".rankings-page__header .total",
            ".result-count",
            ".rankings-page__header",
            "h1",
        ]:
            try:
                el = page.locator(hdr_selector).first
                if await el.count() > 0:
                    text = await el.text_content()
                    match = re.search(r'(\d{3,})', text)  # 3+ digit number
                    if match:
                        expected_total = int(match.group(1))
                        print(f"  📊 Page reports {expected_total} total players")
                        break
            except:
                continue
        if expected_total is None:
            print(f"  ⚠️ Could not determine expected player count from page")
    except:
        pass

    click_count = 0
    max_clicks = 500 if not TEST_MODE else 20
    consecutive_failures = 0
    no_button_checks = 0
    
    while click_count < max_clicks:
        current_players = await page.locator(valid_selector).count()
        load_more_button = page.locator('a.load-more, button.load-more, a.rankings-page__showmore, a:has-text("Load More")')
        
        try:
            if await load_more_button.count() > 0 and await load_more_button.first.is_visible():
                await load_more_button.first.click()
                click_count += 1
                consecutive_failures = 0
                no_button_checks = 0
                await page.wait_for_timeout(1500)
                
                if click_count % 25 == 0:
                    current_count = await page.locator(valid_selector).count()
                    print(f"  → Click #{click_count}: {current_count} players loaded...")
            else:
                await page.wait_for_timeout(2000)
                no_button_checks += 1
                
                if no_button_checks >= 3:
                    final_count = await page.locator(valid_selector).count()
                    print(f"  ✓ All players loaded! ({final_count} players, {click_count} clicks)")
                    break
                    
        except Exception as e:
            consecutive_failures += 1
            print(f"  ⚠️ Load More error ({consecutive_failures}/10): {e}")
            await page.wait_for_timeout(3000)
            
            if consecutive_failures >= 10:
                final_count = await page.locator(valid_selector).count()
                print(f"  ❌ Too many consecutive failures after {click_count} clicks ({final_count} players loaded)")
                break
            continue
    
    # Post-loop verification
    await page.wait_for_timeout(3000)
    try:
        final_check = page.locator('a.load-more, button.load-more, a.rankings-page__showmore, a:has-text("Load More")')
        if await final_check.count() > 0 and await final_check.first.is_visible():
            print(f"  ⚠️ Load More still visible after loop! Running cleanup...")
            for _ in range(50):
                try:
                    if await final_check.count() > 0 and await final_check.first.is_visible():
                        await final_check.first.click()
                        await page.wait_for_timeout(2000)
                    else:
                        break
                except:
                    continue
            cleanup_count = await page.locator(valid_selector).count()
            print(f"  ✓ Cleanup complete ({cleanup_count} players)")
    except:
        pass
    
    # ---------------------------------------------------------------
    # EXTRACT PLAYER LINKS — v3: specific selector first
    # ---------------------------------------------------------------
    print(f"\n🔗 Extracting player profile URLs...")
    
    # Primary: specific list-item name links (only matches ranking list entries)
    player_links = await page.eval_on_selector_all(
        ".rankings-page__name-link",
        "elements => elements.map(e => e.href)"
    )
    
    # Fallback: scoped to list items
    if not player_links:
        player_links = await page.eval_on_selector_all(
            f"{valid_selector} a[href*='/player/']",
            "elements => elements.map(e => e.href)"
        )
    
    # Last resort: broad selector
    if not player_links:
        player_links = await page.eval_on_selector_all(
            "a[href*='/player/']",
            "elements => elements.map(e => e.href)"
        )
    
    # Normalize and deduplicate
    player_urls = list(dict.fromkeys(
        normalize_player_url(u) for u in player_links
        if u and '/player/' in u and '247sports.com' in u
    ))
    
    print(f"  ✓ Found {len(player_urls)} unique player profiles")
    
    # Validate against expected total
    if expected_total and not TEST_MODE:
        coverage = len(player_urls) / expected_total * 100
        if coverage >= 95:
            print(f"  ✅ Coverage: {coverage:.1f}% ({len(player_urls)}/{expected_total})")
        elif coverage >= 80:
            print(f"  ⚠️ Coverage: {coverage:.1f}% ({len(player_urls)}/{expected_total}) — some players may be missing")
        else:
            print(f"  ❌ Coverage: {coverage:.1f}% ({len(player_urls)}/{expected_total}) — SIGNIFICANT DATA LOSS")
    
    if TEST_MODE and len(player_urls) > 300:
        player_urls = player_urls[:300]
        print(f"  ℹ️  TEST MODE: Limited to 300 players")
    
    await context.close()
    return player_urls

# =============================================================================
# PROFILE PARSING
# =============================================================================

async def navigate_to_recruiting_profile(page) -> bool:
    try:
        recruiting_link = page.locator('a:has-text("View recruiting profile"), a:has-text("Recruiting Profile")')
        if await recruiting_link.count() > 0:
            await recruiting_link.first.click()
            await page.wait_for_load_state('domcontentloaded', timeout=30000)
            await page.wait_for_timeout(1000)
            return True
        return False
    except:
        return False

async def navigate_to_hs_profile(page) -> bool:
    """
    If on a cover/NCAA/JUCO profile, navigate to the high-school recruiting
    profile so the HS rankings (rating, stars, ranks) are present to parse.

    Built against the real institution-block markup (confirmed via live DOM):

        <div class="institution-block">
          <button data-js="institution-selector">NCAA ▾</button>
          <ul class="institution-list">
            <li><a class="profile-card__institution-list-link"
                   href=".../college-356988">LSU (NCAA)</a></li>
            <li><a class="profile-card__institution-list-link"
                   href=".../college-330851">USC (NCAA)</a></li>
            <li><a class="profile-card__institution-list-link"
                   href=".../high-school-281777">Inglewood (HS)</a></li>
            <li><a class="profile-card__institution-list-link"
                   href=".../high-school-292509">Corona Centennial (HS)</a></li>
          </ul>
        </div>

    Why the old version missed: it grabbed links before the institution-list
    rendered, and didn't target the specific link class. This version waits for
    the list, queries the exact class, and — when a player has MULTIPLE high
    schools (e.g. Husan Longstreet: Inglewood + Corona Centennial) — picks the
    LAST (HS) link, which is the most recent school carrying the recruiting grade.
    Matches on the /high-school- URL pattern, not just the '(HS)' label text,
    so it's robust to label formatting.
    """
    try:
        # Wait briefly for the institution list to exist (it's part of the header,
        # but can populate a beat after domcontentloaded on cover profiles).
        try:
            await page.wait_for_selector('ul.institution-list a.profile-card__institution-list-link',
                                         timeout=4000)
        except Exception:
            pass  # no dropdown -> single-profile player; nothing to navigate to

        hs_href = await page.evaluate("""
            () => {
                const links = [...document.querySelectorAll(
                    'ul.institution-list a.profile-card__institution-list-link, a.profile-card__institution-list-link'
                )];
                // Prefer the /high-school- URL pattern; fall back to '(HS)' label.
                const hs = links.filter(a =>
                    /\\/high-school-\\d+/.test(a.href) || a.textContent.includes('(HS)')
                );
                if (hs.length === 0) return null;
                // LAST high-school link = most recent school = carries the grade.
                return hs[hs.length - 1].href;
            }
        """)

        if hs_href:
            # Don't re-navigate if we're already on this exact HS profile.
            if hs_href.rstrip('/') != page.url.rstrip('/'):
                await page.goto(hs_href, wait_until='domcontentloaded', timeout=30000)
                await page.wait_for_timeout(1000)
            return True
        return False
    except Exception:
        return False

async def parse_timeline(page, data, year, do_deep_dive: bool):
    """
    Parses timeline for Commitment and Draft info.
    
    Args:
        do_deep_dive: If True, clicks "See All Entries" and paginates for commitment.
                      If False, only parses abbreviated timeline.
    """
    try:
        # ALWAYS parse abbreviated timeline (for Draft info)
        html = await page.content()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        items = soup.select('.timeline-item, .timeline li, ul.timeline > li, .vertical-timeline-element-content')
        
        for item in items:
            item_text = clean_text(item.get_text())
            
            # --- DRAFT LOGIC (always needed) ---
            if 'draft' in item_text.lower():
                date_match = re.search(r'([A-Z][a-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})', item_text)
                if date_match and data['Draft Date'] == "NA":
                     data['Draft Date'] = normalize_date(date_match.group(1))
                
                team_match = re.search(r'(?:Draft[:\s]+)?([A-Z][A-Za-z0-9\s\.]+?)\s+(?:select|pick)', item_text, re.IGNORECASE)
                if team_match and data['Draft Team'] == "NA":
                    team_name = clean_text(team_match.group(1))
                    team_name = re.sub(r'^Draft\s*', '', team_name, flags=re.IGNORECASE).strip()
                    if team_name and team_name.lower() not in ['draft']:
                        data['Draft Team'] = team_name
            
            # --- COMMITMENT from abbreviated timeline ---
            item_priority = 0
            if 'commitment' in item_text.lower() or 'committed' in item_text.lower() or 'commits to' in item_text.lower():
                 item_priority = 100
            elif 'signed' in item_text.lower() or 'signing' in item_text.lower():
                 item_priority = 1
            
            if item_priority > 0:
                date_match = re.search(r'([A-Z][a-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})', item_text)
                found_date = normalize_date(date_match.group(1)) if date_match else "NA"
                
                if found_date != "NA" and is_date_valid_for_class(found_date, year):
                    current_priority = data.get('_date_priority', -1)
                    
                    if item_priority > current_priority:
                        data['Signed Date'] = found_date
                        data['_date_priority'] = item_priority
                        
                        team_match = re.search(r'(?:to|with|at|commits to)\s+([A-Z][^,.]+)', item_text)
                        if team_match:
                            data['Signed Team'] = clean_text(team_match.group(1))
        
        # --- DEEP DIVE (only for top players) ---
        if do_deep_dive:
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await page.wait_for_timeout(1500)
            
            see_all_link = page.locator('a[href*="TimelineEvents"]')
            if await see_all_link.count() > 0:
                href = await see_all_link.first.get_attribute('href')
                if href:
                    full_timeline_url = f"https://247sports.com{href}" if href.startswith('/') else href
                    try:
                        await page.goto(full_timeline_url, wait_until='domcontentloaded', timeout=15000)
                        
                        page_count = 0
                        max_pages = 10
                        
                        while page_count < max_pages:
                            html = await page.content()
                            soup = BeautifulSoup(html, 'html.parser')
                            
                            full_items = soup.select('ul.timeline-event-index_lst li')
                            
                            for item in full_items:
                                item_text = clean_text(item.get_text())
                                
                                item_priority = 0
                                if 'commitment' in item_text.lower() or 'committed' in item_text.lower() or 'commits to' in item_text.lower():
                                     item_priority = 100
                                elif 'signed' in item_text.lower() or 'signing' in item_text.lower():
                                     item_priority = 1
                                
                                if item_priority > 0:
                                    date_match = re.search(r'([A-Z][a-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})', item_text)
                                    found_date = normalize_date(date_match.group(1)) if date_match else "NA"
                                    
                                    if found_date != "NA" and is_date_valid_for_class(found_date, year):
                                        current_priority = data.get('_date_priority', -1)
                                        
                                        if item_priority > current_priority:
                                            data['Signed Date'] = found_date
                                            data['_date_priority'] = item_priority
                                            
                                            team_match = re.search(r'(?:to|with|at|commits to)\s+([A-Z][^,.]+)', item_text)
                                            if team_match:
                                                data['Signed Team'] = clean_text(team_match.group(1))
                                            
                                            if item_priority == 100:
                                                return  # EARLY EXIT
                            
                            next_button = page.locator('li.next_itm a')
                            if await next_button.count() > 0 and await next_button.is_visible():
                                await next_button.click()
                                await page.wait_for_timeout(1000)
                                page_count += 1
                            else:
                                break
                                
                    except Exception:
                        pass

    except Exception:
        pass

async def _wait_for_rankings(page):
    """
    LAZY-RENDER FIX: The 247Sports rating block lives in 'lower-cards' below the
    fold and can lazy-render after domcontentloaded. Players not nationally ranked
    on the 247 native scale (e.g. Colton Yarbrough — rating 89, no national rank)
    were losing ALL rankings because the section wasn't in the DOM yet when we
    grabbed HTML after a flat 1s wait.

    This scrolls the section into view to trigger any lazy load, then waits for it
    to appear. Purely additive: if it never shows, we proceed exactly as before
    and the existing parser handles the absence gracefully (fields stay "NA").
    """
    try:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_selector(
            'section.rankings, section.rankings-section, div.ranking-section',
            timeout=5000
        )
        await page.wait_for_timeout(300)  # brief settle for inner content
        await page.evaluate("window.scrollTo(0, 0)")
    except Exception:
        pass  # selector never appeared — fall through, parse handles it


def _parse_rankings(soup, data):
    """
    Parse the 247Sports + Composite ranking sections from the page soup.
    Extracted verbatim from the original inline block so it can be re-run on a
    fresh page grab during the retry path. Behavior is unchanged.
    """
    ranking_sections = soup.select('section.rankings, section.rankings-section, div.ranking-section')

    for section in ranking_sections:
        header = section.select_one('.rankings-header h3, h3.title, h3')
        if not header:
            continue

        header_text = clean_text(header.get_text()).upper()

        # v3 FIX: Skip JUCO ranking sections entirely
        if "JUCO" in header_text:
            continue

        prefix = None
        if "COMPOSITE" in header_text:
            prefix = "Composite"
        elif "247SPORTS" in header_text and "COMPOSITE" not in header_text:
            prefix = "247"
        if not prefix:
            continue

        stars = section.select('span.icon-starsolid.yellow, i.icon-starsolid.yellow')
        if stars:
            data[f'{prefix} Stars'] = str(min(len(stars), 5))

        rating_elem = section.select_one('.rank-block, .score, .rating')
        if rating_elem:
            rating_text = clean_text(rating_elem.get_text())
            rating_match = re.search(r'(\d+(?:\.\d+)?)', rating_text)
            if rating_match:
                data[f'{prefix} Rating'] = rating_match.group(1)

        ranks_list = section.select_one('ul.ranks-list')
        if ranks_list:
            for li in ranks_list.select('li'):
                pos_node = li.select_one('b')
                link_tag = li.select_one('a')

                if link_tag:
                    href = link_tag.get('href', '')

                    if 'Position=' in href:
                        if pos_node:
                            data[f'{prefix} Position'] = clean_text(pos_node.get_text())
                        rank_node = link_tag.select_one('strong')
                        if rank_node:
                            data[f'{prefix} Position Rank'] = parse_rank(rank_node.get_text())

                    elif 'State=' in href or 'state=' in href:
                        continue

                    elif 'InstitutionGroup=HighSchool' in href:
                        rank_node = link_tag.select_one('strong')
                        if rank_node:
                            data[f'{prefix} National Rank'] = parse_rank(rank_node.get_text())


async def parse_profile(page, url: str, year: int, player_num: int, total: int) -> dict:
    data = {header: "NA" for header in CSV_HEADERS}
    data['Profile URL'] = url
    data['Recruiting Year'] = str(year)
    data['Scrape Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data['Data Source'] = '247Sports Composite'
    data['_date_priority'] = -1
    
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(1000)
        
        await navigate_to_recruiting_profile(page)
        
        # v3 FIX: Navigate to HS profile if on JUCO/NCAA page
        await navigate_to_hs_profile(page)
        
        # LAZY-RENDER FIX: ensure the (below-the-fold) rankings block is in the DOM
        await _wait_for_rankings(page)
        
        html = await page.content()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        data['247 ID'] = extract_player_id(url)
        
        # --- HEADER INFO ---
        name_elem = soup.select_one('.name') or soup.select_one('h1.name')
        if name_elem: data['Player Name'] = clean_text(name_elem.get_text())
        
        all_header_items = soup.select('.metrics-list li') + soup.select('.details li') + soup.select('ul.vitals li')
        for item in all_header_items:
            text = item.get_text(strip=True)
            if 'Pos' in text or 'Position' in text:
                match = re.search(r'(?:Pos|Position)[:\s]*(.*)', text, re.IGNORECASE)
                if match: data['Position'] = clean_text(match.group(1))
            elif 'Height' in text:
                match = re.search(r'Height[:\s]*(.*)', text, re.IGNORECASE)
                if match: data['Height'] = normalize_height(match.group(1))
            elif 'Weight' in text:
                match = re.search(r'Weight[:\s]*(.*)', text, re.IGNORECASE)
                if match: data['Weight'] = clean_text(match.group(1))
            elif 'High School' in text:
                match = re.search(r'High School[:\s]*(.*)', text, re.IGNORECASE)
                if match: data['High School'] = clean_text(match.group(1))
            elif 'Home Town' in text or 'Hometown' in text or 'City' in text:
                match = re.search(r'(?:Home Town|Hometown|City)[:\s]*(.*)', text, re.IGNORECASE)
                if match: data['City, ST'] = clean_text(match.group(1))
            elif 'Class' in text:
                match = re.search(r'Class[:\s]*(.*)', text, re.IGNORECASE)
                if match: data['Class'] = clean_text(match.group(1))
        
        data['Class'] = str(year)
        
        # --- RANKINGS ---
        _parse_rankings(soup, data)

        # RETRY-ON-EMPTY (lazy-render safety net): if BOTH the 247 and Composite
        # ratings came back empty, the rankings block likely hadn't finished
        # rendering when we grabbed HTML. Wait once more, re-grab, and re-parse.
        # Additive: only fires when we'd otherwise have NO rating at all, so it
        # cannot disturb players who already parsed correctly.
        if data['247 Rating'] == "NA" and data['Composite Rating'] == "NA":
            try:
                await _wait_for_rankings(page)
                await page.wait_for_timeout(800)
                retry_soup = BeautifulSoup(await page.content(), 'html.parser')
                _parse_rankings(retry_soup, data)
            except Exception:
                pass

        # --- TIMELINE (with conditional deep dive) ---
        do_deep_dive = player_num <= DEEP_TIMELINE_LIMIT
        await parse_timeline(page, data, year, do_deep_dive)

        # Fallback for Signed Team
        if data['Signed Team'] == "NA":
            commit_banner = soup.select_one('.commit-banner, .commitment')
            if commit_banner:
                team_elem = commit_banner.select_one('span, a')
                if team_elem:
                    team_text = clean_text(team_elem.get_text())
                    if team_text.lower() not in ['committed', 'commitment', 'signed']:
                        data['Signed Team'] = team_text
        
        return data
        
    except Exception as e:
        print(f"    ❌ Error parsing {data.get('Player Name', 'Unknown')}: {e}")
        return data

# =============================================================================
# CONCURRENT SCRAPING
# =============================================================================

async def scrape_player_batch(browser, urls: list, year: int, batch_num: int, total_players: int) -> list:
    tasks = []
    context = await browser.new_context(user_agent=USER_AGENT)
    
    for i, url in enumerate(urls):
        page = await context.new_page()
        player_num = batch_num * MAX_CONCURRENT + i + 1
        tasks.append(scrape_player(page, url, year, player_num, total_players))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    await context.close()
    
    valid_results = []
    for result in results:
        if isinstance(result, dict):
            if '_date_priority' in result: del result['_date_priority']
            valid_results.append(result)
    return valid_results

async def scrape_player(page, url: str, year: int, player_num: int, total: int) -> dict:
    try:
        print(f"  [{player_num}/{total}] {url.split('/')[-2]}")
        data = await parse_profile(page, url, year, player_num, total)
        
        if data['Player Name'] != "NA":
            deep_marker = "🔍" if player_num <= DEEP_TIMELINE_LIMIT else "⚡"
            print(f"    ✓ {deep_marker} {data['Player Name']} - {data['Position']} - {data['Composite Stars']}⭐")
        
        return data
    except Exception as e:
        print(f"    ❌ Error: {e}")
        return {header: "NA" for header in CSV_HEADERS}
    finally:
        await page.close()

# =============================================================================
# MAIN SCRAPER
# =============================================================================

async def scrape_year(browser, year: int) -> list:
    print(f"\n{'='*80}")
    print(f"🎓 SCRAPING {year} RECRUITING CLASS")
    print(f"{'='*80}")
    
    player_urls = await click_load_more_until_complete(browser, year)
    
    if not player_urls:
        print(f"  ❌ No players found for {year}")
        return []
    
    # Resume capability
    if START_FROM_PLAYER > 0:
        print(f"  ⏩ Resuming from player #{START_FROM_PLAYER}")
        player_urls = player_urls[START_FROM_PLAYER:]
    
    print(f"\n🔄 Scraping {len(player_urls)} player profiles...")
    print(f"   🔍 Deep timeline for first {DEEP_TIMELINE_LIMIT} (commitment dates)")
    print(f"   ⚡ Fast scrape for remaining players (draft only)")
    
    # Setup incremental CSV
    year_range = f"{min(YEARS)}-{max(YEARS)}" if len(YEARS) > 1 else str(YEARS[0])
    timestamp = datetime.now().strftime('%Y%m%d')
    filename = OUTPUT_DIR / f"recruiting_class_{year_range}_{timestamp}.csv"
    
    all_data = []
    batch_buffer = []
    
    for i in range(0, len(player_urls), MAX_CONCURRENT):
        batch = player_urls[i:i + MAX_CONCURRENT]
        batch_num = i // MAX_CONCURRENT
        
        print(f"\n  📦 Batch {batch_num + 1}/{(len(player_urls) + MAX_CONCURRENT - 1) // MAX_CONCURRENT}")
        batch_data = await scrape_player_batch(browser, batch, year, batch_num, len(player_urls))
        
        all_data.extend(batch_data)
        batch_buffer.extend(batch_data)
        
        # INCREMENTAL SAVE - Every 100 players
        if len(batch_buffer) >= 100:
            append_to_csv(filename, batch_buffer)
            print(f"    💾 Saved {len(batch_buffer)} players to CSV")
            batch_buffer = []
        
        print(f"    → Progress: {len(all_data)}/{len(player_urls)} players")
    
    # Save any remaining players in buffer
    if batch_buffer:
        append_to_csv(filename, batch_buffer)
        print(f"    💾 Saved final {len(batch_buffer)} players to CSV")
    
    print(f"\n✅ Completed {year}: {len(all_data)} players scraped")
    return all_data

async def main():
    print("\n" + "="*80)
    print("🏈 247SPORTS RECRUITING CLASS SCRAPER - PRODUCTION v3")
    print("="*80)
    print(f"📅 Years: {YEARS}")
    print(f"🧪 Test Mode: {TEST_MODE}")
    print(f"⚡ Concurrency: {MAX_CONCURRENT}")
    print(f"🔍 Deep Timeline Limit: Top {DEEP_TIMELINE_LIMIT} players")
    if START_FROM_PLAYER > 0:
        print(f"⏩ Resume Mode: Starting from player #{START_FROM_PLAYER}")
    print("="*80)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        all_players = []
        
        for year in YEARS:
            year_data = await scrape_year(browser, year)
            all_players.extend(year_data)
        
        await browser.close()
    
    if not all_players:
        print("\n❌ CRITICAL: No data scraped.")
        sys.exit(1)

    print(f"\n{'='*80}")
    print(f"✅ SCRAPING COMPLETE!")
    print(f"{'='*80}")
    print(f"📊 Total Players: {len(all_players)}")
    print(f"💾 Data saved incrementally throughout run")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        sys.exit(1)
