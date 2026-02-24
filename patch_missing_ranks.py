"""
patch_missing_ranks.py
======================
Targeted fix for 2015-2019 high school recruiting data.
Instead of re-scraping all ~20,000 players (4+ hours), this script:

1. Loads each year's composite rankings LIST PAGE
2. Extracts rank + player URL for every listed player  
3. Identifies which players hold the missing rank positions
4. Fetches ONLY those ~72 profiles
5. Outputs a patch CSV that can be merged with the original

Runtime: ~10-15 minutes vs 4+ hours

Usage:
    python patch_missing_ranks.py                    # Runs against known gaps
    python patch_missing_ranks.py --year 2019        # Single year only
    python patch_missing_ranks.py --csv existing.csv # Auto-detect gaps from CSV
"""

import asyncio
import csv
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright

# =============================================================================
# CONFIGURATION
# =============================================================================

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
OUTPUT_DIR = Path("output")

# Known missing ranks from gap analysis (Feb 20 2026)
# These are the rank positions that exist on 247Sports but weren't captured
KNOWN_GAPS = {
    2015: {
        '247': [23, 50, 74, 102, 123, 146, 155, 170, 173, 175, 200, 205, 206, 224, 241, 245],
        'composite': [22, 79, 82, 99, 142, 146, 150, 152, 171, 174, 203, 206, 221, 241, 243, 252, 253, 257, 267, 268, 285, 293],
    },
    2016: {
        '247': [53, 64, 79, 86, 91, 98, 102, 105, 115, 122, 135, 166, 167, 172, 200, 203, 229],
        'composite': [35, 47, 49, 51, 69, 83, 95, 102, 106, 107, 112, 115, 117, 183, 186, 197, 207, 211, 220, 226, 234, 251, 263, 283, 288, 299, 300],
    },
    2017: {
        '247': [48, 113, 131, 140, 164, 206, 227],
        'composite': [80, 133, 141, 151, 153, 160, 190, 206, 222, 227, 231, 234, 236, 256, 264, 266, 289],
    },
    2018: {
        '247': [56, 81, 89, 165, 169, 176, 189, 192, 227],
        'composite': [77, 80, 101, 108, 147, 168, 172, 174, 175, 225, 298],
    },
    2019: {
        '247': [1, 30, 45, 69, 72, 88, 147, 148, 207, 212, 215],
        'composite': [5, 63, 66, 67, 91, 96, 140, 141, 238, 251, 288, 298],
    },
}

CSV_HEADERS = [
    "247 ID", "Player Name", "Position", "Height", "Weight", "High School",
    "City, ST", "Class", "247 Stars", "247 Rating", "247 National Rank",
    "247 Position", "247 Position Rank", "Composite Stars", "Composite Rating",
    "Composite National Rank", "Composite Position", "Composite Position Rank",
    "Signed Date", "Signed Team", "Draft Date", "Draft Team", "Recruiting Year",
    "Profile URL", "Scrape Date", "Data Source"
]

# =============================================================================
# HELPERS
# =============================================================================

def extract_player_id(url: str) -> str:
    match = re.search(r'/player/[^/]+-(\d+)/', url)
    return match.group(1) if match else "NA"

def clean_text(text: str) -> str:
    if not text: return "NA"
    return text.strip().replace('\n', ' ').replace('\r', '')

def normalize_height(height_str: str) -> str:
    if not height_str or height_str == "NA":
        return "NA"
    height_str = height_str.strip().strip("'\"")
    if '-' in height_str or "'" in height_str or (len(height_str) <= 4 and any(c.isdigit() for c in height_str)):
        return f"'{height_str}"
    return height_str

def parse_rank(text: str) -> str:
    if not text: return "NA"
    match = re.search(r'#?(\d+)', text)
    return match.group(1) if match else "NA"

def normalize_date(date_str: str) -> str:
    if not date_str: return "NA"
    date_str = clean_text(date_str)
    for fmt in ["%m/%d/%Y", "%b %d, %Y", "%B %d, %Y"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%m/%d/%Y")
        except ValueError:
            continue
    return date_str

# =============================================================================
# STEP 1: EXTRACT PLAYER URLs FROM LIST PAGE BY RANK
# =============================================================================

async def get_players_from_list(browser, year: int, max_rank_needed: int) -> dict:
    """
    Load the composite rankings list page and extract rank → player URL mapping.
    Only loads enough pages to cover up to max_rank_needed.
    
    Returns: {rank_number: {'url': str, 'name': str}, ...}
    """
    print(f"\n{'─'*60}")
    print(f"  📋 Loading {year} composite rankings list (need up to rank {max_rank_needed})...")
    
    context = await browser.new_context(user_agent=USER_AGENT)
    page = await context.new_page()
    
    # Block ad/overlay scripts that intercept clicks
    await page.route("**/*bouncex*", lambda route: route.abort())
    await page.route("**/*bounceexchange*", lambda route: route.abort())
    await page.route("**/*integralas*", lambda route: route.abort())
    
    url = f"https://247sports.com/season/{year}-football/compositerecruitrankings/"
    
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=60000)
        await page.wait_for_timeout(3000)
        # Dismiss any overlays that loaded
        await page.evaluate("document.querySelectorAll('[id^=\"bx-campaign\"], .bxc, .IL_BASE, [id=\"IL_INSEARCH\"], [id=\"d_IL_INSEARCH\"]').forEach(el => el.remove())")
    except Exception as e:
        print(f"  ❌ Failed to load page: {e}")
        await context.close()
        return {}
    
    # Click Load More until we have enough players
    # Each page load adds ~50 players, so for rank 300 we need ~6 clicks
    clicks_needed = (max_rank_needed // 50) + 5  # Extra buffer
    click_count = 0
    consecutive_failures = 0
    no_button_checks = 0
    
    print(f"  Clicking Load More (~{clicks_needed} clicks needed)...")
    
    for i in range(clicks_needed + 5):  # Small safety margin
        try:
            # Dismiss overlays before each click attempt
            await page.evaluate("document.querySelectorAll('[id^=\"bx-campaign\"], .bxc, .IL_BASE, [id=\"IL_INSEARCH\"], [id=\"d_IL_INSEARCH\"]').forEach(el => el.remove())")
            
            load_more = page.locator('a.load-more, button.load-more, a.rankings-page__showmore, a:has-text("Load More")')
            
            if await load_more.count() > 0 and await load_more.first.is_visible():
                try:
                    await load_more.first.click(timeout=5000)
                except Exception:
                    # Fallback to JS click if overlay respawns
                    await page.evaluate("""
                        () => {
                            const btn = document.querySelector('.rankings-page__showmore') 
                                || document.querySelector('a.load-more')
                                || [...document.querySelectorAll('a, button')].find(b => b.textContent.includes('Load More'));
                            if (btn) btn.click();
                        }
                    """)
                click_count += 1
                consecutive_failures = 0
                no_button_checks = 0
                await page.wait_for_timeout(1500)
                
                if click_count % 5 == 0:
                    items = await page.locator("li.rankings-page__list-item").count()
                    print(f"    Click #{click_count}: {items} players loaded")
                    # Early exit: stop once we have enough players
                    if items > max_rank_needed + 50:
                        print(f"  ✓ Enough players loaded for rank {max_rank_needed}")
                        break
            else:
                await page.wait_for_timeout(2000)
                no_button_checks += 1
                if no_button_checks >= 3:
                    items = await page.locator("li.rankings-page__list-item").count()
                    print(f"  ✓ All loaded ({items} players, {click_count} clicks)")
                    break
        except Exception as e:
            consecutive_failures += 1
            if consecutive_failures >= 5:
                print(f"  ⚠️ Stopping after {consecutive_failures} failures")
                break
            await page.wait_for_timeout(2000)
            continue
    
    # Extract rank + URL + name from each list item
    print(f"  Extracting rank → player mappings...")
    
    rank_to_player = {}
    
    # Use JavaScript to extract structured data from list items
    items_data = await page.evaluate("""
        () => {
            const items = document.querySelectorAll('li.rankings-page__list-item');
            const results = [];
            items.forEach(item => {
                const rankEl = item.querySelector('.rank-column .primary');
                const nameLink = item.querySelector('a.rankings-page__name-link');
                
                if (rankEl && nameLink) {
                    const rankText = rankEl.textContent.trim();
                    const rankMatch = rankText.match(/\\d+/);
                    if (rankMatch) {
                        results.push({
                            rank: parseInt(rankMatch[0]),
                            url: nameLink.href,
                            name: nameLink.textContent.trim()
                        });
                    }
                }
            });
            return results;
        }
    """)
    
    for item in items_data:
        rank_to_player[item['rank']] = {
            'url': item['url'],
            'name': item['name']
        }
    
    print(f"  ✓ Mapped {len(rank_to_player)} ranked players (ranks {min(rank_to_player.keys()) if rank_to_player else '?'}-{max(rank_to_player.keys()) if rank_to_player else '?'})")
    
    await context.close()
    return rank_to_player

# =============================================================================
# STEP 2: PARSE INDIVIDUAL PROFILE (same logic as main scraper)
# =============================================================================

async def parse_profile(page, url: str, year: int) -> dict:
    """Parse a single player profile — mirrors main scraper logic"""
    from bs4 import BeautifulSoup
    
    data = {header: "NA" for header in CSV_HEADERS}
    data['Profile URL'] = url
    data['Recruiting Year'] = str(year)
    data['Scrape Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data['Data Source'] = '247Sports Composite (patch)'
    data['Class'] = str(year)
    
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(1500)
        
        # Try navigating to recruiting profile
        try:
            recruiting_link = page.locator('a:has-text("View recruiting profile"), a:has-text("Recruiting Profile")')
            if await recruiting_link.count() > 0:
                await recruiting_link.first.click()
                await page.wait_for_load_state('domcontentloaded', timeout=30000)
                await page.wait_for_timeout(1000)
        except:
            pass
        
        # FIX: If we landed on a JUCO/NCAA page, navigate to the (HS) profile
        # These players have JUCO rankings showing instead of HS rankings
        try:
            current_url = page.url
            if 'junior-college' in current_url or 'college-' in current_url:
                hs_link = page.locator('a:has-text("(HS)")')
                if await hs_link.count() == 0:
                    hs_link = page.locator('a:has-text("(HS -")')  # handles (HS - FB), (HS - BK)
                if await hs_link.count() > 0:
                    await hs_link.first.click()
                    await page.wait_for_load_state('domcontentloaded', timeout=30000)
                    await page.wait_for_timeout(1000)
                    print(f"      → Navigated from JUCO/NCAA to HS profile")
            else:
                # Even if URL doesn't say JUCO, check if ranking headers say JUCO
                page_text = await page.content()
                if '247SportsJUCO' in page_text and 'InstitutionGroup=HighSchool' not in page_text:
                    hs_link = page.locator('a:has-text("(HS)")')
                    if await hs_link.count() == 0:
                        hs_link = page.locator('a:has-text("(HS -")')
                    if await hs_link.count() > 0:
                        await hs_link.first.click()
                        await page.wait_for_load_state('domcontentloaded', timeout=30000)
                        await page.wait_for_timeout(1000)
                        print(f"      → Navigated from JUCO rankings to HS profile")
        except:
            pass
        
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        data['247 ID'] = extract_player_id(url)
        
        # --- HEADER INFO ---
        name_elem = soup.select_one('.name') or soup.select_one('h1.name')
        if name_elem:
            data['Player Name'] = clean_text(name_elem.get_text())
        
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
        
        # --- RANKINGS ---
        ranking_sections = soup.select('section.rankings, section.rankings-section, div.ranking-section')
        
        for section in ranking_sections:
            header = section.select_one('.rankings-header h3, h3.title, h3')
            if not header: continue
            
            header_text = clean_text(header.get_text()).upper()
            prefix = None
            if "COMPOSITE" in header_text:
                prefix = "Composite"
            elif "247SPORTS" in header_text and "COMPOSITE" not in header_text:
                prefix = "247"
            if not prefix: continue
            
            stars = section.select('span.icon-starsolid.yellow, i.icon-starsolid.yellow')
            if stars: data[f'{prefix} Stars'] = str(min(len(stars), 5))
            
            rating_elem = section.select_one('.rank-block, .score, .rating')
            if rating_elem:
                rating_text = clean_text(rating_elem.get_text())
                rating_match = re.search(r'(\d+(?:\.\d+)?)', rating_text)
                if rating_match: data[f'{prefix} Rating'] = rating_match.group(1)
            
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
        
        # --- SIGNED TEAM FALLBACK ---
        if data['Signed Team'] == "NA":
            commit_banner = soup.select_one('.commit-banner, .commitment')
            if commit_banner:
                team_elem = commit_banner.select_one('span, a')
                if team_elem:
                    team_text = clean_text(team_elem.get_text())
                    if team_text.lower() not in ['committed', 'commitment', 'signed']:
                        data['Signed Team'] = team_text
        
        # --- SAVE DIAGNOSTIC HTML for players where parsing still fails ---
        has_any_rank = data['247 National Rank'] != "NA" or data['Composite National Rank'] != "NA"
        if not has_any_rank:
            os.makedirs('diagnostic_html', exist_ok=True)
            pid = data['247 ID']
            with open(f'diagnostic_html/patch_{year}_{pid}.html', 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"      💾 Saved diagnostic HTML (no ranks parsed)")
        
        return data
        
    except Exception as e:
        print(f"    ❌ Error: {e}")
        return data

# =============================================================================
# STEP 3: MAIN ORCHESTRATOR
# =============================================================================

async def main():
    print("="*60)
    print("🔧 TARGETED PATCH: Missing Ranks for 2015-2019")
    print("="*60)
    
    # Determine which years to process
    target_year = None
    csv_path = None
    
    for arg in sys.argv[1:]:
        if arg == '--year' or arg.startswith('--year='):
            pass  # handled below
        elif arg.isdigit() and 2015 <= int(arg) <= 2019:
            target_year = int(arg)
        elif arg.endswith('.csv'):
            csv_path = arg
    
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == '--year' and i < len(sys.argv):
            target_year = int(sys.argv[i + 1]) if sys.argv[i + 1].isdigit() else None
    
    years_to_fix = [target_year] if target_year else sorted(KNOWN_GAPS.keys())
    
    print(f"📅 Years to patch: {years_to_fix}")
    
    # Calculate total work
    total_gaps = 0
    for year in years_to_fix:
        gaps = KNOWN_GAPS.get(year, {'247': [], 'composite': []})
        max_rank = max(max(gaps['composite'], default=0), max(gaps['247'], default=0))
        total_247 = len(gaps['247'])
        total_comp = len(gaps['composite'])
        total_gaps += total_247 + total_comp
        print(f"  {year}: {total_247} 247 gaps + {total_comp} composite gaps (need list up to rank {max_rank})")
    
    print(f"\n🎯 Total lookups: {total_gaps} (will deduplicate after identifying players)")
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    patch_file = OUTPUT_DIR / f"patch_missing_ranks_{timestamp}.csv"
    diagnostic_file = OUTPUT_DIR / f"patch_diagnostic_{timestamp}.json"
    
    all_patched = []
    diagnostics = {}
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        for year in years_to_fix:
            gaps = KNOWN_GAPS[year]
            max_rank = max(max(gaps['composite'], default=0), max(gaps['247'], default=0))
            
            # STEP 1: Get rank → player mapping from list page
            rank_map = await get_players_from_list(browser, year, max_rank)
            
            if not rank_map:
                print(f"  ❌ Could not load list for {year}, skipping")
                continue
            
            # STEP 2: Identify which players we need to scrape
            # Collect unique player URLs for all missing ranks
            target_players = {}  # url → {reasons, rank_info}
            
            for rank in gaps['composite']:
                if rank in rank_map:
                    player = rank_map[rank]
                    url = player['url']
                    if url not in target_players:
                        target_players[url] = {
                            'name': player['name'],
                            'missing_composite_rank': rank,
                            'missing_247_rank': None,
                        }
                    else:
                        target_players[url]['missing_composite_rank'] = rank
                else:
                    print(f"  ⚠️ {year} Composite rank #{rank} not found on list page")
            
            for rank in gaps['247']:
                # 247-only ranks use a different list URL
                # But many of these players are also on the composite list
                # We'll check if we already have them, if not we note it
                found = False
                for url, info in target_players.items():
                    # Can't match by 247 rank from composite list — we'll fill it from profile
                    pass
                if not found:
                    # We'll need the 247-specific list for these
                    pass
            
            print(f"\n  🎯 {year}: {len(target_players)} unique players to re-scrape")
            
            # STEP 3: Scrape each target profile
            context = await browser.new_context(user_agent=USER_AGENT)
            
            for i, (url, info) in enumerate(target_players.items()):
                print(f"  [{i+1}/{len(target_players)}] {info['name']} (composite #{info.get('missing_composite_rank', '?')})")
                
                page = await context.new_page()
                await page.route("**/*.{png,jpg,jpeg,svg,mp4,woff,woff2}", lambda route: route.abort())
                
                data = await parse_profile(page, url, year)
                await page.close()
                
                # Report what we got
                got_247 = data['247 National Rank'] != "NA"
                got_comp = data['Composite National Rank'] != "NA"
                status_parts = []
                if got_247: status_parts.append(f"247 #{data['247 National Rank']}")
                if got_comp: status_parts.append(f"Comp #{data['Composite National Rank']}")
                status = " | ".join(status_parts) if status_parts else "⚠️ NO RANKS PARSED"
                print(f"    → {data['Player Name']}: {status}")
                
                all_patched.append(data)
                
                # Track diagnostics
                diagnostics[f"{year}_{data['247 ID']}"] = {
                    'year': year,
                    'name': data['Player Name'],
                    'url': url,
                    'expected_composite_rank': info.get('missing_composite_rank'),
                    'got_247_rank': data['247 National Rank'],
                    'got_composite_rank': data['Composite National Rank'],
                    'success': got_247 or got_comp,
                }
                
                await asyncio.sleep(1)  # Polite delay
            
            await context.close()
            
            # Also try the 247-specific list for any 247 rank gaps not covered
            # URL: https://247sports.com/season/{year}-football/recruitrankings/
            missing_247_not_covered = [r for r in gaps['247'] 
                                       if not any(d.get('got_247_rank') == str(r) 
                                                 for d in diagnostics.values() 
                                                 if d['year'] == year)]
            
            if missing_247_not_covered:
                print(f"\n  📋 Loading {year} 247-only rankings for {len(missing_247_not_covered)} remaining 247 gaps...")
                max_247_rank = max(missing_247_not_covered)
                
                r247_url = f"https://247sports.com/season/{year}-football/recruitrankings/"
                context247 = await browser.new_context(user_agent=USER_AGENT)
                page247 = await context247.new_page()
                
                try:
                    await page247.goto(r247_url, wait_until='domcontentloaded', timeout=60000)
                    await page247.wait_for_timeout(3000)
                    
                    # Load More for 247 list (only need ~5 clicks for rank 247)
                    for _ in range(10):
                        try:
                            btn = page247.locator('a.load-more, a.rankings-page__showmore, a:has-text("Load More")')
                            if await btn.count() > 0 and await btn.first.is_visible():
                                await btn.first.click()
                                await page247.wait_for_timeout(1500)
                            else:
                                break
                        except:
                            continue
                    
                    # Extract rank → URL from 247-only list
                    r247_data = await page247.evaluate("""
                        () => {
                            const items = document.querySelectorAll('li.rankings-page__list-item');
                            const results = [];
                            items.forEach(item => {
                                const rankEl = item.querySelector('.rank-column .primary');
                                const nameLink = item.querySelector('a.rankings-page__name-link');
                                if (rankEl && nameLink) {
                                    const rankMatch = rankEl.textContent.trim().match(/\\d+/);
                                    if (rankMatch) {
                                        results.push({
                                            rank: parseInt(rankMatch[0]),
                                            url: nameLink.href,
                                            name: nameLink.textContent.trim()
                                        });
                                    }
                                }
                            });
                            return results;
                        }
                    """)
                    
                    r247_map = {item['rank']: item for item in r247_data}
                    
                    for rank in missing_247_not_covered:
                        if rank in r247_map:
                            player = r247_map[rank]
                            # Check if we already scraped this player from composite
                            already_scraped = any(
                                d['url'] == player['url'] 
                                for d in diagnostics.values()
                            )
                            if not already_scraped:
                                print(f"  [{rank}/247] {player['name']} (247-only)")
                                pg = await context247.new_page()
                                await pg.route("**/*.{png,jpg,jpeg,svg,mp4,woff,woff2}", lambda route: route.abort())
                                data = await parse_profile(pg, player['url'], year)
                                await pg.close()
                                
                                got_247 = data['247 National Rank'] != "NA"
                                print(f"    → {data['Player Name']}: 247 #{data['247 National Rank']}")
                                all_patched.append(data)
                                await asyncio.sleep(1)
                        else:
                            print(f"  ⚠️ {year} 247 rank #{rank} not found on 247-only list either")
                    
                except Exception as e:
                    print(f"  ❌ Failed to load 247-only list: {e}")
                
                await context247.close()
        
        await browser.close()
    
    # =================================================================
    # SAVE RESULTS
    # =================================================================
    
    if all_patched:
        with open(patch_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writeheader()
            writer.writerows(all_patched)
        
        print(f"\n{'='*60}")
        print(f"✅ PATCH COMPLETE")
        print(f"{'='*60}")
        print(f"📊 Players scraped: {len(all_patched)}")
        print(f"💾 Patch CSV: {patch_file}")
        
        # Summary
        success = sum(1 for d in diagnostics.values() if d['success'])
        fail = sum(1 for d in diagnostics.values() if not d['success'])
        print(f"✅ Ranks parsed successfully: {success}")
        print(f"⚠️ Ranks still failing: {fail}")
        
        if fail > 0:
            print(f"\n⚠️ Failed profiles (check diagnostic_html/):")
            for key, d in diagnostics.items():
                if not d['success']:
                    print(f"   {d['year']} {d['name']}: {d['url']}")
    else:
        print("\n❌ No data patched")
    
    # Save diagnostics
    with open(diagnostic_file, 'w') as f:
        json.dump(diagnostics, f, indent=2)
    print(f"📋 Diagnostics: {diagnostic_file}")
    
    print(f"\n💡 To merge: load both CSVs into pandas, concat, drop_duplicates on '247 ID' + 'Recruiting Year'")

if __name__ == "__main__":
    asyncio.run(main())
