"""
European Club Cups Data Scraper
Scrapes match data from Champions League, Europa League, and Conference League
from FlashScore.com. Results are saved as CSV files in the output/ directory.
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Optional
import hashlib
import re
import time
import csv
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.absolute()

COMPETITIONS = {
    "UCL": {
        "name": "Champions League",
        "flashscore_url": "https://www.flashscore.com/football/europe/champions-league/results/"
    },
    "UEL": {
        "name": "Europa League",
        "flashscore_url": "https://www.flashscore.com/football/europe/europa-league/results/"
    },
    "UECL": {
        "name": "Conference League",
        "flashscore_url": "https://www.flashscore.com/football/europe/conference-league/results/"
    }
}

# Expected match counts per competition per phase.
# Same knockout structure for all three; league phase differs for UECL (6 games vs 8).
EXPECTED_MATCHES_PER_PHASE = {
    "UCL":  {"LEAGUE_PHASE": 144, "PLAYOFF": 16, "ROUND_OF_16": 16, "QUARTER_FINAL": 8, "SEMI_FINAL": 4, "FINAL": 1},
    "UEL":  {"LEAGUE_PHASE": 144, "PLAYOFF": 16, "ROUND_OF_16": 16, "QUARTER_FINAL": 8, "SEMI_FINAL": 4, "FINAL": 1},
    "UECL": {"LEAGUE_PHASE": 108, "PLAYOFF": 16, "ROUND_OF_16": 16, "QUARTER_FINAL": 8, "SEMI_FINAL": 4, "FINAL": 1},
}


def load_scraper_params(params_path: Optional[str] = None) -> dict:
    if params_path is None:
        params_path = SCRIPT_DIR.parent / "params" / "scraper_params.json"
    else:
        p = Path(params_path)
        params_path = p if p.is_absolute() else SCRIPT_DIR / params_path

    try:
        with open(params_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Parameters file not found: {params_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing parameters file: {e}")
        sys.exit(1)


def is_match_in_any_phase(match_date: str, competition_code: str, params: dict) -> bool:
    """Returns True if match_date falls within any phase date range for the competition."""
    if not match_date or not params:
        return False

    try:
        match_dt = datetime.strptime(match_date, "%Y-%m-%d")
        for key in params:
            if key.startswith(f"{competition_code}_") and key.endswith("_INITIAL_DATE"):
                phase_name = key.replace(f"{competition_code}_", "").replace("_INITIAL_DATE", "")
                end_key = f"{competition_code}_{phase_name}_END_DATE"
                if end_key not in params:
                    continue
                try:
                    start = datetime.strptime(params[key], "%Y-%m-%d")
                    end = datetime.strptime(params[end_key], "%Y-%m-%d")
                    if start <= match_dt <= end:
                        return True
                except ValueError:
                    continue
        return False
    except ValueError as e:
        print(f"   ⚠️  Date parsing error for {match_date} in {competition_code}: {e}")
        return False
    except Exception as e:
        print(f"   ⚠️  Error checking date ranges for {competition_code}: {e}")
        return True


def get_phase_from_params(match_date: str, competition_code: str, params: Optional[dict]) -> str:
    """Returns the phase name for a match date based on scraper_params.json date ranges."""
    if not match_date or not params:
        return "UNKNOWN"

    try:
        match_dt = datetime.strptime(match_date, "%Y-%m-%d")
        for key in params:
            if key.startswith(f"{competition_code}_") and key.endswith("_INITIAL_DATE"):
                phase = key.replace(f"{competition_code}_", "").replace("_INITIAL_DATE", "")
                end_key = f"{competition_code}_{phase}_END_DATE"
                if end_key not in params:
                    continue
                try:
                    start = datetime.strptime(params[key], "%Y-%m-%d")
                    end = datetime.strptime(params[end_key], "%Y-%m-%d")
                    if start <= match_dt <= end:
                        return phase
                except ValueError:
                    continue
        return "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def validate_matches(matches: List[Dict], competition_code: str) -> bool:
    """Prints expected vs actual match count per phase. Returns True if all completed phases match."""
    expected = EXPECTED_MATCHES_PER_PHASE.get(competition_code, {})

    actual: Dict[str, int] = {}
    for m in matches:
        phase = m["PHASE"]
        actual[phase] = actual.get(phase, 0) + 1

    all_phases = sorted(set(list(expected.keys()) + list(actual.keys())))

    print(f"\n   {'Phase':<20} {'Expected':>8} {'Actual':>8}  Status")
    print(f"   {'-'*52}")

    all_ok = True
    for phase in all_phases:
        exp = expected.get(phase)
        act = actual.get(phase, 0)

        if exp is None:
            status = "⚠️  unexpected phase"
        elif act == exp:
            status = "✅"
        elif act < exp:
            status = f"❌  missing {exp - act}"
            all_ok = False
        else:
            status = f"⚠️  extra {act - exp}"

        exp_str = str(exp) if exp is not None else "?"
        print(f"   {phase:<20} {exp_str:>8} {act:>8}  {status}")

    return all_ok


def generate_match_id(competition: str, season: str, phase: str, home_team: str,
                      away_team: str, match_date: str) -> str:
    match_string = f"{competition}|{season}|{phase}|{home_team}|{away_team}|{match_date}"
    match_hash = hashlib.md5(match_string.encode()).hexdigest()[:8].upper()
    phase_clean = re.sub(r'[^A-Z0-9_]', '_', phase.upper())[:20]
    season_clean = season.replace("/", "_")
    return f"{competition}_{season_clean}_{phase_clean}_{match_hash}"


def clean_team_name(team_name: str) -> str:
    """Remove FlashScore artefacts from team names."""
    if not team_name:
        return team_name
    # Remove "Pen " prefix shown on penalty-round winners (e.g. "Pen Panathinaikos")
    cleaned = re.sub(r'^Pen\s+', '', team_name, flags=re.IGNORECASE)
    # Remove "Advancing to next round" / "Winner: X" labels and everything after them
    cleaned = re.sub(r'Advancing to next round.*$', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'Winner:.*$', '', cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip().rstrip(':').strip()
    # Strip trailing isolated digits (e.g. "Real Madrid2" -> "Real Madrid")
    cleaned = re.sub(r'(?<=[a-zA-Z])\d+$', '', cleaned).strip()
    return cleaned


def parse_date(date_str: str) -> Optional[str]:
    """Parse various date formats to YYYY-MM-DD."""
    if not date_str:
        return None

    date_str = date_str.strip()

    formats = [
        "%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d",
        "%d %B %Y", "%d %b %Y", "%B %d, %Y", "%b %d, %Y",
        "%d.%m.%y", "%d/%m/%y"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue

    date_match = re.search(r'(\d{1,2})[./](\d{1,2})[./](\d{4})', date_str)
    if date_match:
        day, month, year = date_match.groups()
        try:
            return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
        except Exception:
            pass

    # DD.MM without year — infer from context
    date_match = re.search(r'(\d{1,2})\.(\d{1,2})(?!\.)', date_str)
    if date_match:
        day, month = date_match.groups()
        try:
            current_year = datetime.now().year
            year = current_year - 1 if int(month) > datetime.now().month else current_year
            dt = datetime(year, int(month), int(day))
            if dt > datetime.now():
                dt = datetime(year - 1, int(month), int(day))
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    return None


def init_driver(headless: bool = True) -> webdriver.Chrome:
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    try:
        return webdriver.Chrome(options=chrome_options)
    except Exception as e:
        print(f"❌ Error initializing Chrome driver: {e}")
        print("   Make sure ChromeDriver is installed and in your PATH")
        raise


def scrape_flashscore_competition(competition_code: str, params: Optional[dict] = None) -> List[Dict]:
    """Scrape match results from FlashScore for a competition."""
    comp_config = COMPETITIONS.get(competition_code)
    if not comp_config:
        return []

    url = comp_config["flashscore_url"]
    print(f"🔎 Scraping {comp_config['name']} from FlashScore: {url}")

    driver = None
    try:
        driver = init_driver(headless=True)
        driver.get(url)

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.event__match"))
            )
        except Exception:
            time.sleep(8)

        print("   🔄 Loading all matches...")
        max_attempts = 10
        attempts = 0
        previous_match_count = 0

        while attempts < max_attempts:
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                current_matches = driver.find_elements(By.CSS_SELECTOR, "div.event__match")
                previous_match_count = len(current_matches)

                show_more_button = None

                # Priority 1: footer button by class
                try:
                    candidates = driver.find_elements(By.CSS_SELECTOR, "button[class*='wcl-footer__button']")
                    for btn in candidates:
                        if btn.is_displayed() and "more" in btn.text.lower():
                            show_more_button = btn
                            break
                except Exception:
                    pass

                # Priority 2: any <button> with "show more matches" text, excluding sidebar
                if not show_more_button:
                    try:
                        candidates = driver.find_elements(By.XPATH,
                            "//button[contains(translate(normalize-space(text()),"
                            "'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),"
                            "'show more matches')]")
                        for btn in candidates:
                            classes = btn.get_attribute("class") or ""
                            if btn.is_displayed() and "leftMenu" not in classes:
                                show_more_button = btn
                                break
                    except Exception:
                        pass

                # Priority 3: footer container > any clickable "show more"
                if not show_more_button:
                    try:
                        candidates = driver.find_elements(By.CSS_SELECTOR, "[class*='wcl-footer'] *")
                        for btn in candidates:
                            if btn.is_displayed() and "more" in btn.text.lower():
                                show_more_button = btn
                                break
                    except Exception:
                        pass

                if show_more_button:
                    try:
                        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", show_more_button)
                        time.sleep(1)
                        driver.execute_script("arguments[0].click();", show_more_button)
                        attempts += 1
                        print(f"   ✓ Clicked 'Show more matches' button (attempt {attempts}/{max_attempts})")
                        time.sleep(4)

                        new_matches = driver.find_elements(By.CSS_SELECTOR, "div.event__match, div[class*='event__match']")
                        new_match_count = len(new_matches)
                        if new_match_count > previous_match_count:
                            print(f"   ✓ Loaded {new_match_count - previous_match_count} additional matches (total: {new_match_count})")
                        else:
                            print(f"   ⚠️  No new matches detected after click (still {new_match_count} matches)")
                    except Exception as e:
                        print(f"   ⚠️  Error clicking button: {str(e)}")
                        attempts += 1
                        time.sleep(1)
                else:
                    print("   ✓ No 'Show more matches' button found. All matches loaded.")
                    break

            except Exception as e:
                print(f"   ⚠️  Error while looking for 'Show more matches' button: {str(e)}")
                attempts += 1
                time.sleep(1)

        if attempts >= max_attempts:
            print(f"   ⚠️  Reached maximum attempts ({max_attempts}). Proceeding with current matches.")

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        # Find match elements — try Selenium selectors first, fall back to BeautifulSoup
        match_selectors = [
            "div.event__match",
            "div[class*='event__match']",
            "div.event__match--twoLine",
            "div[data-testid='match-row']",
            "div.sportName",
            "div[class*='event']"
        ]

        event_matches_selenium = []
        for selector in match_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements and len(elements) > 10:
                    event_matches_selenium = elements
                    print(f"   Found {len(elements)} matches using selector: {selector}")
                    break
            except Exception:
                continue

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        if not event_matches_selenium:
            event_matches = soup.find_all('div', class_=re.compile(r'event__match', re.I))
            if not event_matches:
                event_matches = soup.find_all('div', attrs={'data-testid': re.compile(r'match', re.I)})
            if not event_matches:
                all_divs = soup.find_all('div')
                for div in all_divs:
                    if re.search(r'\d+\s*:\s*\d+', div.get_text()):
                        event_matches.append(div)
            print(f"   Found {len(event_matches)} potential match elements (BeautifulSoup)")
            event_matches_list = event_matches
        else:
            event_matches_list = []
            for elem in event_matches_selenium:
                html = elem.get_attribute('outerHTML')
                soup_elem = BeautifulSoup(html, 'html.parser')
                event_matches_list.append(soup_elem.find('div') or soup_elem)

        print(f"   Found {len(event_matches_list)} potential match elements")

        matches = extract_matches_from_flashscore_elements(event_matches_list, competition_code, params)

        print(f"✅ Found {len(matches)} matches from {comp_config['name']}")
        if matches:
            print(f"   Sample: {matches[0]['HOME_TEAM']} {matches[0]['HOME_GOALS']}-{matches[0]['AWAY_GOALS']} {matches[0]['AWAY_TEAM']}")
        return matches

    except Exception as e:
        print(f"❌ Error scraping FlashScore: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        if driver:
            driver.quit()


def extract_matches_from_flashscore_elements(elements, competition_code: str,
                                             params: Optional[dict] = None) -> List[Dict]:
    """Extract matches from FlashScore HTML elements."""
    matches = []
    current_date = None

    print(f"   Processing {len(elements)} elements...")
    successful = 0
    skipped = 0  # outside date range
    errors = 0   # actual extraction failures
    no_score = 0
    no_teams = 0

    for element in elements:
        try:
            if hasattr(element, 'find_all'):
                match_element = element
            else:
                html = element.get_attribute('outerHTML')
                match_element = BeautifulSoup(html, 'html.parser').find('div') or BeautifulSoup(html, 'html.parser')

            full_text = match_element.get_text(separator=' | ', strip=True)

            # --- Team extraction ---
            home_team = None
            away_team = None

            # Method 1: participant class elements
            participants = match_element.find_all(['span', 'div', 'a'],
                                                  class_=re.compile(r'event__participant|participant', re.I))
            if len(participants) >= 2:
                home_team = clean_team_name(participants[0].get_text(strip=True))
                away_team = clean_team_name(participants[1].get_text(strip=True))
                if home_team == away_team and len(participants) >= 3:
                    away_team = clean_team_name(participants[2].get_text(strip=True))

            # Method 2: pipe-separated text
            if not home_team or not away_team:
                parts = [p.strip() for p in full_text.split('|')]
                team_candidates = []
                for part in parts:
                    if not part.isdigit() and not re.match(r'^\d{1,2}\.\d{1,2}', part):
                        if len(part) > 2:
                            team_candidates.append(part)
                            if len(team_candidates) >= 2:
                                break
                if len(team_candidates) >= 2:
                    if not home_team:
                        home_team = clean_team_name(team_candidates[0])
                    if not away_team:
                        candidate = team_candidates[1] if team_candidates[1] != home_team else (team_candidates[2] if len(team_candidates) > 2 else team_candidates[1])
                        away_team = clean_team_name(candidate)

            # Method 3: any element with substantial text
            if not home_team or not away_team:
                seen_texts = set()
                text_elements = []
                for e in match_element.find_all(['span', 'div', 'a']):
                    text = e.get_text(strip=True)
                    if text and len(text) > 3 and not text.isdigit() and text not in seen_texts:
                        seen_texts.add(text)
                        text_elements.append(e)
                    if len(text_elements) >= 3:
                        break
                if len(text_elements) >= 2:
                    if not home_team:
                        home_team = clean_team_name(text_elements[0].get_text(strip=True))
                    if not away_team:
                        t = clean_team_name(text_elements[1].get_text(strip=True))
                        away_team = t if t != home_team else (clean_team_name(text_elements[2].get_text(strip=True)) if len(text_elements) >= 3 else t)

            if not home_team or not away_team:
                no_teams += 1
                continue

            # Normalize team names
            home_team = re.sub(r'\s+', ' ', re.sub(r'^\d+\.?\s*', '', clean_team_name(home_team))).strip()
            away_team = re.sub(r'\s+', ' ', re.sub(r'^\d+\.?\s*', '', clean_team_name(away_team))).strip()

            # If still same, retry from pipe-separated text
            if home_team == away_team:
                parts = [p.strip() for p in full_text.split('|')]
                candidates = []
                for part in parts:
                    if not part.isdigit() and not re.match(r'^\d{1,2}\.\d{1,2}', part):
                        cleaned = clean_team_name(part)
                        if len(cleaned) > 2 and cleaned not in candidates:
                            candidates.append(cleaned)
                        if len(candidates) >= 2:
                            break
                if len(candidates) >= 2:
                    home_team, away_team = candidates[0], candidates[1]

            if len(home_team) < 2 or len(away_team) < 2 or home_team == away_team:
                no_teams += 1
                continue

            # --- Score extraction ---
            home_goals = None
            away_goals = None
            # Penalty shootout: winning team is labelled "Pen" by FlashScore.
            # Pen matches end level at 90+ET; we want that draw score, not the shootout digits.
            has_pen = bool(re.search(r'\bpen\b', full_text, re.IGNORECASE))
            parent = match_element.find_parent()

            def best_score(pairs, pen):
                draw = [(h, a) for h, a in pairs if h == a]
                no_draw = [(h, a) for h, a in pairs if h != a]
                return (draw or pairs)[0] if pen else (no_draw or pairs)[-1]

            # Method 1: full-time score elements
            ft_elems = match_element.find_all(['span', 'div'],
                                              class_=re.compile(r'event__score.*ft|event__score.*final|event__part.*ft|event__result.*ft|event__result.*final', re.I))
            if not ft_elems and parent:
                ft_elems = parent.find_all(['span', 'div'],
                                           class_=re.compile(r'event__score.*ft|event__score.*final|event__part.*ft|event__result.*ft|event__result.*final', re.I))
            if ft_elems:
                pairs = [(int(m.group(1)), int(m.group(2))) for e in ft_elems for m in [re.search(r'(\d+)\s*[:|]\s*(\d+)', e.get_text(strip=True))] if m]
                if pairs:
                    home_goals, away_goals = best_score(pairs, has_pen)

            # Method 2: generic score elements
            if home_goals is None:
                score_elems = match_element.find_all(['span', 'div'],
                                                     class_=re.compile(r'event__score|event__result|score', re.I))
                if not score_elems and parent:
                    score_elems = parent.find_all(['span', 'div'],
                                                  class_=re.compile(r'event__score|event__result|score', re.I))
                if score_elems:
                    pairs = [(int(m.group(1)), int(m.group(2))) for e in score_elems for m in [re.search(r'(\d+)\s*[:|]\s*(\d+)', e.get_text(strip=True))] if m]
                    if pairs:
                        home_goals, away_goals = best_score(pairs, has_pen)

            # Method 3: X:Y patterns in full text
            if home_goals is None:
                pairs = [(int(m.group(1)), int(m.group(2))) for m in re.finditer(r'(\d+)\s*[:]\s*(\d+)', full_text)]
                if pairs:
                    home_goals, away_goals = best_score(pairs, has_pen)

            # Method 4: consecutive digit pairs in pipe-separated text
            if home_goals is None:
                parts = [p.strip() for p in full_text.split('|')]
                pairs = [(int(parts[i]), int(parts[i+1])) for i in range(len(parts) - 1) if parts[i].isdigit() and parts[i+1].isdigit()]
                if pairs:
                    home_goals, away_goals = best_score(pairs, has_pen)

            if home_goals is None or away_goals is None:
                no_score += 1
                continue

            # --- Date extraction ---
            match_date = None

            # Method 1: pipe-separated text (DD.MM or DD.MM.YYYY)
            parts = [p.strip() for p in full_text.split('|')]
            for part in parts:
                m = re.search(r'(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?', part)
                if m:
                    day, month, year = m.groups()
                    try:
                        if year:
                            match_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                        else:
                            cur_year = datetime.now().year
                            year = cur_year - 1 if int(month) > datetime.now().month else cur_year
                            dt = datetime(year, int(month), int(day))
                            if dt > datetime.now():
                                dt = datetime(year - 1, int(month), int(day))
                            match_date = dt.strftime("%Y-%m-%d")
                        current_date = match_date
                        break
                    except Exception:
                        pass

            # Method 2: date class elements
            if not match_date:
                date_elems = match_element.find_all(['span', 'div'],
                                                    class_=re.compile(r'event__time|event__date|time|date', re.I))
                if not date_elems and parent:
                    date_elems = parent.find_all(['span', 'div'],
                                                 class_=re.compile(r'event__time|event__date|time|date', re.I))
                if date_elems:
                    parsed = parse_date(date_elems[0].get_text(strip=True))
                    if parsed:
                        match_date = parsed
                        current_date = parsed

            # Method 3: DD.MM.YYYY in full text
            if not match_date:
                m = re.search(r'(\d{1,2})[./](\d{1,2})[./](\d{4})', full_text)
                if m:
                    day, month, year = m.groups()
                    try:
                        match_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                        current_date = match_date
                    except Exception:
                        pass

            # Method 4: DD.MM without year
            if not match_date:
                m = re.search(r'(\d{1,2})\.(\d{1,2})(?!\.)', full_text)
                if m:
                    day, month = m.groups()
                    try:
                        cur_year = datetime.now().year
                        year = cur_year - 1 if int(month) > datetime.now().month else cur_year
                        dt = datetime(year, int(month), int(day))
                        if dt > datetime.now():
                            dt = datetime(year - 1, int(month), int(day))
                        match_date = dt.strftime("%Y-%m-%d")
                        current_date = match_date
                    except Exception:
                        pass

            if not match_date:
                match_date = current_date or datetime.now().strftime("%Y-%m-%d")

            # --- Season & phase ---
            if params and params.get("SEASON"):
                season = params["SEASON"]
            else:
                try:
                    y, mo = int(match_date.split('-')[0]), int(match_date.split('-')[1])
                    season = f"{y}/{y+1}" if mo >= 7 else f"{y-1}/{y}"
                except Exception:
                    season = f"{datetime.now().year - 1}/{datetime.now().year}"

            phase = get_phase_from_params(match_date, competition_code, params)

            # --- Date range filter ---
            if params and not is_match_in_any_phase(match_date, competition_code, params):
                skipped += 1
                continue

            match_id = generate_match_id(competition_code, season, phase, home_team, away_team, match_date)
            matches.append({
                "MATCH_ID": match_id,
                "COMPETITION": competition_code,
                "SEASON": season,
                "PHASE": phase,
                "MATCH_DATE": match_date,
                "HOME_TEAM": home_team,
                "AWAY_TEAM": away_team,
                "HOME_GOALS": home_goals,
                "AWAY_GOALS": away_goals
            })
            successful += 1

        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"   ⚠️  Extraction error: {str(e)[:50]}")
            continue

    print(f"   Extracted {successful} matches")
    print(f"   Stats: {no_score} no score, {no_teams} no teams, {skipped} outside date range, {errors} errors")
    return matches


def fetch_all_competitions(save_csv: bool = True, params: Optional[dict] = None) -> Dict[str, List[Dict]]:
    """Scrape matches from all three European club competitions."""
    if params is None:
        try:
            params = load_scraper_params()
            print(f"📋 Loaded scraper parameters:")
            print(f"   Season: {params.get('SEASON', 'Not set')}")
            print(f"   UCL League Phase: {params.get('UCL_LEAGUE_PHASE_INITIAL_DATE', 'N/A')} to {params.get('UCL_LEAGUE_PHASE_END_DATE', 'N/A')}")
            print(f"   UEL League Phase: {params.get('UEL_LEAGUE_PHASE_INITIAL_DATE', 'N/A')} to {params.get('UEL_LEAGUE_PHASE_END_DATE', 'N/A')}")
            print(f"   UECL League Phase: {params.get('UECL_LEAGUE_PHASE_INITIAL_DATE', 'N/A')} to {params.get('UECL_LEAGUE_PHASE_END_DATE', 'N/A')}\n")
        except Exception as e:
            print(f"⚠️  Warning: Could not load scraper parameters: {e}")
            print("   Continuing without date filtering...\n")
            params = None

    all_matches_by_competition = {}

    for competition_code, comp_config in COMPETITIONS.items():
        print(f"\n{'='*80}")
        print(f"Scraping {comp_config['name']} ({competition_code})")
        print(f"{'='*80}\n")

        matches = scrape_flashscore_competition(competition_code, params)
        matches.sort(key=lambda x: x.get("MATCH_DATE", ""), reverse=True)

        all_matches_by_competition[competition_code] = matches

        print(f"✅ Retrieved {len(matches)} matches from {comp_config['name']}")
        validate_matches(matches, competition_code)

        if save_csv and matches:
            save_matches_to_csv(matches, competition_code)

        time.sleep(3)

    return all_matches_by_competition


def save_matches_to_csv(matches: List[Dict], competition_code: str, filename: Optional[str] = None) -> str:
    """Save matches to a CSV file in the output/ directory."""
    if not matches:
        print(f"⚠️  No matches to save for {competition_code}")
        return ""

    files_dir = SCRIPT_DIR.parent / "output"
    files_dir.mkdir(parents=True, exist_ok=True)

    if not filename:
        comp_name = COMPETITIONS.get(competition_code, {}).get("name", competition_code)
        safe_name = comp_name.replace(" ", "_").replace("-", "_").lower()
        filename = f"{competition_code}_{safe_name}_matches.csv"

    file_path = files_dir / filename

    fieldnames = ["MATCH_ID", "COMPETITION", "SEASON", "PHASE", "MATCH_DATE",
                  "HOME_TEAM", "AWAY_TEAM", "HOME_GOALS", "AWAY_GOALS"]

    try:
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for match in matches:
                writer.writerow({field: match.get(field, '') for field in fieldnames})
        print(f"💾 Saved {len(matches)} matches to: {file_path}")
        return str(file_path)
    except Exception as e:
        print(f"❌ Error saving CSV file {file_path}: {e}")
        return ""


def print_matches_summary(matches: List[Dict]):
    """Print a formatted summary of fetched matches."""
    if not matches:
        print("⚠️ No matches to display")
        return

    print(f"\n{'='*80}")
    print(f"SUMMARY: {len(matches)} Total Matches Retrieved")
    print(f"{'='*80}\n")

    by_competition: Dict[str, List[Dict]] = {}
    for match in matches:
        comp = match["COMPETITION"]
        by_competition.setdefault(comp, []).append(match)

    for comp_code, comp_matches in by_competition.items():
        comp_name = COMPETITIONS.get(comp_code, {}).get("name", comp_code)
        print(f"\n{comp_name} ({comp_code}): {len(comp_matches)} matches")
        print("-" * 80)
        for match in comp_matches[:10]:
            print(
                f"[{match['SEASON']} | {match['MATCH_DATE']} | {match['PHASE']}] "
                f"{match['HOME_TEAM']} {match['HOME_GOALS']} - "
                f"{match['AWAY_GOALS']} {match['AWAY_TEAM']}"
            )
        if len(comp_matches) > 10:
            print(f"... and {len(comp_matches) - 10} more matches")


if __name__ == "__main__":
    try:
        print("=" * 80)
        print("European Club Cups Data Scraper")
        print("Scraping from FlashScore.com")
        print("Champions League, Europa League, Conference League")
        print("=" * 80)
        print()

        matches_by_competition = fetch_all_competitions(save_csv=True)

        all_matches = [m for comp in matches_by_competition.values() for m in comp]

        if all_matches:
            print_matches_summary(all_matches)
            print(f"\n✅ Total matches ready for database: {len(all_matches)}")
            print(f"\n📁 CSV files created:")
            for comp_code, comp_matches in matches_by_competition.items():
                if comp_matches:
                    comp_name = COMPETITIONS.get(comp_code, {}).get("name", comp_code)
                    safe_name = comp_name.replace(" ", "_").replace("-", "_").lower()
                    print(f"   - {comp_code}_{safe_name}_matches.csv ({len(comp_matches)} matches)")
        else:
            print("\n⚠️  No matches were retrieved.")
            print("   This could be due to website structure changes, ChromeDriver issues, or network problems.")

    except Exception as e:
        print(f"❌ Fatal Error: {e}")
        import traceback
        traceback.print_exc()
