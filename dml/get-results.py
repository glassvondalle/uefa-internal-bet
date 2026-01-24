"""
European Club Cups Data Scraper
Scrapes match data from Champions League, Europa League, and Conference League
from FlashScore.com and formats it for the EUROPEAN_CLUB_CUPS_MATCHES table.

Only includes CLUB teams - filters out any national team results.
Uses Selenium for JavaScript-heavy sites like FlashScore.
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Optional
import hashlib
import re
import time
import csv
import os
import json
import sys
from pathlib import Path

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.absolute()

# Competition configurations for FlashScore
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
        "flashscore_url": "https://www.flashscore.com/football/europe/europa-conference-league/results/"
    }
}

# Common national team indicators to filter out
NATIONAL_TEAM_INDICATORS = [
    "national team", "national squad", "country team"
]

# Common club indicators
CLUB_INDICATORS = ["FC", "CF", "AC", "AS", "SC", "United", "City", "Real", "Bayern", 
                   "Barcelona", "Madrid", "Chelsea", "Arsenal", "Liverpool", "Manchester",
                   "Club", "Athletic", "Sporting", "Olympique", "Paris", "Milan", "Inter"]


def is_club_team(team_name: str) -> bool:
    """
    Check if a team is a club team (not a national team).
    Filters out national teams based on common patterns.
    """
    if not team_name or len(team_name.strip()) < 3:
        return False
    
    team_lower = team_name.lower().strip()
    
    # Check for explicit national team indicators
    for indicator in NATIONAL_TEAM_INDICATORS:
        if indicator in team_lower:
            return False
    
    # Check if it's just a country name (likely national team)
    country_only_patterns = [
        r'^(england|spain|france|germany|italy|portugal|netherlands|belgium|'
        r'poland|greece|turkey|russia|ukraine|sweden|norway|denmark|'
        r'croatia|serbia|romania|bulgaria|hungary|czech|slovakia|switzerland|'
        r'austria|scotland|wales|ireland|finland|iceland)$'
    ]
    
    for pattern in country_only_patterns:
        if re.match(pattern, team_lower):
            has_club_indicator = any(ind.lower() in team_lower for ind in CLUB_INDICATORS)
            if not has_club_indicator:
                return False
    
    # If team name contains club indicators, it's definitely a club
    if any(ind.lower() in team_lower for ind in CLUB_INDICATORS):
        return True
    
    # Default: assume it's a club (most teams in these competitions are clubs)
    return True


def load_scraper_params(params_path: Optional[str] = None) -> dict:
    """
    Load scraper parameters from JSON file.
    
    Args:
        params_path: Path to the parameters file (default: scraper_params.json in PARAMS directory at root level)
    
    Returns:
        Dictionary with scraper parameters
    """
    # If no path provided, use default in PARAMS directory at root level
    if params_path is None:
        # Get root directory (parent of script directory)
        root_dir = SCRIPT_DIR.parent
        params_path = root_dir / "PARAMS" / "scraper_params.json"
    else:
        # If relative path, make it relative to script directory
        if not os.path.isabs(params_path):
            params_path = SCRIPT_DIR / params_path
        else:
            params_path = Path(params_path)
    
    try:
        with open(params_path, 'r') as f:
            params = json.load(f)
        return params
    except FileNotFoundError:
        print(f"❌ Parameters file not found: {params_path}")
        print(f"   Looking in: {params_path.absolute()}")
        print(f"   Script directory: {SCRIPT_DIR}")
        print(f"   Root directory: {SCRIPT_DIR.parent}")
        print(f"   Expected PARAMS directory: {SCRIPT_DIR.parent / 'PARAMS'}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing parameters file: {e}")
        sys.exit(1)


def is_match_in_league_phase(match_date: str, competition_code: str, params: dict) -> bool:
    """
    Check if a match date falls within the league phase date range for the competition.
    
    Args:
        match_date: Match date in YYYY-MM-DD format
        competition_code: Competition code (UCL, UEL, UECL)
        params: Dictionary with scraper parameters
    
    Returns:
        True if match is within league phase dates, False otherwise
    """
    if not match_date or match_date == "2024-01-01":
        return False
    
    try:
        # Get date range for this competition
        initial_date_key = f"{competition_code}_LEAGUE_PHASE_INITIAL_DATE"
        end_date_key = f"{competition_code}_LEAGUE_PHASE_END_DATE"
        
        initial_date_str = params.get(initial_date_key)
        end_date_str = params.get(end_date_key)
        
        if not initial_date_str or not end_date_str:
            print(f"   ⚠️  Warning: Missing date range for {competition_code}. Including all matches.")
            return True
        
        # Parse dates
        match_dt = datetime.strptime(match_date, "%Y-%m-%d")
        initial_dt = datetime.strptime(initial_date_str, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")
        
        # Check if match date is within range (inclusive)
        is_in_range = initial_dt <= match_dt <= end_dt
        return is_in_range
        
    except ValueError as e:
        # Date parsing error - might be wrong format
        print(f"   ⚠️  Date parsing error for {match_date} in {competition_code}: {e}")
        return False  # Exclude match if date can't be parsed
    except Exception as e:
        print(f"   ⚠️  Error checking date range for {competition_code}: {e}")
        return True  # Include match if there's an error


def generate_match_id(competition: str, season: str, phase: str, home_team: str, 
                      away_team: str, match_date: str) -> str:
    """
    Generate a unique MATCH_ID based on match attributes.
    """
    match_string = f"{competition}|{season}|{phase}|{home_team}|{away_team}|{match_date}"
    match_hash = hashlib.md5(match_string.encode()).hexdigest()[:8].upper()
    phase_clean = re.sub(r'[^A-Z0-9_]', '_', phase.upper())[:20]
    season_clean = season.replace("/", "_")
    return f"{competition}_{season_clean}_{phase_clean}_{match_hash}"


def normalize_phase(phase_text: str) -> str:
    """
    Normalize phase information from scraped text.
    """
    if not phase_text:
        return "UNKNOWN"
    
    phase_lower = phase_text.lower().strip()
    
    # Map common phase names
    phase_mapping = {
        "league phase": "LEAGUE_PHASE",
        "league": "LEAGUE_PHASE",
        "group stage": "LEAGUE_PHASE",  # Legacy support
        "group": "LEAGUE_PHASE",  # Legacy support
        "knockout phase": "KNOCKOUT_PHASE",
        "knockout": "KNOCKOUT_PHASE",
        "ko phase": "KNOCKOUT_PHASE",
        "round of 16": "ROUND_OF_16",
        "ro16": "ROUND_OF_16",
        "1/8 final": "ROUND_OF_16",
        "round of 8": "QUARTER_FINAL",
        "ro8": "QUARTER_FINAL",
        "1/4 final": "QUARTER_FINAL",
        "quarter": "QUARTER_FINAL",
        "quarter-final": "QUARTER_FINAL",
        "semi": "SEMI_FINAL",
        "semi-final": "SEMI_FINAL",
        "semi final": "SEMI_FINAL",
        "final": "FINAL",
        "play-off": "PLAY_OFF",
        "playoff": "PLAY_OFF",
        "qualifying": "QUALIFYING",
        "preliminary": "PRELIMINARY"
    }
    
    for key, value in phase_mapping.items():
        if key in phase_lower:
            return value
    
    # Clean and return uppercase version
    return re.sub(r'[^A-Z0-9_]', '_', phase_text.upper())[:30]


def infer_phase_from_date(competition_code: str, match_date: str, season: str) -> str:
    """
    Infer the competition phase based on match date and competition code.
    Uses the new format dates for European competitions.
    
    Args:
        competition_code: UCL, UEL, or UECL
        match_date: Date in YYYY-MM-DD format
        season: Season in YYYY/YYYY format (e.g., "2024/2025")
    
    Returns:
        Phase string (LEAGUE_PHASE, KNOCKOUT_PHASE, ROUND_OF_16, QUARTER_FINAL, SEMI_FINAL, FINAL)
    """
    if not match_date or match_date == "2024-01-01":
        return "UNKNOWN"
    
    try:
        # Parse the date
        match_dt = datetime.strptime(match_date, "%Y-%m-%d")
        year = match_dt.year
        month = match_dt.month
        day = match_dt.day
        
        # Extract season year (first year of the season)
        season_year = int(season.split('/')[0])
        
        # UCL (Champions League) dates
        if competition_code == "UCL":
            # League phase: Sept 16 - Jan 28, 2026
            league_start = datetime(season_year, 9, 16)
            league_end = datetime(season_year + 1, 1, 28)
            
            # KO phase: Feb 17-25
            ko_start = datetime(season_year + 1, 2, 17)
            ko_end = datetime(season_year + 1, 2, 25)
            
            # RO16: Mar 10-18
            ro16_start = datetime(season_year + 1, 3, 10)
            ro16_end = datetime(season_year + 1, 3, 18)
            
            # RO8 (Quarter): Apr 7-15
            ro8_start = datetime(season_year + 1, 4, 7)
            ro8_end = datetime(season_year + 1, 4, 15)
            
            # Semi: Apr 28 - May 6
            semi_start = datetime(season_year + 1, 4, 28)
            semi_end = datetime(season_year + 1, 5, 6)
            
            # Final: May 30
            final_date = datetime(season_year + 1, 5, 30)
            
            if league_start <= match_dt <= league_end:
                return "LEAGUE_PHASE"
            elif ko_start <= match_dt <= ko_end:
                return "KNOCKOUT_PHASE"
            elif ro16_start <= match_dt <= ro16_end:
                return "ROUND_OF_16"
            elif ro8_start <= match_dt <= ro8_end:
                return "QUARTER_FINAL"
            elif semi_start <= match_dt <= semi_end:
                return "SEMI_FINAL"
            elif match_dt.date() == final_date.date():
                return "FINAL"
        
        # UEL (Europa League) dates
        elif competition_code == "UEL":
            # KO: Feb 19-25
            ko_start = datetime(season_year + 1, 2, 19)
            ko_end = datetime(season_year + 1, 2, 25)
            
            # RO16: Mar 12-19
            ro16_start = datetime(season_year + 1, 3, 12)
            ro16_end = datetime(season_year + 1, 3, 19)
            
            # RO8: Apr 9-16
            ro8_start = datetime(season_year + 1, 4, 9)
            ro8_end = datetime(season_year + 1, 4, 16)
            
            # Semi: Apr 30 - May 7
            semi_start = datetime(season_year + 1, 4, 30)
            semi_end = datetime(season_year + 1, 5, 7)
            
            # Final: May 20
            final_date = datetime(season_year + 1, 5, 20)
            
            # League phase: Sept 16 - Jan 28 (same as UCL)
            league_start = datetime(season_year, 9, 16)
            league_end = datetime(season_year + 1, 1, 28)
            
            if league_start <= match_dt <= league_end:
                return "LEAGUE_PHASE"
            elif ko_start <= match_dt <= ko_end:
                return "KNOCKOUT_PHASE"
            elif ro16_start <= match_dt <= ro16_end:
                return "ROUND_OF_16"
            elif ro8_start <= match_dt <= ro8_end:
                return "QUARTER_FINAL"
            elif semi_start <= match_dt <= semi_end:
                return "SEMI_FINAL"
            elif match_dt.date() == final_date.date():
                return "FINAL"
        
        # UECL (Conference League) dates - same as UEL except final
        elif competition_code == "UECL":
            # KO: Feb 19-25
            ko_start = datetime(season_year + 1, 2, 19)
            ko_end = datetime(season_year + 1, 2, 25)
            
            # RO16: Mar 12-19
            ro16_start = datetime(season_year + 1, 3, 12)
            ro16_end = datetime(season_year + 1, 3, 19)
            
            # RO8: Apr 9-16
            ro8_start = datetime(season_year + 1, 4, 9)
            ro8_end = datetime(season_year + 1, 4, 16)
            
            # Semi: Apr 30 - May 7
            semi_start = datetime(season_year + 1, 4, 30)
            semi_end = datetime(season_year + 1, 5, 7)
            
            # Final: May 27 (different from UEL)
            final_date = datetime(season_year + 1, 5, 27)
            
            # League phase: Sept 16 - Jan 28 (same as UCL)
            league_start = datetime(season_year, 9, 16)
            league_end = datetime(season_year + 1, 1, 28)
            
            if league_start <= match_dt <= league_end:
                return "LEAGUE_PHASE"
            elif ko_start <= match_dt <= ko_end:
                return "KNOCKOUT_PHASE"
            elif ro16_start <= match_dt <= ro16_end:
                return "ROUND_OF_16"
            elif ro8_start <= match_dt <= ro8_end:
                return "QUARTER_FINAL"
            elif semi_start <= match_dt <= semi_end:
                return "SEMI_FINAL"
            elif match_dt.date() == final_date.date():
                return "FINAL"
        
        return "UNKNOWN"
        
    except Exception as e:
        return "UNKNOWN"


def parse_date(date_str: str) -> Optional[str]:
    """
    Parse various date formats to YYYY-MM-DD.
    """
    if not date_str:
        return None
    
    date_str = date_str.strip()
    
    # Try common formats
    formats = [
        "%d.%m.%Y",
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%d %B %Y",
        "%d %b %Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%d.%m.%y",
        "%d/%m/%y"
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except:
            continue
    
    # Try to extract date from string with regex (with year)
    date_match = re.search(r'(\d{1,2})[./](\d{1,2})[./](\d{4})', date_str)
    if date_match:
        day, month, year = date_match.groups()
        try:
            dt = datetime(int(year), int(month), int(day))
            return dt.strftime("%Y-%m-%d")
        except:
            pass
    
    # Try DD.MM format (without year) - infer year
    date_match = re.search(r'(\d{1,2})\.(\d{1,2})(?!\.)', date_str)
    if date_match:
        day, month = date_match.groups()
        try:
            current_year = datetime.now().year
            # If month is in future, likely previous year
            if int(month) > datetime.now().month:
                year = current_year - 1
            else:
                year = current_year
            
            dt = datetime(year, int(month), int(day))
            # If date is too far in future, it's probably previous year
            if dt > datetime.now():
                year = year - 1
                dt = datetime(year, int(month), int(day))
            
            return dt.strftime("%Y-%m-%d")
        except:
            pass
    
    return None


def init_driver(headless: bool = True) -> webdriver.Chrome:
    """
    Initialize Chrome WebDriver with appropriate options.
    """
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
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        print(f"❌ Error initializing Chrome driver: {e}")
        print("   Make sure ChromeDriver is installed and in your PATH")
        print("   Download from: https://chromedriver.chromium.org/")
        raise


def scrape_flashscore_competition(competition_code: str, limit: Optional[int] = None, params: Optional[dict] = None) -> List[Dict]:
    """
    Scrape match results from FlashScore for a competition.
    
    Args:
        competition_code: UCL, UEL, or UECL
        limit: Maximum number of matches to return (None for all)
        params: Dictionary with scraper parameters (season and date ranges)
    
    Returns:
        List of match dictionaries
    """
    comp_config = COMPETITIONS.get(competition_code)
    if not comp_config:
        return []
    
    url = comp_config["flashscore_url"]
    print(f"🔎 Scraping {comp_config['name']} from FlashScore: {url}")
    
    driver = None
    try:
        driver = init_driver(headless=True)
        driver.get(url)
        
        # Wait for page to load
        time.sleep(5)
        
        # Click "Show more matches" button repeatedly to load all matches
        print("   🔄 Looking for 'Show more matches' button to load additional matches...")
        max_attempts = 5
        attempts = 0
        previous_match_count = 0
        
        while attempts < max_attempts:
            try:
                # Scroll to bottom to ensure button is visible
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.5)
                
                # Count current matches before clicking
                try:
                    current_matches = driver.find_elements(By.CSS_SELECTOR, "div.event__match, div[class*='event__match']")
                    previous_match_count = len(current_matches)
                except:
                    previous_match_count = 0
                
                # Try multiple methods to find the "Show more matches" button
                show_more_button = None
                
                # Method 1: Exact text match "Show more matches" (case-insensitive)
                try:
                    # Try as link
                    buttons = driver.find_elements(By.XPATH, "//a[contains(translate(normalize-space(text()), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'show more matches')]")
                    if not buttons:
                        # Try as button
                        buttons = driver.find_elements(By.XPATH, "//button[contains(translate(normalize-space(text()), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'show more matches')]")
                    if not buttons:
                        # Try as div/span with click handler
                        buttons = driver.find_elements(By.XPATH, "//*[contains(translate(normalize-space(text()), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'show more matches')]")
                    
                    for btn in buttons:
                        if btn.is_displayed():
                            show_more_button = btn
                            break
                except Exception as e:
                    pass
                
                # Method 2: Try by partial text match
                if not show_more_button:
                    try:
                        buttons = driver.find_elements(By.PARTIAL_LINK_TEXT, "Show more")
                        if not buttons:
                            buttons = driver.find_elements(By.PARTIAL_LINK_TEXT, "more matches")
                        for btn in buttons:
                            if btn.is_displayed() and "more matches" in btn.text.lower():
                                show_more_button = btn
                                break
                    except:
                        pass
                
                # Method 3: Try by class names commonly used by FlashScore
                if not show_more_button:
                    try:
                        selectors = [
                            "a.event__more",
                            "button.event__more",
                            "div.event__more a",
                            "div.event__more button",
                            "[class*='event__more']",
                            "[class*='show-more']",
                            "[class*='load-more']"
                        ]
                        for selector in selectors:
                            try:
                                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                                for elem in elements:
                                    if elem.is_displayed() and ("more" in elem.text.lower() or "matches" in elem.text.lower()):
                                        show_more_button = elem
                                        break
                                if show_more_button:
                                    break
                            except:
                                continue
                    except:
                        pass
                
                if show_more_button:
                    try:
                        # Get button text for debugging
                        button_text = show_more_button.text.strip()
                        print(f"   🔍 Found button with text: '{button_text}'")
                        
                        # Scroll button into view
                        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", show_more_button)
                        time.sleep(1)
                        
                        # Try clicking with JavaScript (more reliable)
                        driver.execute_script("arguments[0].click();", show_more_button)
                        attempts += 1
                        print(f"   ✓ Clicked 'Show more matches' button (attempt {attempts}/{max_attempts})")
                        
                        # Wait for new content to load (longer wait for dynamic content)
                        time.sleep(4)
                        
                        # Verify new matches were loaded
                        try:
                            new_matches = driver.find_elements(By.CSS_SELECTOR, "div.event__match, div[class*='event__match']")
                            new_match_count = len(new_matches)
                            if new_match_count > previous_match_count:
                                print(f"   ✓ Loaded {new_match_count - previous_match_count} additional matches (total: {new_match_count})")
                            else:
                                print(f"   ⚠️  No new matches detected after click (still {new_match_count} matches)")
                        except:
                            pass
                    except Exception as e:
                        print(f"   ⚠️  Error clicking button: {str(e)}")
                        attempts += 1
                        time.sleep(1)
                else:
                    print(f"   ✓ No 'Show more matches' button found. All matches should be loaded.")
                    break
                    
            except Exception as e:
                print(f"   ⚠️  Error while looking for 'Show more matches' button: {str(e)}")
                attempts += 1
                time.sleep(1)
        
        if attempts >= max_attempts:
            print(f"   ⚠️  Reached maximum attempts ({max_attempts}). Proceeding with current matches.")
        
        # Final scroll to ensure all content is loaded
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        # Try using Selenium to find match elements directly
        matches = []
        
        # FlashScore uses specific class patterns - try multiple selectors
        # Based on FlashScore structure analysis
        match_selectors = [
            "div.event__match",  # Most common
            "div[class*='event__match']",
            "div.event__match--twoLine",  # Alternative format
            "div[data-testid='match-row']",  # Data attribute
            "div.sportName",
            "div[class*='event']"  # Fallback
        ]
        
        event_matches_selenium = []
        for selector in match_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements and len(elements) > 10:  # Make sure we got a good number
                    event_matches_selenium = elements
                    print(f"   Found {len(elements)} matches using selector: {selector}")
                    break
            except:
                continue
        
        # Get page source for BeautifulSoup
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # If Selenium didn't find them, try BeautifulSoup
        if not event_matches_selenium:
            # FlashScore uses specific class names for matches
            event_matches = soup.find_all('div', class_=re.compile(r'event__match', re.I))
            
            if not event_matches:
                event_matches = soup.find_all('div', attrs={'data-testid': re.compile(r'match', re.I)})
            
            if not event_matches:
                # Try to find by score pattern
                all_divs = soup.find_all('div')
                for div in all_divs:
                    text = div.get_text()
                    if re.search(r'\d+\s*:\s*\d+', text):
                        event_matches.append(div)
            
            print(f"   Found {len(event_matches)} potential match elements (BeautifulSoup)")
            
            # Convert to list for processing
            event_matches_list = event_matches
        else:
            # Use Selenium elements - convert to BeautifulSoup for parsing
            event_matches_list = []
            for elem in event_matches_selenium:
                html = elem.get_attribute('outerHTML')
                soup_elem = BeautifulSoup(html, 'html.parser')
                event_matches_list.append(soup_elem.find('div') or soup_elem)  # Get the main div
        
        print(f"   Found {len(event_matches_list)} potential match elements")
        
        # Try to extract matches using improved method
        matches = extract_matches_from_flashscore_elements(event_matches_list, soup, competition_code, limit, params)
        
        # If still no matches, try the broader HTML parsing
        # If we didn't find matches with the above method, try parsing the HTML more broadly
        if not matches:
            print("   Trying alternative extraction method...")
            alt_matches = extract_matches_from_html_structure(soup, competition_code)
            matches.extend(alt_matches)
        
        print(f"✅ Found {len(matches)} club matches from {comp_config['name']}")
        
        # Debug: Show first few matches if found
        if matches and len(matches) > 0:
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


def extract_matches_from_flashscore_elements(elements, soup: BeautifulSoup, 
                                            competition_code: str, 
                                            limit: Optional[int],
                                            params: Optional[dict] = None) -> List[Dict]:
    """
    Extract matches from FlashScore elements with FlashScore-specific parsing.
    FlashScore structure: event__match > event__participant (teams) + event__score (score)
    """
    matches = []
    current_date = None
    current_phase = "UNKNOWN"
    
    print(f"   Processing {len(elements)} elements...")
    successful = 0
    failed = 0
    no_score = 0
    no_teams = 0
    
    # Debug: Inspect first few elements
    for i, element in enumerate(elements[:3]):  # Check first 3 for debugging
        if hasattr(element, 'get_text'):
            text = element.get_text(separator=' | ', strip=True)[:200]
        else:
            text = str(element)[:200]
        print(f"   Debug element {i+1}: {text[:150]}...")
    
    for element in elements:
        try:
            # Get the element - handle both BeautifulSoup and Selenium elements
            if hasattr(element, 'find_all'):
                # BeautifulSoup element
                match_element = element
            else:
                # Selenium element - convert to BeautifulSoup
                html = element.get_attribute('outerHTML')
                match_element = BeautifulSoup(html, 'html.parser').find('div') or BeautifulSoup(html, 'html.parser')
            
            # Get full text first to understand the structure
            full_text = match_element.get_text(separator=' | ', strip=True)
            
            # FlashScore structure: Look for participant names
            # Class names: event__participant, event__participant--home, event__participant--away
            participants = match_element.find_all(['span', 'div', 'a'], 
                                                 class_=re.compile(r'event__participant|participant', re.I))
            
            home_team = None
            away_team = None
            
            # Method 1: Extract from participant elements
            if len(participants) >= 2:
                home_team = participants[0].get_text(strip=True)
                away_team = participants[1].get_text(strip=True)
                # Ensure they're different (sometimes DOM can have duplicates)
                if home_team == away_team and len(participants) >= 3:
                    away_team = participants[2].get_text(strip=True)
            
            # Method 2: Parse from pipe-separated text format "Team1 | Team2 | Score1 | Score2"
            if not home_team or not away_team:
                parts = [p.strip() for p in full_text.split('|')]
                # Teams are usually the first two non-numeric, non-date parts
                team_candidates = []
                for part in parts:
                    # Skip if it's a number (score) or date pattern
                    if not part.isdigit() and not re.match(r'^\d{1,2}\.\d{1,2}', part):
                        if len(part) > 2:  # Team names are usually longer
                            team_candidates.append(part)
                            if len(team_candidates) >= 2:
                                break
                
                if len(team_candidates) >= 2:
                    # Only set if not already set, and ensure they're different
                    if not home_team:
                        home_team = team_candidates[0]
                    if not away_team:
                        # Make sure away_team is different from home_team
                        if team_candidates[1] != home_team:
                            away_team = team_candidates[1]
                        elif len(team_candidates) > 2 and team_candidates[2] != home_team:
                            away_team = team_candidates[2]
                        else:
                            # If all candidates are the same, try the first candidate again
                            away_team = team_candidates[1] if len(team_candidates) > 1 else team_candidates[0]
            
            # Method 3: Look for any element with team-like text
            if not home_team or not away_team:
                all_text_elements = match_element.find_all(['span', 'div', 'a'])
                # Filter for elements with substantial text (likely team names)
                # Also remove duplicates and parent/child duplicates
                seen_texts = set()
                text_elements = []
                for e in all_text_elements:
                    text = e.get_text(strip=True)
                    if text and len(text) > 3 and not text.isdigit():
                        # Only add if we haven't seen this text before
                        if text not in seen_texts:
                            seen_texts.add(text)
                            text_elements.append(e)
                        if len(text_elements) >= 2:
                            break
                
                if len(text_elements) >= 2:
                    home_team_text = text_elements[0].get_text(strip=True)
                    away_team_text = text_elements[1].get_text(strip=True)
                    # Only set if not already set and they're different
                    if not home_team:
                        home_team = home_team_text
                    if not away_team and away_team_text != home_team:
                        away_team = away_team_text
                    elif not away_team:
                        # If they're the same, try next element
                        if len(text_elements) >= 3:
                            away_team = text_elements[2].get_text(strip=True)
            
            if not home_team or not away_team:
                no_teams += 1
                if no_teams <= 3:  # Debug first few
                    print(f"   ⚠️  No teams found. Text: {full_text[:200]}")
                continue
            
            # Clean team names
            home_team = re.sub(r'^\d+\.?\s*', '', home_team).strip()
            home_team = re.sub(r'\s+', ' ', home_team)
            away_team = re.sub(r'^\d+\.?\s*', '', away_team).strip()
            away_team = re.sub(r'\s+', ' ', away_team)
            
            # Final check: if teams are the same, try to fix from pipe-separated text
            if home_team == away_team and home_team:
                # Try to extract again from pipe-separated format
                parts = [p.strip() for p in full_text.split('|')]
                team_candidates = []
                for part in parts:
                    if not part.isdigit() and not re.match(r'^\d{1,2}\.\d{1,2}', part):
                        if len(part) > 2 and part not in team_candidates:
                            team_candidates.append(part)
                            if len(team_candidates) >= 2:
                                break
                
                if len(team_candidates) >= 2:
                    home_team = team_candidates[0]
                    away_team = team_candidates[1]
            
            if len(home_team) < 2 or len(away_team) < 2:
                no_teams += 1
                continue
            
            # Final validation: teams must be different
            if home_team == away_team:
                no_teams += 1
                if no_teams <= 3:
                    print(f"   ⚠️  Teams are the same: {home_team} vs {away_team}. Text: {full_text[:200]}")
                continue
            
            # Extract score - FlashScore format can be "2:2" or "2 | 2" or just "2 2"
            # full_text is already defined above
            home_goals = None
            away_goals = None
            
            # Method 1: Look for score pattern with colon "2:2"
            score_match = re.search(r'(\d+)\s*[:]\s*(\d+)', full_text)
            if score_match:
                home_goals = int(score_match.group(1))
                away_goals = int(score_match.group(2))
            
            # Method 2: Look for score pattern with pipe "2 | 2" (common in FlashScore)
            if home_goals is None:
                # Split by | and look for two consecutive numbers
                parts = [p.strip() for p in full_text.split('|')]
                for i in range(len(parts) - 1):
                    if parts[i].isdigit() and parts[i+1].isdigit():
                        home_goals = int(parts[i])
                        away_goals = int(parts[i+1])
                        break
            
            # Method 3: Look for two consecutive numbers in text (separated by space or |)
            if home_goals is None:
                score_match = re.search(r'(\d+)\s*[|]\s*(\d+)', full_text)
                if score_match:
                    home_goals = int(score_match.group(1))
                    away_goals = int(score_match.group(2))
            
            # Method 4: Look in specific score elements
            if home_goals is None:
                score_elements = match_element.find_all(['span', 'div'], 
                                                       class_=re.compile(r'event__score|event__result|score', re.I))
                for score_elem in score_elements:
                    score_text = score_elem.get_text(strip=True)
                    score_match = re.search(r'(\d+)\s*[:|]\s*(\d+)', score_text)
                    if score_match:
                        home_goals = int(score_match.group(1))
                        away_goals = int(score_match.group(2))
                        break
            
            if home_goals is None or away_goals is None:
                no_score += 1
                if no_score <= 3:  # Debug first few
                    print(f"   ⚠️  No score found. Text: {full_text[:200]}")
                continue
            
            # Extract date - FlashScore format is typically DD.MM or DD.MM.YYYY
            # From debug output: "Ath Bilbao | PSG | 0 | 0 | 10.12...."
            # Date appears after the scores in pipe-separated format
            match_date = None
            parent = match_element.find_parent()  # Initialize parent early so it's available for phase extraction later
            
            # Method 1: Parse from the pipe-separated text (date is usually after scores)
            # Format: Team1 | Team2 | Score1 | Score2 | Date
            parts = [p.strip() for p in full_text.split('|')]
            for part in parts:
                # Look for date pattern DD.MM or DD.MM.YYYY
                date_match = re.search(r'(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?', part)
                if date_match:
                    day, month, year = date_match.groups()
                    if year:
                        # Full date with year
                        try:
                            match_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                            current_date = match_date
                            break
                        except:
                            pass
                    else:
                        # Date without year - infer from current date and season
                        try:
                            current_year = datetime.now().year
                            # If month is in future (like 12 for December), might be previous year
                            # For European competitions, matches are usually in current season
                            if int(month) > datetime.now().month:
                                # Likely previous year
                                year = current_year - 1
                            else:
                                year = current_year
                            
                            # Try to create the date
                            test_date = datetime(year, int(month), int(day))
                            # If date is too far in future, it's probably previous year
                            if test_date > datetime.now():
                                year = year - 1
                            
                            match_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                            current_date = match_date
                            break
                        except:
                            pass
            
            # Method 2: Look in FlashScore date elements
            if not match_date:
                date_elements = match_element.find_all(['span', 'div'], 
                                                       class_=re.compile(r'event__time|event__date|time|date', re.I))
                
                # Also check parent and siblings for date
                if parent and not date_elements:
                    date_elements = parent.find_all(['span', 'div'], 
                                                   class_=re.compile(r'event__time|event__date|time|date', re.I))
                
                if date_elements:
                    date_str = date_elements[0].get_text(strip=True)
                    parsed_date = parse_date(date_str)
                    if parsed_date:
                        match_date = parsed_date
                        current_date = parsed_date
            
            # Method 3: Look for date pattern in full text (DD.MM.YYYY format)
            if not match_date:
                date_match = re.search(r'(\d{1,2})[./](\d{1,2})[./](\d{4})', full_text)
                if date_match:
                    day, month, year = date_match.groups()
                    try:
                        match_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                        current_date = match_date
                    except:
                        pass
            
            # Method 4: Look for DD.MM format without year
            if not match_date:
                date_match = re.search(r'(\d{1,2})\.(\d{1,2})(?!\.)', full_text)
                if date_match:
                    day, month = date_match.groups()
                    try:
                        current_year = datetime.now().year
                        # Infer year based on month
                        if int(month) > datetime.now().month:
                            year = current_year - 1
                        else:
                            year = current_year
                        
                        test_date = datetime(year, int(month), int(day))
                        if test_date > datetime.now():
                            year = year - 1
                        
                        match_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                        current_date = match_date
                    except:
                        pass
            
            # Fallback: use current_date from previous match or current date
            if not match_date:
                if current_date:
                    match_date = current_date
                else:
                    # Last resort: use current date (but this shouldn't happen often)
                    match_date = datetime.now().strftime("%Y-%m-%d")
            
            # Extract phase - look for round/stage information
            phase = current_phase
            phase_elements = match_element.find_all(['span', 'div'], 
                                                    class_=re.compile(r'event__stage|event__round|round|stage|phase', re.I))
            if not phase_elements and parent:
                phase_elements = parent.find_all(['span', 'div'], 
                                                  class_=re.compile(r'event__stage|event__round|round|stage|phase', re.I))
            
            if phase_elements:
                phase_text = phase_elements[0].get_text(strip=True)
                if phase_text:
                    phase = normalize_phase(phase_text)
                    current_phase = phase
            
            # Determine season from params file (if provided), otherwise infer from date
            if params and params.get("SEASON"):
                season = params["SEASON"]
            elif match_date and match_date != "2024-01-01":
                try:
                    year = int(match_date.split('-')[0])
                    month = int(match_date.split('-')[1])
                    if month >= 7:  # Season starts in July/August
                        season = f"{year}/{year + 1}"
                    else:
                        season = f"{year - 1}/{year}"
                except:
                    season = f"{datetime.now().year - 1}/{datetime.now().year}"
            else:
                season = f"{datetime.now().year - 1}/{datetime.now().year}"
            
            # If phase is still UNKNOWN, try to infer it from the match date
            if phase == "UNKNOWN" and match_date and match_date != "2024-01-01":
                inferred_phase = infer_phase_from_date(competition_code, match_date, season)
                if inferred_phase != "UNKNOWN":
                    phase = inferred_phase
                    current_phase = phase  # Update current_phase so subsequent matches can use it
            
            # Debug: Show first few matches being processed
            if successful + failed < 3:
                print(f"   🔍 Processing: {home_team} vs {away_team}, date={match_date}, home_club={is_club_team(home_team)}, away_club={is_club_team(away_team)}")
            
            # Only add if both teams are clubs
            if is_club_team(home_team) and is_club_team(away_team):
                # Check if date is valid before filtering
                if not match_date or match_date == "2024-01-01":
                    if successful + failed < 3:
                        print(f"   ⚠️  Skipped (invalid date): {home_team} vs {away_team}, date={match_date}")
                    failed += 1
                    continue
                
                # Filter by date range if params provided (only include league phase matches)
                if params:
                    is_in_range = is_match_in_league_phase(match_date, competition_code, params)
                    if not is_in_range:
                        # Debug: show why match was filtered
                        if successful + failed < 5:  # Show first few filtered matches
                            print(f"   ⚠️  Filtered out (date outside range): {home_team} vs {away_team} on {match_date}")
                        failed += 1
                        continue  # Skip this match if it's outside the league phase date range
                
                match_id = generate_match_id(
                    competition_code, season, phase, home_team, away_team, match_date
                )
                
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
                
                if limit and len(matches) >= limit:
                    break
            else:
                failed += 1
                # Debug: show why it failed
                if failed <= 3:  # Only show first 3 failures
                    if not is_club_team(home_team):
                        print(f"   ⚠️  Skipped (not club): {home_team}")
                    if not is_club_team(away_team):
                        print(f"   ⚠️  Skipped (not club): {away_team}")
                    
        except Exception as e:
            failed += 1
            if failed <= 3:  # Only show first 3 errors
                print(f"   ⚠️  Extraction error: {str(e)[:50]}")
            continue
    
    print(f"   Extracted {successful} matches")
    print(f"   Stats: {no_score} no score, {no_teams} no teams, {failed} errors")
    return matches


def extract_match_from_flashscore_element(element, competition_code: str, 
                                          default_date: Optional[str], 
                                          default_phase: str) -> Optional[Dict]:
    """
    Extract match data from a FlashScore HTML element.
    """
    try:
        text = element.get_text(separator=' ', strip=True)
        
        # Look for score pattern
        score_match = re.search(r'(\d+)\s*[:]\s*(\d+)', text)
        if not score_match:
            return None
        
        home_goals = int(score_match.group(1))
        away_goals = int(score_match.group(2))
        
        # Extract team names - they're usually before and after the score
        # Or in separate spans/divs
        team_elements = element.find_all(['span', 'div', 'a'], class_=re.compile(r'team|participant', re.I))
        
        home_team = None
        away_team = None
        
        if len(team_elements) >= 2:
            home_team = team_elements[0].get_text(strip=True)
            away_team = team_elements[1].get_text(strip=True)
        else:
            # Try to extract from text
            parts = re.split(r'\d+\s*:\s*\d+', text)
            if len(parts) >= 2:
                home_team = parts[0].strip()
                away_team = parts[1].strip()
        
        if not home_team or not away_team:
            return None
        
        # Extract date
        date_element = element.find_parent().find(['span', 'div'], class_=re.compile(r'date|time', re.I))
        if date_element:
            date_str = date_element.get_text(strip=True)
            match_date = parse_date(date_str)
        else:
            match_date = default_date or datetime.now().strftime("%Y-%m-%d")
        
        # Extract phase if available
        phase = default_phase
        phase_element = element.find_parent().find(['span', 'div'], class_=re.compile(r'round|stage|phase', re.I))
        if phase_element:
            phase = normalize_phase(phase_element.get_text(strip=True))
        
        # Determine season from date
        if match_date:
            year = int(match_date.split('-')[0])
            month = int(match_date.split('-')[1])
            if month >= 7:  # Season starts in July/August
                season = f"{year}/{year + 1}"
            else:
                season = f"{year - 1}/{year}"
        else:
            season = "UNKNOWN"
        
        # If phase is still UNKNOWN, try to infer it from the match date
        if phase == "UNKNOWN" and match_date and match_date != "2024-01-01":
            inferred_phase = infer_phase_from_date(competition_code, match_date, season)
            if inferred_phase != "UNKNOWN":
                phase = inferred_phase
        
        match_id = generate_match_id(
            competition_code, season, phase, home_team, away_team, 
            match_date or "2024-01-01"
        )
        
        return {
            "MATCH_ID": match_id,
            "COMPETITION": competition_code,
            "SEASON": season,
            "PHASE": phase,
            "MATCH_DATE": match_date or "2024-01-01",
            "HOME_TEAM": home_team,
            "AWAY_TEAM": away_team,
            "HOME_GOALS": home_goals,
            "AWAY_GOALS": away_goals
        }
        
    except:
        return None


def extract_matches_from_html_structure(soup: BeautifulSoup, competition_code: str) -> List[Dict]:
    """
    Alternative method to extract matches by parsing HTML structure more broadly.
    """
    matches = []
    
    # Look for all text that contains score patterns
    all_text = soup.get_text()
    
    # Find score patterns with context
    score_pattern = re.compile(r'([A-Za-z\s]+?)\s+(\d+)\s*:\s*(\d+)\s+([A-Za-z\s]+)', re.MULTILINE)
    
    for match in score_pattern.finditer(all_text):
        home_team = match.group(1).strip()
        home_goals = int(match.group(2))
        away_goals = int(match.group(3))
        away_team = match.group(4).strip()
        
        # Basic validation
        if (len(home_team) > 2 and len(away_team) > 2 and 
            is_club_team(home_team) and is_club_team(away_team)):
            
            # Try to find date nearby in the HTML
            match_date = datetime.now().strftime("%Y-%m-%d")
            season = f"{datetime.now().year - 1}/{datetime.now().year}"
            
            # Try to infer phase from date
            phase = infer_phase_from_date(competition_code, match_date, season)
            
            match_id = generate_match_id(
                competition_code, season, phase, home_team, away_team, match_date
            )
            
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
    
    return matches


def fetch_all_competitions(limit_per_competition: Optional[int] = None, 
                          save_csv: bool = True, params: Optional[dict] = None) -> Dict[str, List[Dict]]:
    """
    Scrape matches from all three European club competitions.
    
    Args:
        limit_per_competition: Maximum matches per competition (None for all)
        save_csv: Whether to save CSV files for each competition
        params: Dictionary with scraper parameters (season and date ranges). If None, will load from file.
    
    Returns:
        Dictionary with competition codes as keys and lists of matches as values
    """
    # Load params if not provided
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
    all_matches = []
    
    for competition_code in COMPETITIONS.keys():
        comp_config = COMPETITIONS[competition_code]
        print(f"\n{'='*80}")
        print(f"Scraping {comp_config['name']} ({competition_code})")
        print(f"{'='*80}\n")
        
        matches = scrape_flashscore_competition(competition_code, limit_per_competition, params)
        
        # Final filter to ensure only club teams
        club_matches = [
            m for m in matches 
            if is_club_team(m["HOME_TEAM"]) and is_club_team(m["AWAY_TEAM"])
        ]
        
        # Sort matches by date descending
        club_matches.sort(key=lambda x: x.get("MATCH_DATE", ""), reverse=True)
        
        all_matches_by_competition[competition_code] = club_matches
        all_matches.extend(club_matches)
        
        print(f"✅ Retrieved {len(club_matches)} club matches from {comp_config['name']}\n")
        
        # Save CSV file for this competition
        if save_csv and club_matches:
            save_matches_to_csv(club_matches, competition_code)
        
        # Delay between competitions
        time.sleep(3)
    
    return all_matches_by_competition


def save_matches_to_csv(matches: List[Dict], competition_code: str, filename: Optional[str] = None) -> str:
    """
    Save matches to a CSV file in the FILES directory at root level.
    
    Args:
        matches: List of match dictionaries
        competition_code: Competition code (UCL, UEL, UECL)
        filename: Optional custom filename (default: competition_code_matches.csv)
    
    Returns:
        Path to the created CSV file
    """
    if not matches:
        print(f"⚠️  No matches to save for {competition_code}")
        return ""
    
    # Get the script directory and create files folder at same level
    script_dir = Path(__file__).parent.absolute()
    # files folder should be at the same level as the script's folder
    # e.g., if script is in DML/, files should be in files/ at same level
    parent_dir = script_dir.parent
    files_dir = parent_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate filename if not provided
    if not filename:
        comp_name = COMPETITIONS.get(competition_code, {}).get("name", competition_code)
        # Clean filename (remove spaces, special chars)
        safe_name = comp_name.replace(" ", "_").replace("-", "_").lower()
        filename = f"{competition_code}_{safe_name}_matches.csv"
    
    # Full path to the CSV file
    file_path = files_dir / filename
    
    # CSV column order matching database table structure
    fieldnames = [
        "MATCH_ID",
        "COMPETITION",
        "SEASON",
        "PHASE",
        "MATCH_DATE",
        "HOME_TEAM",
        "AWAY_TEAM",
        "HOME_GOALS",
        "AWAY_GOALS"
    ]
    
    # Write CSV file
    try:
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for match in matches:
                # Ensure all fields are present
                row = {field: match.get(field, '') for field in fieldnames}
                writer.writerow(row)
        
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
    print(f"SUMMARY: {len(matches)} Total Club Matches Retrieved")
    print(f"{'='*80}\n")
    
    # Group by competition
    by_competition = {}
    for match in matches:
        comp = match["COMPETITION"]
        if comp not in by_competition:
            by_competition[comp] = []
        by_competition[comp].append(match)
    
    for comp_code, comp_matches in by_competition.items():
        comp_name = COMPETITIONS.get(comp_code, {}).get("name", comp_code)
        print(f"\n{comp_name} ({comp_code}): {len(comp_matches)} matches")
        print("-" * 80)
        
        for match in comp_matches[:10]:  # Show first 10 per competition
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
        print("ℹ️  Only CLUB teams are included - national teams are filtered out.\n")
        print("⚠️  Note: This requires ChromeDriver to be installed.")
        print("   Download from: https://chromedriver.chromium.org/\n")
        print()
        
        # Scrape all competitions and save CSV files
        matches_by_competition = fetch_all_competitions(limit_per_competition=None, save_csv=True)
        
        # Combine all matches for summary
        all_matches = []
        for comp_matches in matches_by_competition.values():
            all_matches.extend(comp_matches)
        
        # Print summary
        if all_matches:
            print_matches_summary(all_matches)
            print(f"\n✅ Total club matches ready for database: {len(all_matches)}")
            print(f"\n📁 CSV files created:")
            for comp_code, comp_matches in matches_by_competition.items():
                if comp_matches:
                    comp_name = COMPETITIONS.get(comp_code, {}).get("name", comp_code)
                    safe_name = comp_name.replace(" ", "_").replace("-", "_").lower()
                    filename = f"{comp_code}_{safe_name}_matches.csv"
                    print(f"   - {filename} ({len(comp_matches)} matches)")
        else:
            print("\n⚠️  No matches were retrieved.")
            print("   This could be due to:")
            print("   - Website structure changes")
            print("   - ChromeDriver not installed or not in PATH")
            print("   - Network issues")
            print("   - Anti-scraping measures")
        
    except Exception as e:
        print(f"❌ Fatal Error: {e}")
        import traceback
        traceback.print_exc()
