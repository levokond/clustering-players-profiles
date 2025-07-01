"""
FBref scraper module for match-level data extraction.
Handles URL building and HTML parsing for six match-log categories.
"""

import os
import time
import random
import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup, Comment
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import requests

# Initialize cloudscraper session
scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "windows", "desktop": True}
)

BASE_URL = "https://fbref.com"

# Top-5 leagues: name -> comp_id
LEAGUES = {
    'Premier League': 9,
    'La Liga': 12,
    'Bundesliga': 20,
    'Serie A': 11,
    'Ligue 1': 13,
}

# Match-log categories for the six tables
MATCH_LOG_CATEGORIES = {
    'passing': 'passing',
    'pass_types': 'pass_types', 
    'gca': 'gca',
    'defense': 'defense',
    'possession': 'possession',
    'misc': 'misc',
}

# Request throttling and retry settings
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 '
    '(KHTML, like Gecko) Version/16.5 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0',
]

DELAY_MIN = 3
DELAY_MAX = 6
MAX_RETRIES = 5
BACKOFF_FACTOR = 2


def get_with_retries(url: str, referer: Optional[str] = None) -> requests.Response:
    """
    Fetch URL with retries on 429/network errors. Exponential backoff + jitter.
    """
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        if referer:
            headers['Referer'] = referer
            
        try:
            resp = scraper.get(url, headers=headers)
            if resp.status_code == 429:
                raise requests.exceptions.HTTPError('429 Too Many Requests')
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            if attempt == MAX_RETRIES:
                print(f"[Error] {url} failed after {MAX_RETRIES} attempts: {e}")
                raise
            wait = (BACKOFF_FACTOR ** (attempt - 1)) + random.uniform(0, 1)
            print(f"[Retry {attempt}] {url} error: {e}. Sleeping {wait:.1f}s...")
            time.sleep(wait)
    raise last_err


def get_recent_fixtures(league_name: str, comp_id: int, days_back: int = 7) -> List[Dict]:
    """
    Get fixtures from the last N days for a specific league.
    Returns list of fixture dicts with match_id, teams, date, etc.
    """
    print(f"Fetching recent fixtures for {league_name}...")
    
    # Get league summary page
    url = f"{BASE_URL}/en/comps/{comp_id}/{league_name.replace(' ', '-')}-Stats"
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    resp = get_with_retries(url)
    soup = BeautifulSoup(resp.text, 'lxml')
    
    # Find fixtures table (usually in a commented section)
    fixtures = []
    cutoff_date = datetime.now() - timedelta(days=days_back)
    
    # Look for fixtures table in commented HTML
    fixtures_comment = soup.find(string=lambda t: isinstance(t, Comment) and 'fixtures' in str(t).lower())
    if fixtures_comment:
        fixtures_soup = BeautifulSoup(fixtures_comment, 'lxml')
        fixtures_table = fixtures_soup.find('table')
    else:
        # Fallback: look for fixtures table directly
        fixtures_table = soup.find('table', {'id': lambda x: x and 'fixtures' in x.lower()})
    
    if fixtures_table and fixtures_table.tbody:
        for row in fixtures_table.tbody.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 6:  # Ensure we have enough columns
                try:
                    # Extract match data (adjust indices based on actual FBref structure)
                    date_cell = cells[1] if len(cells) > 1 else None
                    home_team_cell = cells[3] if len(cells) > 3 else None
                    away_team_cell = cells[5] if len(cells) > 5 else None
                    
                    if date_cell and home_team_cell and away_team_cell:
                        # Parse date
                        date_text = date_cell.get_text(strip=True)
                        match_date = datetime.strptime(date_text, '%Y-%m-%d')
                        
                        if match_date >= cutoff_date:
                            # Extract match URL for match_id
                            match_link = row.find('a', href=lambda x: x and '/matches/' in x)
                            match_id = None
                            if match_link:
                                href = match_link.get('href', '')
                                match_id = href.split('/')[-2] if '/matches/' in href else None
                            
                            fixture = {
                                'match_id': match_id,
                                'match_date': match_date.strftime('%Y-%m-%d'),
                                'home_team': home_team_cell.get_text(strip=True),
                                'away_team': away_team_cell.get_text(strip=True),
                                'league': league_name,
                                'comp_id': comp_id
                            }
                            fixtures.append(fixture)
                except (ValueError, AttributeError) as e:
                    # Skip rows with parsing errors
                    continue
    
    print(f"Found {len(fixtures)} recent fixtures for {league_name}")
    return fixtures


def scrape_match_logs(match_id: str, category: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Scrape match logs for a specific match and category.
    Returns tuple of (team_stats_df, player_stats_df).
    """
    if category not in MATCH_LOG_CATEGORIES:
        raise ValueError(f"Invalid category: {category}")
    
    category_path = MATCH_LOG_CATEGORIES[category]
    url = f"{BASE_URL}/en/matches/{match_id}/{category_path}"
    
    print(f"Scraping {category} match logs for match {match_id}...")
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    resp = get_with_retries(url)
    soup = BeautifulSoup(resp.text, 'lxml')
    
    team_stats = pd.DataFrame()
    player_stats = pd.DataFrame()
    
    # Extract team-level stats (if available)
    team_table = soup.find('table', {'id': lambda x: x and 'team_stats' in str(x).lower()})
    if team_table:
        try:
            team_stats = pd.read_html(str(team_table))[0]
            team_stats['match_id'] = match_id
            team_stats['category'] = category
        except Exception as e:
            print(f"Error parsing team stats for {match_id} {category}: {e}")
    
    # Extract player-level stats
    player_tables = soup.find_all('table', {'id': lambda x: x and 'player' in str(x).lower()})
    player_frames = []
    
    for table in player_tables:
        try:
            df = pd.read_html(str(table))[0]
            df['match_id'] = match_id
            df['category'] = category
            
            # Determine team from table context
            table_section = table.find_parent('div', class_='section_wrapper')
            if table_section:
                header = table_section.find('h2')
                if header:
                    team_name = header.get_text(strip=True).split(' ')[0]  # Extract team name
                    df['team'] = team_name
            
            player_frames.append(df)
        except Exception as e:
            print(f"Error parsing player table for {match_id} {category}: {e}")
    
    if player_frames:
        player_stats = pd.concat(player_frames, ignore_index=True)
    
    return team_stats, player_stats


def get_all_recent_fixtures(days_back: int = 7) -> List[Dict]:
    """
    Get recent fixtures across all five leagues.
    """
    all_fixtures = []
    for league_name, comp_id in LEAGUES.items():
        try:
            fixtures = get_recent_fixtures(league_name, comp_id, days_back)
            all_fixtures.extend(fixtures)
        except Exception as e:
            print(f"Error getting fixtures for {league_name}: {e}")
    
    return all_fixtures


def scrape_fixtures_for_category(fixtures: List[Dict], category: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Scrape a specific category for multiple fixtures.
    Returns combined DataFrames for team and player stats.
    """
    team_frames = []
    player_frames = []
    
    for fixture in fixtures:
        match_id = fixture.get('match_id')
        if not match_id:
            continue
            
        try:
            team_df, player_df = scrape_match_logs(match_id, category)
            
            # Add fixture metadata
            for df in [team_df, player_df]:
                if not df.empty:
                    df['league'] = fixture['league']
                    df['match_date'] = fixture['match_date']
            
            if not team_df.empty:
                team_frames.append(team_df)
            if not player_df.empty:
                player_frames.append(player_df)
                
        except Exception as e:
            print(f"Error scraping {category} for match {match_id}: {e}")
    
    # Combine all matches
    combined_team = pd.concat(team_frames, ignore_index=True) if team_frames else pd.DataFrame()
    combined_player = pd.concat(player_frames, ignore_index=True) if player_frames else pd.DataFrame()
    
    return combined_team, combined_player


if __name__ == "__main__":
    # Test functionality
    fixtures = get_all_recent_fixtures(days_back=7)
    print(f"Found {len(fixtures)} total recent fixtures")
    
    if fixtures:
        # Test scraping one category
        team_df, player_df = scrape_fixtures_for_category(fixtures[:2], 'passing')
        print(f"Team stats shape: {team_df.shape}")
        print(f"Player stats shape: {player_df.shape}") 