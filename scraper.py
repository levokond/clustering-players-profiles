"""
FBref scraper module for match-level data extraction.
Production-ready implementation following FBref's actual URL patterns and HTML structure.
"""

import os
import time
import random
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup, Comment
from datetime import date, timedelta, datetime
from typing import Dict, List, Optional, Tuple
from sqlalchemy import create_engine, text
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE = "https://fbref.com"

# Top-5 leagues: name -> competition-id (from FBref URLs)
LEAGUES = {
    "Premier League": 9,
    "La Liga": 12,
    "Bundesliga": 20,
    "Serie A": 11,
    "Ligue 1": 13,
}

# League names for filtering match logs
LEAGUE_NAMES = {
    "Premier League", "La Liga", "Bundesliga", "Serie A", "Ligue 1"
}

# Match-log categories with FBref category codes
CODES = {
    "passing": ("0", "passing"),
    "pass_types": ("2", "pass_types"),
    "gca": ("9", "gca"),
    "defense": ("5", "defense"),
    "poss": ("6", "possession"),
    "misc": ("7", "misc"),
}

# URL templates
TEAM_TMPL = BASE + "/squads/{tid}/{season}/matchlogs/c{code}/{cat}/{team_slug}"
PLYR_TMPL = BASE + "/players/{pid}/matchlogs/{season}/{cat}/{player_slug}"

# Request settings
DELAY_MIN = 2
DELAY_MAX = 4
MAX_RETRIES = 3
TIMEOUT = 30

# Look back window
SINCE = date.today() - timedelta(days=7)


def get_with_retries(url: str) -> requests.Response:
    """Fetch URL with retries and rate limiting."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            resp = requests.get(url, headers=headers, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                logger.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts: {e}")
                raise
            wait = (2 ** attempt) + random.uniform(0, 1)
            time.sleep(wait)
    

def fixtures_since(league_id: int, year: int) -> List[str]:
    """Return fixture URLs for league games played >= SINCE."""
    url = f"{BASE}/en/comps/{league_id}/{year}-{year+1}/schedule/{year}-{year+1}-{league_id}-Scores-and-Fixtures"
    logger.info(f"Fetching fixtures from {url}")
    
    soup = BeautifulSoup(get_with_retries(url).text, "lxml")
    rows = soup.select("table.stats_table tbody tr")
    urls = []
    
    for r in rows:
        if r.get("class") == ["spacer"]:  # blank row
            continue
            
        date_cell = r.find("th", {"data-stat": "date"})
        if not date_cell or date_cell.text.strip() == "":
            continue
            
        try:
            match_date = date.fromisoformat(date_cell["csk"][:10])
            if match_date < SINCE:
                continue
                
            match_report_cell = r.find("td", {"data-stat": "match_report"})
            if match_report_cell and match_report_cell.a:
                slug = match_report_cell.a["href"]
                urls.append(BASE + slug)
        except (ValueError, KeyError, TypeError) as e:
            logger.warning(f"Error parsing fixture row: {e}")
            continue
    
    logger.info(f"Found {len(urls)} recent fixtures for league {league_id}")
    return urls


def get_all_recent_fixtures() -> List[str]:
    """Get recent fixture URLs across all five leagues."""
    current_year = date.today().year
    # Handle season boundary (Aug-May)
    if date.today().month >= 8:
        season_year = current_year
    else:
        season_year = current_year - 1
    
    all_fixtures = []
    for league_name, league_id in LEAGUES.items():
        try:
            fixtures = fixtures_since(league_id, season_year)
            all_fixtures.extend(fixtures)
        except Exception as e:
            logger.error(f"Error getting fixtures for {league_name}: {e}")
    
    logger.info(f"Total recent fixtures across all leagues: {len(all_fixtures)}")
    return all_fixtures


def table_from_comment(html: str, wanted_id: str) -> pd.DataFrame:
    """Extract table from HTML comment section."""
    soup = BeautifulSoup(html, "lxml")
    
    try:
        comment = next(
            c for c in soup.find_all(string=lambda t: isinstance(t, Comment))
            if wanted_id in c
        )
        inner = BeautifulSoup(comment, "lxml")
        table = inner.find("table", id=wanted_id)
        if table:
            return pd.read_html(str(table))[0]
    except (StopIteration, ValueError, IndexError) as e:
        logger.warning(f"Could not extract table {wanted_id}: {e}")
    
    return pd.DataFrame()


def extract_match_metadata(fixture_url: str) -> Dict:
    """Extract match_id, teams, and season from fixture URL."""
    # Example: /en/matches/abc123def/Arsenal-Chelsea-January-1-2025/
    match_id = fixture_url.split('/matches/')[1].split('/')[0]
    
    # Get season year from current date
    current_year = date.today().year
    if date.today().month >= 8:
        season_year = current_year + 1  # e.g., 2024-2025 season -> 2025
    else:
        season_year = current_year
    
    season_str = f"{season_year-1}-{season_year}"  # e.g., "2024-2025"
    
    return {
        'match_id': match_id,
        'season': season_year,
        'season_str': season_str
    }


def get_teams_and_players_from_match(fixture_url: str) -> Tuple[List[Dict], List[Dict]]:
    """Extract team IDs and player IDs from match page."""
    soup = BeautifulSoup(get_with_retries(fixture_url).text, "lxml")
    
    teams = []
    players = []
    
    # Find team links in lineups section
    team_links = soup.find_all('a', href=re.compile(r'/en/squads/[^/]+/'))
    seen_teams = set()
    
    for link in team_links:
        href = link['href']
        team_id = href.split('/squads/')[1].split('/')[0]
        team_name = link.text.strip()
        
        if team_id not in seen_teams:
            teams.append({
                'team_id': team_id,
                'team_name': team_name,
                'team_slug': href.split('/')[-1]  # last part of URL
            })
            seen_teams.add(team_id)
    
    # Find player links in starting XI tables
    player_links = soup.find_all('a', href=re.compile(r'/en/players/[^/]+/'))
    seen_players = set()
    
    for link in player_links:
        href = link['href']
        player_id = href.split('/players/')[1].split('/')[0]
        player_name = link.text.strip()
        
        if player_id not in seen_players and player_name:
            players.append({
                'player_id': player_id,
                'player_name': player_name,
                'player_slug': href.split('/')[-1]  # last part of URL
            })
            seen_players.add(player_id)
    
    return teams, players


def scrape_team_match_logs(teams: List[Dict], category: str, metadata: Dict) -> pd.DataFrame:
    """Scrape team match logs for a category."""
    code, cat_path = CODES[category]
    season_str = metadata['season_str']
    match_id = metadata['match_id']
    
    team_frames = []
    
    for team in teams:
        try:
            url = TEAM_TMPL.format(
                tid=team['team_id'],
                season=season_str,
                code=code,
                cat=cat_path,
                team_slug=team['team_slug']
            )
            
            logger.info(f"Fetching team {category} data: {team['team_name']}")
            html = get_with_retries(url).text
            
            # For teams, use "matchlogs_{category}_squads" (the "For" table)
            table_id = f"matchlogs_{cat_path}_squads"
            df = table_from_comment(html, table_id)
            
            if not df.empty:
                # Filter to league matches only
                if 'Comp' in df.columns:
                    df = df[df['Comp'].isin(LEAGUE_NAMES)]
                
                # Add metadata
                df['match_id'] = match_id
                df['team_id'] = team['team_id']
                df['team_name'] = team['team_name']
                df['category'] = category
                df['season'] = metadata['season']
                df['created_at'] = pd.Timestamp.utcnow()
                df['updated_at'] = pd.Timestamp.utcnow()
                
                team_frames.append(df)
                
        except Exception as e:
            logger.error(f"Error scraping team {team['team_name']} {category}: {e}")
    
    if team_frames:
        combined = pd.concat(team_frames, ignore_index=True)
        # Convert numeric columns
        numeric_cols = combined.select_dtypes('object').columns
        combined[numeric_cols] = combined[numeric_cols].apply(pd.to_numeric, errors='ignore')
        return combined
    
    return pd.DataFrame()


def scrape_player_match_logs(players: List[Dict], category: str, metadata: Dict) -> pd.DataFrame:
    """Scrape player match logs for a category."""
    code, cat_path = CODES[category]
    season_str = metadata['season_str']
    match_id = metadata['match_id']
    
    player_frames = []
    
    for player in players:
        try:
            url = PLYR_TMPL.format(
                pid=player['player_id'],
                season=season_str,
                cat=cat_path,
                player_slug=player['player_slug']
            )
            
            logger.info(f"Fetching player {category} data: {player['player_name']}")
            html = get_with_retries(url).text
            
            # For players, use "matchlogs_{category}"
            table_id = f"matchlogs_{cat_path}"
            df = table_from_comment(html, table_id)
            
            if not df.empty:
                # Filter to league matches only
                if 'Comp' in df.columns:
                    df = df[df['Comp'].isin(LEAGUE_NAMES)]
                
                # Add metadata
                df['match_id'] = match_id
                df['player_id'] = player['player_id']
                df['player_name'] = player['player_name']
                df['category'] = category
                df['season'] = metadata['season']
                df['created_at'] = pd.Timestamp.utcnow()
                df['updated_at'] = pd.Timestamp.utcnow()
                
                player_frames.append(df)
                
        except Exception as e:
            logger.error(f"Error scraping player {player['player_name']} {category}: {e}")
    
    if player_frames:
        combined = pd.concat(player_frames, ignore_index=True)
        # Convert numeric columns
        numeric_cols = combined.select_dtypes('object').columns
        combined[numeric_cols] = combined[numeric_cols].apply(pd.to_numeric, errors='ignore')
        return combined
    
    return pd.DataFrame()


def dtype_sql(series: pd.Series) -> str:
    """Map pandas dtype to SQL type."""
    if pd.api.types.is_integer_dtype(series):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(series):
        return "REAL"
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    elif pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    else:
        return "TEXT"


def upsert_dataframe(df: pd.DataFrame, target_table: str, pk_cols: List[str], engine):
    """Upsert DataFrame using temporary staging table."""
    if df.empty:
        logger.warning(f"Empty DataFrame for {target_table}, skipping upsert")
        return
    
    with engine.begin() as conn:
        # 1) Create temporary staging table
        col_defs = ', '.join(f'{c} {dtype_sql(df[c])}' for c in df.columns)
        conn.execute(text(f"CREATE TEMP TABLE stg ({col_defs}) ON COMMIT DROP;"))
        
        # 2) Load data into staging
        df.to_sql("stg", conn, if_exists="append", index=False)
        
        # 3) Merge via UPSERT
        col_list = ", ".join(df.columns)
        set_list = ", ".join(f"{c}=EXCLUDED.{c}" for c in df.columns if c not in pk_cols)
        pk = ", ".join(pk_cols)
        
        upsert_sql = f"""
            INSERT INTO {target_table} ({col_list})
            SELECT {col_list} FROM stg
            ON CONFLICT ({pk}) DO UPDATE SET {set_list};
        """
        
        result = conn.execute(text(upsert_sql))
        logger.info(f"Upserted {result.rowcount} rows to {target_table}")


def scrape_category_for_fixtures(fixtures: List[str], category: str, engine) -> Tuple[int, int]:
    """Scrape one category across all fixtures and load to database."""
    logger.info(f"Scraping {category} category for {len(fixtures)} fixtures")
    
    team_rows = 0
    player_rows = 0
    
    for fixture_url in fixtures:
        try:
            # Extract metadata
            metadata = extract_match_metadata(fixture_url)
            
            # Get teams and players from match page
            teams, players = get_teams_and_players_from_match(fixture_url)
            
            # Scrape team data
            team_df = scrape_team_match_logs(teams, category, metadata)
            if not team_df.empty:
                upsert_dataframe(
                    team_df, 
                    "team_match_stats",
                    ["match_id", "team_id", "category"],
                    engine
                )
                team_rows += len(team_df)
            
            # Scrape player data  
            player_df = scrape_player_match_logs(players, category, metadata)
            if not player_df.empty:
                upsert_dataframe(
                    player_df,
                    "player_match_stats", 
                    ["match_id", "player_id", "category"],
                    engine
                )
                player_rows += len(player_df)
                
        except Exception as e:
            logger.error(f"Error processing fixture {fixture_url}: {e}")
    
    logger.info(f"Category {category}: {team_rows} team rows, {player_rows} player rows")
    return team_rows, player_rows


if __name__ == "__main__":
    # Test functionality
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    print("Testing FBref scraper...")
    
    # Test fixture discovery
    fixtures = get_all_recent_fixtures()
    print(f"Found {len(fixtures)} recent fixtures")
    
    if fixtures:
        # Test metadata extraction
        metadata = extract_match_metadata(fixtures[0])
        print(f"Sample metadata: {metadata}")
        
        # Test team/player extraction
        teams, players = get_teams_and_players_from_match(fixtures[0])
        print(f"Found {len(teams)} teams, {len(players)} players")
        
        # Test scraping one category (without database)
        if teams:
            team_df = scrape_team_match_logs(teams[:1], 'passing', metadata)
            print(f"Sample team data shape: {team_df.shape}")
            
        if players:
            player_df = scrape_player_match_logs(players[:2], 'passing', metadata)
            print(f"Sample player data shape: {player_df.shape}") 