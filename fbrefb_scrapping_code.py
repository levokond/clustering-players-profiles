import os
import time
import random
import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup, Comment
import requests

# Initialize cloudscraper session
scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "windows", "desktop": True}
)
BASE_URL = "https://fbref.com"

# Top-5 leagues: name -> comp_id
LEAGUES = {
    'Premier League': 9,
    'La Liga':       12,
    'Bundesliga':    20,
    'Serie A':       11,
    'Ligue 1':       13,
}
# Stats categories: key -> URL segment ('' means overall)
CATEGORIES = {
    'standard':    '',
    'shooting':    'shooting',
    'passing':     'passing',
    'gca':         'gca',
    'defense':     'defense',
    'possession':  'possession',
    'misc':        'misc',
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
PROXIES = []  # e.g. ['http://proxy1:port', ...]
DELAY_MIN = 3
DELAY_MAX = 6
MAX_RETRIES = 5
BACKOFF_FACTOR = 2


def get_with_retries(url, referer=None):
    """
    Fetch URL with retries on 429/network errors. Exponential backoff + jitter.
    """
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        if referer:
            headers['Referer'] = referer
        proxies = None
        if PROXIES:
            proxy = random.choice(PROXIES)
            proxies = {'http': proxy, 'https': proxy}
        try:
            resp = scraper.get(url, headers=headers, proxies=proxies)
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


def scrape_league_category(league_name: str, comp_id: int, category_key: str) -> pd.DataFrame:
    """
    Scrape player stats for one league and category.
    Returns DataFrame with extra columns: League, Category.
    """
    segment = CATEGORIES[category_key]
    path = f"{segment}/" if segment else ''
    # Build URL
    url = f"{BASE_URL}/en/comps/{comp_id}/{path}{league_name.replace(' ', '-')}-Stats"
    print(f"Fetching {league_name} [{category_key}] stats...")
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    resp = get_with_retries(url)
    soup = BeautifulSoup(resp.text, 'lxml')

    # Target player stats table in commented block
    block_id = f"div_stats_{segment or 'standard'}"
    comment = soup.find(string=lambda t: isinstance(t, Comment) and block_id in t)
    if comment:
        tbl_soup = BeautifulSoup(comment, 'lxml')
        table = tbl_soup.find('table')
    else:
        # fallback: first player stats table by id prefix
        table = soup.find('table', id=lambda x: x and x.startswith('stats_'))

    if table is None or not table.tbody:
        print(f"[Warning] No player table for {league_name} {category_key}")
        return pd.DataFrame()

    df = pd.read_html(str(table))[0]
    # Label league and category
    df['League'] = league_name
    df['Category'] = category_key
    return df


def scrape_all_categories():
    """
    Scrape all categories across all leagues, saving each category into its own DataFrame and CSV.
    Returns dict: category_key -> DataFrame
    """
    os.makedirs('data', exist_ok=True)
    category_dfs = {}
    for category_key in CATEGORIES.keys():
        frames = []
        for league, comp_id in LEAGUES.items():
            try:
                df = scrape_league_category(league, comp_id, category_key)
                if not df.empty:
                    frames.append(df)
            except Exception as e:
                print(f"Error scraping {league} {category_key}: {e}")
        # Combine leagues for this category
        if frames:
            cat_df = pd.concat(frames, ignore_index=True)
        else:
            cat_df = pd.DataFrame()
        # Save to CSV
        cat_df.to_csv(f"data/{category_key}_player_stats.csv", index=False)
        category_dfs[category_key] = cat_df
        print(f"Saved {category_key} stats: {cat_df.shape[0]} rows")
    return category_dfs


def main():
    dfs = scrape_all_categories()
    # Example access: dfs['defense'] for combined defensive stats
    return dfs

if __name__ == '__main__':
    all_category_dfs = main()
    # Print head of one category
    print(all_category_dfs['standard'].head())
