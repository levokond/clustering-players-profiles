
"""
fbref_update.py

Weekly updater for FBref player and team statistics.

Features
--------
* Scrapes player AND team (squad) statistics for the current season across the
  top‑5 European leagues (Premier League, La Liga, Bundesliga, Serie A, Ligue 1).
* Writes each dataset to a relational database (SQLite, PostgreSQL, etc.)
  using SQLAlchemy.  Table names follow the pattern <kind>_<category> (e.g.
  player_standard, team_passing).
* Designed for incremental weekly runs: if the table already exists the script
  drops it and replaces it with the fresh scrape, so you always have a clean,
  up‑to‑date snapshot.
* Respects FBref’s rate‑limits with randomised delays and exponential back‑off.
* Minimal dependencies: pandas, requests, cloudscraper, SQLAlchemy, beautifulsoup4.
* Can be scheduled via `cron` (example included) or run manually.

Usage
-----
# First time: install requirements
$ pip install -r requirements.txt

# Run and write to a local SQLite DB
$ python fbref_update.py --db sqlite:///fbref.db

# Run and write to PostgreSQL (ensure DB exists and you have permission)
$ python fbref_update.py --db postgresql://user:password@localhost:5432/fbref

# Cron example: every Monday at 03:15
# 15 3 * * MON /usr/bin/python3 /path/to/fbref_update.py --db sqlite:///fbref.db >> /var/log/fbref_update.log 2>&1
"""

import argparse
import logging
import os
import random
import time
from datetime import datetime
import numpy as np
from typing import Dict, List

import cloudscraper
import pandas as pd
import requests
import sqlalchemy
from bs4 import BeautifulSoup, Comment
from sqlalchemy.engine import Engine
import numpy as np
import certifi
from io import StringIO


BASE_URL = "https://fbref.com"

LEAGUES: Dict[str, int] = {
    "Premier League": 9,
    "La Liga": 12,
    "Bundesliga": 20,
    "Serie A": 11,
    "Ligue 1": 13,
}

PLAYER_CATEGORIES: Dict[str, str] = {
    "standard": "",
    "shooting": "shooting",
    "passing": "passing",
    "passing_types": "passing_types",
    "gca": "gca",
    "defense": "defense",
    "possession": "possession",
    "misc": "misc",
}

# Team (squad) categories share the same segments as the player pages
TEAM_CATEGORIES: Dict[str, str] = PLAYER_CATEGORIES.copy()

# Throttling - Increased delays to be more respectful of fbref.com
DELAY_MIN = 3.0
DELAY_MAX = 6.0
MAX_RETRIES = 5
BACKOFF_FACTOR = 2.0

# --------------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------------- #

def create_scraper() -> cloudscraper.CloudScraper:
    """Return a CloudScraper session with a desktop Chrome profile."""
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )
    # Configure SSL verification with certifi certificates
    scraper.verify = certifi.where()
    return scraper


scraper = create_scraper()


def get_with_retries(url: str, referer: str | None = None) -> requests.Response:
    """GET *url* with retries + exponential back‑off."""
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {"User-Agent": scraper.headers["User-Agent"]}
            if referer:
                headers["Referer"] = referer
            resp = scraper.get(url, headers=headers, timeout=30)
            if resp.status_code == 429:
                raise requests.exceptions.HTTPError("429 Too Many Requests")
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            wait = BACKOFF_FACTOR ** (attempt - 1) + random.uniform(0, 1)
            logging.warning("[%s] %s – retrying in %.1fs", attempt, url, wait)
            time.sleep(wait)
    assert last_err is not None
    raise last_err


def extract_table(soup: BeautifulSoup, block_id: str, table_prefix: str) -> pd.DataFrame:
    """Return a DataFrame of the first table that matches *block_id* or *table_prefix*."""
    comment = soup.find(
        string=lambda t: isinstance(t, Comment) and block_id in t  # type: ignore[arg-type]
    )
    if comment:
        inner = BeautifulSoup(comment, "lxml").find("table")
    else:
        inner = soup.find("table", id=lambda x: x and x.startswith(table_prefix))
    if inner is None or inner.tbody is None:
        return pd.DataFrame()
    return pd.read_html(StringIO(str(inner)))[0]


def scrape_player_category(
    league: str, comp_id: int, category_key: str
) -> pd.DataFrame:
    """Scrape *category_key* player stats for *league*."""
    segment = PLAYER_CATEGORIES[category_key]
    path = f"{segment}/" if segment else ""
    url = f"{BASE_URL}/en/comps/{comp_id}/{path}{league.replace(' ', '-')}-Stats"
    logging.info("Player %s – %s", league, category_key)
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    resp = get_with_retries(url)
    soup = BeautifulSoup(resp.text, "lxml")

    df = extract_table(soup, f"div_stats_{segment or 'standard'}", "stats_")
    if df.empty:
        logging.warning("No player table found for %s %s", league, category_key)
        return df

    df["league"] = league
    df["season"] = datetime.now().year  # crude but adequate for weekly refresh
    df["category"] = category_key
    return df


def scrape_team_category(
    league: str, comp_id: int, category_key: str
) -> pd.DataFrame:
    """Scrape *category_key* squad stats for *league*."""
    # Team stats live on the same pages as player stats
    segment = TEAM_CATEGORIES[category_key]
    path = f"{segment}/" if segment else ""
    url = f"{BASE_URL}/en/comps/{comp_id}/{path}{league.replace(' ', '-')}-Stats"
    logging.info("Team %s – %s", league, category_key)
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    resp = get_with_retries(url)
    soup = BeautifulSoup(resp.text, "lxml")

    df = extract_table(
        soup, f"div_stats_squads_{segment or 'standard'}", "stats_squads_"
    )
    if df.empty:
        logging.warning("No team table found for %s %s", league, category_key)
        return df

    df["league"] = league
    df["season"] = datetime.now().year
    df["category"] = category_key
    return df


def combine_frames(frames: List[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate *frames* ignoring index; return empty DF if all empty."""
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# --------------------------------------------------------------------------- #
# Database helpers
# --------------------------------------------------------------------------- #

def init_engine(db_url: str) -> Engine:
    """Return a SQLAlchemy engine for *db_url*."""
    return sqlalchemy.create_engine(db_url, future=True)


def write_frame(df: pd.DataFrame, name: str, engine: Engine) -> None:
    """Write *df* to *name* in *engine*, replacing existing table."""
    if df.empty:
        logging.warning("%s empty – skipping write", name)
        return
    df.to_sql(name, engine, if_exists="replace", index=False)
    logging.info("Wrote %s (%d rows)", name, len(df))


# --------------------------------------------------------------------------- #
# Main runner
# --------------------------------------------------------------------------- #

def run(db_url: str) -> None:
    engine = init_engine(db_url)

    for cat in PLAYER_CATEGORIES:
        player_frames = [
            scrape_player_category(league, comp, cat) for league, comp in LEAGUES.items()
        ]
        combined = combine_frames(player_frames)
        write_frame(combined, f"player_{cat}", engine)

    for cat in TEAM_CATEGORIES:
        team_frames = [
            scrape_team_category(league, comp, cat) for league, comp in LEAGUES.items()
        ]
        combined = combine_frames(team_frames)
        write_frame(combined, f"team_{cat}", engine)


def cli() -> None:
    parser = argparse.ArgumentParser(
        description="Weekly FBref updater (player + team stats)"
    )
    parser.add_argument(
        "--db",
        default="sqlite:///fbref.db",
        help="SQLAlchemy‑compatible database URL (default: %(default)s)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logging.info("Starting FBref update → %s", args.db)
    start = time.time()
    run(args.db)
    logging.info("Finished in %.1fs", time.time() - start)


if __name__ == "__main__":  # pragma: no cover
    cli()
