#!/usr/bin/env python3
"""
FBref Weekly Update Script
Runs every Monday to fetch match data from the previous 7 days
and load it into Supabase using temporary staging tables.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import List, Dict
import traceback

# Import our modules
from scraper import get_all_recent_fixtures, scrape_fixtures_for_category, MATCH_LOG_CATEGORIES
from db import SupabaseDB

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/fbref_update.log', mode='a'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def load_environment():
    """
    Load environment variables from .env file if it exists.
    """
    try:
        from dotenv import load_dotenv
        load_dotenv()
        logger.info("Environment variables loaded from .env file")
    except ImportError:
        logger.info("python-dotenv not available, using system environment variables")


def validate_environment():
    """
    Validate that required environment variables are set.
    """
    required_vars = ['SUPABASE_DB_URL']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    logger.info("Environment validation passed")


def get_fixture_window_logic() -> List[Dict]:
    """
    Implement fixture-window logic to identify matches from the last 7 days.
    Returns list of fixtures to process.
    """
    logger.info("Fetching fixtures from the last 7 days...")
    
    try:
        fixtures = get_all_recent_fixtures(days_back=7)
        
        # Filter to ensure we only get league matches
        valid_leagues = {'Premier League', 'La Liga', 'Bundesliga', 'Serie A', 'Ligue 1'}
        filtered_fixtures = [
            fixture for fixture in fixtures 
            if fixture.get('league') in valid_leagues and fixture.get('match_id')
        ]
        
        logger.info(f"Found {len(filtered_fixtures)} valid league fixtures from the last 7 days")
        
        # Log fixture summary
        league_counts = {}
        for fixture in filtered_fixtures:
            league = fixture.get('league', 'Unknown')
            league_counts[league] = league_counts.get(league, 0) + 1
        
        for league, count in league_counts.items():
            logger.info(f"  {league}: {count} fixtures")
        
        return filtered_fixtures
        
    except Exception as e:
        logger.error(f"Error fetching fixtures: {e}")
        logger.error(traceback.format_exc())
        return []


def process_category(fixtures: List[Dict], category: str, db: SupabaseDB) -> bool:
    """
    Process a single category for all fixtures.
    Returns True if successful, False otherwise.
    """
    try:
        logger.info(f"Processing category: {category}")
        
        # Scrape data for this category
        team_df, player_df = scrape_fixtures_for_category(fixtures, category)
        
        logger.info(f"Scraped {len(team_df)} team records and {len(player_df)} player records for {category}")
        
        # Load into database using temporary staging
        if not team_df.empty:
            db.bulk_upsert_team_stats(team_df, category)
        
        if not player_df.empty:
            db.bulk_upsert_player_stats(player_df, category)
        
        logger.info(f"Successfully processed category: {category}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing category {category}: {e}")
        logger.error(traceback.format_exc())
        return False


def main():
    """
    Main execution function for the weekly update.
    """
    start_time = datetime.now()
    logger.info("=" * 50)
    logger.info(f"Starting FBref weekly update at {start_time}")
    logger.info("=" * 50)
    
    try:
        # Load and validate environment
        load_environment()
        validate_environment()
        
        # Initialize database connection
        logger.info("Initializing database connection...")
        db = SupabaseDB()
        
        # Get initial table stats
        initial_stats = db.get_table_stats()
        logger.info(f"Initial table counts: {initial_stats}")
        
        # Get fixtures for the last week
        fixtures = get_fixture_window_logic()
        
        if not fixtures:
            logger.warning("No fixtures found for the last 7 days. Exiting.")
            return
        
        # Process each category
        success_count = 0
        total_categories = len(MATCH_LOG_CATEGORIES)
        
        for category in MATCH_LOG_CATEGORIES.keys():
            success = process_category(fixtures, category, db)
            if success:
                success_count += 1
        
        # Get final table stats
        final_stats = db.get_table_stats()
        logger.info(f"Final table counts: {final_stats}")
        
        # Calculate changes
        team_added = final_stats['team_match_stats'] - initial_stats['team_match_stats']
        player_added = final_stats['player_match_stats'] - initial_stats['player_match_stats']
        
        logger.info(f"Records added - Team: {team_added}, Player: {player_added}")
        
        # Close database connection
        db.close()
        
        # Summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 50)
        logger.info(f"Weekly update completed at {end_time}")
        logger.info(f"Duration: {duration}")
        logger.info(f"Categories processed successfully: {success_count}/{total_categories}")
        logger.info(f"Total fixtures processed: {len(fixtures)}")
        logger.info("=" * 50)
        
        # Exit with appropriate code
        if success_count == total_categories:
            logger.info("All categories processed successfully")
            sys.exit(0)
        else:
            logger.warning(f"Only {success_count}/{total_categories} categories processed successfully")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error in weekly update: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main() 