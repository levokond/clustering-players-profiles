#!/usr/bin/env python3
"""
Weekly FBref data update script.
Runs every Monday at 03:15 to collect match-log data from the last 7 days.
"""

import os
import sys
import logging
import traceback
from datetime import datetime
from typing import Dict, Any

from scraper import get_all_recent_fixtures, scrape_category_for_fixtures, CODES
from db import SupabaseDB


def setup_logging() -> logging.Logger:
    """Configure logging for the update script."""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # File handler (if in production)
    handlers = [console_handler]
    if os.path.exists('/var/log'):
        file_handler = logging.FileHandler('/var/log/fbref_update.log')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(file_handler)
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=handlers,
        force=True
    )
    
    return logging.getLogger(__name__)


def validate_environment() -> bool:
    """Check that all required environment variables are present."""
    required_vars = [
        'SUPABASE_HOST',
        'SUPABASE_DB_NAME', 
        'SUPABASE_USER',
        'SUPABASE_PASSWORD'
    ]
    
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        print(f"Missing required environment variables: {missing}")
        return False
    
    return True


def run_weekly_update() -> Dict[str, Any]:
    """
    Main update logic: discover recent fixtures and scrape all categories.
    Returns summary statistics.
    """
    logger = logging.getLogger(__name__)
    
    # Initialize database connection
    logger.info("Connecting to Supabase...")
    db = SupabaseDB()
    
    try:
        # Ensure tables exist
        db.create_tables()
        logger.info("Database tables verified")
        
        # Get recent fixtures
        logger.info("Discovering recent fixtures...")
        fixtures = get_all_recent_fixtures()
        
        if not fixtures:
            logger.warning("No recent fixtures found")
            return {'status': 'success', 'fixtures': 0, 'categories': 0, 'total_rows': 0}
        
        logger.info(f"Found {len(fixtures)} recent fixtures")
        
        # Process each category
        results = {}
        total_team_rows = 0
        total_player_rows = 0
        
        for category in CODES.keys():
            logger.info(f"Processing category: {category}")
            
            try:
                team_rows, player_rows = scrape_category_for_fixtures(
                    fixtures, category, db.engine
                )
                
                results[category] = {
                    'team_rows': team_rows,
                    'player_rows': player_rows,
                    'status': 'success'
                }
                
                total_team_rows += team_rows
                total_player_rows += player_rows
                
                logger.info(f"Category {category} completed: {team_rows} team rows, {player_rows} player rows")
                
            except Exception as e:
                logger.error(f"Error processing category {category}: {e}")
                results[category] = {
                    'team_rows': 0,
                    'player_rows': 0,
                    'status': 'error',
                    'error': str(e)
                }
        
        # Summary
        successful_categories = sum(1 for r in results.values() if r['status'] == 'success')
        
        summary = {
            'status': 'success',
            'timestamp': datetime.utcnow().isoformat(),
            'fixtures_processed': len(fixtures),
            'categories_processed': successful_categories,
            'total_categories': len(CODES),
            'total_team_rows': total_team_rows,
            'total_player_rows': total_player_rows,
            'category_results': results
        }
        
        logger.info(f"Update completed successfully:")
        logger.info(f"  - Fixtures: {len(fixtures)}")
        logger.info(f"  - Categories: {successful_categories}/{len(CODES)}")
        logger.info(f"  - Team rows: {total_team_rows}")
        logger.info(f"  - Player rows: {total_player_rows}")
        
        return summary
        
    except Exception as e:
        logger.error(f"Update failed: {e}")
        logger.error(traceback.format_exc())
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    finally:
        db.close()
        logger.info("Database connection closed")


def main():
    """Main entry point."""
    logger = setup_logging()
    logger.info("=== FBref Weekly Update Started ===")
    
    # Validate environment
    if not validate_environment():
        logger.error("Environment validation failed")
        sys.exit(1)
    
    # Run update
    try:
        results = run_weekly_update()
        
        if results['status'] == 'success':
            logger.info("=== FBref Weekly Update Completed Successfully ===")
            sys.exit(0)
        else:
            logger.error("=== FBref Weekly Update Failed ===")
            logger.error(f"Error: {results.get('error', 'Unknown error')}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Update interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main() 