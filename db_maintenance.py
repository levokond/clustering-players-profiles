#!/usr/bin/env python3
"""
FBref Daily Maintenance Script
Runs daily to perform read-only maintenance tasks:
- Refresh materialized views
- Database housekeeping
- Generate summary statistics
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, List
import traceback

# Import our database module
from db import SupabaseDB
from sqlalchemy import text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/db_maint.log', mode='a'),
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


def create_materialized_views(db: SupabaseDB):
    """
    Create materialized views if they don't exist.
    These provide pre-aggregated data for common queries.
    """
    logger.info("Creating/updating materialized views...")
    
    views_sql = {
        'mv_player_season_stats': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_player_season_stats AS
        SELECT 
            player_id,
            player_name,
            team_name,
            league,
            EXTRACT(YEAR FROM TO_DATE(match_date, 'YYYY-MM-DD')) as season_year,
            category,
            COUNT(*) as matches_played,
            AVG(minutes_played) as avg_minutes,
            created_at as last_updated
        FROM player_match_stats
        WHERE match_date >= CURRENT_DATE - INTERVAL '2 years'
        GROUP BY player_id, player_name, team_name, league, season_year, category, created_at;
        """,
        
        'mv_team_season_stats': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_team_season_stats AS
        SELECT 
            team_id,
            team_name,
            league,
            EXTRACT(YEAR FROM TO_DATE(match_date, 'YYYY-MM-DD')) as season_year,
            category,
            COUNT(*) as matches_played,
            COUNT(CASE WHEN venue = 'Home' THEN 1 END) as home_matches,
            COUNT(CASE WHEN venue = 'Away' THEN 1 END) as away_matches,
            created_at as last_updated
        FROM team_match_stats
        WHERE match_date >= CURRENT_DATE - INTERVAL '2 years'
        GROUP BY team_id, team_name, league, season_year, category, created_at;
        """,
        
        'mv_recent_match_summary': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_recent_match_summary AS
        SELECT 
            match_date,
            league,
            COUNT(DISTINCT match_id) as total_matches,
            COUNT(DISTINCT team_name) as teams_involved,
            MAX(created_at) as data_last_updated
        FROM team_match_stats
        WHERE match_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY match_date, league
        ORDER BY match_date DESC;
        """
    }
    
    try:
        with db.engine.begin() as conn:
            for view_name, sql in views_sql.items():
                try:
                    conn.execute(text(sql))
                    logger.info(f"Created/verified materialized view: {view_name}")
                except Exception as e:
                    logger.error(f"Error creating view {view_name}: {e}")
        
        # Create indexes on materialized views
        index_sql = [
            "CREATE INDEX IF NOT EXISTS idx_mv_player_season ON mv_player_season_stats(league, season_year, category);",
            "CREATE INDEX IF NOT EXISTS idx_mv_team_season ON mv_team_season_stats(league, season_year, category);",
            "CREATE INDEX IF NOT EXISTS idx_mv_recent_match ON mv_recent_match_summary(match_date, league);"
        ]
        
        with db.engine.begin() as conn:
            for sql in index_sql:
                try:
                    conn.execute(text(sql))
                except Exception as e:
                    logger.error(f"Error creating index: {e}")
                    
    except Exception as e:
        logger.error(f"Error in create_materialized_views: {e}")
        logger.error(traceback.format_exc())


def refresh_materialized_views(db: SupabaseDB):
    """
    Refresh all materialized views with fresh data.
    """
    logger.info("Refreshing materialized views...")
    
    views_to_refresh = [
        'mv_player_season_stats',
        'mv_team_season_stats', 
        'mv_recent_match_summary'
    ]
    
    try:
        db.refresh_materialized_views()
        
        # Manually refresh our specific views
        with db.engine.begin() as conn:
            for view in views_to_refresh:
                try:
                    # Use non-concurrent refresh for reliability
                    conn.execute(text(f"REFRESH MATERIALIZED VIEW {view}"))
                    logger.info(f"Refreshed materialized view: {view}")
                except Exception as e:
                    logger.error(f"Error refreshing view {view}: {e}")
                    
    except Exception as e:
        logger.error(f"Error in refresh_materialized_views: {e}")
        logger.error(traceback.format_exc())


def database_housekeeping(db: SupabaseDB):
    """
    Perform database housekeeping tasks.
    """
    logger.info("Performing database housekeeping...")
    
    try:
        # Clean up old data (keep 2 years)
        days_to_keep = int(os.getenv('DATA_RETENTION_DAYS', '730'))  # 2 years default
        db.cleanup_old_data(days_to_keep)
        
        # Analyze tables for query planner
        with db.engine.begin() as conn:
            tables = ['team_match_stats', 'player_match_stats']
            for table in tables:
                try:
                    conn.execute(text(f"ANALYZE {table}"))
                    logger.info(f"Analyzed table: {table}")
                except Exception as e:
                    logger.error(f"Error analyzing table {table}: {e}")
        
        logger.info("Database housekeeping completed")
        
    except Exception as e:
        logger.error(f"Error in database_housekeeping: {e}")
        logger.error(traceback.format_exc())


def generate_summary_stats(db: SupabaseDB) -> Dict:
    """
    Generate and log summary statistics about the database.
    """
    logger.info("Generating summary statistics...")
    
    stats = {}
    
    try:
        # Basic table counts
        table_stats = db.get_table_stats()
        stats.update(table_stats)
        
        # Additional stats
        with db.engine.connect() as conn:
            # Recent data counts
            recent_team_matches = conn.execute(text("""
                SELECT COUNT(*) FROM team_match_stats 
                WHERE match_date >= CURRENT_DATE - INTERVAL '7 days'
            """)).scalar()
            
            recent_player_matches = conn.execute(text("""
                SELECT COUNT(*) FROM player_match_stats 
                WHERE match_date >= CURRENT_DATE - INTERVAL '7 days'
            """)).scalar()
            
            # League distribution
            league_counts = conn.execute(text("""
                SELECT league, COUNT(DISTINCT match_id) as match_count
                FROM team_match_stats
                WHERE match_date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY league
                ORDER BY match_count DESC
            """)).fetchall()
            
            # Data freshness
            latest_match = conn.execute(text("""
                SELECT MAX(match_date) FROM team_match_stats
            """)).scalar()
            
            stats.update({
                'recent_team_matches_7d': recent_team_matches,
                'recent_player_matches_7d': recent_player_matches,
                'latest_match_date': str(latest_match) if latest_match else None,
                'league_distribution_30d': dict(league_counts) if league_counts else {}
            })
        
        # Log statistics
        logger.info("=== DATABASE SUMMARY STATISTICS ===")
        logger.info(f"Total team match records: {stats.get('team_match_stats', 0):,}")
        logger.info(f"Total player match records: {stats.get('player_match_stats', 0):,}")
        logger.info(f"Recent team matches (7d): {stats.get('recent_team_matches_7d', 0):,}")
        logger.info(f"Recent player matches (7d): {stats.get('recent_player_matches_7d', 0):,}")
        logger.info(f"Latest match date: {stats.get('latest_match_date', 'N/A')}")
        
        logger.info("League distribution (last 30 days):")
        for league, count in stats.get('league_distribution_30d', {}).items():
            logger.info(f"  {league}: {count:,} matches")
        
        logger.info("=" * 40)
        
        return stats
        
    except Exception as e:
        logger.error(f"Error generating summary stats: {e}")
        logger.error(traceback.format_exc())
        return stats


def main():
    """
    Main execution function for daily maintenance.
    """
    start_time = datetime.now()
    logger.info("=" * 50)
    logger.info(f"Starting FBref daily maintenance at {start_time}")
    logger.info("=" * 50)
    
    success = True
    
    try:
        # Load and validate environment
        load_environment()
        validate_environment()
        
        # Initialize database connection
        logger.info("Initializing database connection...")
        db = SupabaseDB()
        
        # Create materialized views if needed
        create_materialized_views(db)
        
        # Refresh materialized views
        refresh_materialized_views(db)
        
        # Perform housekeeping
        database_housekeeping(db)
        
        # Generate summary statistics
        summary_stats = generate_summary_stats(db)
        
        # Close database connection
        db.close()
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 50)
        logger.info(f"Daily maintenance completed at {end_time}")
        logger.info(f"Duration: {duration}")
        logger.info("All maintenance tasks completed successfully")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Fatal error in daily maintenance: {e}")
        logger.error(traceback.format_exc())
        success = False
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main() 