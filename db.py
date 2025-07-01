"""
Database module for Supabase connection and UPSERT operations.
Handles SQLAlchemy engine creation and bulk data loading with proper conflict resolution.
"""

import os
import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Integer, Float, 
    DateTime, Text, Index, UniqueConstraint, text
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from datetime import datetime
from typing import Optional, Dict, Any, List
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SupabaseDB:
    """
    Supabase database connection and operations handler.
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize database connection.
        
        Args:
            connection_string: PostgreSQL connection string. If None, reads from env vars.
        """
        if connection_string is None:
            # Read from environment variables
            db_url = os.getenv('SUPABASE_DB_URL')
            if not db_url:
                raise ValueError("SUPABASE_DB_URL environment variable not set")
            connection_string = db_url
        
        # Create engine with connection pooling for Supabase
        self.engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=2,  # Conservative for Supabase free tier
            max_overflow=2,
            pool_pre_ping=True,
            connect_args={
                "sslmode": "require",
                "application_name": "fbref_pipeline"
            }
        )
        
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)
        self._create_tables()
    
    def _create_tables(self):
        """
        Create the main tables if they don't exist.
        """
        # Team match stats table
        self.team_match_stats = Table(
            'team_match_stats',
            self.metadata,
            Column('match_id', String(50), nullable=False),
            Column('team_id', String(100), nullable=False),
            Column('category', String(20), nullable=False),
            Column('league', String(50), nullable=False),
            Column('match_date', String(10), nullable=False),  # YYYY-MM-DD format
            Column('team_name', String(100), nullable=False),
            Column('opponent', String(100)),
            Column('venue', String(10)),  # 'Home' or 'Away'
            
            # Flexible JSON-like columns for different stat categories
            Column('stats_json', Text),  # JSON string of all stats
            
            # Common timestamp columns
            Column('created_at', DateTime, default=datetime.utcnow),
            Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
            
            # Primary key ensures idempotent UPSERTs
            UniqueConstraint('match_id', 'team_id', 'category', name='uq_team_match_category'),
        )
        
        # Player match stats table
        self.player_match_stats = Table(
            'player_match_stats',
            self.metadata,
            Column('match_id', String(50), nullable=False),
            Column('player_id', String(100), nullable=False),
            Column('category', String(20), nullable=False),
            Column('league', String(50), nullable=False),
            Column('match_date', String(10), nullable=False),  # YYYY-MM-DD format
            Column('player_name', String(100), nullable=False),
            Column('team_name', String(100), nullable=False),
            Column('opponent', String(100)),
            Column('venue', String(10)),  # 'Home' or 'Away'
            Column('minutes_played', Integer),
            Column('position', String(10)),
            
            # Flexible JSON-like columns for different stat categories
            Column('stats_json', Text),  # JSON string of all stats
            
            # Common timestamp columns
            Column('created_at', DateTime, default=datetime.utcnow),
            Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
            
            # Primary key ensures idempotent UPSERTs
            UniqueConstraint('match_id', 'player_id', 'category', name='uq_player_match_category'),
        )
        
        # Create indexes for performance
        Index('idx_team_match_date', self.team_match_stats.c.match_date)
        Index('idx_team_league', self.team_match_stats.c.league)
        Index('idx_player_match_date', self.player_match_stats.c.match_date)
        Index('idx_player_league', self.player_match_stats.c.league)
        Index('idx_player_team', self.player_match_stats.c.team_name)
        
        # Create all tables
        self.metadata.create_all(self.engine)
        logger.info("Database tables created/verified")
    
    def create_temp_staging_table(self, base_table_name: str, connection) -> str:
        """
        Create a temporary staging table for bulk operations.
        Returns the temp table name.
        """
        temp_table_name = f"temp_{base_table_name}_{int(datetime.now().timestamp())}"
        
        if base_table_name == 'team_match_stats':
            base_table = self.team_match_stats
        elif base_table_name == 'player_match_stats':
            base_table = self.player_match_stats
        else:
            raise ValueError(f"Unknown base table: {base_table_name}")
        
        # Create temp table with same structure
        temp_ddl = f"""
        CREATE TEMP TABLE {temp_table_name} (
            LIKE {base_table_name} INCLUDING DEFAULTS
        ) ON COMMIT DROP;
        """
        
        connection.execute(text(temp_ddl))
        logger.info(f"Created temporary table: {temp_table_name}")
        return temp_table_name
    
    def bulk_upsert_team_stats(self, df: pd.DataFrame, category: str):
        """
        Bulk upsert team match stats using temporary staging table.
        """
        if df.empty:
            logger.info("No team stats to upsert")
            return
        
        logger.info(f"Upserting {len(df)} team stat records for category: {category}")
        
        with self.engine.begin() as conn:
            # Create temporary staging table
            temp_table = self.create_temp_staging_table('team_match_stats', conn)
            
            # Prepare data for insertion
            records = []
            for _, row in df.iterrows():
                # Convert stats to JSON (excluding metadata columns)
                excluded_cols = {'match_id', 'team_id', 'category', 'league', 'match_date', 
                               'team_name', 'opponent', 'venue'}
                stats_dict = {k: v for k, v in row.items() if k not in excluded_cols and pd.notna(v)}
                
                record = {
                    'match_id': str(row.get('match_id', '')),
                    'team_id': str(row.get('team_name', '')),  # Using team_name as team_id for now
                    'category': category,
                    'league': str(row.get('league', '')),
                    'match_date': str(row.get('match_date', '')),
                    'team_name': str(row.get('team_name', '')),
                    'opponent': str(row.get('opponent', '')),
                    'venue': str(row.get('venue', '')),
                    'stats_json': str(stats_dict) if stats_dict else None,
                }
                records.append(record)
            
            # Insert into temp table
            temp_df = pd.DataFrame(records)
            temp_df.to_sql(temp_table, conn, if_exists='append', index=False, method='multi')
            
            # UPSERT from temp table to main table
            upsert_sql = f"""
            INSERT INTO team_match_stats 
            SELECT * FROM {temp_table}
            ON CONFLICT (match_id, team_id, category) 
            DO UPDATE SET
                league = EXCLUDED.league,
                match_date = EXCLUDED.match_date,
                team_name = EXCLUDED.team_name,
                opponent = EXCLUDED.opponent,
                venue = EXCLUDED.venue,
                stats_json = EXCLUDED.stats_json,
                updated_at = NOW()
            """
            
            result = conn.execute(text(upsert_sql))
            logger.info(f"Upserted {result.rowcount} team stat records")
    
    def bulk_upsert_player_stats(self, df: pd.DataFrame, category: str):
        """
        Bulk upsert player match stats using temporary staging table.
        """
        if df.empty:
            logger.info("No player stats to upsert")
            return
        
        logger.info(f"Upserting {len(df)} player stat records for category: {category}")
        
        with self.engine.begin() as conn:
            # Create temporary staging table
            temp_table = self.create_temp_staging_table('player_match_stats', conn)
            
            # Prepare data for insertion
            records = []
            for _, row in df.iterrows():
                # Convert stats to JSON (excluding metadata columns)
                excluded_cols = {'match_id', 'player_id', 'category', 'league', 'match_date',
                               'player_name', 'team_name', 'opponent', 'venue', 'minutes_played', 'position'}
                stats_dict = {k: v for k, v in row.items() if k not in excluded_cols and pd.notna(v)}
                
                # Generate player_id from name and team (could be improved with actual FBref player IDs)
                player_name = str(row.get('Player', row.get('player_name', '')))
                team_name = str(row.get('team', row.get('team_name', '')))
                player_id = f"{player_name}_{team_name}".replace(' ', '_').lower()
                
                record = {
                    'match_id': str(row.get('match_id', '')),
                    'player_id': player_id,
                    'category': category,
                    'league': str(row.get('league', '')),
                    'match_date': str(row.get('match_date', '')),
                    'player_name': player_name,
                    'team_name': team_name,
                    'opponent': str(row.get('opponent', '')),
                    'venue': str(row.get('venue', '')),
                    'minutes_played': int(row.get('Min', row.get('minutes_played', 0))) if pd.notna(row.get('Min', row.get('minutes_played', 0))) else None,
                    'position': str(row.get('Pos', row.get('position', ''))),
                    'stats_json': str(stats_dict) if stats_dict else None,
                }
                records.append(record)
            
            # Insert into temp table
            temp_df = pd.DataFrame(records)
            temp_df.to_sql(temp_table, conn, if_exists='append', index=False, method='multi')
            
            # UPSERT from temp table to main table
            upsert_sql = f"""
            INSERT INTO player_match_stats 
            SELECT * FROM {temp_table}
            ON CONFLICT (match_id, player_id, category) 
            DO UPDATE SET
                league = EXCLUDED.league,
                match_date = EXCLUDED.match_date,
                player_name = EXCLUDED.player_name,
                team_name = EXCLUDED.team_name,
                opponent = EXCLUDED.opponent,
                venue = EXCLUDED.venue,
                minutes_played = EXCLUDED.minutes_played,
                position = EXCLUDED.position,
                stats_json = EXCLUDED.stats_json,
                updated_at = NOW()
            """
            
            result = conn.execute(text(upsert_sql))
            logger.info(f"Upserted {result.rowcount} player stat records")
    
    def get_recent_matches(self, days_back: int = 7) -> List[str]:
        """
        Get list of match_ids from recent matches in the database.
        """
        with self.engine.connect() as conn:
            query = text("""
                SELECT DISTINCT match_id 
                FROM team_match_stats 
                WHERE match_date >= CURRENT_DATE - INTERVAL '%s days'
                ORDER BY match_date DESC
            """)
            result = conn.execute(query, (days_back,))
            return [row[0] for row in result.fetchall()]
    
    def refresh_materialized_views(self):
        """
        Refresh materialized views (to be implemented based on specific view requirements).
        """
        with self.engine.connect() as conn:
            logger.info("Refreshing materialized views...")
            # Example views - implement based on actual requirements
            views_to_refresh = [
                # 'mv_player_season_stats',
                # 'mv_team_season_stats',
                # 'mv_league_standings'
            ]
            
            for view in views_to_refresh:
                try:
                    conn.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}"))
                    logger.info(f"Refreshed view: {view}")
                except Exception as e:
                    logger.error(f"Error refreshing view {view}: {e}")
    
    def cleanup_old_data(self, days_to_keep: int = 365):
        """
        Clean up old data beyond retention period.
        """
        with self.engine.begin() as conn:
            cutoff_sql = f"CURRENT_DATE - INTERVAL '{days_to_keep} days'"
            
            # Clean team stats
            team_result = conn.execute(text(f"""
                DELETE FROM team_match_stats 
                WHERE match_date < {cutoff_sql}
            """))
            
            # Clean player stats  
            player_result = conn.execute(text(f"""
                DELETE FROM player_match_stats 
                WHERE match_date < {cutoff_sql}
            """))
            
            logger.info(f"Cleaned up {team_result.rowcount} old team records")
            logger.info(f"Cleaned up {player_result.rowcount} old player records")
    
    def get_table_stats(self) -> Dict[str, int]:
        """
        Get basic statistics about the tables.
        """
        with self.engine.connect() as conn:
            team_count = conn.execute(text("SELECT COUNT(*) FROM team_match_stats")).scalar()
            player_count = conn.execute(text("SELECT COUNT(*) FROM player_match_stats")).scalar()
            
            return {
                'team_match_stats': team_count,
                'player_match_stats': player_count
            }
    
    def close(self):
        """
        Close database connections.
        """
        self.engine.dispose()
        logger.info("Database connections closed")


if __name__ == "__main__":
    # Test database connectivity
    try:
        db = SupabaseDB()
        stats = db.get_table_stats()
        print(f"Database connection successful!")
        print(f"Table stats: {stats}")
        db.close()
    except Exception as e:
        print(f"Database connection failed: {e}") 