"""
Database manager for AC Core library.

This module handles all database operations including connection management
and signal insertion with upsert functionality for Supabase PostgreSQL.
"""

import os
import logging
from datetime import date, datetime
from typing import List, Dict, Optional, Any, Union
import pandas as pd
import psycopg2
from psycopg2 import Error as PgError
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

from .models import SignalRaw

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manages database connections and operations for the AC Core library.
    
    This class provides a simplified interface to PostgreSQL database operations
    specifically designed for Supabase integration. It handles connection management,
    signal insertion with upsert functionality, and error handling.
    
    Attributes:
        database_url: Full database connection URL (preferred for Supabase)
        host: Database host (fallback if no URL)
        port: Database port (fallback if no URL)
        user: Database username (fallback if no URL)
        password: Database password (fallback if no URL)
        database: Database name (fallback if no URL)
        _connection: Internal connection object
    
    Example:
        # Using environment variables (recommended)
        db_manager = DatabaseManager()
        
        # Using explicit connection parameters
        db_manager = DatabaseManager(
            host='your-supabase-host',
            port=5432,
            user='postgres',
            password='your-password',
            database='postgres'
        )
        
        # Using database URL (recommended for Supabase)
        db_manager = DatabaseManager(
            database_url='postgresql://user:pass@host:port/db?sslmode=require'
        )
    """
    
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None, 
                 user: Optional[str] = None, password: Optional[str] = None, 
                 database: Optional[str] = None, database_url: Optional[str] = None):
        """
        Initialize database manager with connection parameters.
        
        Args:
            host: Database host (defaults to DB_HOST env var or '127.0.0.1')
            port: Database port (defaults to DB_PORT env var or 5432)
            user: Database username (defaults to DB_USER env var or 'postgres')
            password: Database password (defaults to DB_PASSWORD env var or '')
            database: Database name (defaults to DB_NAME env var or 'postgres')
            database_url: Full database URL (defaults to DATABASE_URL env var)
        
        Note:
            If database_url is provided, individual connection parameters are ignored.
            For Supabase, database_url is the recommended approach.
        """
        self.database_url = database_url or os.getenv('DATABASE_URL')
        
        # Ensure sslmode=require for Supabase when using DATABASE_URL
        if self.database_url:
            url_lower = self.database_url.lower()
            has_query = '?' in self.database_url
            has_ssl = 'sslmode=' in url_lower
            if not has_ssl:
                separator = '&' if has_query else '?'
                self.database_url = f"{self.database_url}{separator}sslmode=require"
        
        # Fallback connection parameters
        self.host = host or os.getenv('DB_HOST', '127.0.0.1')
        self.port = port or int(os.getenv('DB_PORT', '5432'))
        self.user = user or os.getenv('DB_USER', 'postgres')
        self.password = password or os.getenv('DB_PASSWORD', '')
        self.database = database or os.getenv('DB_NAME', 'postgres')
        
        self._connection = None
    
    def connect(self) -> bool:
        """
        Establish connection to the database.
        
        Returns:
            True if connection successful, False otherwise
            
        Example:
            db_manager = DatabaseManager()
            if db_manager.connect():
                print("Connected successfully")
            else:
                print("Connection failed")
        """
        try:
            # Connection parameters optimized for Supabase
            common_kwargs = {
                "connect_timeout": 10,
                # Enable TCP keepalives to prevent silent connection drops
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 3,
            }
            
            if self.database_url:
                # Use DATABASE_URL if available (recommended for Supabase)
                self._connection = psycopg2.connect(self.database_url, **common_kwargs)
            else:
                # Use individual connection parameters
                self._connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    sslmode="require",  # Required for Supabase
                    **common_kwargs
                )
            
            if self._connection:
                self._connection.autocommit = True
                logger.info(f"Connected to PostgreSQL database: {self.database}")
                return True
                
        except PgError as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            return False
        return False
    
    def disconnect(self):
        """
        Close database connection.
        
        Example:
            db_manager = DatabaseManager()
            db_manager.connect()
            # ... use database ...
            db_manager.disconnect()
        """
        if self._connection and not self._connection.closed:
            self._connection.close()
            logger.info("PostgreSQL connection closed")
    
    def is_connected(self) -> bool:
        """
        Check if database connection is active.
        
        Returns:
            True if connected, False otherwise
        """
        return self._connection and not self._connection.closed
    
    def ensure_connection(self) -> bool:
        """
        Ensure database connection is active, reconnect if necessary.
        
        Returns:
            True if connected, False if connection failed
        """
        if not self.is_connected():
            return self.connect()
        return True
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[pd.DataFrame]:
        """
        Execute a SELECT query and return results as DataFrame.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            DataFrame with query results, or None if error
            
        Example:
            df = db_manager.execute_query(
                "SELECT * FROM signal_raw WHERE ticker = %s",
                ('AAPL',)
            )
        """
        if not self.ensure_connection():
            logger.error("Cannot execute query: no database connection")
            return None
        
        try:
            with self._connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                if results:
                    return pd.DataFrame(results)
                else:
                    return pd.DataFrame()
                    
        except PgError as e:
            logger.error(f"Error executing query: {e}")
            return None
    
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """
        Execute a query multiple times with different parameters.
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples
            
        Returns:
            Number of rows affected, or 0 if error
            
        Example:
            params = [('AAPL', 0.75), ('MSFT', 0.82)]
            count = db_manager.execute_many(
                "INSERT INTO signals (ticker, value) VALUES (%s, %s)",
                params
            )
        """
        if not self.ensure_connection():
            logger.error("Cannot execute query: no database connection")
            return 0
        
        if not params_list:
            return 0
        
        try:
            with self._connection.cursor() as cursor:
                cursor.executemany(query, params_list)
                return cursor.rowcount
                
        except PgError as e:
            logger.error(f"Error executing batch query: {e}")
            return 0
    
    def store_signals_raw(self, signals: List[SignalRaw]) -> int:
        """
        Store raw signals in the database with upsert functionality.
        
        This method inserts signals into the signal_raw table, replacing existing
        records with the same (asof_date, ticker, signal_name) combination.
        
        Args:
            signals: List of SignalRaw objects to store
            
        Returns:
            Number of signals stored/updated
            
        Example:
            signals = [
                SignalRaw(
                    asof_date=date(2024, 1, 15),
                    ticker='AAPL',
                    signal_name='SENTIMENT_YT',
                    value=0.75
                )
            ]
            count = db_manager.store_signals_raw(signals)
            print(f"Stored {count} signals")
        """
        if not signals:
            logger.warning("No signals provided for storage")
            return 0
        
        # PostgreSQL upsert query (ON CONFLICT ... DO UPDATE)
        # Use the unique constraint name for the conflict resolution
        query = """
        INSERT INTO signal_raw (asof_date, ticker, signal_name, value, metadata, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (asof_date, ticker, signal_name) DO UPDATE SET 
            value = EXCLUDED.value,
            metadata = EXCLUDED.metadata,
            created_at = EXCLUDED.created_at
        """
        
        params_list = []
        for signal in signals:
            # Convert metadata to JSON string if present
            metadata_json = None
            if signal.metadata:
                import json
                metadata_json = json.dumps(signal.metadata)
            
            params_list.append((
                signal.asof_date,
                signal.ticker,
                signal.signal_name,
                signal.value,
                metadata_json,
                signal.created_at or datetime.now()
            ))
        
        stored_count = self.execute_many(query, params_list)
        logger.info(f"Stored {stored_count} signals in signal_raw table")
        return stored_count
    
    def get_signals_raw(self, tickers: Optional[List[str]] = None,
                       signal_names: Optional[List[str]] = None,
                       start_date: Optional[date] = None,
                       end_date: Optional[date] = None) -> pd.DataFrame:
        """
        Retrieve raw signals from the database with optional filtering.
        
        Args:
            tickers: Optional list of ticker symbols to filter by
            signal_names: Optional list of signal names to filter by
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
            
        Returns:
            DataFrame with signal data
            
        Example:
            # Get all signals for AAPL and MSFT
            df = db_manager.get_signals_raw(tickers=['AAPL', 'MSFT'])
            
            # Get sentiment signals for last 30 days
            from datetime import date, timedelta
            end_date = date.today()
            start_date = end_date - timedelta(days=30)
            df = db_manager.get_signals_raw(
                signal_names=['SENTIMENT_YT'],
                start_date=start_date,
                end_date=end_date
            )
        """
        query = "SELECT * FROM signal_raw WHERE 1=1"
        params = []
        
        if tickers:
            placeholders = ','.join(['%s'] * len(tickers))
            query += f" AND ticker IN ({placeholders})"
            params.extend(tickers)
        
        if signal_names:
            placeholders = ','.join(['%s'] * len(signal_names))
            query += f" AND signal_name IN ({placeholders})"
            params.extend(signal_names)
        
        if start_date:
            query += " AND asof_date >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND asof_date <= %s"
            params.append(end_date)
        
        query += " ORDER BY asof_date DESC, ticker, signal_name"
        
        return self.execute_query(query, tuple(params) if params else None)
    
    def create_signal_raw_table(self) -> bool:
        """
        Create the signal_raw table if it doesn't exist.
        
        This method ensures the required table structure exists in the database.
        It's safe to call multiple times as it uses CREATE TABLE IF NOT EXISTS.
        
        Returns:
            True if table exists or was created successfully, False otherwise
            
        Example:
            if db_manager.create_signal_raw_table():
                print("Table ready for signal insertion")
            else:
                print("Failed to create table")
        """
        if not self.ensure_connection():
            return False
        
        # First, check if table exists and get its structure
        try:
            with self._connection.cursor() as cursor:
                # Check if table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'signal_raw'
                    )
                """)
                table_exists = cursor.fetchone()[0]
                
                if table_exists:
                    logger.info("signal_raw table already exists")
                    return True
                
                # Create table with proper PostgreSQL syntax
                cursor.execute("""
                    CREATE TABLE signal_raw (
                        id SERIAL PRIMARY KEY,
                        asof_date DATE NOT NULL,
                        ticker VARCHAR(20) NOT NULL,
                        signal_name VARCHAR(100) NOT NULL,
                        value FLOAT NOT NULL,
                        metadata JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(asof_date, ticker, signal_name)
                    )
                """)
                
                # Create indexes
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_raw_asof_date ON signal_raw (asof_date)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_raw_ticker ON signal_raw (ticker)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_raw_signal_name ON signal_raw (signal_name)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_raw_date_ticker ON signal_raw (asof_date, ticker)")
                
                logger.info("signal_raw table created successfully")
                return True
                
        except PgError as e:
            logger.error(f"Error creating signal_raw table: {e}")
            return False
    
    def reset_sequence(self) -> bool:
        """
        Reset the signal_raw table sequence to prevent ID conflicts.
        
        Returns:
            True if sequence was reset successfully, False otherwise
        """
        if not self.ensure_connection():
            return False
        
        try:
            with self._connection.cursor() as cursor:
                # Reset the sequence to the maximum ID + 1
                cursor.execute("""
                    SELECT setval('signal_raw_id_seq', 
                        COALESCE((SELECT MAX(id) FROM signal_raw), 0) + 1, 
                        false)
                """)
                logger.info("Signal_raw sequence reset successfully")
                return True
                
        except PgError as e:
            logger.error(f"Error resetting sequence: {e}")
            return False
    
    def test_connection(self) -> bool:
        """
        Test database connection with a simple query.
        
        Returns:
            True if connection is working, False otherwise
            
        Example:
            if db_manager.test_connection():
                print("Database connection is working")
            else:
                print("Database connection failed")
        """
        try:
            result = self.execute_query("SELECT 1 as test")
            return result is not None and not result.empty
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
