"""
Signal inserter for AC Core library.

This module provides the main SignalInserter class that offers a high-level
interface for inserting signals into the Supabase database from various sources.
"""

import os
import logging
from datetime import date, datetime
from typing import List, Dict, Optional, Any, Union
from pathlib import Path
import pandas as pd

from .models import SignalRaw, DataFrameConverter
from .database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class SignalInserter:
    """
    High-level interface for inserting signals into the Alpha Crucible Quant database.
    
    This class provides convenient methods for inserting signals from DataFrames
    and CSV files, with automatic validation, error handling, and upsert functionality.
    
    The inserter automatically handles:
    - Database connection management
    - Data validation and type conversion
    - Upsert operations (replace existing signals with same key)
    - Error handling and logging
    - Table creation if needed
    
    Attributes:
        db_manager: DatabaseManager instance for database operations
        auto_create_table: Whether to automatically create the signal_raw table
    
    Example:
        # Initialize with environment variables
        inserter = SignalInserter()
        
        # Insert from DataFrame
        df = pd.DataFrame({
            'asof_date': [date(2024, 1, 15)],
            'ticker': ['AAPL'],
            'signal_name': ['SENTIMENT_YT'],
            'value': [0.75]
        })
        result = inserter.insert_from_dataframe(df)
        
        # Insert from CSV file
        result = inserter.insert_from_csv('signals.csv')
    """
    
    def __init__(self, database_url: Optional[str] = None, 
                 host: Optional[str] = None, port: Optional[int] = None,
                 user: Optional[str] = None, password: Optional[str] = None,
                 database: Optional[str] = None, auto_create_table: bool = True):
        """
        Initialize the SignalInserter.
        
        Args:
            database_url: Full database connection URL (preferred for Supabase)
            host: Database host (fallback if no URL)
            port: Database port (fallback if no URL)
            user: Database username (fallback if no URL)
            password: Database password (fallback if no URL)
            database: Database name (fallback if no URL)
            auto_create_table: Whether to automatically create signal_raw table if missing
        
        Note:
            If no connection parameters are provided, the inserter will use
            environment variables (DATABASE_URL, DB_HOST, etc.).
        """
        self.db_manager = DatabaseManager(
            database_url=database_url,
            host=host, port=port, user=user, password=password, database=database
        )
        self.auto_create_table = auto_create_table
        
        logger.info("SignalInserter initialized")
    
    def insert_from_dataframe(self, df: pd.DataFrame, 
                            validate: bool = True,
                            batch_size: int = 1000) -> Dict[str, Any]:
        """
        Insert signals from a pandas DataFrame.
        
        This method takes a DataFrame containing signal data and inserts it into
        the database. The DataFrame should have columns: asof_date, ticker, signal_name, value.
        Optional columns: metadata, created_at.
        
        Args:
            df: DataFrame with signal data
            validate: Whether to validate DataFrame structure before insertion
            batch_size: Number of records to process in each batch
            
        Returns:
            Dictionary with insertion results:
            {
                'success': bool,
                'records_processed': int,
                'records_inserted': int,
                'errors': List[str],
                'warnings': List[str]
            }
        
        Raises:
            ValueError: If DataFrame validation fails
            ConnectionError: If database connection fails
            
        Example:
            # Create sample DataFrame
            df = pd.DataFrame({
                'asof_date': [date(2024, 1, 15), date(2024, 1, 16)],
                'ticker': ['AAPL', 'MSFT'],
                'signal_name': ['SENTIMENT_YT', 'SENTIMENT_YT'],
                'value': [0.75, 0.82],
                'metadata': [{'source': 'youtube'}, {'source': 'youtube'}]
            })
            
            # Insert signals
            result = inserter.insert_from_dataframe(df)
            if result['success']:
                print(f"Successfully inserted {result['records_inserted']} signals")
            else:
                print(f"Insertion failed: {result['errors']}")
        """
        result = {
            'success': False,
            'records_processed': 0,
            'records_inserted': 0,
            'errors': [],
            'warnings': []
        }
        
        try:
            # Validate DataFrame if requested
            if validate:
                validation_errors = DataFrameConverter.validate_dataframe(df)
                if validation_errors:
                    result['errors'].extend(validation_errors)
                    logger.error(f"DataFrame validation failed: {validation_errors}")
                    return result
            
            # Ensure database connection
            if not self.db_manager.ensure_connection():
                result['errors'].append("Failed to connect to database")
                return result
            
            # Create table if needed
            if self.auto_create_table:
                if not self.db_manager.create_signal_raw_table():
                    result['errors'].append("Failed to create signal_raw table")
                    return result
                
                # Reset sequence to prevent ID conflicts
                self.db_manager.reset_sequence()
            
            # Convert DataFrame to SignalRaw objects
            try:
                signals = DataFrameConverter.dataframe_to_signals_raw(df)
            except Exception as e:
                result['errors'].append(f"Failed to convert DataFrame to signals: {str(e)}")
                return result
            
            result['records_processed'] = len(signals)
            
            # Insert signals in batches
            total_inserted = 0
            for i in range(0, len(signals), batch_size):
                batch = signals[i:i + batch_size]
                try:
                    inserted_count = self.db_manager.store_signals_raw(batch)
                    total_inserted += inserted_count
                    logger.info(f"Inserted batch {i//batch_size + 1}: {inserted_count} signals")
                except Exception as e:
                    error_msg = f"Failed to insert batch {i//batch_size + 1}: {str(e)}"
                    result['errors'].append(error_msg)
                    logger.error(error_msg)
            
            result['records_inserted'] = total_inserted
            result['success'] = len(result['errors']) == 0
            
            if result['success']:
                logger.info(f"Successfully inserted {total_inserted} signals from DataFrame")
            else:
                logger.warning(f"Partial success: {total_inserted}/{len(signals)} signals inserted")
            
        except Exception as e:
            result['errors'].append(f"Unexpected error during insertion: {str(e)}")
            logger.error(f"Unexpected error in insert_from_dataframe: {e}")
        
        return result
    
    def insert_from_csv(self, csv_path: Union[str, Path], 
                       validate: bool = True,
                       batch_size: int = 1000,
                       **pandas_kwargs) -> Dict[str, Any]:
        """
        Insert signals from a CSV file.
        
        This method reads signal data from a CSV file and inserts it into the database.
        The CSV should have columns: asof_date, ticker, signal_name, value.
        Optional columns: metadata, created_at.
        
        Args:
            csv_path: Path to the CSV file
            validate: Whether to validate DataFrame structure before insertion
            batch_size: Number of records to process in each batch
            **pandas_kwargs: Additional arguments passed to pd.read_csv()
            
        Returns:
            Dictionary with insertion results (same format as insert_from_dataframe)
        
        Raises:
            FileNotFoundError: If CSV file doesn't exist
            ValueError: If CSV file is empty or malformed
            ConnectionError: If database connection fails
            
        Example:
            # Insert from CSV with default settings
            result = inserter.insert_from_csv('signals.csv')
            
            # Insert from CSV with custom pandas options
            result = inserter.insert_from_csv(
                'signals.csv',
                sep=';',
                parse_dates=['asof_date'],
                encoding='utf-8'
            )
            
            # Check results
            if result['success']:
                print(f"Successfully inserted {result['records_inserted']} signals")
            else:
                print(f"Insertion failed: {result['errors']}")
        """
        result = {
            'success': False,
            'records_processed': 0,
            'records_inserted': 0,
            'errors': [],
            'warnings': []
        }
        
        try:
            # Convert to Path object for easier handling
            csv_path = Path(csv_path)
            
            # Check if file exists
            if not csv_path.exists():
                result['errors'].append(f"CSV file not found: {csv_path}")
                return result
            
            # Check if file is empty
            if csv_path.stat().st_size == 0:
                result['errors'].append(f"CSV file is empty: {csv_path}")
                return result
            
            # Set default pandas options for CSV reading
            default_kwargs = {
                'parse_dates': ['asof_date'],
            }
            default_kwargs.update(pandas_kwargs)
            
            # Read CSV file
            try:
                df = pd.read_csv(csv_path, **default_kwargs)
                logger.info(f"Read {len(df)} records from CSV file: {csv_path}")
            except Exception as e:
                result['errors'].append(f"Failed to read CSV file: {str(e)}")
                return result
            
            # Check if DataFrame is empty
            if df.empty:
                result['errors'].append("CSV file contains no data")
                return result
            
            # Convert date column if it's not already datetime
            if 'asof_date' in df.columns:
                try:
                    df['asof_date'] = pd.to_datetime(df['asof_date']).dt.date
                except Exception as e:
                    result['errors'].append(f"Failed to parse asof_date column: {str(e)}")
                    return result
            
            # Insert using DataFrame method
            return self.insert_from_dataframe(df, validate=validate, batch_size=batch_size)
            
        except Exception as e:
            result['errors'].append(f"Unexpected error reading CSV file: {str(e)}")
            logger.error(f"Unexpected error in insert_from_csv: {e}")
        
        return result
    
    def insert_single_signal(self, asof_date: date, ticker: str, signal_name: str, 
                           value: float, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Insert a single signal record.
        
        This is a convenience method for inserting individual signals without
        creating DataFrames or CSV files.
        
        Args:
            asof_date: Date for the signal
            ticker: Stock ticker symbol
            signal_name: Name of the signal
            value: Signal value
            metadata: Optional metadata dictionary
            
        Returns:
            True if insertion successful, False otherwise
            
        Example:
            success = inserter.insert_single_signal(
                asof_date=date(2024, 1, 15),
                ticker='AAPL',
                signal_name='SENTIMENT_YT',
                value=0.75,
                metadata={'source': 'youtube_comments', 'confidence': 0.9}
            )
            if success:
                print("Signal inserted successfully")
        """
        try:
            signal = SignalRaw(
                asof_date=asof_date,
                ticker=ticker,
                signal_name=signal_name,
                value=value,
                metadata=metadata
            )
            
            # Ensure database connection
            if not self.db_manager.ensure_connection():
                logger.error("Failed to connect to database")
                return False
            
            # Create table if needed
            if self.auto_create_table:
                if not self.db_manager.create_signal_raw_table():
                    logger.error("Failed to create signal_raw table")
                    return False
                
                # Reset sequence to prevent ID conflicts
                self.db_manager.reset_sequence()
            
            # Insert signal
            inserted_count = self.db_manager.store_signals_raw([signal])
            success = inserted_count > 0
            
            if success:
                logger.info(f"Successfully inserted signal: {ticker} {signal_name} on {asof_date}")
            else:
                logger.error(f"Failed to insert signal: {ticker} {signal_name} on {asof_date}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error inserting single signal: {e}")
            return False
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection is working, False otherwise
            
        Example:
            if inserter.test_connection():
                print("Database connection is working")
            else:
                print("Database connection failed")
        """
        return self.db_manager.test_connection()
    
    def get_existing_signals(self, tickers: Optional[List[str]] = None,
                           signal_names: Optional[List[str]] = None,
                           start_date: Optional[date] = None,
                           end_date: Optional[date] = None) -> pd.DataFrame:
        """
        Retrieve existing signals from the database.
        
        This method can be used to check what signals already exist before
        inserting new ones, or to retrieve signals for analysis.
        
        Args:
            tickers: Optional list of ticker symbols to filter by
            signal_names: Optional list of signal names to filter by
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
            
        Returns:
            DataFrame with existing signal data
            
        Example:
            # Get all signals for specific tickers
            existing = inserter.get_existing_signals(tickers=['AAPL', 'MSFT'])
            
            # Get sentiment signals for last 30 days
            from datetime import date, timedelta
            end_date = date.today()
            start_date = end_date - timedelta(days=30)
            existing = inserter.get_existing_signals(
                signal_names=['SENTIMENT_YT'],
                start_date=start_date,
                end_date=end_date
            )
        """
        try:
            if not self.db_manager.ensure_connection():
                logger.error("Failed to connect to database")
                return pd.DataFrame()
            
            return self.db_manager.get_signals_raw(
                tickers=tickers,
                signal_names=signal_names,
                start_date=start_date,
                end_date=end_date
            )
            
        except Exception as e:
            logger.error(f"Error retrieving existing signals: {e}")
            return pd.DataFrame()
    
    def close(self):
        """
        Close database connection.
        
        Example:
            inserter = SignalInserter()
            # ... use inserter ...
            inserter.close()
        """
        self.db_manager.disconnect()
        logger.info("SignalInserter connection closed")
