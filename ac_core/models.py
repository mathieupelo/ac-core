"""
Data models for AC Core library.

This module defines the data structures used for signal insertion operations.
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List, Optional, Any
import pandas as pd
import json


@dataclass
class SignalRaw:
    """
    Represents raw signal data for a specific ticker and date.
    
    This is the core data model for signal records that will be inserted
    into the signal_raw table in the Supabase database.
    
    Attributes:
        asof_date: The date for which this signal is calculated
        ticker: Stock ticker symbol (e.g., 'AAPL', 'MSFT')
        signal_name: Name of the signal (e.g., 'SENTIMENT_YT', 'RSI')
        value: The signal value/score
        metadata: Optional metadata dictionary containing additional information
        created_at: Timestamp when this record was created (auto-generated if None)
    
    Example:
        signal = SignalRaw(
            asof_date=date(2024, 1, 15),
            ticker='AAPL',
            signal_name='SENTIMENT_YT',
            value=0.75,
            metadata={'source': 'youtube_comments', 'confidence': 0.9}
        )
    """
    asof_date: date
    ticker: str
    signal_name: str
    value: float
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None

    def __post_init__(self):
        """Validate and set default values after initialization."""
        if self.created_at is None:
            self.created_at = datetime.now()
        
        # Validate required fields
        if not self.ticker or not isinstance(self.ticker, str):
            raise ValueError("ticker must be a non-empty string")
        if not self.signal_name or not isinstance(self.signal_name, str):
            raise ValueError("signal_name must be a non-empty string")
        if not isinstance(self.value, (int, float)):
            raise ValueError("value must be a number")
        if not isinstance(self.asof_date, date):
            raise ValueError("asof_date must be a date object")


class DataFrameConverter:
    """
    Utility class to convert between DataFrames and SignalRaw objects.
    
    This class provides static methods for converting between pandas DataFrames
    and lists of SignalRaw objects, which is useful for bulk operations.
    """
    
    @staticmethod
    def signals_raw_to_dataframe(signals: List[SignalRaw]) -> pd.DataFrame:
        """
        Convert list of SignalRaw objects to DataFrame.
        
        Args:
            signals: List of SignalRaw objects to convert
            
        Returns:
            pandas.DataFrame with columns: asof_date, ticker, signal_name, value, metadata, created_at
            
        Example:
            signals = [SignalRaw(...), SignalRaw(...)]
            df = DataFrameConverter.signals_raw_to_dataframe(signals)
        """
        if not signals:
            return pd.DataFrame(columns=['asof_date', 'ticker', 'signal_name', 'value', 'metadata', 'created_at'])
        
        data = []
        for signal in signals:
            data.append({
                'asof_date': signal.asof_date,
                'ticker': signal.ticker,
                'signal_name': signal.signal_name,
                'value': signal.value,
                'metadata': json.dumps(signal.metadata) if signal.metadata else None,
                'created_at': signal.created_at
            })
        return pd.DataFrame(data)
    
    @staticmethod
    def dataframe_to_signals_raw(df: pd.DataFrame) -> List[SignalRaw]:
        """
        Convert DataFrame to list of SignalRaw objects.
        
        Args:
            df: DataFrame with columns: asof_date, ticker, signal_name, value, metadata (optional), created_at (optional)
            
        Returns:
            List of SignalRaw objects
            
        Raises:
            ValueError: If required columns are missing or data is invalid
            
        Example:
            df = pd.DataFrame({
                'asof_date': [date(2024, 1, 15)],
                'ticker': ['AAPL'],
                'signal_name': ['SENTIMENT_YT'],
                'value': [0.75]
            })
            signals = DataFrameConverter.dataframe_to_signals_raw(df)
        """
        required_columns = ['asof_date', 'ticker', 'signal_name', 'value']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"DataFrame missing required columns: {missing_columns}")
        
        signals = []
        for _, row in df.iterrows():
            # Parse metadata if present
            metadata = None
            if 'metadata' in df.columns and pd.notna(row.get('metadata')):
                try:
                    if isinstance(row['metadata'], str):
                        metadata = json.loads(row['metadata'])
                    elif isinstance(row['metadata'], dict):
                        metadata = row['metadata']
                except (json.JSONDecodeError, TypeError):
                    metadata = None
            
            # Parse created_at if present
            created_at = row.get('created_at')
            if pd.isna(created_at):
                created_at = None
            
            signals.append(SignalRaw(
                asof_date=row['asof_date'],
                ticker=row['ticker'],
                signal_name=row['signal_name'],
                value=row['value'],
                metadata=metadata,
                created_at=created_at
            ))
        return signals
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame) -> List[str]:
        """
        Validate DataFrame structure and data for signal insertion.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            List of validation error messages (empty if valid)
            
        Example:
            errors = DataFrameConverter.validate_dataframe(df)
            if errors:
                print("Validation errors:", errors)
        """
        errors = []
        
        # Check required columns
        required_columns = ['asof_date', 'ticker', 'signal_name', 'value']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
        
        if df.empty:
            errors.append("DataFrame is empty")
            return errors
        
        # Check data types and values
        for col in required_columns:
            if col in df.columns:
                if col == 'asof_date':
                    if not pd.api.types.is_datetime64_any_dtype(df[col]) and not all(isinstance(x, date) for x in df[col]):
                        errors.append(f"Column '{col}' must contain date objects")
                elif col == 'ticker':
                    if not df[col].dtype == 'object' or df[col].isna().any():
                        errors.append(f"Column '{col}' must contain non-null strings")
                elif col == 'signal_name':
                    if not df[col].dtype == 'object' or df[col].isna().any():
                        errors.append(f"Column '{col}' must contain non-null strings")
                elif col == 'value':
                    if not pd.api.types.is_numeric_dtype(df[col]):
                        errors.append(f"Column '{col}' must contain numeric values")
        
        # Check for duplicate combinations of asof_date, ticker, signal_name
        if all(col in df.columns for col in ['asof_date', 'ticker', 'signal_name']):
            duplicates = df.duplicated(subset=['asof_date', 'ticker', 'signal_name'])
            if duplicates.any():
                errors.append(f"Found {duplicates.sum()} duplicate signal records (same asof_date, ticker, signal_name)")
        
        return errors
