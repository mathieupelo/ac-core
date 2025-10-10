"""
Tests for AC Core models module.
"""

import pytest
import pandas as pd
from datetime import date, datetime
from ac_core.models import SignalRaw, DataFrameConverter


class TestSignalRaw:
    """Test SignalRaw data model."""
    
    def test_signal_raw_creation(self):
        """Test basic SignalRaw creation."""
        signal = SignalRaw(
            asof_date=date(2024, 1, 15),
            ticker='AAPL',
            signal_name='SENTIMENT_YT',
            value=0.75
        )
        
        assert signal.asof_date == date(2024, 1, 15)
        assert signal.ticker == 'AAPL'
        assert signal.signal_name == 'SENTIMENT_YT'
        assert signal.value == 0.75
        assert signal.metadata is None
        assert signal.created_at is not None
        assert isinstance(signal.created_at, datetime)
    
    def test_signal_raw_with_metadata(self):
        """Test SignalRaw creation with metadata."""
        metadata = {'source': 'youtube', 'confidence': 0.9}
        signal = SignalRaw(
            asof_date=date(2024, 1, 15),
            ticker='AAPL',
            signal_name='SENTIMENT_YT',
            value=0.75,
            metadata=metadata
        )
        
        assert signal.metadata == metadata
    
    def test_signal_raw_validation(self):
        """Test SignalRaw validation."""
        # Valid signal
        signal = SignalRaw(
            asof_date=date(2024, 1, 15),
            ticker='AAPL',
            signal_name='SENTIMENT_YT',
            value=0.75
        )
        assert signal.ticker == 'AAPL'
        
        # Invalid ticker
        with pytest.raises(ValueError, match="ticker must be a non-empty string"):
            SignalRaw(
                asof_date=date(2024, 1, 15),
                ticker='',
                signal_name='SENTIMENT_YT',
                value=0.75
            )
        
        # Invalid signal_name
        with pytest.raises(ValueError, match="signal_name must be a non-empty string"):
            SignalRaw(
                asof_date=date(2024, 1, 15),
                ticker='AAPL',
                signal_name='',
                value=0.75
            )
        
        # Invalid value
        with pytest.raises(ValueError, match="value must be a number"):
            SignalRaw(
                asof_date=date(2024, 1, 15),
                ticker='AAPL',
                signal_name='SENTIMENT_YT',
                value='invalid'
            )
        
        # Invalid asof_date
        with pytest.raises(ValueError, match="asof_date must be a date object"):
            SignalRaw(
                asof_date='2024-01-15',
                ticker='AAPL',
                signal_name='SENTIMENT_YT',
                value=0.75
            )


class TestDataFrameConverter:
    """Test DataFrameConverter utility class."""
    
    def test_signals_to_dataframe(self):
        """Test converting signals to DataFrame."""
        signals = [
            SignalRaw(
                asof_date=date(2024, 1, 15),
                ticker='AAPL',
                signal_name='SENTIMENT_YT',
                value=0.75,
                metadata={'source': 'youtube'}
            ),
            SignalRaw(
                asof_date=date(2024, 1, 16),
                ticker='MSFT',
                signal_name='SENTIMENT_YT',
                value=0.82
            )
        ]
        
        df = DataFrameConverter.signals_raw_to_dataframe(signals)
        
        assert len(df) == 2
        assert list(df.columns) == ['asof_date', 'ticker', 'signal_name', 'value', 'metadata', 'created_at']
        assert df.iloc[0]['ticker'] == 'AAPL'
        assert df.iloc[1]['ticker'] == 'MSFT'
        assert df.iloc[0]['metadata'] == '{"source": "youtube"}'
        assert pd.isna(df.iloc[1]['metadata'])
    
    def test_signals_to_dataframe_empty(self):
        """Test converting empty signals list to DataFrame."""
        df = DataFrameConverter.signals_raw_to_dataframe([])
        
        assert len(df) == 0
        assert list(df.columns) == ['asof_date', 'ticker', 'signal_name', 'value', 'metadata', 'created_at']
    
    def test_dataframe_to_signals(self):
        """Test converting DataFrame to signals."""
        df = pd.DataFrame({
            'asof_date': [date(2024, 1, 15), date(2024, 1, 16)],
            'ticker': ['AAPL', 'MSFT'],
            'signal_name': ['SENTIMENT_YT', 'SENTIMENT_YT'],
            'value': [0.75, 0.82],
            'metadata': ['{"source": "youtube"}', None]
        })
        
        signals = DataFrameConverter.dataframe_to_signals_raw(df)
        
        assert len(signals) == 2
        assert signals[0].ticker == 'AAPL'
        assert signals[1].ticker == 'MSFT'
        assert signals[0].metadata == {'source': 'youtube'}
        assert signals[1].metadata is None
    
    def test_dataframe_to_signals_missing_columns(self):
        """Test converting DataFrame with missing required columns."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'signal_name': ['SENTIMENT_YT'],
            'value': [0.75]
            # Missing 'asof_date'
        })
        
        with pytest.raises(ValueError, match="DataFrame missing required columns"):
            DataFrameConverter.dataframe_to_signals_raw(df)
    
    def test_validate_dataframe_valid(self):
        """Test DataFrame validation with valid data."""
        df = pd.DataFrame({
            'asof_date': [date(2024, 1, 15), date(2024, 1, 16)],
            'ticker': ['AAPL', 'MSFT'],
            'signal_name': ['SENTIMENT_YT', 'SENTIMENT_YT'],
            'value': [0.75, 0.82]
        })
        
        errors = DataFrameConverter.validate_dataframe(df)
        assert len(errors) == 0
    
    def test_validate_dataframe_missing_columns(self):
        """Test DataFrame validation with missing columns."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'signal_name': ['SENTIMENT_YT'],
            'value': [0.75]
            # Missing 'asof_date'
        })
        
        errors = DataFrameConverter.validate_dataframe(df)
        assert len(errors) > 0
        assert any('Missing required columns' in error for error in errors)
    
    def test_validate_dataframe_empty(self):
        """Test DataFrame validation with empty DataFrame."""
        df = pd.DataFrame()
        
        errors = DataFrameConverter.validate_dataframe(df)
        assert len(errors) > 0
        assert any('DataFrame is empty' in error for error in errors)
    
    def test_validate_dataframe_duplicates(self):
        """Test DataFrame validation with duplicate records."""
        df = pd.DataFrame({
            'asof_date': [date(2024, 1, 15), date(2024, 1, 15)],
            'ticker': ['AAPL', 'AAPL'],
            'signal_name': ['SENTIMENT_YT', 'SENTIMENT_YT'],
            'value': [0.75, 0.82]
        })
        
        errors = DataFrameConverter.validate_dataframe(df)
        assert len(errors) > 0
        assert any('duplicate signal records' in error for error in errors)
    
    def test_validate_dataframe_invalid_types(self):
        """Test DataFrame validation with invalid data types."""
        df = pd.DataFrame({
            'asof_date': ['2024-01-15', '2024-01-16'],  # Should be date objects
            'ticker': ['AAPL', 'MSFT'],
            'signal_name': ['SENTIMENT_YT', 'SENTIMENT_YT'],
            'value': [0.75, 0.82]
        })
        
        errors = DataFrameConverter.validate_dataframe(df)
        assert len(errors) > 0
        assert any('must contain date objects' in error for error in errors)
