"""
Pytest configuration and fixtures for AC Core tests.
"""

import pytest
import pandas as pd
from datetime import date, datetime
from ac_core.models import SignalRaw


@pytest.fixture
def sample_signal():
    """Create a sample SignalRaw object."""
    return SignalRaw(
        asof_date=date(2024, 1, 15),
        ticker='AAPL',
        signal_name='SENTIMENT_YT',
        value=0.75,
        metadata={'source': 'youtube', 'confidence': 0.9}
    )


@pytest.fixture
def sample_signals():
    """Create a list of sample SignalRaw objects."""
    return [
        SignalRaw(
            asof_date=date(2024, 1, 15),
            ticker='AAPL',
            signal_name='SENTIMENT_YT',
            value=0.75,
            metadata={'source': 'youtube', 'confidence': 0.9}
        ),
        SignalRaw(
            asof_date=date(2024, 1, 15),
            ticker='MSFT',
            signal_name='SENTIMENT_YT',
            value=0.82,
            metadata={'source': 'youtube', 'confidence': 0.85}
        ),
        SignalRaw(
            asof_date=date(2024, 1, 16),
            ticker='GOOGL',
            signal_name='SENTIMENT_YT',
            value=0.68,
            metadata={'source': 'youtube', 'confidence': 0.92}
        )
    ]


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame with signal data."""
    return pd.DataFrame({
        'asof_date': [date(2024, 1, 15), date(2024, 1, 15), date(2024, 1, 16)],
        'ticker': ['AAPL', 'MSFT', 'GOOGL'],
        'signal_name': ['SENTIMENT_YT', 'SENTIMENT_YT', 'SENTIMENT_YT'],
        'value': [0.75, 0.82, 0.68],
        'metadata': [
            {'source': 'youtube', 'confidence': 0.9},
            {'source': 'youtube', 'confidence': 0.85},
            {'source': 'youtube', 'confidence': 0.92}
        ]
    })


@pytest.fixture
def invalid_dataframe():
    """Create an invalid DataFrame for testing validation."""
    return pd.DataFrame({
        'ticker': ['AAPL', 'MSFT'],
        'signal_name': ['SENTIMENT_YT', 'SENTIMENT_YT'],
        'value': [0.75, 0.82]
        # Missing 'asof_date' column
    })


@pytest.fixture
def empty_dataframe():
    """Create an empty DataFrame for testing."""
    return pd.DataFrame()


@pytest.fixture
def duplicate_dataframe():
    """Create a DataFrame with duplicate records for testing validation."""
    return pd.DataFrame({
        'asof_date': [date(2024, 1, 15), date(2024, 1, 15)],
        'ticker': ['AAPL', 'AAPL'],
        'signal_name': ['SENTIMENT_YT', 'SENTIMENT_YT'],
        'value': [0.75, 0.82]
    })
