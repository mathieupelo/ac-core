"""
AC Core - Shared Utilities Library

A utilities library for Alpha Crucible projects providing:
- Signal insertion into Supabase database
- Database connection management
- Common utilities for signal processing

Example Usage:
    from ac_core import SignalInserter
    
    # Initialize with environment variables
    inserter = SignalInserter()
    
    # Insert signals
    inserter.insert_single_signal(
        asof_date=date.today(),
        ticker='AAPL',
        signal_name='SENTIMENT_YT',
        value=0.75
    )
"""

from .models import SignalRaw
from .database_manager import DatabaseManager
from .signal_inserter import SignalInserter

__version__ = "0.1.0"
__author__ = "Alpha Crucible Team"
__email__ = "team@alphacrucible.com"

__all__ = [
    "SignalRaw",
    "DatabaseManager", 
    "SignalInserter",
]
