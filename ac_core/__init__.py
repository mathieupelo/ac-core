"""
AC Core - Signal insertion library for Alpha Crucible Quant framework.

This library provides a simple interface for inserting trading signals into
the Alpha Crucible Quant Supabase database. It supports both DataFrame-based
and CSV file-based signal insertion with automatic upsert functionality.

Main Components:
- SignalInserter: Main class for inserting signals
- SignalRaw: Data model for signal records
- DatabaseManager: Low-level database operations

Example Usage:
    from ac_core import SignalInserter
    
    # Initialize with environment variables or explicit connection
    inserter = SignalInserter()
    
    # Insert from DataFrame
    inserter.insert_from_dataframe(df)
    
    # Insert from CSV file
    inserter.insert_from_csv('signals.csv')
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
