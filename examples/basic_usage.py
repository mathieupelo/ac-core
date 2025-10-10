#!/usr/bin/env python3
"""
Basic usage examples for AC Core library.

This script demonstrates the main features of the AC Core signal insertion library.
"""

import os
import sys
from datetime import date, timedelta
from pathlib import Path
import pandas as pd

# Add the parent directory to the path so we can import ac_core
sys.path.insert(0, str(Path(__file__).parent.parent))

from ac_core import SignalInserter, SignalRaw


def example_environment_setup():
    """Example of setting up environment variables."""
    print("=== Environment Setup Example ===")
    
    # Example environment variables (replace with your actual values)
    env_example = """
# Create a .env file with your Supabase credentials:

# Option 1: Use DATABASE_URL (recommended for Supabase)
DATABASE_URL=postgresql://postgres:your-password@your-supabase-host:5432/postgres?sslmode=require

# Option 2: Use individual connection parameters
DB_HOST=your-supabase-host
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your-password
DB_NAME=postgres
"""
    print(env_example)


def example_connection_test():
    """Example of testing database connection."""
    print("\n=== Connection Test Example ===")
    
    try:
        # Initialize inserter (uses environment variables)
        inserter = SignalInserter()
        
        # Test connection
        if inserter.test_connection():
            print("[OK] Database connection successful")
            return inserter
        else:
            print("[ERROR] Database connection failed")
            print("Please check your environment variables and database credentials")
            return None
            
    except Exception as e:
        print(f"[ERROR] Error during connection test: {e}")
        return None


def example_single_signal_insertion(inserter):
    """Example of inserting a single signal."""
    print("\n=== Single Signal Insertion Example ===")
    
    if not inserter:
        print("Skipping - no database connection")
        return
    
    try:
        # Insert a single signal
        success = inserter.insert_single_signal(
            asof_date=date.today(),
            ticker='AAPL',
            signal_name='SENTIMENT_YT2',
            value=0.75,
            metadata={
                'source': 'youtube_comments',
                'confidence': 0.9,
                'comment_count': 150
            }
        )
        
        if success:
            print("[OK] Single signal inserted successfully")
        else:
            print("[ERROR] Failed to insert single signal")
            
    except Exception as e:
        print(f"[ERROR] Error inserting single signal: {e}")


def example_dataframe_insertion(inserter):
    """Example of inserting signals from a DataFrame."""
    print("\n=== DataFrame Insertion Example ===")
    
    if not inserter:
        print("Skipping - no database connection")
        return
    
    try:
        # Create sample signal data
        today = date.today()
        df = pd.DataFrame({
            'asof_date': [today, today, today - timedelta(days=1)],
            'ticker': ['AAPL', 'MSFT', 'GOOGL'],
            'signal_name': ['SENTIMENT_YT2', 'SENTIMENT_YT2', 'SENTIMENT_YT2'],
            'value': [0.75, 0.82, 0.68],
            'metadata': [
                {'source': 'youtube', 'confidence': 0.9, 'comment_count': 150},
                {'source': 'youtube', 'confidence': 0.85, 'comment_count': 200},
                {'source': 'youtube', 'confidence': 0.92, 'comment_count': 120}
            ]
        })
        
        print(f"Created DataFrame with {len(df)} signals:")
        print(df)
        
        # Insert signals
        result = inserter.insert_from_dataframe(df)
        
        if result['success']:
            print(f"[OK] Successfully inserted {result['records_inserted']} signals")
        else:
            print(f"[ERROR] Insertion failed: {result['errors']}")
            
    except Exception as e:
        print(f"[ERROR] Error in DataFrame insertion: {e}")


def example_csv_insertion(inserter):
    """Example of inserting signals from a CSV file."""
    print("\n=== CSV Insertion Example ===")
    
    if not inserter:
        print("Skipping - no database connection")
        return
    
    try:
        # Create a sample CSV file
        csv_path = Path("sample_signals.csv")
        
        csv_data = """asof_date,ticker,signal_name,value,metadata
2024-01-15,AAPL,SENTIMENT_YT2,0.75,"{""source"": ""youtube"", ""confidence"": 0.9, ""comment_count"": 150}"
2024-01-15,MSFT,SENTIMENT_YT2,0.82,"{""source"": ""youtube"", ""confidence"": 0.85, ""comment_count"": 200}"
2024-01-15,GOOGL,SENTIMENT_YT2,0.68,"{""source"": ""youtube"", ""confidence"": 0.92, ""comment_count"": 120}"
2024-01-16,AAPL,SENTIMENT_YT2,0.78,"{""source"": ""youtube"", ""confidence"": 0.88, ""comment_count"": 180}"
2024-01-16,MSFT,SENTIMENT_YT2,0.80,"{""source"": ""youtube"", ""confidence"": 0.87, ""comment_count"": 220}"
"""
        
        with open(csv_path, 'w') as f:
            f.write(csv_data)
        
        print(f"Created sample CSV file: {csv_path}")
        
        # Insert from CSV
        result = inserter.insert_from_csv(csv_path)
        
        if result['success']:
            print(f"[OK] Successfully inserted {result['records_inserted']} signals from CSV")
        else:
            print(f"[ERROR] CSV insertion failed: {result['errors']}")
        
        # Clean up
        csv_path.unlink()
        print("Cleaned up sample CSV file")
        
    except Exception as e:
        print(f"[ERROR] Error in CSV insertion: {e}")


def example_retrieve_signals(inserter):
    """Example of retrieving existing signals."""
    print("\n=== Retrieve Signals Example ===")
    
    if not inserter:
        print("Skipping - no database connection")
        return
    
    try:
        # Get signals for specific tickers
        existing = inserter.get_existing_signals(
            tickers=['AAPL', 'MSFT', 'GOOGL'],
            signal_names=['SENTIMENT_YT']
        )
        
        if not existing.empty:
            print(f"Found {len(existing)} existing signals:")
            print(existing.head())
        else:
            print("No existing signals found")
            
    except Exception as e:
        print(f"[ERROR] Error retrieving signals: {e}")


def example_error_handling():
    """Example of error handling and validation."""
    print("\n=== Error Handling Example ===")
    
    try:
        # Create invalid DataFrame (missing required column)
        invalid_df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT'],
            'signal_name': ['SENTIMENT_YT2', 'SENTIMENT_YT2'],
            'value': [0.75, 0.82]
            # Missing 'asof_date' column
        })
        
        inserter = SignalInserter()
        result = inserter.insert_from_dataframe(invalid_df, validate=True)
        
        if not result['success']:
            print("[OK] Validation correctly caught errors:")
            for error in result['errors']:
                print(f"  - {error}")
        else:
            print("[ERROR] Validation should have failed")
            
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")


def main():
    """Run all examples."""
    print("AC Core Library - Basic Usage Examples")
    print("=" * 50)
    
    # Show environment setup
    example_environment_setup()
    
    # Test connection
    inserter = example_connection_test()
    
    # Run examples if connection is available
    if inserter:
        example_single_signal_insertion(inserter)
        example_dataframe_insertion(inserter)
        example_csv_insertion(inserter)
        example_retrieve_signals(inserter)
        
        # Close connection
        inserter.close()
        print("\n[OK] Database connection closed")
    
    # Show error handling example
    example_error_handling()
    
    print("\n" + "=" * 50)
    print("Examples completed!")


if __name__ == "__main__":
    main()
