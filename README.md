# AC Core - Shared Utilities Library

A Python utilities library for Alpha Crucible projects. This package provides shared functionality for signal insertion, database operations, and other common utilities used across multiple repositories.

## Features

- **Signal Insertion**: Insert trading signals into Supabase database from DataFrames or CSV files
- **Database Management**: Simplified database connection and operations for PostgreSQL/Supabase
- **Data Validation**: Comprehensive validation of signal data before insertion
- **Automatic Upsert**: Replace existing signals instead of creating duplicates
- **Batch Processing**: Efficient batch insertion for large datasets

## Installation

### From Git Repository (Recommended)

Add to your project's `requirements.txt`:

```txt
-e git+https://github.com/your-org/ac-core.git@main#egg=ac-core
```

Then install:
```bash
pip install -r requirements.txt
```

### Local Development (Editable Install)

If you have `ac-core` cloned locally in the same parent directory:

```bash
# In your project directory
pip install -e ../ac-core
```

Or add to `requirements.txt`:
```txt
-e ../ac-core
```

## Quick Start

### Signal Insertion

```python
from ac_core import SignalInserter
from datetime import date
import pandas as pd

# Initialize with environment variables
inserter = SignalInserter()

# Insert a single signal
inserter.insert_single_signal(
    asof_date=date.today(),
    ticker='AAPL',
    signal_name='SENTIMENT_YT',
    value=0.75,
    metadata={'source': 'youtube', 'confidence': 0.9}
)

# Insert from DataFrame
df = pd.DataFrame({
    'asof_date': [date.today()],
    'ticker': ['AAPL'],
    'signal_name': ['SENTIMENT_YT'],
    'value': [0.75]
})
result = inserter.insert_from_dataframe(df)

# Insert from CSV
result = inserter.insert_from_csv('signals.csv')
```

### Environment Setup

Set up your database credentials via environment variables:

```bash
# Option 1: Use DATABASE_URL (recommended for Supabase)
DATABASE_URL=postgresql://postgres:password@host:5432/postgres?sslmode=require

# Option 2: Use individual connection parameters
DB_HOST=your-host
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your-password
DB_NAME=postgres
```

## API Reference

### SignalInserter

Main class for inserting signals into the database.

```python
from ac_core import SignalInserter

inserter = SignalInserter(
    database_url=None,  # Optional: full connection URL
    host=None,         # Optional: database host
    port=None,         # Optional: database port
    user=None,         # Optional: database user
    password=None,     # Optional: database password
    database=None,     # Optional: database name
    auto_create_table=True  # Auto-create signal_raw table if missing
)
```

#### Methods

- `insert_from_dataframe(df, validate=True, batch_size=1000)` - Insert signals from DataFrame
- `insert_from_csv(csv_path, validate=True, batch_size=1000, **kwargs)` - Insert signals from CSV
- `insert_single_signal(asof_date, ticker, signal_name, value, metadata=None)` - Insert single signal
- `get_existing_signals(tickers=None, signal_names=None, start_date=None, end_date=None)` - Retrieve existing signals
- `test_connection()` - Test database connection
- `close()` - Close database connection

### SignalRaw

Data model for signal records.

```python
from ac_core import SignalRaw
from datetime import date

signal = SignalRaw(
    asof_date=date(2024, 1, 15),
    ticker='AAPL',
    signal_name='SENTIMENT_YT',
    value=0.75,
    metadata={'source': 'youtube'}
)
```

### DatabaseManager

Low-level database operations (typically used internally by SignalInserter).

```python
from ac_core import DatabaseManager

db_manager = DatabaseManager()
db_manager.connect()
# ... use database ...
db_manager.disconnect()
```

## Usage in Airflow/Docker

When using in Airflow or Docker containers, add to your repo's `requirements.txt`:

```txt
-e git+https://github.com/your-org/ac-core.git@main#egg=ac-core
```

The Dockerfile will automatically install it during the build process.

## Contributing

This is a shared utilities library. When adding new utilities:

1. Keep functions focused and reusable
2. Add proper documentation
3. Update this README with usage examples
4. Ensure compatibility with existing projects

## License

MIT License
