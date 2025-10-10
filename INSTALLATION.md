# AC Core Installation Guide

This guide provides detailed instructions for installing and setting up the AC Core library.

## Prerequisites

- Python 3.8 or higher
- Access to a Supabase PostgreSQL database
- pip package manager

## Installation Methods

### Method 1: Install from Source (Recommended for Development)

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-org/ac-core.git
   cd ac-core
   ```

2. **Create a virtual environment (recommended):**
   ```bash
   python -m venv venv
   
   # On Windows:
   venv\Scripts\activate
   
   # On macOS/Linux:
   source venv/bin/activate
   ```

3. **Install the package:**
   ```bash
   # Install in development mode
   pip install -e .
   
   # Or install with development dependencies
   pip install -e ".[dev]"
   ```

### Method 2: Install from PyPI (When Published)

```bash
pip install ac-core
```

### Method 3: Install with Development Dependencies

```bash
# From source
pip install -e ".[dev]"

# From PyPI (when available)
pip install ac-core[dev]
```

## Environment Setup

### 1. Create Environment File

Copy the example environment file and fill in your database credentials:

```bash
cp env.example .env
```

### 2. Configure Database Connection

Edit the `.env` file with your Supabase credentials:

```bash
# Option 1: Use DATABASE_URL (recommended for Supabase)
DATABASE_URL=postgresql://postgres:your-password@your-supabase-host:5432/postgres?sslmode=require

# Option 2: Use individual connection parameters
DB_HOST=your-supabase-host
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your-password
DB_NAME=postgres
```

### 3. Get Supabase Connection Details

1. Go to your [Supabase project dashboard](https://supabase.com/dashboard)
2. Navigate to **Settings** → **Database**
3. Copy the connection string or individual parameters
4. Replace the placeholder values in your `.env` file

## Verification

### 1. Test Installation

```python
import ac_core
print(f"AC Core version: {ac_core.__version__}")
```

### 2. Test Database Connection

```python
from ac_core import SignalInserter

# Initialize inserter
inserter = SignalInserter()

# Test connection
if inserter.test_connection():
    print("✅ Database connection successful")
else:
    print("❌ Database connection failed")
    print("Please check your .env file and database credentials")

# Close connection
inserter.close()
```

### 3. Run Example Script

```bash
python examples/basic_usage.py
```

## Development Setup

### 1. Install Development Dependencies

```bash
pip install -e ".[dev]"
```

This installs:
- `pytest` - Testing framework
- `pytest-cov` - Coverage reporting
- `black` - Code formatting
- `flake8` - Linting
- `mypy` - Type checking

### 2. Run Tests

```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=ac_core

# Run specific test file
pytest tests/test_models.py
```

### 3. Code Quality Checks

```bash
# Format code
black .

# Check linting
flake8 .

# Type checking
mypy .
```

## Troubleshooting

### Common Issues

#### 1. Database Connection Failed

**Error:** `Failed to connect to database`

**Solutions:**
- Verify your `.env` file has correct credentials
- Check if your Supabase project is active
- Ensure your IP is whitelisted in Supabase (if using IP restrictions)
- Verify the connection string format

#### 2. SSL Connection Error

**Error:** `SSL connection required`

**Solutions:**
- Ensure your `DATABASE_URL` includes `?sslmode=require`
- Check if you're using the correct Supabase connection string

#### 3. Import Error

**Error:** `ModuleNotFoundError: No module named 'ac_core'`

**Solutions:**
- Ensure you're in the correct virtual environment
- Reinstall the package: `pip install -e .`
- Check your Python path

#### 4. Permission Denied

**Error:** `Permission denied` when creating tables

**Solutions:**
- Verify your database user has CREATE TABLE permissions
- Check if the `signal_raw` table already exists
- Ensure you're using the correct database credentials

### Getting Help

1. **Check the logs:** Enable detailed logging to see what's happening:
   ```python
   import logging
   logging.basicConfig(level=logging.INFO)
   ```

2. **Verify environment variables:**
   ```python
   import os
   from dotenv import load_dotenv
   load_dotenv()
   print("DATABASE_URL:", os.getenv('DATABASE_URL'))
   ```

3. **Test database connection manually:**
   ```python
   import psycopg2
   conn = psycopg2.connect(os.getenv('DATABASE_URL'))
   print("Connection successful!")
   conn.close()
   ```

## Next Steps

After successful installation:

1. **Read the documentation:** Check out the [README.md](README.md) for usage examples
2. **Run examples:** Try the examples in the `examples/` directory
3. **Explore the API:** Review the API reference in the README
4. **Start building:** Begin integrating AC Core into your signal processing workflows

## Support

If you encounter issues:

1. Check this installation guide
2. Review the [README.md](README.md) documentation
3. Search [existing issues](https://github.com/your-org/ac-core/issues)
4. Create a [new issue](https://github.com/your-org/ac-core/issues/new) with:
   - Your Python version
   - Installation method used
   - Full error message
   - Steps to reproduce
