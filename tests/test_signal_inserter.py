"""
Tests for AC Core SignalInserter class.
"""

import pytest
import pandas as pd
from datetime import date, datetime
from unittest.mock import Mock, patch, MagicMock
from ac_core.signal_inserter import SignalInserter
from ac_core.models import SignalRaw


class TestSignalInserter:
    """Test SignalInserter class."""
    
    @pytest.fixture
    def mock_db_manager(self):
        """Create a mock database manager."""
        mock = Mock()
        mock.ensure_connection.return_value = True
        mock.create_signal_raw_table.return_value = True
        mock.store_signals_raw.return_value = 1
        mock.test_connection.return_value = True
        mock.get_signals_raw.return_value = pd.DataFrame()
        return mock
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample DataFrame for testing."""
        return pd.DataFrame({
            'asof_date': [date(2024, 1, 15), date(2024, 1, 16)],
            'ticker': ['AAPL', 'MSFT'],
            'signal_name': ['SENTIMENT_YT', 'SENTIMENT_YT'],
            'value': [0.75, 0.82],
            'metadata': [{'source': 'youtube'}, None]
        })
    
    @patch('ac_core.signal_inserter.DatabaseManager')
    def test_initialization(self, mock_db_manager_class, mock_db_manager):
        """Test SignalInserter initialization."""
        mock_db_manager_class.return_value = mock_db_manager
        
        inserter = SignalInserter()
        
        assert inserter.db_manager == mock_db_manager
        assert inserter.auto_create_table is True
        mock_db_manager_class.assert_called_once()
    
    @patch('ac_core.signal_inserter.DatabaseManager')
    def test_initialization_with_params(self, mock_db_manager_class, mock_db_manager):
        """Test SignalInserter initialization with custom parameters."""
        mock_db_manager_class.return_value = mock_db_manager
        
        inserter = SignalInserter(
            database_url='test-url',
            auto_create_table=False
        )
        
        assert inserter.auto_create_table is False
        mock_db_manager_class.assert_called_once_with(
            database_url='test-url',
            host=None, port=None, user=None, password=None, database=None
        )
    
    @patch('ac_core.signal_inserter.DatabaseManager')
    def test_insert_from_dataframe_success(self, mock_db_manager_class, mock_db_manager, sample_dataframe):
        """Test successful DataFrame insertion."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_db_manager.store_signals_raw.return_value = 2
        
        inserter = SignalInserter()
        result = inserter.insert_from_dataframe(sample_dataframe)
        
        assert result['success'] is True
        assert result['records_processed'] == 2
        assert result['records_inserted'] == 2
        assert len(result['errors']) == 0
        
        # Verify database operations
        mock_db_manager.ensure_connection.assert_called_once()
        mock_db_manager.create_signal_raw_table.assert_called_once()
        mock_db_manager.store_signals_raw.assert_called_once()
    
    @patch('ac_core.signal_inserter.DatabaseManager')
    def test_insert_from_dataframe_validation_error(self, mock_db_manager_class, mock_db_manager):
        """Test DataFrame insertion with validation errors."""
        mock_db_manager_class.return_value = mock_db_manager
        
        # Create invalid DataFrame (missing required column)
        invalid_df = pd.DataFrame({
            'ticker': ['AAPL'],
            'signal_name': ['SENTIMENT_YT'],
            'value': [0.75]
            # Missing 'asof_date'
        })
        
        inserter = SignalInserter()
        result = inserter.insert_from_dataframe(invalid_df, validate=True)
        
        assert result['success'] is False
        assert len(result['errors']) > 0
        assert any('Missing required columns' in error for error in result['errors'])
        
        # Database operations should not be called
        mock_db_manager.ensure_connection.assert_not_called()
        mock_db_manager.store_signals_raw.assert_not_called()
    
    @patch('ac_core.signal_inserter.DatabaseManager')
    def test_insert_from_dataframe_connection_error(self, mock_db_manager_class, mock_db_manager, sample_dataframe):
        """Test DataFrame insertion with connection error."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_db_manager.ensure_connection.return_value = False
        
        inserter = SignalInserter()
        result = inserter.insert_from_dataframe(sample_dataframe)
        
        assert result['success'] is False
        assert len(result['errors']) > 0
        assert any('Failed to connect to database' in error for error in result['errors'])
    
    @patch('ac_core.signal_inserter.DatabaseManager')
    def test_insert_from_dataframe_batch_processing(self, mock_db_manager_class, mock_db_manager):
        """Test DataFrame insertion with batch processing."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_db_manager.store_signals_raw.return_value = 1
        
        # Create DataFrame with more records than batch size
        large_df = pd.DataFrame({
            'asof_date': [date(2024, 1, 15)] * 5,
            'ticker': ['AAPL'] * 5,
            'signal_name': ['SENTIMENT_YT'] * 5,
            'value': [0.75] * 5
        })
        
        inserter = SignalInserter()
        result = inserter.insert_from_dataframe(large_df, batch_size=2)
        
        assert result['success'] is True
        assert result['records_processed'] == 5
        assert result['records_inserted'] == 5
        
        # Should be called multiple times for batching
        assert mock_db_manager.store_signals_raw.call_count == 3  # 2+2+1 records
    
    @patch('ac_core.signal_inserter.DatabaseManager')
    @patch('pandas.read_csv')
    def test_insert_from_csv_success(self, mock_read_csv, mock_db_manager_class, mock_db_manager, sample_dataframe):
        """Test successful CSV insertion."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_read_csv.return_value = sample_dataframe
        mock_db_manager.store_signals_raw.return_value = 2
        
        inserter = SignalInserter()
        result = inserter.insert_from_csv('test.csv')
        
        assert result['success'] is True
        assert result['records_processed'] == 2
        assert result['records_inserted'] == 2
        
        mock_read_csv.assert_called_once_with('test.csv', parse_dates=['asof_date'], date_parser=pd.to_datetime)
    
    @patch('ac_core.signal_inserter.DatabaseManager')
    def test_insert_from_csv_file_not_found(self, mock_db_manager_class, mock_db_manager):
        """Test CSV insertion with file not found."""
        mock_db_manager_class.return_value = mock_db_manager
        
        inserter = SignalInserter()
        result = inserter.insert_from_csv('nonexistent.csv')
        
        assert result['success'] is False
        assert len(result['errors']) > 0
        assert any('CSV file not found' in error for error in result['errors'])
    
    @patch('ac_core.signal_inserter.DatabaseManager')
    def test_insert_single_signal_success(self, mock_db_manager_class, mock_db_manager):
        """Test successful single signal insertion."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_db_manager.store_signals_raw.return_value = 1
        
        inserter = SignalInserter()
        success = inserter.insert_single_signal(
            asof_date=date(2024, 1, 15),
            ticker='AAPL',
            signal_name='SENTIMENT_YT',
            value=0.75,
            metadata={'source': 'youtube'}
        )
        
        assert success is True
        mock_db_manager.ensure_connection.assert_called_once()
        mock_db_manager.create_signal_raw_table.assert_called_once()
        mock_db_manager.store_signals_raw.assert_called_once()
    
    @patch('ac_core.signal_inserter.DatabaseManager')
    def test_insert_single_signal_connection_error(self, mock_db_manager_class, mock_db_manager):
        """Test single signal insertion with connection error."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_db_manager.ensure_connection.return_value = False
        
        inserter = SignalInserter()
        success = inserter.insert_single_signal(
            asof_date=date(2024, 1, 15),
            ticker='AAPL',
            signal_name='SENTIMENT_YT',
            value=0.75
        )
        
        assert success is False
    
    @patch('ac_core.signal_inserter.DatabaseManager')
    def test_test_connection(self, mock_db_manager_class, mock_db_manager):
        """Test connection testing."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_db_manager.test_connection.return_value = True
        
        inserter = SignalInserter()
        result = inserter.test_connection()
        
        assert result is True
        mock_db_manager.test_connection.assert_called_once()
    
    @patch('ac_core.signal_inserter.DatabaseManager')
    def test_get_existing_signals(self, mock_db_manager_class, mock_db_manager):
        """Test retrieving existing signals."""
        mock_db_manager_class.return_value = mock_db_manager
        expected_df = pd.DataFrame({'test': [1, 2, 3]})
        mock_db_manager.get_signals_raw.return_value = expected_df
        
        inserter = SignalInserter()
        result = inserter.get_existing_signals(
            tickers=['AAPL'],
            signal_names=['SENTIMENT_YT']
        )
        
        assert result.equals(expected_df)
        mock_db_manager.get_signals_raw.assert_called_once_with(
            tickers=['AAPL'],
            signal_names=['SENTIMENT_YT'],
            start_date=None,
            end_date=None
        )
    
    @patch('ac_core.signal_inserter.DatabaseManager')
    def test_close(self, mock_db_manager_class, mock_db_manager):
        """Test closing the inserter."""
        mock_db_manager_class.return_value = mock_db_manager
        
        inserter = SignalInserter()
        inserter.close()
        
        mock_db_manager.disconnect.assert_called_once()
