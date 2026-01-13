"""Tests for CLI module."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys


class TestCLI:
    """Tests for CLI main function."""
    
    @patch("sqlazo.cli.parse_file_path")
    @patch("sqlazo.cli.ConnectionConfig.from_env")
    @patch("sqlazo.cli.get_connection")
    @patch("sqlazo.cli.get_handler_for_db_type")
    @patch("sqlazo.cli.format_result")
    def test_run_query_success(self, mock_format, mock_get_handler, mock_get_conn, mock_from_env, mock_parse):
        from sqlazo.cli import main
        
        # Setup mocks
        mock_parse.return_value = Mock(
            query="SELECT 1",
            get_connection_params=Mock(return_value={"db_type": "mysql"})
        )
        mock_from_env.return_value = Mock(
            db_type="mysql",
            merge_with=Mock(return_value=Mock(
                db_type="mysql",
                host="localhost",
                port=3306,
                database="test",
                user="test",
                validate=Mock(return_value=[])
            ))
        )
        
        mock_handler = Mock()
        mock_handler.execute_query.return_value = Mock(columns=["?"], rows=[(1,)], is_select=True)
        mock_get_handler.return_value = mock_handler
        
        mock_format.return_value = "| ? |\n| 1 |"
        
        with patch.object(sys, 'argv', ['sqlazo', 'test.sql']):
            with patch('builtins.print'):
                try:
                    main()
                except SystemExit:
                    pass  # CLI may exit
        
        mock_parse.assert_called_once()
    
    @patch("sqlazo.cli.parse_file")
    @patch("sqlazo.cli.ConnectionConfig.from_env")
    def test_empty_query_error(self, mock_from_env, mock_parse):
        from sqlazo.cli import main
        
        mock_parse.return_value = Mock(
            query="   ",
            get_connection_params=Mock(return_value={})
        )
        mock_from_env.return_value = Mock(
            db_type="mysql",
            merge_with=Mock(return_value=Mock(
                db_type="mysql",
                validate=Mock(return_value=[])
            ))
        )
        
        with patch.object(sys, 'argv', ['sqlazo', '-']):
            with patch('sys.stdin.read', return_value="-- url: mysql://localhost/db\n\n"):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 1
    
    @patch("sqlazo.cli.parse_file_path")
    @patch("sqlazo.cli.ConnectionConfig.from_env")
    def test_validation_error(self, mock_from_env, mock_parse):
        from sqlazo.cli import main
        
        mock_parse.return_value = Mock(
            query="SELECT 1",
            get_connection_params=Mock(return_value={})
        )
        mock_config = Mock()
        mock_config.db_type = "mysql"
        mock_config.validate.return_value = ["Missing user"]
        mock_from_env.return_value = Mock(
            merge_with=Mock(return_value=mock_config)
        )
        
        with patch.object(sys, 'argv', ['sqlazo', 'test.sql']):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
    
    def test_file_not_found(self):
        from sqlazo.cli import main
        
        with patch.object(sys, 'argv', ['sqlazo', 'nonexistent_file_xyz.sql']):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
