"""Tests for CLI module."""

import sys
from io import StringIO
from unittest.mock import patch

import pytest


class TestCLI:
    """Tests for CLI main function."""

    @patch("sqlazo.cli.run_query_command")
    def test_run_query_success(self, mock_run_query):
        from sqlazo.cli import main

        with patch.object(sys, "argv", ["sqlazo", "test.sql"]):
            main()

        mock_run_query.assert_called_once_with("test.sql", "table", False, False)

    @patch("sqlazo.cli.run_query_command")
    def test_run_query_with_options(self, mock_run_query):
        from sqlazo.cli import main

        with patch.object(sys, "argv", ["sqlazo", "query", "-f", "json-meta", "--schema", "-"]):
            main()

        mock_run_query.assert_called_once_with("-", "json-meta", False, True)

    @patch("sqlazo.cli.run_query_command", side_effect=ValueError("No query found in file."))
    def test_query_error(self, mock_run_query):
        from sqlazo.cli import main

        with patch.object(sys, "argv", ["sqlazo", "-"]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1

    @patch("sqlazo.cli.run_query_command", side_effect=FileNotFoundError())
    def test_file_not_found(self, mock_run_query):
        from sqlazo.cli import main

        with patch.object(sys, "argv", ["sqlazo", "nonexistent_file_xyz.sql"]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1

    def test_version_flag(self):
        """Test --version flag displays version and exits."""
        from sqlazo import __version__
        from sqlazo.cli import main

        with patch.object(sys, "argv", ["sqlazo", "--version"]):
            with patch("sys.stdout", StringIO()) as stdout:
                with pytest.raises(SystemExit) as exc_info:
                    main()

        assert exc_info.value.code == 0
        output = stdout.getvalue()
        assert __version__ in output
        assert "sqlazo" in output
