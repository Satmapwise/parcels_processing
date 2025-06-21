import unittest
from unittest.mock import patch, MagicMock, call
import os
import sys

# This script is designed to be run from the root of the 'parcels_processing' workspace.
# It ensures that the 'parcels_convert' module can be found.
# To run: python -m unittest parcels_convert/verify_refactor.py

# Mock psycopg2 since we don't need real database connections for testing
sys.modules['psycopg2'] = MagicMock()
sys.modules['psycopg2.extras'] = MagicMock()

# Import directly from the logic file to avoid syntax errors in the old parcels_convert.py
import parcels_convert_logic

class TestParcelProcessingRefactor(unittest.TestCase):
    """
    Tests the refactored parcel processing logic.
    This test suite uses mocking to verify the orchestration logic of `process_raw_data`
    without executing external commands or database operations.
    """

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_alachua_processing_orchestration(
        self,
        mock_connect,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """
        Verifies that process_raw_data correctly orchestrates the calls
        for Alachua County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_alachua_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)
        mock_connection.cursor.assert_called()

        expected_preprocess_calls = [
            call(command="sed -e 's:\\\\:/:g' /fake/path/processing/source_data/Land.txt > /fake/path/processing/source_data/Land2.txt", description=''),
            call(command="sed -e 's:\\\\:/:g' /fake/path/processing/source_data/res_char.txt > /fake/path/processing/source_data/res_char2.txt", description=''),
            call(command="sed -e 's:\\\\:/:g' /fake/path/processing/source_data/own.txt > /fake/path/processing/source_data/own2.txt", description='')
        ]
        mock_run_external_command.assert_has_calls(expected_preprocess_calls, any_order=True)

        mock_run_sql_file.assert_called_once_with(
            sql_file_path="/srv/parcels/db_processing_scripts/county_table_creation/create_raw_tables_alachua.sql",
            psql_path=pg_psql
        )

        expected_load_calls = [
            call(table_name='raw_own', file_name='/fake/path/processing/source_data/own2.txt', psql_path=pg_psql, header=True, delimiter="','", null_as="''"),
            call(table_name='raw_jur', file_name='/fake/path/processing/source_data/jur.txt', psql_path=pg_psql, header=True, delimiter="','", null_as="''"),
            call(table_name='raw_legal', file_name='/fake/path/processing/source_data/legal.txt', psql_path=pg_psql, header=True, delimiter="','", null_as="''"),
            call(table_name='raw_land', file_name='/fake/path/processing/source_data/Land2.txt', psql_path=pg_psql, header=True, delimiter="','", null_as="''"),
            call(table_name='raw_bldg', file_name='/fake/path/processing/source_data/res_char2.txt', psql_path=pg_psql, header=True, delimiter="','", null_as="''")
        ]
        mock_psql_copy.assert_has_calls(expected_load_calls, any_order=True)

        self.assertEqual(mock_execute_sql.call_count, 3)
        mock_execute_sql.assert_any_call(mock_connection, config['postprocess_sql'][-1], mock_connection.cursor())

        mock_connection.close.assert_called_once()


if __name__ == '__main__':
    # This allows running the tests directly
    unittest.main() 