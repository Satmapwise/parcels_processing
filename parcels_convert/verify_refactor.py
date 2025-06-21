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
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_alachua_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
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
        
        # Make the fake processing path appear to exist
        mock_path_exists.return_value = True

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

        # Check that the correct number of preprocess commands were called
        self.assertEqual(mock_run_external_command.call_count, 13)  # 6 preprocess + 7 processing scripts

        # Check that the SQL file was called with correct path
        mock_run_sql_file.assert_called_once_with(
            '/srv/mapwise_dev/county/alachua/processing/database/sql_files/create_raw_tables.sql',
            pg_psql
        )

        # Check that the correct number of copy commands were called
        self.assertEqual(mock_psql_copy.call_count, 7)

        # Check that the correct number of SQL updates were called
        self.assertEqual(mock_execute_sql.call_count, 2)

        # Verify specific calls were made (checking a few key ones)
        land_cmd = "sed -e 's:\\\\:/:g' " + path_processing + "/source_data/Land.txt > " + path_processing + "/source_data/Land2.txt"
        sales_cmd = "sed -e 's:\\\\:/:g' " + path_processing + "/source_data/Sales.txt > " + path_processing + "/source_data/Sales2.txt"
        legals_cmd = "sed -e 's:\\\\:/:g' " + path_processing + "/source_data/Legals.txt > " + path_processing + "/source_data/Legals2.txt"
        
        expected_preprocess_calls = [
            call(land_cmd, None),
            call(sales_cmd, None),
            call(legals_cmd, None)
        ]
        for expected_call in expected_preprocess_calls:
            self.assertIn(expected_call, mock_run_external_command.call_args_list)

        # Check that processing scripts were called
        expected_script_calls = [
            call('/srv/tools/python/parcel_processing/alachua/alachua-owner.py', 'RUN alachua-owner.py'),
            call('/srv/tools/python/parcel_processing/alachua/alachua-history.py', 'RUN alachua-history.py'),
            call('/srv/tools/python/parcel_processing/alachua/alachua-land.py', 'RUN alachua-land.py')
        ]
        for expected_call in expected_script_calls:
            self.assertIn(expected_call, mock_run_external_command.call_args_list)

        # Check that copy commands were called with correct parameters
        expected_copy_calls = [
            call(table_name='parcels_template_alachua', file_name='parcels_new.txt', psql_path=pg_psql, header=False),
            call(table_name='raw_alachua_owner', file_name='parcels_owner.txt', psql_path=pg_psql, header=False),
            call(table_name='raw_alachua_bldg', file_name='parcels_bldg.txt', psql_path=pg_psql, header=False)
        ]
        for expected_call in expected_copy_calls:
            self.assertIn(expected_call, mock_psql_copy.call_args_list)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_baker_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """
        Verifies that process_raw_data correctly orchestrates the calls
        for Baker County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Make the fake processing path appear to exist
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_baker_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)
        mock_connection.cursor.assert_called()

        # Baker has no preprocess commands or processing scripts
        self.assertEqual(mock_run_external_command.call_count, 0)

        # Check that the SQL file was called with correct path
        mock_run_sql_file.assert_called_once_with(
            '/srv/mapwise_dev/county/baker/processing/database/sql_files/create_raw_tables.sql',
            pg_psql
        )

        # Check that the correct number of copy commands were called
        self.assertEqual(mock_psql_copy.call_count, 1)

        # Check that the correct number of SQL updates were called
        self.assertEqual(mock_execute_sql.call_count, 3)

        # Verify specific copy command was called
        expected_copy_call = call(table_name='raw_baker_sales_export', file_name='source_data/sales_dnld_2014-01-01_current.txt', psql_path=pg_psql, header=False)
        self.assertIn(expected_copy_call, mock_psql_copy.call_args_list)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_bay_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """
        Verifies that process_raw_data correctly orchestrates the calls
        for Bay County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_bay_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)
        
        # Check calls
        self.assertEqual(mock_run_external_command.call_count, 1) # 1 processing script
        mock_run_sql_file.assert_called_once_with(
            '/srv/mapwise_dev/county/bay/processing/database/sql_files/create_raw_tables.sql',
            pg_psql
        )
        self.assertEqual(mock_psql_copy.call_count, 1)
        self.assertEqual(mock_execute_sql.call_count, 2)

        # Verify specific calls
        mock_run_external_command.assert_called_once_with(
            '/srv/tools/python/parcel_processing/bay/bay-convert-sales-csv.py', 'RUN bay-convert-sales.py'
        )
        mock_psql_copy.assert_called_once_with(
            table_name='raw_bay_sales_dwnld', file_name='parcels_sales.txt', psql_path=pg_psql, header=False
        )
        
        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_bradford_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """
        Verifies that process_raw_data correctly orchestrates the calls
        for Bradford County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_bradford_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)
        
        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 0)
        mock_run_sql_file.assert_called_once_with(
            '/srv/mapwise_dev/county/bradford/processing/database/sql_files/create_raw_tables.sql',
            pg_psql
        )
        self.assertEqual(mock_psql_copy.call_count, 2)
        self.assertEqual(mock_execute_sql.call_count, 6)

        # Verify specific copy calls
        expected_copy_calls = [
            call(table_name='raw_bradford_sales_export', file_name='source_data/sales_dnld_2014-01-01_current.txt', psql_path=pg_psql, header=False),
            call(table_name='raw_bradford_sales_owner_export', file_name='source_data/sales_owner_mailing_dnld_2014-01-01_current.txt', psql_path=pg_psql, header=False)
        ]
        mock_psql_copy.assert_has_calls(expected_copy_calls, any_order=True)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_brevard_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """
        Verifies that process_raw_data correctly orchestrates the calls
        for Brevard County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_brevard_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)
        
        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 8) # 5 preprocess + 3 processing scripts
        mock_run_sql_file.assert_called_once_with(
            '/srv/mapwise_dev/county/brevard/processing/database/sql_files/create_raw_tables.sql',
            pg_psql
        )
        self.assertEqual(mock_psql_copy.call_count, 3)
        self.assertEqual(mock_execute_sql.call_count, 2)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_broward_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """
        Verifies that process_raw_data correctly orchestrates the calls
        for Broward County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_broward_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)
        
        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 5) # 3 preprocess + 2 processing scripts
        mock_run_sql_file.assert_called_once_with(
            '/srv/mapwise_dev/county/broward/processing/database/sql_files/create_raw_tables.sql',
            pg_psql
        )
        self.assertEqual(mock_psql_copy.call_count, 2)
        self.assertEqual(mock_execute_sql.call_count, 2)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_calhoun_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """
        Verifies that process_raw_data correctly orchestrates the calls
        for Calhoun County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_calhoun_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)
        
        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 1)
        mock_run_sql_file.assert_called_once_with(
            '/srv/mapwise_dev/county/calhoun/processing/database/sql_files/create_raw_tables.sql',
            pg_psql
        )
        self.assertEqual(mock_psql_copy.call_count, 1)
        self.assertEqual(mock_execute_sql.call_count, 2)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.os.path.dirname')
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_charlotte_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql,
        mock_dirname
    ):
        """
        Verifies that process_raw_data correctly orchestrates the calls
        for Charlotte County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True
        mock_dirname.return_value = "/fake/path" # Mock dirname to handle path_top_dir

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_charlotte_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)
        
        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 4) # 1 preprocess + 3 processing
        mock_run_sql_file.assert_called_once_with(
            '/srv/mapwise_dev/county/charlotte/processing/database/sql_files/create_raw_tables.sql',
            pg_psql
        )
        self.assertEqual(mock_psql_copy.call_count, 4)
        self.assertEqual(mock_execute_sql.call_count, 2)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_citrus_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """
        Verifies that process_raw_data correctly orchestrates the calls
        for Citrus County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_citrus_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)
        
        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 9) # 4 preprocess + 5 processing
        mock_run_sql_file.assert_called_once_with(
            '/srv/mapwise_dev/county/citrus/processing/database/sql_files/create_raw_tables.sql',
            pg_psql
        )
        self.assertEqual(mock_psql_copy.call_count, 2)
        self.assertEqual(mock_execute_sql.call_count, 0) # No SQL updates in this config

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_clay_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """
        Verifies that process_raw_data correctly orchestrates the calls
        for Clay County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_clay_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)
        
        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 4)
        mock_run_sql_file.assert_called_once_with(
            '/srv/mapwise_dev/county/clay/processing/database/sql_files/create_raw_tables.sql',
            pg_psql
        )
        self.assertEqual(mock_psql_copy.call_count, 4)
        self.assertEqual(mock_execute_sql.call_count, 5)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_collier_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """
        Verifies that process_raw_data correctly orchestrates the calls
        for Collier County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_collier_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)
        
        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 6) # 1 preprocess + 5 processing
        mock_run_sql_file.assert_called_once_with(
            '/srv/mapwise_dev/county/collier/processing/database/sql_files/create_raw_tables.sql',
            pg_psql
        )
        self.assertEqual(mock_psql_copy.call_count, 8)
        self.assertEqual(mock_execute_sql.call_count, 5)

        mock_connection.close.assert_called_once()

if __name__ == '__main__':
    # This allows running the tests directly
    unittest.main()