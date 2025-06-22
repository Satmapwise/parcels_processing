import unittest
from unittest.mock import patch, MagicMock, call, ANY
import os
import sys
import textwrap

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

    def setUp(self):
        """Set up test environment."""
        self.path_processing = "/srv/mapwise_dev/county/test/processing/database/current"
        self.pg_connection_string = "dbname='test_db' user='test_user' host='localhost' password='test_password'"
        self.pg_psql_path = "/usr/bin/psql"
        self.mock_connection = MagicMock()

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
        
        # Check external command calls
        path_source_data = f"{path_processing}/source_data"
        expected_external_calls = [
            call(f"java -jar /srv/tools/ajack-1.0.0.jar -o -f POSTGRES_CSV -t bcpa_tax_roll -d {path_source_data}/export {path_source_data}/BCPA_TAX_ROLL.mdb", None),
            call(f"sed 's/\\t//g' {path_source_data}/export/bcpa_tax_roll.csv > {path_source_data}/export/bcpa_tax_roll2.csv", None),
            call(f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/export/bcpa_tax_roll2.csv > {path_source_data}/export/bcpa_tax_roll3.csv", None),
            call('/srv/tools/python/parcel_processing/broward/broward-convert-current.py', 'RUN broward-convert-current.py'),
            call('/srv/tools/python/parcel_processing/broward/broward-raw-bldg.py', 'RUN broward-raw-bldg.py')
        ]
        mock_run_external_command.assert_has_calls(expected_external_calls, any_order=False)


        mock_run_sql_file.assert_called_once_with(
            '/srv/mapwise_dev/county/broward/processing/database/sql_files/create_raw_tables.sql',
            pg_psql
        )
        self.assertEqual(mock_psql_copy.call_count, 2)
        
        # Check copy calls
        expected_copy_calls = [
            call(table_name='parcels_template_broward', file_name='parcels_new.txt', psql_path=pg_psql, header=False),
            call(table_name='raw_broward_bldg', file_name='parcels_bldg.txt', psql_path=pg_psql, header=False)
        ]
        mock_psql_copy.assert_has_calls(expected_copy_calls, any_order=True)

        self.assertEqual(mock_execute_sql.call_count, 2)

        # Check sql update calls
        expected_sql_calls = [
            call(mock_connection, config['sql_updates'][0]['sql'], ANY),
            call(mock_connection, config['sql_updates'][1]['sql'], ANY)
        ]
        mock_execute_sql.assert_has_calls(expected_sql_calls, any_order=False)


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

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_columbia_processing_orchestration(
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
        for Columbia County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_columbia_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)
        
        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 0)
        mock_run_sql_file.assert_called_once_with(
            '/srv/mapwise_dev/county/columbia/processing/database/sql_files/create_raw_tables.sql',
            pg_psql
        )
        self.assertEqual(mock_psql_copy.call_count, 2)
        self.assertEqual(mock_execute_sql.call_count, 6)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_desoto_processing_orchestration(
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
        for DeSoto County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_desoto_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)
        
        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 1)
        mock_run_sql_file.assert_called_once_with(
            '/srv/mapwise_dev/county/desoto/processing/database/sql_files/create_raw_tables.sql',
            pg_psql
        )
        self.assertEqual(mock_psql_copy.call_count, 3)
        self.assertEqual(mock_execute_sql.call_count, 7)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_dixie_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """Verify the orchestration logic for Dixie County."""
        
        # Setup mock return values
        mock_path_exists.return_value = True
        mock_connect.return_value = self.mock_connection

        # Get configuration
        config = parcels_convert_logic.get_dixie_config(
            self.path_processing, self.pg_connection_string, self.pg_psql_path
        )
        
        # Execute the processing
        parcels_convert_logic.process_raw_data(config)
        
        # Assertions
        mock_path_exists.assert_called_once_with(self.path_processing)
        mock_chdir.assert_called_once_with(self.path_processing)
        
        mock_run_sql_file.assert_called_once_with(
            "/srv/mapwise_dev/county/dixie/processing/database/sql_files/create_raw_tables.sql",
            self.pg_psql_path
        )

        mock_run_external_command.assert_called_once_with(
            '/srv/tools/python/parcel_processing/dixie/dixie-convert-sales-csv.py',
            'RUN dixie-convert-sales.py'
        )

        mock_psql_copy.assert_called_once_with(
            table_name='raw_dixie_sales_dwnld',
            file_name='parcels_sales.txt',
            psql_path=self.pg_psql_path,
            header=False
        )

        expected_sql_calls = [
            call(self.mock_connection, "SELECT process_raw_fdor('DIXIE');", ANY),
            call(self.mock_connection, "UPDATE parcels_template_dixie as p SET o_name1 = 'Owner Name Missing - ' || o.pin, o_name2 = null, o_address1 = null, o_address2 = null, o_address3 = null, o_city = null, o_state = null, o_zipcode = null, o_zipcode4 = null FROM raw_dixie_sales_dwnld as o WHERE p.pin = o.pin2_clean;", ANY),
            call(self.mock_connection, "UPDATE parcels_template_dixie as p SET s_city = null WHERE p.s_city = 'UNINCORPORATED';", ANY),
            call(self.mock_connection, "UPDATE parcels_template_dixie as p SET s_city = o.po_name FROM zip_codes as o WHERE p.s_city is null and o.zip = p.s_zipcode;", ANY)
        ]
        mock_execute_sql.assert_has_calls(expected_sql_calls)

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_duval_processing_orchestration(
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
        for Duval County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_duval_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)

        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 4) # 1 preprocess + 3 processing
        self.assertEqual(mock_psql_copy.call_count, 7)
        self.assertEqual(mock_execute_sql.call_count, 0)

        # Check external command calls
        expected_external_calls = [
            call('sort sales_new.txt | uniq > sales_new2.txt', None),
            call('/srv/tools/python/parcel_processing/duval/duval-sales-current.py', 'RUN duval-sales-current.py'),
            call('/srv/tools/python/parcel_processing/duval/duval-owner-current.py', 'RUN duval-owner-current.py'),
            call('/srv/tools/python/parcel_processing/duval/duval-unpack-combined-file.py', 'RUN duval-unpack-combined-file.py')
        ]
        mock_run_external_command.assert_has_calls(expected_external_calls, any_order=False)

        # Check sql file call
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # Check copy calls
        expected_copy_calls = [
            call(table_name='raw_duval_sales', file_name='sales_new2.txt', psql_path=pg_psql, header=False),
            call(table_name='raw_duval_owner', file_name='owner_new.txt', psql_path=pg_psql, header=False),
            call(table_name='raw_duval_situs', file_name='situs.txt', psql_path=pg_psql, header=False),
            call(table_name='parcels_template_duval', file_name='parcel.txt', psql_path=pg_psql, header=False),
            call(table_name='raw_duval_building1', file_name='building1.txt', psql_path=pg_psql, header=False),
            call(table_name='raw_duval_building3', file_name='building3.txt', psql_path=pg_psql, header=False),
            call(table_name='raw_duval_building4', file_name='building4.txt', psql_path=pg_psql, header=False)
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
    def test_escambia_processing_orchestration(
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
        for Escambia County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_escambia_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)

        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 2)
        self.assertEqual(mock_psql_copy.call_count, 3)
        self.assertEqual(mock_execute_sql.call_count, 4)

        # Check external command calls
        expected_external_calls = [
            call('/srv/tools/python/parcel_processing/escambia/escambia-convert-sales.py', 'RUN escambia-convert-sales.py'),
            call('/srv/tools/python/parcel_processing/escambia/escambia-convert-sales-owner.py', 'RUN escambia-convert-sales-owner.py')
        ]
        mock_run_external_command.assert_has_calls(expected_external_calls, any_order=False)

        # Check sql file call
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # Check copy calls
        expected_copy_calls = [
            call(table_name='raw_escambia_sales', file_name='parcels_sales.txt', psql_path=pg_psql, header=False),
            call(table_name='raw_escambia_owner', file_name='parcels_owner.txt', psql_path=pg_psql, header=False),
            call(table_name='raw_escambia_bldg', file_name='parcels_cert_bldg.txt', psql_path=pg_psql, header=False)
        ]
        mock_psql_copy.assert_has_calls(expected_copy_calls, any_order=True)

        # Check sql update calls
        expected_sql_calls = [
            call(mock_connection, config['sql_updates'][0]['sql'], ANY),
            call(mock_connection, config['sql_updates'][1]['sql'], ANY),
            call(mock_connection, config['sql_updates'][2]['sql'], ANY),
            call(mock_connection, config['sql_updates'][3]['sql'], ANY)
        ]
        mock_execute_sql.assert_has_calls(expected_sql_calls, any_order=False)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_flagler_processing_orchestration(
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
        for Flagler County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_flagler_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)

        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 2)
        self.assertEqual(mock_psql_copy.call_count, 2)
        self.assertEqual(mock_execute_sql.call_count, 4)

        # Check external command calls
        expected_external_calls = [
            call('/srv/tools/python/parcel_processing/flagler/flagler-convert-sales-csv.py', 'RUN flagler-convert-sales.py'),
            call('/srv/tools/python/parcel_processing/flagler/flagler-bldg.py', 'RUN flagler-bldg.py')
        ]
        mock_run_external_command.assert_has_calls(expected_external_calls, any_order=False)

        # Check sql file call
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # Check copy calls
        expected_copy_calls = [
            call(table_name='raw_flagler_sales_dwnld', file_name='parcels_sales.txt', psql_path=pg_psql, header=False),
            call(table_name='raw_flagler_bldg', file_name='parcels_bldg.txt', psql_path=pg_psql, header=False)
        ]
        mock_psql_copy.assert_has_calls(expected_copy_calls, any_order=True)

        # Check sql update calls
        expected_sql_calls = [
            call(mock_connection, config['sql_updates'][0]['sql'], ANY),
            call(mock_connection, config['sql_updates'][1]['sql'], ANY),
            call(mock_connection, config['sql_updates'][2]['sql'], ANY),
            call(mock_connection, config['sql_updates'][3]['sql'], ANY)
        ]
        mock_execute_sql.assert_has_calls(expected_sql_calls, any_order=False)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_franklin_processing_orchestration(
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
        for Franklin County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_franklin_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)

        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 1)
        self.assertEqual(mock_psql_copy.call_count, 1)
        self.assertEqual(mock_execute_sql.call_count, 2)

        # Check external command calls
        mock_run_external_command.assert_called_once_with(
            '/srv/tools/python/parcel_processing/franklin/franklin-convert-sales-csv.py', 
            'RUN franklin-convert-sales-csv.py'
        )

        # Check sql file call
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # Check copy calls
        mock_psql_copy.assert_called_once_with(
            table_name='raw_franklin_sales_dwnld', 
            file_name='parcels_sales.txt', 
            psql_path=pg_psql, 
            header=False
        )

        # Check sql update calls
        expected_sql_calls = [
            call(mock_connection, config['sql_updates'][0]['sql'], ANY),
            call(mock_connection, config['sql_updates'][1]['sql'], ANY)
        ]
        mock_execute_sql.assert_has_calls(expected_sql_calls, any_order=False)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_gadsden_processing_orchestration(
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
        for Gadsden County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_gadsden_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)

        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 1)
        self.assertEqual(mock_psql_copy.call_count, 1)
        self.assertEqual(mock_execute_sql.call_count, 2)

        # Check external command calls
        mock_run_external_command.assert_called_once_with(
            '/srv/tools/python/parcel_processing/gadsden/gadsden-convert-sales-csv.py', 
            'RUN gadsden-convert-sales-csv.py'
        )

        # Check sql file call
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # Check copy calls
        mock_psql_copy.assert_called_once_with(
            table_name='raw_gadsden_sales_dwnld', 
            file_name='parcels_sales.txt', 
            psql_path=pg_psql, 
            header=False
        )

        # Check sql update calls
        expected_sql_calls = [
            call(mock_connection, config['sql_updates'][0]['sql'], ANY),
            call(mock_connection, config['sql_updates'][1]['sql'], ANY)
        ]
        mock_execute_sql.assert_has_calls(expected_sql_calls, any_order=False)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_gilchrist_processing_orchestration(
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
        for Gilchrist County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_gilchrist_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)

        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 1)
        self.assertEqual(mock_psql_copy.call_count, 1)
        self.assertEqual(mock_execute_sql.call_count, 3)

        # Check external command calls
        mock_run_external_command.assert_called_once_with(
            '/srv/tools/python/parcel_processing/gilchrist/gilchrist-convert-sales-csv.py', 
            'RUN gilchrist-convert-sales-csv.py'
        )

        # Check sql file call
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # Check copy calls
        mock_psql_copy.assert_called_once_with(
            table_name='raw_gilchrist_sales_dwnld', 
            file_name='parcels_sales.txt', 
            psql_path=pg_psql, 
            header=False
        )

        # Check sql update calls
        expected_sql_calls = [
            call(mock_connection, config['sql_updates'][0]['sql'], ANY),
            call(mock_connection, config['sql_updates'][1]['sql'], ANY),
            call(mock_connection, config['sql_updates'][2]['sql'], ANY)
        ]
        mock_execute_sql.assert_has_calls(expected_sql_calls, any_order=False)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_glades_processing_orchestration(
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
        for Glades County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_glades_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)

        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 1)
        self.assertEqual(mock_psql_copy.call_count, 1)
        self.assertEqual(mock_execute_sql.call_count, 2)

        # Check external command calls
        mock_run_external_command.assert_called_once_with(
            '/srv/tools/python/parcel_processing/glades/glades-convert-sales-csv.py', 
            'RUN glades-convert-sales.py'
        )

        # Check sql file call
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # Check copy calls
        mock_psql_copy.assert_called_once_with(
            table_name='raw_glades_sales_dwnld', 
            file_name='parcels_sales.txt', 
            psql_path=pg_psql, 
            header=False
        )

        # Check sql update calls
        expected_sql_calls = [
            call(mock_connection, config['sql_updates'][0]['sql'], ANY),
            call(mock_connection, config['sql_updates'][1]['sql'], ANY)
        ]
        mock_execute_sql.assert_has_calls(expected_sql_calls, any_order=False)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_gulf_processing_orchestration(
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
        for Gulf County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_gulf_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)

        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 1)
        self.assertEqual(mock_psql_copy.call_count, 1)
        self.assertEqual(mock_execute_sql.call_count, 2)

        # Check external command calls
        mock_run_external_command.assert_called_once_with(
            '/srv/tools/python/parcel_processing/gulf/gulf-convert-sales-csv.py', 
            'RUN gulf-convert-sales-csv.py'
        )

        # Check sql file call
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # Check copy calls
        mock_psql_copy.assert_called_once_with(
            table_name='raw_gulf_sales_dwnld', 
            file_name='parcels_sales.txt', 
            psql_path=pg_psql, 
            header=False
        )

        # Check sql update calls
        expected_sql_calls = [
            call(mock_connection, config['sql_updates'][0]['sql'], ANY),
            call(mock_connection, config['sql_updates'][1]['sql'], ANY)
        ]
        mock_execute_sql.assert_has_calls(expected_sql_calls, any_order=False)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_hamilton_processing_orchestration(
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
        for Hamilton County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_hamilton_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)

        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 1)
        self.assertEqual(mock_psql_copy.call_count, 1)
        self.assertEqual(mock_execute_sql.call_count, 2)

        # Check external command calls
        mock_run_external_command.assert_called_once_with(
            '/srv/tools/python/parcel_processing/hamilton/hamilton-convert-sales-csv.py', 
            'RUN hamilton-convert-sales-csv.py'
        )

        # Check sql file call
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # Check copy calls
        mock_psql_copy.assert_called_once_with(
            table_name='raw_hamilton_sales_dwnld', 
            file_name='parcels_sales.txt', 
            psql_path=pg_psql, 
            header=False
        )

        # Check sql update calls
        expected_sql_calls = [
            call(mock_connection, config['sql_updates'][0]['sql'], ANY),
            call(mock_connection, config['sql_updates'][1]['sql'], ANY)
        ]
        mock_execute_sql.assert_has_calls(expected_sql_calls, any_order=False)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_hendry_processing_orchestration(
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
        for Hendry County based on its configuration.
        """
        # 1. Setup Mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        # 2. Define test data and get config
        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"
        
        config = parcels_convert_logic.get_hendry_config(path_processing, pg_connection, pg_psql)

        # 3. Run the process
        parcels_convert_logic.process_raw_data(config)

        # 4. Assertions
        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)

        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 0) # No processing scripts or preprocess
        self.assertEqual(mock_psql_copy.call_count, 1)
        self.assertEqual(mock_execute_sql.call_count, 7)

        # Check sql file call
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # Check copy calls
        mock_psql_copy.assert_called_once_with(
            table_name='raw_hendry_sales_dwnld', 
            file_name='source_data/sales_current.csv', 
            psql_path=pg_psql, 
            header=True,
            null_as=''
        )

        # Check sql update calls
        expected_sql_calls = [
            call(mock_connection, config['sql_updates'][0]['sql'], ANY),
            call(mock_connection, config['sql_updates'][1]['sql'], ANY),
            call(mock_connection, config['sql_updates'][2]['sql'], ANY),
            call(mock_connection, config['sql_updates'][3]['sql'], ANY),
            call(mock_connection, config['sql_updates'][4]['sql'], ANY),
            call(mock_connection, config['sql_updates'][5]['sql'], ANY),
            call(mock_connection, config['sql_updates'][6]['sql'], ANY)
        ]
        mock_execute_sql.assert_has_calls(expected_sql_calls, any_order=False)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_hernando_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """Verify Hernando orchestration."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        path_processing="/fake/path/processing"
        pg_connection="fake_conn"
        pg_psql="/usr/bin/psql"
        config=parcels_convert_logic.get_hernando_config(path_processing,pg_connection,pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'],pg_psql)
        # Expect 3 external commands (2 preprocess + 1 script)
        self.assertEqual(mock_run_external_command.call_count, 3)

        # Hernando executes raw table creation script once
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # Verify processing script execution
        self.assertIn(
            call('/srv/tools/python/parcel_processing/hernando/hernando-erafile-current.py','RUN hernando-erafile-current.py'),
            mock_run_external_command.call_args_list
        )

        mock_psql_copy.assert_called_once_with(table_name='parcels_template_hernando',file_name='parcels_new.txt',psql_path=pg_psql,header=False)

        # No SQL updates expected
        mock_execute_sql.assert_not_called()

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_highlands_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """Verify Highlands orchestration."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        path_processing="/fake/path/processing"
        pg_connection="fake_conn"
        pg_psql="/usr/bin/psql"
        config=parcels_convert_logic.get_highlands_config(path_processing,pg_connection,pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'],pg_psql)
        # Expect 1 preprocess + 1 processing script
        self.assertEqual(mock_run_external_command.call_count, 2)

        self.assertIn(
            call('/srv/tools/python/parcel_processing/highlands/highlands-convert-generic.py', 'RUN highlands-convert-generic.py'),
            mock_run_external_command.call_args_list
        )

        self.assertEqual(mock_psql_copy.call_count, 2)
        self.assertEqual(mock_execute_sql.call_count, 2)

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_hillsborough_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """Verify Hillsborough orchestration."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_hillsborough_config(path_processing, pg_connection, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)

        # Hillsborough: 5 preprocess + 2 processing scripts = 7 external commands
        self.assertEqual(mock_run_external_command.call_count, 7)

        # SQL file executed once
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # Four copy commands
        self.assertEqual(mock_psql_copy.call_count, 4)

        # No SQL updates provided
        mock_execute_sql.assert_not_called()

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_holmes_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """Verify Holmes orchestration."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_holmes_config(path_processing, pg_connection, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)

        # 1 processing script
        mock_run_external_command.assert_called_once_with('/srv/tools/python/parcel_processing/holmes/holmes-convert-sales-csv.py', 'RUN holmes-convert-sales-csv.py')

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        mock_psql_copy.assert_called_once_with(
            table_name='raw_holmes_sales_dwnld', 
            file_name='parcels_sales.txt', 
            psql_path=pg_psql, 
            header=False
        )

        self.assertEqual(mock_execute_sql.call_count, 2)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_indian_river_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """Verify Indian River orchestration."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_indian_river_config(path_processing, pg_connection, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)

        # 2 preprocess + 6 processing scripts = 8 external commands
        self.assertEqual(mock_run_external_command.call_count, 8)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        self.assertEqual(mock_psql_copy.call_count, 5)

        self.assertEqual(mock_execute_sql.call_count, 4)

        mock_connection.close.assert_called_once()

    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_jackson_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """Verify Jackson orchestration."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        path_processing = "/fake/path/processing"
        pg_connection = "fake_connection_string"
        pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_jackson_config(path_processing, pg_connection, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        mock_connect.assert_called_once_with(pg_connection)
        
        # Check call counts
        self.assertEqual(mock_run_external_command.call_count, 1)
        self.assertEqual(mock_psql_copy.call_count, 1)
        self.assertEqual(mock_execute_sql.call_count, 2)

        # Check external command calls
        mock_run_external_command.assert_called_once_with(
            '/srv/tools/python/parcel_processing/jackson/jackson-convert-sales-csv.py', 
            'RUN jackson-convert-sales-csv.py'
        )

        # Check sql file call
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # Check copy calls
        mock_psql_copy.assert_called_once_with(
            table_name='raw_jackson_sales_dwnld', 
            file_name='parcels_sales.txt', 
            psql_path=pg_psql, 
            header=False
        )

        # Check sql update calls
        expected_sql_calls = [
            call(mock_connection, config['sql_updates'][0]['sql'], ANY),
            call(mock_connection, config['sql_updates'][1]['sql'], ANY)
        ]
        mock_execute_sql.assert_has_calls(expected_sql_calls, any_order=False)

        mock_connection.close.assert_called_once()

    # ------------------------------------------------------------------
    # LEE COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_lee_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        path_processing="/fake/path/processing"
        pg_connection="fake_conn"
        pg_psql="/usr/bin/psql"

        config=parcels_convert_logic.get_lee_config(path_processing,pg_connection,pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 4 preprocess + 1 script = 5 external commands
        self.assertEqual(mock_run_external_command.call_count,5)

        # No SQL file executed
        mock_run_sql_file.assert_not_called()

        mock_psql_copy.assert_called_once_with(table_name='parcels_template_lee',file_name='parcels_new.txt',psql_path=pg_psql,header=False)

        mock_execute_sql.assert_not_called()

        mock_connection.close.assert_called_once()

    # ------------------------------------------------------------------
    # LEON COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_leon_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        mock_connection=MagicMock()
        mock_connect.return_value=mock_connection
        mock_path_exists.return_value=True

        path_processing="/fake/path/processing"
        pg_connection="fake_conn"
        pg_psql="/usr/bin/psql"

        config=parcels_convert_logic.get_leon_config(path_processing,pg_connection,pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'],pg_psql)

        # 4 preprocess + 3 scripts = 7 external commands
        self.assertEqual(mock_run_external_command.call_count,7)

        # 3 copy commands
        self.assertEqual(mock_psql_copy.call_count,3)

        # No SQL updates present
        mock_execute_sql.assert_not_called()

        mock_connection.close.assert_called_once()

    # ------------------------------------------------------------------
    # LEVY COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_levy_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        mock_connection=MagicMock()
        mock_connect.return_value=mock_connection
        mock_path_exists.return_value=True

        path_processing="/fake/path/processing"
        pg_connection="fake_conn"
        pg_psql="/usr/bin/psql"

        config=parcels_convert_logic.get_levy_config(path_processing,pg_connection,pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'],pg_psql)

        # Only one processing script
        mock_run_external_command.assert_called_once_with('/srv/tools/python/parcel_processing/levy/levy-convert-sales-csv.py','RUN levy-convert-sales-csv.py')

        mock_psql_copy.assert_called_once_with(table_name='raw_levy_sales_dwnld',file_name='parcels_sales.txt',psql_path=pg_psql,header=False)

        self.assertEqual(mock_execute_sql.call_count,2)

        mock_connection.close.assert_called_once()

    # ------------------------------------------------------------------
    # LIBERTY COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_liberty_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """Verify Liberty orchestration."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        path_processing = "/fake/path/processing"
        pg_connection = "fake_conn"
        pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_liberty_config(path_processing, pg_connection, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # One processing script
        mock_run_external_command.assert_called_once_with('/srv/tools/python/parcel_processing/liberty/liberty-convert-sales-csv.py', 'RUN liberty-convert-sales-csv.py')

        # SQL file executed once
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # One copy command
        mock_psql_copy.assert_called_once_with(table_name='raw_liberty_sales_dwnld', file_name='parcels_sales.txt', psql_path=pg_psql, header=False)

        # Two SQL updates
        self.assertEqual(mock_execute_sql.call_count, 2)

        mock_connection.close.assert_called_once()

    # ------------------------------------------------------------------
    # MADISON COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_madison_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """Verify Madison orchestration."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        path_processing = "/fake/path/processing"
        pg_connection = "fake_conn"
        pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_madison_config(path_processing, pg_connection, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # One processing script
        mock_run_external_command.assert_called_once_with('/srv/tools/python/parcel_processing/madison/madison-convert-sales-csv.py', 'RUN madison-convert-sales-csv.py')

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        mock_psql_copy.assert_called_once_with(table_name='raw_madison_sales_dwnld', file_name='parcels_sales.txt', psql_path=pg_psql, header=False)

        self.assertEqual(mock_execute_sql.call_count, 2)

        mock_connection.close.assert_called_once()

    # ------------------------------------------------------------------
    # MANATEE COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_manatee_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        """Verify Manatee orchestration."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        path_processing = "/fake/path/processing"
        pg_connection = "fake_conn"
        pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_manatee_config(path_processing, pg_connection, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 11 processing scripts
        self.assertEqual(mock_run_external_command.call_count, 11)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # 12 copy commands
        self.assertEqual(mock_psql_copy.call_count, 12)

        # 3 SQL updates
        self.assertEqual(mock_execute_sql.call_count, 3)

        # Verify first script present
        self.assertIn(
            call('/srv/tools/python/parcel_processing/manatee/manatee-parcels.py', 'RUN manatee-parcels.py'),
            mock_run_external_command.call_args_list
        )

        mock_connection.close.assert_called_once()

    # ------------------------------------------------------------------
    # MARION COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_marion_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        path_processing = "/fake/path/processing"
        pg_connection = "fake_conn"
        pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_marion_config(path_processing, pg_connection, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 3 processing scripts
        self.assertEqual(mock_run_external_command.call_count, 3)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # 3 copy commands
        self.assertEqual(mock_psql_copy.call_count, 3)

        # 3 SQL updates
        self.assertEqual(mock_execute_sql.call_count, 3)

        mock_connection.close.assert_called_once()

    # ------------------------------------------------------------------
    # MARTIN COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_martin_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        path_processing = "/fake/path/processing"
        pg_connection = "fake_conn"
        pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_martin_config(path_processing, pg_connection, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 4 preprocess + 5 scripts = 9 external commands
        self.assertEqual(mock_run_external_command.call_count, 9)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # 5 copy commands
        self.assertEqual(mock_psql_copy.call_count, 5)

        # 1 SQL update
        self.assertEqual(mock_execute_sql.call_count, 1)

        mock_connection.close.assert_called_once()

    # ------------------------------------------------------------------
    # MIAMI-DADE COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_miami_dade_processing_orchestration(
        self,
        mock_connect,
        mock_path_exists,
        mock_chdir,
        mock_run_external_command,
        mock_run_sql_file,
        mock_psql_copy,
        mock_execute_sql
    ):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_path_exists.return_value = True

        path_processing = "/fake/path/processing"
        pg_connection = "fake_conn"
        pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_miami_dade_config(path_processing, pg_connection, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 3 preprocess + 3 scripts = 6 external commands
        self.assertEqual(mock_run_external_command.call_count, 6)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # 3 copy commands
        self.assertEqual(mock_psql_copy.call_count, 3)

        # No SQL updates
        mock_execute_sql.assert_not_called()

        mock_connection.close.assert_called_once()

    # ------------------------------------------------------------------
    # MONROE COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_monroe_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock()
        mock_path_exists.return_value = True
        path_processing="/fake/path/processing"
        pg_conn="fake"
        pg_psql="/usr/bin/psql"
        config=parcels_convert_logic.get_monroe_config(path_processing,pg_conn,pg_psql)
        parcels_convert_logic.process_raw_data(config)
        mock_chdir.assert_called_once_with(path_processing)
        self.assertEqual(mock_run_external_command.call_count,2)  # 1 preprocess + 1 script
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'],pg_psql)
        mock_psql_copy.assert_called_once_with(table_name='parcels_template2_monroe',file_name='parcels_new.txt',psql_path=pg_psql,header=False)
        mock_execute_sql.assert_not_called()

    # ------------------------------------------------------------------
    # NASSAU COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_nassau_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value=MagicMock(); mock_path_exists.return_value=True
        path_processing="/fake/path/processing"; pg_conn="fake"; pg_psql="/usr/bin/psql"
        config=parcels_convert_logic.get_nassau_config(path_processing,pg_conn,pg_psql)
        parcels_convert_logic.process_raw_data(config)
        mock_chdir.assert_called_once_with(path_processing)
        mock_run_external_command.assert_called_once_with('/srv/tools/python/parcel_processing/nassau/nassau-convert-sales-csv.py','RUN nassau-convert-sales-csv.py')
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'],pg_psql)
        mock_psql_copy.assert_called_once_with(table_name='raw_nassau_sales',file_name='parcels_sales.txt',psql_path=pg_psql,header=False)
        self.assertEqual(mock_execute_sql.call_count,2)

    # ------------------------------------------------------------------
    # OKALOOSA COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_okaloosa_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value=MagicMock(); mock_path_exists.return_value=True
        path_processing="/fake/path/processing"; pg_conn="fake"; pg_psql="/usr/bin/psql"
        config=parcels_convert_logic.get_okaloosa_config(path_processing,pg_conn,pg_psql)
        parcels_convert_logic.process_raw_data(config)
        mock_chdir.assert_called_once_with(path_processing)
        self.assertEqual(mock_run_external_command.call_count,5)  # 3 preprocess + 2 scripts
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'],pg_psql)
        self.assertEqual(mock_psql_copy.call_count,2)
        self.assertEqual(mock_execute_sql.call_count,1)

    # ------------------------------------------------------------------
    # OKEECHOBEE COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_okeechobee_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_okeechobee_config(path_processing, pg_conn, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # No external commands (no preprocess or scripts)
        self.assertEqual(mock_run_external_command.call_count, 0)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # Two COPY operations (sales export + owner mailing)
        self.assertEqual(mock_psql_copy.call_count, 2)

        # Six SQL updates bundled from legacy logic
        self.assertEqual(mock_execute_sql.call_count, 6)

    # ------------------------------------------------------------------
    # ORANGE COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_orange_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_orange_config(path_processing, pg_conn, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 1 preprocess + 1 script = 2 external commands
        self.assertEqual(mock_run_external_command.call_count, 2)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        mock_psql_copy.assert_called_once_with(
            table_name='parcels_template_orange', file_name='parcels_new.txt', psql_path=pg_psql, header=False
        )

        self.assertEqual(mock_execute_sql.call_count, 4)

    # ------------------------------------------------------------------
    # OSCEOLA COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_osceola_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_osceola_config(path_processing, pg_conn, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 3 preprocess + 5 scripts = 8 external commands
        self.assertEqual(mock_run_external_command.call_count, 8)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        self.assertEqual(mock_psql_copy.call_count, 6)

        # No SQL updates in simplified config
        mock_execute_sql.assert_not_called()

    # ------------------------------------------------------------------
    # PALM BEACH COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_palm_beach_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_palm_beach_config(path_processing, pg_conn, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 5 preprocess + 9 scripts = 14 external commands
        self.assertEqual(mock_run_external_command.call_count, 14)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        self.assertEqual(mock_psql_copy.call_count, 9)

        self.assertEqual(mock_execute_sql.call_count, 2)

    # ------------------------------------------------------------------
    # PASCO COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_pasco_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_pasco_config(path_processing, pg_conn, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 6 preprocess + 8 scripts = 14 external commands
        self.assertEqual(mock_run_external_command.call_count, 14)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        self.assertEqual(mock_psql_copy.call_count, 3)

        # No ad-hoc SQL updates in simplified config
        mock_execute_sql.assert_not_called()

    # ------------------------------------------------------------------
    # PINELLAS COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_pinellas_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_pinellas_config(path_processing, pg_conn, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 4 preprocess + 4 scripts = 8 external commands
        self.assertEqual(mock_run_external_command.call_count, 8)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        self.assertEqual(mock_psql_copy.call_count, 3)

        self.assertEqual(mock_execute_sql.call_count, 2)

    # ------------------------------------------------------------------
    # POLK COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_polk_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_polk_config(path_processing, pg_conn, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 2 preprocess + 8 scripts = 10 external commands
        self.assertEqual(mock_run_external_command.call_count, 10)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # 8 COPY operations
        self.assertEqual(mock_psql_copy.call_count, 8)

        # 4 SQL updates
        self.assertEqual(mock_execute_sql.call_count, 4)

    # ------------------------------------------------------------------
    # PUTNAM COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_putnam_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_putnam_config(path_processing, pg_conn, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # No preprocess or scripts
        self.assertEqual(mock_run_external_command.call_count, 0)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # 2 COPY operations
        self.assertEqual(mock_psql_copy.call_count, 2)

        # Single SQL update
        self.assertEqual(mock_execute_sql.call_count, 1)

    # ------------------------------------------------------------------
    # SANTA ROSA COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_santa_rosa_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_santa_rosa_config(path_processing, pg_conn, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 1 preprocess + 9 scripts = 10 external commands
        self.assertEqual(mock_run_external_command.call_count, 10)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # 9 COPY operations
        self.assertEqual(mock_psql_copy.call_count, 9)

        # 6 SQL updates
        self.assertEqual(mock_execute_sql.call_count, 6)

    # ------------------------------------------------------------------
    # SARASOTA COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_sarasota_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_sarasota_config(path_processing, pg_conn, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 1 preprocess + 6 scripts = 7 external commands
        self.assertEqual(mock_run_external_command.call_count, 7)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # 8 COPY operations
        self.assertEqual(mock_psql_copy.call_count, 8)

        # 3 SQL updates
        self.assertEqual(mock_execute_sql.call_count, 3)

    # ------------------------------------------------------------------
    # SEMINOLE COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_seminole_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_seminole_config(path_processing, pg_conn, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 3 preprocess + 5 scripts = 8 external commands
        self.assertEqual(mock_run_external_command.call_count, 8)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # 5 COPY operations
        self.assertEqual(mock_psql_copy.call_count, 5)

        # 3 SQL updates
        self.assertEqual(mock_execute_sql.call_count, 3)

    # ------------------------------------------------------------------
    # ST JOHNS COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_st_johns_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_st_johns_config(path_processing, pg_conn, pg_psql)

        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)

        # 4 preprocess + 4 scripts = 8 external commands
        self.assertEqual(mock_run_external_command.call_count, 8)

        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)

        # 4 COPY operations
        self.assertEqual(mock_psql_copy.call_count, 4)

        # 3 SQL updates
        self.assertEqual(mock_execute_sql.call_count, 3)

    # ------------------------------------------------------------------
    # ST LUCIE COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_st_lucie_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_st_lucie_config(path_processing, pg_conn, pg_psql)
        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        self.assertEqual(mock_run_external_command.call_count, 11)  # 2 preprocess + 9 scripts
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)
        self.assertEqual(mock_psql_copy.call_count, 9)
        self.assertEqual(mock_execute_sql.call_count, 4)

    # ------------------------------------------------------------------
    # SUMTER COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_sumter_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_sumter_config(path_processing, pg_conn, pg_psql)
        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        self.assertEqual(mock_run_external_command.call_count, 2)  # 0 preprocess + 2 scripts
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)
        self.assertEqual(mock_psql_copy.call_count, 3)
        self.assertEqual(mock_execute_sql.call_count, 4)

    # ------------------------------------------------------------------
    # SUWANNEE COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_suwannee_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_suwannee_config(path_processing, pg_conn, pg_psql)
        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        self.assertEqual(mock_run_external_command.call_count, 0)  # no preprocess/scripts
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)
        self.assertEqual(mock_psql_copy.call_count, 2)
        self.assertEqual(mock_execute_sql.call_count, 1)

    # ------------------------------------------------------------------
    # TAYLOR COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_taylor_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_taylor_config(path_processing, pg_conn, pg_psql)
        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        # 1 script = 1 external command
        self.assertEqual(mock_run_external_command.call_count, 1)
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)
        self.assertEqual(mock_psql_copy.call_count, 1)
        self.assertEqual(mock_execute_sql.call_count, 1)

    # ------------------------------------------------------------------
    # UNION COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_union_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_union_config(path_processing, pg_conn, pg_psql)
        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        # No preprocess/scripts
        self.assertEqual(mock_run_external_command.call_count, 0)
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)
        self.assertEqual(mock_psql_copy.call_count, 2)
        self.assertEqual(mock_execute_sql.call_count, 5)

    # ------------------------------------------------------------------
    # VOLUSIA COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_volusia_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_volusia_config(path_processing, pg_conn, pg_psql)
        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        # 6 preprocess + 8 scripts = 14 external commands
        self.assertEqual(mock_run_external_command.call_count, 14)
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)
        self.assertEqual(mock_psql_copy.call_count, 8)
        self.assertEqual(mock_execute_sql.call_count, 2)

    # ------------------------------------------------------------------
    # WAKULLA COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_wakulla_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_wakulla_config(path_processing, pg_conn, pg_psql)
        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        self.assertEqual(mock_run_external_command.call_count, 1)  # 0 preprocess + 1 script
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)
        self.assertEqual(mock_psql_copy.call_count, 1)
        self.assertEqual(mock_execute_sql.call_count, 2)

    # ------------------------------------------------------------------
    # WALTON COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_walton_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_walton_config(path_processing, pg_conn, pg_psql)
        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        # 2 preprocess + 2 scripts = 4 external commands
        self.assertEqual(mock_run_external_command.call_count, 4)
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)
        self.assertEqual(mock_psql_copy.call_count, 3)
        self.assertEqual(mock_execute_sql.call_count, 4)

    # ------------------------------------------------------------------
    # WASHINGTON COUNTY
    # ------------------------------------------------------------------
    @patch('parcels_convert_logic.execute_sql')
    @patch('parcels_convert_logic.psql_copy')
    @patch('parcels_convert_logic.run_sql_file')
    @patch('parcels_convert_logic.run_external_command')
    @patch('parcels_convert_logic.os.chdir')
    @patch('parcels_convert_logic.os.path.exists')
    @patch('parcels_convert_logic.psycopg2.connect')
    def test_washington_processing_orchestration(
        self, mock_connect, mock_path_exists, mock_chdir,
        mock_run_external_command, mock_run_sql_file, mock_psql_copy, mock_execute_sql):

        mock_connect.return_value = MagicMock(); mock_path_exists.return_value = True
        path_processing = "/fake/path/processing"; pg_conn = "fake"; pg_psql = "/usr/bin/psql"

        config = parcels_convert_logic.get_washington_config(path_processing, pg_conn, pg_psql)
        parcels_convert_logic.process_raw_data(config)

        mock_chdir.assert_called_once_with(path_processing)
        self.assertEqual(mock_run_external_command.call_count, 1)  # 0 preprocess + 1 script
        mock_run_sql_file.assert_called_once_with(config['create_raw_tables_sql'], pg_psql)
        self.assertEqual(mock_psql_copy.call_count, 1)
        self.assertEqual(mock_execute_sql.call_count, 2)

if __name__ == '__main__':
    # This allows running the tests directly
    unittest.main()