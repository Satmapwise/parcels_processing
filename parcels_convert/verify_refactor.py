import os
import sys
from unittest.mock import MagicMock, patch

# To allow importing from the parent directory
# sys.path.insert(1, os.path.join(sys.path[0], '..'))

# We need to mock modules that might not be installed in the test environment
# and that interact with external systems.
# sys.modules['psycopg2'] = MagicMock()

# Now we can import the function to be tested
from .parcels_convert_chunk import process_raw_grizzly
from . import parcels_convert_chunk

# Define mock objects for variables that would be defined in the global scope of the main script
pg_connection = "dbname='test' user='user' host='localhost' password='password'"
pathProcessing = '/srv/mapwise_dev/county_data'
pg_psql = 'psql -d test -U user'

def process_raw_fdor(county):
    """Mock version of process_raw_fdor."""
    print("--- Mock process_raw_fdor called for {} ---".format(county))


@patch('parcels_convert.parcels_convert_chunk.process_raw_fdor', side_effect=process_raw_fdor)
@patch('parcels_convert.parcels_convert_chunk.os.system')
@patch('parcels_convert.parcels_convert_chunk.psycopg2.connect')
@patch('parcels_convert.parcels_convert_chunk.os.chdir')
def run_verification(mock_chdir, mock_connect, mock_os_system, mock_process_raw_fdor):
    """
    Runs a dry-run of the process_raw_grizzly function, capturing all
    shell commands and SQL queries that would have been executed.
    """
    # Set up the mock for the database connection and cursor
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor

    # These would be defined in the global scope of the real script
    parcels_convert_chunk.pg_connection = pg_connection
    parcels_convert_chunk.pathProcessing = pathProcessing
    parcels_convert_chunk.pg_psql = pg_psql

    counties = [
        'bradford', 'columbia', 'desoto', 'lafayette', 'okeechobee',
        'suwannee', 'union'
    ]

    for county in counties:
        print("=================================================================")
        print("            VERIFYING: {}".format(county.upper()))
        print("=================================================================")

        # Reset mocks for each run
        mock_os_system.reset_mock()
        mock_cursor.execute.reset_mock()
        mock_process_raw_fdor.reset_mock()
        mock_chdir.reset_mock()

        # Run the actual function
        process_raw_grizzly(county)

        # --- Report Captured Data ---
        print("\n--- Captured os.system calls: ---")
        if mock_os_system.call_args_list:
            for i, call in enumerate(mock_os_system.call_args_list):
                print("{}: {}".format(i + 1, call[0][0]))
        else:
            print("No os.system calls were made.")

        print("\n--- Captured SQL queries: ---")
        if mock_cursor.execute.call_args_list:
            for i, call in enumerate(mock_cursor.execute.call_args_list):
                # Clean up whitespace for readability
                query = ' '.join(str(call[0][0]).split())
                print("{}: {}".format(i + 1, query))
        else:
            print("No SQL queries were executed.")

        print("\n--- Captured process_raw_fdor calls: ---")
        if mock_process_raw_fdor.call_args_list:
            for i, call in enumerate(mock_process_raw_fdor.call_args_list):
                print("{}: Called with county '{}'".format(i + 1, call[0][0]))
        else:
            print("process_raw_fdor was not called.")

        print("\n")

if __name__ == '__main__':
    # Add the necessary global variables to the chunk module's namespace before running
    # This is a bit of a hack to make the chunk file runnable standalone for testing
    import parcels_convert.parcels_convert_chunk
    import psycopg2
    parcels_convert.parcels_convert_chunk.os = os
    parcels_convert.parcels_convert_chunk.psycopg2 = psycopg2

    run_verification() 