#!/usr/bin/python

# Copyright Brian May 2011, Refactored 2024
#
# This file contains the refactored logic for parcel processing.
# The main entry point is the `process_raw_data` function, which
# uses a configuration-based approach to handle county-specific workflows.

import os
import psycopg2
import textwrap

# ----------------------------------- HELPER FUNCTIONS -----------------------------------

def execute_sql(connection, sql_query, cursor=None):
    """Executes a SQL query and commits the transaction."""
    if not sql_query or not connection:
        return
    if cursor is None:
        cursor = connection.cursor()
    print(f"Executing SQL:\n{textwrap.shorten(sql_query, width=120, placeholder='...')}")
    try:
        cursor.execute(sql_query)
        connection.commit()
    except Exception as e:
        print(f"SQL Error: {e}")
        connection.rollback()

def run_external_command(command, description=""):
    """Runs an external shell command and prints a description."""
    if not command:
        return
    if description:
        print(description)
    print(f"Running command: {command}")
    os.system(command)

def psql_copy(table_name, file_name, psql_path, delimiter=r"E'\\t'", null_as="''", header=False):
    """Constructs and runs a psql \\copy command."""
    if not table_name or not file_name or not psql_path:
        return
    options = f"with delimiter as {delimiter} null as '{null_as}'"
    if header:
        options += " CSV HEADER"
    sql = f"\\copy {table_name} from '{file_name}' {options}"
    command = f'{psql_path} -c "{sql}"'
    run_external_command(command, description=f"\\copy: Loading '{file_name}' into '{table_name}'")

def run_sql_file(sql_file_path, psql_path):
    """Executes a .sql file using psql."""
    if not sql_file_path or not psql_path:
        return
    command = f'{psql_path} -f "{sql_file_path}"'
    run_external_command(command, description=f"Running SQL file: {sql_file_path}")


# ========================= CORE DATA PROCESSING ORCHESTRATOR =========================

def process_raw_data(config):
    """
    Orchestrates the data processing for a county based on a configuration object.
    This function handles the common workflow for all counties.
    """
    county_name = config.get('county_name', 'Unknown')
    path_processing = config.get('path_processing')
    pg_connection = config.get('pg_connection')
    pg_psql = config.get('pg_psql')

    print(f"--- Starting processing for {county_name} County ---")

    # 1. Change to the processing directory
    if path_processing and os.path.exists(path_processing):
        os.chdir(path_processing)
        print(f"Current working directory: {os.getcwd()}")
    else:
        print(f"Error: Processing path not found at {path_processing}")
        return

    # 2. Establish database connection
    try:
        connection = psycopg2.connect(pg_connection)
        cursor = connection.cursor()
        print("Database connection established.")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # 3. Run pre-processing shell commands (e.g., sed, tr, sort)
    if config.get('preprocess_commands'):
        print("\nRunning pre-processing commands...")
        for cmd_info in config['preprocess_commands']:
            run_external_command(cmd_info.get('command'), cmd_info.get('description'))

    # 4. Create raw tables from a .sql file
    if config.get('create_raw_tables_sql'):
        print("\nCreating raw tables...")
        run_sql_file(config['create_raw_tables_sql'], pg_psql)

    # 5. Run external Python scripts to process raw files
    if config.get('processing_scripts'):
        print("\nRunning processing scripts...")
        for script_info in config['processing_scripts']:
            run_external_command(script_info.get('script'), script_info.get('description'))

    # 6. Load data into tables using psql \copy
    if config.get('copy_commands'):
        print("\nLoading data into raw tables...")
        for copy_info in config['copy_commands']:
            psql_copy(
                table_name=copy_info.get('table'),
                file_name=copy_info.get('file'),
                psql_path=pg_psql,
                header=copy_info.get('header', False)
            )

    # 7. Run SQL update queries
    if config.get('sql_updates'):
        print("\nRunning SQL updates...")
        for sql_info in config['sql_updates']:
            print(sql_info.get('description', ''))
            execute_sql(connection, sql_info.get('sql', ''), cursor)

    # 8. Close the database connection
    cursor.close()
    connection.close()
    print(f"--- Finished processing for {county_name} County ---")


# ========================= COUNTY CONFIGURATIONS =========================

def get_alachua_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Alachua County."""
    
    config = {
        'county_name': 'Alachua',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'preprocess_commands': [
            {'command': f"sed -e 's:\\\\:/:g' {path_processing}/source_data/Land.txt > {path_processing}/source_data/Land2.txt"},
            {'command': f"sed -e 's:\\\\:/:g' {path_processing}/source_data/Sales.txt > {path_processing}/source_data/Sales2.txt"},
            {'command': f"sed -e 's:\\\\:/:g' {path_processing}/source_data/Legals.txt > {path_processing}/source_data/Legals2.txt"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_processing}/source_data/Legals2.txt > {path_processing}/source_data/Legals3.txt"},
            {'command': f"cat {path_processing}/source_data/Property.txt | sed '1d' | sort -u -t'\\t' -k1,1 > {path_processing}/source_data/Property2.txt"},
            {'command': f"sed -e 's:SURVEY\\\\r\\\\n:SURVEY :g' {path_processing}/source_data/Property2.txt > {path_processing}/source_data/Property3.txt"}
        ],
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/alachua/processing/database/sql_files/create_raw_tables.sql",

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/alachua/alachua-owner.py', 'description': 'RUN alachua-owner.py'},
            {'script': '/srv/tools/python/parcel_processing/alachua/alachua-history.py', 'description': 'RUN alachua-history.py'},
            {'script': '/srv/tools/python/parcel_processing/alachua/alachua-bldg-imprv.py', 'description': 'RUN alachua-bldg-imprv.py'},
            {'script': '/srv/tools/python/parcel_processing/alachua/alachua-legal.py', 'description': 'RUN alachua-legal.py'},
            {'script': '/srv/tools/python/parcel_processing/alachua/alachua-land.py', 'description': 'RUN alachua-land.py'},
            {'script': '/srv/tools/python/parcel_processing/alachua/alachua-property.py', 'description': 'RUN alachua-property.py'},
            {'script': '/srv/tools/python/parcel_processing/alachua/alachua-sale.py', 'description': 'RUN alachua-sale.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_alachua', 'file': 'parcels_new.txt'},
            {'table': 'raw_alachua_history', 'file': 'parcels_valuations.txt'},
            {'table': 'raw_alachua_owner', 'file': 'parcels_owner.txt'},
            {'table': 'raw_alachua_legal_denormal', 'file': 'parcels_legal.txt'},
            {'table': 'raw_alachua_landpub', 'file': 'parcels_landpub.txt'},
            {'table': 'raw_alachua_bldg', 'file': 'parcels_bldg.txt'},
            {'table': 'raw_alachua_sales', 'file': 'parcels_sales.txt'}
        ],

        'sql_updates': [
            {
                'description': 'Create building stats summary table for Alachua.',
                'sql': """
                    DROP TABLE IF EXISTS raw_alachua_bldg_stats;
                    SELECT
                        bldg.pin,
                        min(cast(bldg.yrblt_act as integer)) as min_yrblt_act,
                        max(cast(bldg.yrblt_eff as integer)) as max_yrblt_eff,
                        sum(cast(bldg.sqft_htd as integer)) as sum_sqft_htd,
                        sum(cast(bldg.sqft_tot as integer)) as sum_sqft_tot,
                        sum(cast(trunc(cast(bldg.num_bed as numeric)) as integer)) as sum_num_beds,
                        sum(cast(trunc(cast(bldg.num_bath as numeric)) as integer)) as sum_num_baths
                    INTO raw_alachua_bldg_stats
                    FROM raw_alachua_bldg as bldg
                    GROUP BY bldg.pin;
                """
            },
            {
                'description': 'Update parcels_template_alachua with building stats.',
                'sql': """
                    UPDATE parcels_template_alachua
                    SET
                        yrblt_act = bldg.min_yrblt_act,
                        yrblt_eff = bldg.max_yrblt_eff,
                        sqft_htd = bldg.sum_sqft_htd,
                        sqft_tot = bldg.sum_sqft_tot,
                        num_bath = bldg.sum_num_baths,
                        num_bed = bldg.sum_num_beds
                    FROM raw_alachua_bldg_stats as bldg
                    WHERE parcels_template_alachua.pin = bldg.pin;
                """
            }
        ]
    }
    return config

if __name__ == '__main__':
    # This is an example of how to run the process for a county.
    # It requires environment variables or another method to be set up
    # to provide the necessary paths and connection strings.
    
    # Example placeholder values:
    # These would need to be replaced with actual configuration values.
    pg_connection_string = os.environ.get('PG_CONNECTION_STRING') # e.g., "dbname='...' user='...' host='...' password='...'"
    pg_psql_path = os.environ.get('PG_PSQL_PATH') # e.g., "/usr/bin/psql"
    processing_path = "/srv/mapwise_dev/county/alachua/processing/database/current"
    
    if not all([pg_connection_string, pg_psql_path]):
        print("Please set PG_CONNECTION_STRING and PG_PSQL_PATH environment variables.")
    else:
        # Get the configuration for the county
        alachua_config = get_alachua_config(processing_path, pg_connection_string, pg_psql_path)
        
        # Run the processing
        process_raw_data(alachua_config) 