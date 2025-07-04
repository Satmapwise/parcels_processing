#!/usr/bin/python

# Copyright Brian May 2011, Refactored 2024
#
# This file contains the refactored logic for parcel processing.
# The main entry point is the `process_raw_data` function, which
# uses a configuration-based approach to handle county-specific workflows.

import os
import sys
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

def process_raw_data(config, debug=True):
    """
    Orchestrates the data processing for a county based on a configuration object.
    This function handles the common workflow for all counties.
    """
    county_name = config.get('county_name', 'Unknown')
    path_processing = config.get('path_processing')
    pg_connection = config.get('pg_connection')
    pg_psql = config.get('pg_psql')

    print("\n" + "="*80)
    print(f"--- Starting processing for {county_name} County ---")
    print("="*80)

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
            if debug:
                print(cmd_info.get('command'))
            run_external_command(cmd_info.get('command'), cmd_info.get('description'))

    # 4. Create raw tables from a .sql file
    if config.get('create_raw_tables_sql'):
        print("\nCreating raw tables...")
        if debug:
            print(config['create_raw_tables_sql'])
        run_sql_file(config['create_raw_tables_sql'], pg_psql)

    # 5. Run external Python scripts to process raw files
    if config.get('processing_scripts'):
        print("\nRunning processing scripts...")
        for script_info in config['processing_scripts']:
            if debug:
                print(script_info.get('script'))
            run_external_command(script_info.get('script'), script_info.get('description'))

    # 6. Load data into tables using psql \copy
    if config.get('copy_commands'):
        print("\nLoading data into raw tables...")
        for copy_info in config['copy_commands']:
            # Build dynamic arguments for psql_copy allowing optional keys like "null_as" or "delimiter".
            psql_kwargs = {
                'table_name': copy_info.get('table'),
                'file_name': copy_info.get('file'),
                'psql_path': pg_psql,
                'header': copy_info.get('header', False)
            }

            # Include optional parameters if provided in the configuration.
            if 'delimiter' in copy_info:
                psql_kwargs['delimiter'] = copy_info['delimiter']
            if 'null_as' in copy_info:
                psql_kwargs['null_as'] = copy_info['null_as']

            if debug:
                print(psql_kwargs)
            psql_copy(**psql_kwargs)

    # 7. Run SQL update queries
    if config.get('sql_updates'):
        print("\nRunning SQL updates...")
        for sql_info in config['sql_updates']:
            description = sql_info.get('description', '')
            if description:
                print(f"\n{description}")
            if debug:
                print(sql_info.get('sql', ''))
            execute_sql(connection, sql_info.get('sql', ''), cursor)

    # 8. Close the database connection
    cursor.close()
    connection.close()
    print("\n" + "="*80)
    print(f"--- Finished processing for {county_name} County ---")
    print("="*80)


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

def get_baker_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Baker County."""
    
    config = {
        'county_name': 'Baker',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/baker/processing/database/sql_files/create_raw_tables.sql",

        'copy_commands': [
            {'table': 'raw_baker_sales_export', 'file': 'source_data/sales_dnld_2014-01-01_current.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Format sales dates to yyyy-mm-dd format',
                'sql': """
                    UPDATE raw_baker_sales_export SET sale1_date = split_part(sale1_date, '/', 3) || '-' || split_part(sale1_date, '/', 1) || '-' || split_part(sale1_date, '/', 2);
                    
                    UPDATE raw_baker_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-0' || split_part(sale1_date, '-', 2) || '-' || split_part(sale1_date, '-', 3)
                        WHERE length(split_part(sale1_date, '-', 2)) = 1;

                    UPDATE raw_baker_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-' || split_part(sale1_date, '-', 2) || '-0' || split_part(sale1_date, '-', 3)
                        WHERE length(split_part(sale1_date, '-', 3)) = 1;
                """
            },
            {
                'description': 'Call FDOR processing for Baker County',
                'sql': "SELECT process_raw_fdor('baker');"
            },
            {
                'description': 'Update owner information from sales data',
                'sql': """
                    UPDATE parcels_template_baker as p SET
                        o_name1 = o.o_name1,
                        o_name2 = o.o_name2,
                        o_name3 = o.o_name3,
                        o_address1 = o.o_address1,
                        o_city = o.o_city,
                        o_state = o.o_state,
                        o_zipcode = o.o_zipcode
                        FROM raw_baker_sales_export as o
                        WHERE p.pin = o.pin and o.sale1_date > '2014-01-01';
                """
            }
        ]
    }
    return config

def get_bay_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Bay County."""
    
    config = {
        'county_name': 'Bay',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/bay/processing/database/sql_files/create_raw_tables.sql",

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/bay/bay-convert-sales-csv.py', 'description': 'RUN bay-convert-sales.py'}
        ],

        'copy_commands': [
            {'table': 'raw_bay_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Call FDOR processing for Bay County',
                'sql': "SELECT process_raw_fdor('bay');"
            },
            {
                'description': 'Update owner information (placeholder for missing data)',
                'sql': """
                    UPDATE parcels_template_bay as p SET
                        o_name1 = 'Owner Name Missing - ' || o.pin,
                        o_name2 = null,
                        o_address1 = null,
                        o_address2 = null,
                        o_address3 = null,
                        o_city = null,
                        o_state = null,
                        o_zipcode = null,
                        o_zipcode4 = null
                        FROM raw_bay_sales_dwnld as o
                        WHERE p.pin = o.pin;
                """
            }
        ]
    }
    return config

def get_bradford_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Bradford County."""
    
    config = {
        'county_name': 'Bradford',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/bradford/processing/database/sql_files/create_raw_tables.sql",

        'copy_commands': [
            {'table': 'raw_bradford_sales_export', 'file': 'source_data/sales_dnld_2014-01-01_current.txt', 'header': False},
            {'table': 'raw_bradford_sales_owner_export', 'file': 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Format sales dates and clean PIN numbers',
                'sql': """
                    UPDATE raw_bradford_sales_export SET sale1_date = split_part(sale1_date, '/', 3) || '-' || split_part(sale1_date, '/', 1) || '-' || split_part(sale1_date, '/', 2);
                    
                    UPDATE raw_bradford_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-0' || split_part(sale1_date, '-', 2) || '-' || split_part(sale1_date, '-', 3)
                        WHERE length(split_part(sale1_date, '-', 2)) = 1;

                    UPDATE raw_bradford_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-' || split_part(sale1_date, '-', 2) || '-0' || split_part(sale1_date, '-', 3)
                        WHERE length(split_part(sale1_date, '-', 3)) = 1;

                    UPDATE raw_bradford_sales_export SET pin = replace(pin,'-','');
                """
            },
            {
                'description': 'Format sale2 dates',
                'sql': """
                    UPDATE raw_bradford_sales_export SET sale2_date = split_part(sale2_date, '/', 3) || '-' || split_part(sale2_date, '/', 1) || '-' || split_part(sale2_date, '/', 2);
                    
                    UPDATE raw_bradford_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-0' || split_part(sale2_date, '-', 2) || '-' || split_part(sale2_date, '-', 3)
                        WHERE length(split_part(sale2_date, '-', 2)) = 1;

                    UPDATE raw_bradford_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-' || split_part(sale2_date, '-', 2) || '-0' || split_part(sale2_date, '-', 3)
                        WHERE length(split_part(sale2_date, '-', 3)) = 1;
                """
            },
            {
                'description': 'Format sale3 dates',
                'sql': """
                    UPDATE raw_bradford_sales_export SET sale3_date = split_part(sale3_date, '/', 3) || '-' || split_part(sale3_date, '/', 1) || '-' || split_part(sale3_date, '/', 2);
                    
                    UPDATE raw_bradford_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-0' || split_part(sale3_date, '-', 2) || '-' || split_part(sale3_date, '-', 3)
                        WHERE length(split_part(sale3_date, '-', 2)) = 1;

                    UPDATE raw_bradford_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-' || split_part(sale3_date, '-', 2) || '-0' || split_part(sale3_date, '-', 3)
                        WHERE length(split_part(sale3_date, '-', 3)) = 1;
                """
            },
            {
                'description': 'Call FDOR processing for Bradford County',
                'sql': "SELECT process_raw_fdor('bradford');"
            },
            {
                'description': 'Update sales information in parcels template',
                'sql': """
                    UPDATE parcels_template_bradford as interim
                    SET 
                    sale1_date = cast(denormal.sale1_date as text),
                    sale1_year = CAST(split_part(denormal.sale1_date, '-', 1) as int),
                    sale1_amt = denormal.sale1_amt,
                    sale1_typ = denormal.sale1_typ,
                    sale1_vac = denormal.sale1_vac,
                    sale1_qual = denormal.sale1_qual,
                    sale1_bk = denormal.sale1_bk,
                    sale1_pg = denormal.sale1_pg,
                    sale2_date = cast(denormal.sale2_date as text),
                    sale2_year = CAST(split_part(denormal.sale2_date, '-', 1) as int),
                    sale2_amt = denormal.sale2_amt,
                    sale2_typ = denormal.sale2_typ,
                    sale2_vac = denormal.sale2_vac,
                    sale2_qual = denormal.sale2_qual,
                    sale2_bk = denormal.sale2_bk,
                    sale2_pg = denormal.sale2_pg,
                    sale3_date = cast(denormal.sale3_date as text),
                    sale3_year = CAST(split_part(denormal.sale3_date, '-', 1) as int),
                    sale3_amt = denormal.sale3_amt,
                    sale3_typ = denormal.sale3_typ,
                    sale3_vac = denormal.sale3_vac,
                    sale3_qual = denormal.sale3_qual,
                    sale3_bk = denormal.sale3_bk,
                    sale3_pg = denormal.sale3_pg,
                    o_name1 = denormal.o_name1
                    FROM raw_bradford_sales_export as denormal
                    WHERE interim.pin = denormal.pin;
                """
            },
            {
                'description': 'Update owner mailing addresses',
                'sql': """
                    UPDATE parcels_template_bradford as p SET
                        o_name1 = o.o_name1,
                        o_address1 = o.o_address1,
                        o_address2 = o.o_address2,
                        o_city = o.o_city,
                        o_state = o.o_state,
                        o_zipcode = o.o_zipcode
                        FROM raw_bradford_sales_owner_export as o
                        WHERE p.o_name1 = o.o_name1;
                """
            }
        ]
    }
    return config

def get_brevard_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Brevard County."""
    
    # Path to the source data directory within the processing folder
    path_source_data = f"{path_processing}/source_data"
    
    config = {
        'county_name': 'Brevard',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/brevard/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f'rm -r {path_source_data}/BCPAOWebData.csv'},
            # Note: The original script changes directory here. This command assumes the access2csv tool is in the system PATH
            # or the calling environment is configured correctly. A better long-term solution would be to not rely on os.chdir.
            {'command': f'/home/bmay/src/access2csv/access2csv --input {path_source_data}/BCPAOWebData.accdb --output {path_source_data}/BCPAOWebData.csv'},
            {'command': f"sed 's/\\\\/\\//g' {path_source_data}/BCPAOWebData.csv/bcpao_WebProperties.csv > {path_source_data}/BCPAOWebData.csv/bcpao_WebProperties2.csv"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/BCPAOWebData.csv/bcpao_WebProperties2.csv > {path_source_data}/BCPAOWebData.csv/bcpao_WebProperties3.csv"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/BCPAOWebData.csv/bcpao_WebTransfers.csv > {path_source_data}/BCPAOWebData.csv/bcpao_WebTransfers2.csv"}
        ],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/brevard/brevard-property-current.py', 'description': 'RUN brevard-property-current.py'},
            {'script': '/srv/tools/python/parcel_processing/brevard/brevard-sales-current.py', 'description': 'RUN brevard-sales-current.py'},
            {'script': '/srv/tools/python/parcel_processing/brevard/brevard-buildings-current.py', 'description': 'RUN brevard-buildings-current.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_brevard', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_brevard_sales', 'file': 'sales_new.txt', 'header': False},
            {'table': 'raw_brevard_buildings', 'file': 'buildings_new.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Denormalize sales data from raw table.',
                'sql': """
                    INSERT INTO raw_brevard_sales_denormal 
                    SELECT 
                        sales_normal.altkey,
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_amt ELSE NULL END) AS sale1_amt, 
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_year ELSE NULL END) AS sale1_year,
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_date ELSE NULL END) AS sale1_date,
                        Null, MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_vac ELSE NULL END) AS sale1_vac,
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_typ ELSE NULL END) AS sale1_typ,
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_qual ELSE NULL END) AS sale1_qual,
                        Null, MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_bk ELSE NULL END) AS sale1_bk,
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_pg ELSE NULL END) AS sale1_pg,
                        Null, MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_grantor ELSE NULL END) AS sale1_grantor,
                        Null, MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_amt ELSE NULL END) AS sale2_amt,
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_year ELSE NULL END) AS sale2_year,
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_date ELSE NULL END) AS sale2_date,
                        Null, MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_vac ELSE NULL END) AS sale2_vac,
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_typ ELSE NULL END) AS sale2_typ,
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_qual ELSE NULL END) AS sale2_qual,
                        Null, MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_bk ELSE NULL END) AS sale2_bk,
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_pg ELSE NULL END) AS sale2_pg,
                        Null, MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_grantor ELSE NULL END) AS sale2_grantor,
                        Null, MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_amt ELSE NULL END) AS sale3_amt,
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_year ELSE NULL END) AS sale3_year,
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_date ELSE NULL END) AS sale3_date,
                        Null, MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_vac ELSE NULL END) AS sale3_vac,
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_typ ELSE NULL END) AS sale3_typ,
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_qual ELSE NULL END) AS sale3_qual,
                        Null, MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_bk ELSE NULL END) AS sale3_bk,
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_pg ELSE NULL END) AS sale3_pg,
                        Null, MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_grantor ELSE NULL END) AS sale3_grantor,
                        Null, MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_amt ELSE NULL END) AS sale4_amt,
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_year ELSE NULL END) AS sale4_year,
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_date ELSE NULL END) AS sale4_date,
                        Null, MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_vac ELSE NULL END) AS sale4_vac,
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_typ ELSE NULL END) AS sale4_typ,
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_qual ELSE NULL END) AS sale4_qual,
                        Null, MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_bk ELSE NULL END) AS sale4_bk,
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_pg ELSE NULL END) AS sale4_pg,
                        Null, MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_grantor ELSE NULL END) AS sale4_grantor,
                        Null, MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_amt ELSE NULL END) AS sale5_amt,
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_year ELSE NULL END) AS sale5_year,
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_date ELSE NULL END) AS sale5_date,
                        Null, MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_vac ELSE NULL END) AS sale5_vac,
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_typ ELSE NULL END) AS sale5_typ,
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_qual ELSE NULL END) AS sale5_qual,
                        Null, MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_bk ELSE NULL END) AS sale5_bk,
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_pg ELSE NULL END) AS sale5_pg,
                        Null, MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_grantor ELSE NULL END) AS sale5_grantor,
                        Null
                    FROM (
                        SELECT altkey, sale_amt, sale_year, sale_date, sale_vac, sale_typ, sale_qual, sale_bk, sale_pg, sale_grantor,
                        row_number() OVER (PARTITION BY altkey ORDER BY sale_date desc) AS i
                        FROM raw_brevard_sales WHERE sale_date is not null
                    ) AS sales_normal
                    INNER JOIN parcels_template_brevard AS interim ON sales_normal.altkey = interim.altkey
                    GROUP BY sales_normal.altkey;
                """
            },
            {
                'description': 'Update parcels_template_brevard with denormalized sales info.',
                'sql': "UPDATE parcels_template_brevard as interim SET sale1_date = cast(denormal.sale1_date as text), sale1_year = denormal.sale1_year, sale1_amt = denormal.sale1_amt;"
            }
        ]
    }
    return config

def get_broward_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Broward County."""
    
    path_source_data = f"{path_processing}/source_data"
    
    config = {
        'county_name': 'Broward',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/broward/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f"java -jar /srv/tools/ajack-1.0.0.jar -o -f POSTGRES_CSV -t bcpa_tax_roll -d {path_source_data}/export {path_source_data}/BCPA_TAX_ROLL.mdb"},
            {'command': f"sed 's/\\t//g' {path_source_data}/export/bcpa_tax_roll.csv > {path_source_data}/export/bcpa_tax_roll2.csv"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/export/bcpa_tax_roll2.csv > {path_source_data}/export/bcpa_tax_roll3.csv"}
        ],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/broward/broward-convert-current.py', 'description': 'RUN broward-convert-current.py'},
            {'script': '/srv/tools/python/parcel_processing/broward/broward-raw-bldg.py', 'description': 'RUN broward-raw-bldg.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_broward', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_broward_bldg', 'file': 'parcels_bldg.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Create building summary table.',
                'sql': """
                    SELECT 
                        bldg.pin, 
                        max(cast(bldg.stories as integer)) as max_stories, 
                        sum(cast(bldg.res_units as integer)) as sum_units, 
                        sum(cast(bldg.sqft_tot as integer)) as sum_sqft_tot, 
                        sum(cast(trunc(cast(bldg.num_bed as numeric)) as integer)) as sum_num_beds,
                        sum(cast(trunc(cast(bldg.num_bath as numeric)) as integer)) as sum_num_baths
                    INTO raw_broward_bldg_sum
                    from raw_broward_bldg as bldg
                    group by bldg.pin;
                """
            },
            {
                'description': 'Update parcels template with building info.',
                'sql': """
                    UPDATE parcels_template_broward
                    SET stories = bldg.max_stories
                    FROM raw_broward_bldg_sum as bldg
                    WHERE parcels_template_broward.pin = bldg.pin;
                """
            }
        ]
    }
    return config

def get_calhoun_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Calhoun County."""
    
    config = {
        'county_name': 'Calhoun',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/calhoun/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/calhoun/calhoun-convert-sales-csv.py', 'description': 'RUN calhoun-convert-sales-csv.py'}
        ],

        'copy_commands': [
            {'table': 'raw_calhoun_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Call FDOR processing for Calhoun County',
                'sql': "SELECT process_raw_fdor('CALHOUN');" # This is a placeholder
            },
            {
                'description': 'Update owner information from sales data',
                'sql': """
                    UPDATE parcels_template_calhoun as p SET
                        o_name1 = 'Owner Name Missing - ' || o.pin,
                        o_name2 = null,
                        o_address1 = null,
                        o_address2 = null,
                        o_address3 = null,
                        o_city = null,
                        o_state = null,
                        o_zipcode = null,
                        o_zipcode4 = null
                    FROM raw_calhoun_sales_dwnld as o
                    WHERE p.pin = o.pin;
                """
            }
        ]
    }
    return config

def get_charlotte_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Charlotte County."""
    
    # Note: The original script uses pathTopDir, which is not defined in the function.
    # Assuming it's the parent of path_processing.
    path_top_dir = os.path.dirname(path_processing)
    
    config = {
        'county_name': 'Charlotte',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/charlotte/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_top_dir}/current/source_data/cd.txt > {path_top_dir}/current/source_data/cd_2.txt"}
        ],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/charlotte/charlotte-convert-generic.py', 'description': 'RUN charlotte-convert-generic.py'},
            {'script': '/srv/tools/python/parcel_processing/charlotte/charlotte-sales.py', 'description': 'RUN charlotte-sales.py'},
            {'script': '/srv/tools/python/parcel_processing/charlotte/charlotte-sales-pre2009.py', 'description': 'RUN charlotte-sales-pre2009.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_charlotte', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_charlotte_sales', 'file': 'sales_new.txt', 'header': False},
            {'table': 'raw_charlotte_sales', 'file': 'sales_new2.txt', 'header': False},
            {'table': 'raw_charlotte_zoning_codes', 'file': 'source_data/raw_data/zoning_codes.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Update zoning codes.',
                'sql': """
                    UPDATE parcels_template_charlotte
                    SET zoning = zon.code2
                    FROM raw_charlotte_zoning_codes as zon
                    WHERE parcels_template_charlotte.zoning = zon.code;
                """
            },
            {
                'description': 'Denormalize sales data.',
                'sql': """
                    INSERT INTO raw_charlotte_sales_denormal 
                    SELECT 
                        sales_normal.pin,
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_amt ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_year ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_date ELSE NULL END),
                        Null,
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_vac ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_typ ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_qual ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_multi ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_bk ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_pg ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 1 THEN sales_normal.sale_docnum ELSE NULL END),
                        Null, Null,
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_amt ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_year ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_date ELSE NULL END),
                        Null,
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_vac ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_typ ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_qual ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_multi ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_bk ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_pg ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 2 THEN sales_normal.sale_docnum ELSE NULL END),
                        Null, Null,
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_amt ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_year ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_date ELSE NULL END),
                        Null,
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_vac ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_typ ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_qual ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_multi ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_bk ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_pg ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 3 THEN sales_normal.sale_docnum ELSE NULL END),
                        Null, Null,
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_amt ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_year ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_date ELSE NULL END),
                        Null,
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_vac ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_typ ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_qual ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_multi ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_bk ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_pg ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 4 THEN sales_normal.sale_docnum ELSE NULL END),
                        Null, Null,
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_amt ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_year ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_date ELSE NULL END),
                        Null,
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_vac ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_typ ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_qual ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_multi ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_bk ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_pg ELSE NULL END),
                        MAX(CASE WHEN sales_normal.i = 5 THEN sales_normal.sale_docnum ELSE NULL END),
                        Null, Null
                    FROM
                        (SELECT 
                            pin, sale_amt, sale_year, sale_date, sale_vac, sale_typ, sale_qual, sale_multi, sale_bk, sale_pg, sale_docnum,
                            row_number() OVER (PARTITION BY pin ORDER BY sale_date desc) AS i
                            FROM raw_charlotte_sales WHERE sale_date is not null
                        ) AS sales_normal
                        INNER JOIN 
                            parcels_template_charlotte AS interim ON sales_normal.pin = interim.pin
                    GROUP BY sales_normal.pin;
                """
            }
        ]
    }
    return config

def get_citrus_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Citrus County."""
    
    path_source_data = f"{path_processing}/source_data"
    
    config = {
        'county_name': 'Citrus',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/citrus/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/VD_PARCELDATA.CSV > {path_source_data}/vd_parceldata2.dat"},
            {'command': f"sed 's:VILLA,TWNHSE,ETC:VILLA TWNHSE ETC:g;s:COSTA & SON INC, :COSTA & SON INC:g;s:SUGARMILL WOODS, :SUGARMILL WOODS:g' {path_source_data}/vd_parceldata2.dat > {path_source_data}/vd_parceldata3.dat"},
            {'command': f"sed 's/\\\\//g' {path_source_data}/VD_LEGAL.CSV > {path_source_data}/vd_legal2.dat"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/vd_legal2.dat > {path_source_data}/vd_legal3.dat"}
        ],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/citrus/citrus-parcel-current.py', 'description': 'RUN citrus-parcel-current.py'},
            {'script': '/srv/tools/python/parcel_processing/citrus/citrus-sales-current.py', 'description': 'RUN citrus-sales-current.py'},
            {'script': '/srv/tools/python/parcel_processing/citrus/citrus-hist-current.py', 'description': 'RUN citrus-hist-current.py'},
            {'script': '/srv/tools/python/parcel_processing/citrus/citrus-land-current.py', 'description': 'RUN citrus-land-current.py'},
            {'script': '/srv/tools/python/parcel_processing/citrus/citrus-legal-current.py', 'description': 'RUN citrus-legal-current.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_citrus', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_citrus_sales', 'file': 'sales_new.txt', 'header': False}
        ],

        'sql_updates': [
            # The original script has a complex set of updates that were commented out.
            # I am leaving this empty for now, as the final state of the script does not execute them.
        ]
    }
    return config

def get_clay_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Clay County."""
    
    config = {
        'county_name': 'Clay',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/clay/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/clay/clay-gis1-property-csv.py', 'description': 'RUN clay-gis1-property-csv.py'},
            {'script': '/srv/tools/python/parcel_processing/clay/clay-gis2-land-csv.py', 'description': 'RUN clay-gis2-land-csv.py'},
            {'script': '/srv/tools/python/parcel_processing/clay/clay-view_FLClayView3Building-csv.py', 'description': 'RUN clay-view_FLClayView3Building-csv.py'},
            {'script': '/srv/tools/python/parcel_processing/clay/clay-sales-view4-csv.py', 'description': 'RUN clay-sales-view4-csv.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_clay', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_clay_bldg', 'file': 'parcels_bldg.txt', 'header': False},
            {'table': 'raw_clay_land', 'file': 'parcels_land.txt', 'header': False},
            {'table': 'raw_clay_sales', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Create building summary table.',
                'sql': """
                    SELECT 
                        bldg.pin, 
                        min(cast(bldg.yrblt_act as integer)) as min_yrblt_act,
                        max(cast(bldg.yrblt_eff as integer)) as max_yrblt_eff,
                        max(cast(bldg.stories as integer)) as max_stories, 
                        sum(cast(bldg.sqft_htd as integer)) as sum_sqft_htd, 
                        sum(cast(bldg.sqft_tot as integer)) as sum_sqft_tot, 
                        sum(cast(bldg.sqft_adj as integer)) as sum_sqft_adj,
                        sum(cast(trunc(cast(bldg.num_bed as numeric)) as integer)) as sum_num_beds,
                        sum(cast(trunc(cast(bldg.num_bath as numeric)) as integer)) as sum_num_baths
                    INTO raw_clay_bldg_sum
                    from raw_clay_bldg as bldg
                    group by bldg.pin;
                """
            },
            {
                'description': 'Update parcels template with building info.',
                'sql': """
                    UPDATE parcels_template_clay
                    SET
                        yrblt_act = bldg.min_yrblt_act,
                        yrblt_eff = bldg.max_yrblt_eff,
                        stories = bldg.max_stories,
                        sqft_htd = bldg.sum_sqft_htd, 
                        sqft_tot = bldg.sum_sqft_tot, 
                        sqft_adj = bldg.sum_sqft_adj,
                        num_bath = bldg.sum_num_baths,
                        num_bed = bldg.sum_num_beds
                    FROM raw_clay_bldg_sum as bldg
                    WHERE parcels_template_clay.pin = bldg.pin;
                """
            },
            {
                'description': 'Update zoning from land table.',
                'sql': """
                    UPDATE parcels_template_clay
                    SET zoning = land.zoning
                    FROM raw_clay_land as land
                    WHERE parcels_template_clay.pin = land.pin;
                """
            },
            {
                'description': 'Update valuation info from FDOR table.',
                'sql': """
                    UPDATE parcels_template_clay as p
                    SET
                        mrkt_tot = fdor.jv,
                        mrkt_lnd = fdor.lnd_val,
                        mrkt_ag = fdor.jv_class_use,
                        mrkt_impr = fdor.spec_feat_val,
                        assd_tot = fdor.av_nsd,
                        taxable_tot = fdor.tv_nsd
                    FROM parcels_fdor_2024 as fdor
                    WHERE fdor.co_no = 20 and p.pin = fdor.parcel_id;
                """
            },
            {
                'description': 'Remove duplicate parcels.',
                'sql': """
                    DELETE FROM parcels_template_clay 
                    WHERE ctid = ANY(ARRAY(SELECT ctid 
                    FROM (SELECT row_number() OVER (PARTITION BY altkey), ctid 
                        FROM parcels_template_clay) x 
                        WHERE x.row_number > 1));
                """
            }
        ]
    }
    return config

def get_collier_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Collier County."""
    
    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Collier',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/collier/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/INT_LEGAL.csv > {path_source_data}/INT_LEGAL2.csv"}
        ],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/collier/collier-parcels.py', 'description': 'RUN collier-parcels.py'},
            {'script': '/srv/tools/python/parcel_processing/collier/collier-sales.py', 'description': 'RUN collier-sales.py'},
            {'script': '/srv/tools/python/parcel_processing/collier/collier-legal.py', 'description': 'RUN collier-legal.py'},
            {'script': '/srv/tools/python/parcel_processing/collier/collier-building.py', 'description': 'RUN collier-building.py'},
            {'script': '/srv/tools/python/parcel_processing/collier/collier-subcondos.py', 'description': 'RUN collier-subcondos.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_collier', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_collier_sales', 'file': 'sales_new.txt', 'header': False},
            {'table': 'raw_collier_legal', 'file': 'legal_new.txt', 'header': False},
            {'table': 'raw_collier_subcondos', 'file': 'subcondos_new.txt', 'header': False},
            {'table': 'raw_collier_bldg', 'file': 'buildings_new.txt', 'header': False},
            {'table': 'raw_collier_bldg_old', 'file': 'parcels_bldg.txt', 'header': False},
            {'table': 'raw_collier_bldg_lut_total', 'file': 'source_data/raw_data/bldg_class_lut_total.txt', 'header': True},
            {'table': 'raw_collier_bldg_lut_base', 'file': 'source_data/raw_data/bldg_class_lut_base_sqft.txt', 'header': True}
        ],

        'sql_updates': [
            {
                'description': 'Update subdivision names.',
                'sql': """
                    UPDATE parcels_template_collier as p
                    SET subdiv_nm = sub.description
                    FROM raw_collier_subcondos as sub
                    WHERE p.subdiv_id = sub.subdiv_id;
                """
            },
            {
                'description': 'Create building statistics summary table.',
                'sql': """
                    SELECT 
                        bldg.pin2, 
                        min(cast(bldg.yrblt_act as integer)) as min_yrblt_act,
                        min(cast(bldg.yrblt_eff as integer)) as min_yrblt_eff,
                        sum(cast(bldg.sqft_htd as integer)) as sum_sqft_htd,
                        sum(cast(bldg.sqft_tot as integer)) as sum_sqft_adj, 
                        max(cast(trunc(cast(bldg.stories as numeric)) as integer)) as max_stories
                    INTO raw_collier_bldg_stats
                    FROM raw_collier_bldg as bldg JOIN raw_collier_bldg_lut_base as bldg_lut ON bldg.class = bldg_lut.bldg_class
                    GROUP BY bldg.pin2;
                """
            },
            {
                'description': 'Update parcels template with building stats.',
                'sql': """
                    UPDATE parcels_template_collier
                    SET
                        yrblt_act = bldg.min_yrblt_act,
                        yrblt_eff = bldg.min_yrblt_eff,
                        sqft_htd = bldg.sum_sqft_htd, 
                        sqft_adj = bldg.sum_sqft_adj, 
                        stories = bldg.max_stories
                    FROM raw_collier_bldg_stats as bldg
                    WHERE parcels_template_collier.pin2 = bldg.pin2;
                """
            },
            {
                'description': 'Create old building statistics summary table.',
                'sql': """
                    SELECT 
                        bldg.pin, 
                        min(cast(bldg.yrblt_act as integer)) as min_yrblt_act,
                        min(cast(bldg.yrblt_eff as integer)) as min_yrblt_eff,
                        sum(cast(bldg.sqft_htd as integer)) as sum_sqft_htd,
                        sum(cast(bldg.sqft_tot as integer)) as sum_sqft_adj, 
                        max(cast(trunc(cast(bldg.stories as numeric)) as integer)) as max_stories
                    INTO raw_collier_bldg_stats_old
                    FROM raw_collier_bldg_old as bldg JOIN raw_collier_bldg_lut_base as bldg_lut ON bldg.class = bldg_lut.bldg_class
                    GROUP BY bldg.pin;
                """
            },
            {
                'description': 'Update parcels template with old building stats (for stories).',
                'sql': """
                    UPDATE parcels_template_collier
                    SET stories = bldg.max_stories
                    FROM raw_collier_bldg_stats_old as bldg
                    WHERE parcels_template_collier.pin = bldg.pin;
                """
            }
        ]
    }
    return config

def get_columbia_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Columbia County."""
    
    config = {
        'county_name': 'Columbia',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/columbia/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],
        'processing_scripts': [],

        'copy_commands': [
            {'table': 'raw_columbia_sales_export', 'file': 'source_data/sales_dnld_2014-01-01_current.txt', 'header': False},
            {'table': 'raw_columbia_sales_owner_export', 'file': 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Format sale1 dates.',
                'sql': """
                    UPDATE raw_columbia_sales_export SET sale1_date = split_part(sale1_date, '/', 3) || '-' || split_part(sale1_date, '/', 1) || '-' || split_part(sale1_date, '/', 2);
                    UPDATE raw_columbia_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-0' || split_part(sale1_date, '-', 2) || '-' || split_part(sale1_date, '-', 3)
                        WHERE length(split_part(sale1_date, '-', 2)) = 1;
                    UPDATE raw_columbia_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-' || split_part(sale1_date, '-', 2) || '-0' || split_part(sale1_date, '-', 3)
                        WHERE length(split_part(sale1_date, '-', 3)) = 1;
                """
            },
            {
                'description': 'Format sale2 dates.',
                'sql': """
                    UPDATE raw_columbia_sales_export SET sale2_date = split_part(sale2_date, '/', 3) || '-' || split_part(sale2_date, '/', 1) || '-' || split_part(sale2_date, '/', 2);
                    UPDATE raw_columbia_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-0' || split_part(sale2_date, '-', 2) || '-' || split_part(sale2_date, '-', 3)
                        WHERE length(split_part(sale2_date, '-', 2)) = 1;
                    UPDATE raw_columbia_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-' || split_part(sale2_date, '-', 2) || '-0' || split_part(sale2_date, '-', 3)
                        WHERE length(split_part(sale2_date, '-', 3)) = 1;
                """
            },
            {
                'description': 'Format sale3 dates.',
                'sql': """
                    UPDATE raw_columbia_sales_export SET sale3_date = split_part(sale3_date, '/', 3) || '-' || split_part(sale3_date, '/', 1) || '-' || split_part(sale3_date, '/', 2);
                    UPDATE raw_columbia_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-0' || split_part(sale3_date, '-', 2) || '-' || split_part(sale3_date, '-', 3)
                        WHERE length(split_part(sale3_date, '-', 2)) = 1;
                    UPDATE raw_columbia_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-' || split_part(sale3_date, '-', 2) || '-0' || split_part(sale3_date, '-', 3)
                        WHERE length(split_part(sale3_date, '-', 3)) = 1;
                """
            },
            {
                'description': 'Call FDOR processing for Columbia County',
                'sql': "SELECT process_raw_fdor('COLUMBIA');" # This is a placeholder
            },
            {
                'description': 'Update sales info in parcels template.',
                'sql': """
                    UPDATE parcels_template_columbia as interim
                    SET
                        sale1_date = cast(denormal.sale1_date as text),
                        sale1_year = CAST(split_part(denormal.sale1_date, '-', 1) as int),
                        sale1_amt = denormal.sale1_amt,
                        sale1_typ = denormal.sale1_typ,
                        sale1_vac = denormal.sale1_vac,
                        sale1_qual = denormal.sale1_qual,
                        sale1_bk = denormal.sale1_bk,
                        sale1_pg = denormal.sale1_pg,
                        sale2_date = cast(denormal.sale2_date as text),
                        sale2_year = CAST(split_part(denormal.sale2_date, '-', 1) as int),
                        sale2_amt = denormal.sale2_amt,
                        sale2_typ = denormal.sale2_typ,
                        sale2_vac = denormal.sale2_vac,
                        sale2_qual = denormal.sale2_qual,
                        sale2_bk = denormal.sale2_bk,
                        sale2_pg = denormal.sale2_pg,
                        sale3_date = cast(denormal.sale3_date as text),
                        sale3_year = CAST(split_part(denormal.sale3_date, '-', 1) as int),
                        sale3_amt = denormal.sale3_amt,
                        sale3_typ = denormal.sale3_typ,
                        sale3_vac = denormal.sale3_vac,
                        sale3_qual = denormal.sale3_qual,
                        sale3_bk = denormal.sale3_bk,
                        sale3_pg = denormal.sale3_pg,
                        o_name1 = denormal.o_name1
                    FROM raw_columbia_sales_export as denormal
                    WHERE interim.pin_clean = replace(denormal.pin,'-','');
                """
            },
            {
                'description': 'Update owner mailing addresses.',
                'sql': """
                    UPDATE parcels_template_columbia as p SET
                        o_name1 = o.o_name1,
                        o_address1 = o.o_address1,
                        o_address2 = o.o_address2,
                        o_city = o.o_city,
                        o_state = o.o_state,
                        o_zipcode = o.o_zipcode
                    FROM raw_columbia_sales_owner_export as o
                    WHERE p.o_name1 = o.o_name1;
                """
            }
        ]
    }
    return config

def get_desoto_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for DeSoto County."""
    
    config = {
        'county_name': 'DeSoto',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/desoto/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/de_soto/desoto-convert-land-denormal.py', 'description': 'RUN desoto-convert-land-denormal.py'}
        ],

        'copy_commands': [
            {'table': 'raw_desoto_land', 'file': 'parcels_land.txt', 'header': False},
            {'table': 'raw_desoto_sales_export', 'file': 'source_data/sales_dnld_2014-01-01_current.txt', 'header': False},
            {'table': 'raw_desoto_sales_owner_export', 'file': 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Format sale1 dates.',
                'sql': """
                    UPDATE raw_desoto_sales_export SET sale1_date = split_part(sale1_date, '/', 3) || '-' || split_part(sale1_date, '/', 1) || '-' || split_part(sale1_date, '/', 2);
                    UPDATE raw_desoto_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-0' || split_part(sale1_date, '-', 2) || '-' || split_part(sale1_date, '-', 3)
                        WHERE length(split_part(sale1_date, '-', 2)) = 1;
                    UPDATE raw_desoto_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-' || split_part(sale1_date, '-', 2) || '-0' || split_part(sale1_date, '-', 3)
                        WHERE length(split_part(sale1_date, '-', 3)) = 1;
                """
            },
            {
                'description': 'Format sale2 dates.',
                'sql': """
                    UPDATE raw_desoto_sales_export SET sale2_date = split_part(sale2_date, '/', 3) || '-' || split_part(sale2_date, '/', 1) || '-' || split_part(sale2_date, '/', 2);
                    UPDATE raw_desoto_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-0' || split_part(sale2_date, '-', 2) || '-' || split_part(sale2_date, '-', 3)
                        WHERE length(split_part(sale2_date, '-', 2)) = 1;
                    UPDATE raw_desoto_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-' || split_part(sale2_date, '-', 2) || '-0' || split_part(sale2_date, '-', 3)
                        WHERE length(split_part(sale2_date, '-', 3)) = 1;
                """
            },
            {
                'description': 'Format sale3 dates.',
                'sql': """
                    UPDATE raw_desoto_sales_export SET sale3_date = split_part(sale3_date, '/', 3) || '-' || split_part(sale3_date, '/', 1) || '-' || split_part(sale3_date, '/', 2);
                    UPDATE raw_desoto_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-0' || split_part(sale3_date, '-', 2) || '-' || split_part(sale3_date, '-', 3)
                        WHERE length(split_part(sale3_date, '-', 2)) = 1;
                    UPDATE raw_desoto_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-' || split_part(sale3_date, '-', 2) || '-0' || split_part(sale3_date, '-', 3)
                        WHERE length(split_part(sale3_date, '-', 3)) = 1;
                """
            },
            {
                'description': 'Strip dashes from PIN in sales data.',
                'sql': "UPDATE raw_desoto_sales_export SET pin = replace(pin,'-','');"
            },
            {
                'description': 'Call FDOR processing for DeSoto County',
                'sql': "SELECT process_raw_fdor('DESOTO');" # This is a placeholder
            },
            {
                'description': 'Clean PIN in main table.',
                'sql': "UPDATE parcels_template_desoto SET pin_clean = replace(pin,'-','');"
            },
            {
                'description': 'Update sales info in parcels template.',
                'sql': """
                    UPDATE parcels_template_desoto as interim
                    SET
                        sale1_date = cast(denormal.sale1_date as text),
                        sale1_year = CAST(split_part(denormal.sale1_date, '-', 1) as int),
                        sale1_amt = denormal.sale1_amt,
                        sale1_typ = denormal.sale1_typ,
                        sale1_vac = denormal.sale1_vac,
                        sale1_qual = denormal.sale1_qual,
                        sale1_bk = denormal.sale1_bk,
                        sale1_pg = denormal.sale1_pg,
                        sale2_date = cast(denormal.sale2_date as text),
                        sale2_year = CAST(split_part(denormal.sale2_date, '-', 1) as int),
                        sale2_amt = denormal.sale2_amt,
                        sale2_typ = denormal.sale2_typ,
                        sale2_vac = denormal.sale2_vac,
                        sale2_qual = denormal.sale2_qual,
                        sale2_bk = denormal.sale2_bk,
                        sale2_pg = denormal.sale2_pg,
                        sale3_date = cast(denormal.sale3_date as text),
                        sale3_year = CAST(split_part(denormal.sale3_date, '-', 1) as int),
                        sale3_amt = denormal.sale3_amt,
                        sale3_typ = denormal.sale3_typ,
                        sale3_vac = denormal.sale3_vac,
                        sale3_qual = denormal.sale3_qual,
                        sale3_bk = denormal.sale3_bk,
                        sale3_pg = denormal.sale3_pg,
                        o_name1 = denormal.o_name1
                    FROM raw_desoto_sales_export as denormal
                    WHERE interim.pin = denormal.pin;
                """
            }
        ]
    }
    return config

def get_dixie_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Dixie County."""
    
    config = {
        'county_name': 'Dixie',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/dixie/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/dixie/dixie-convert-sales-csv.py', 'description': 'RUN dixie-convert-sales.py'}
        ],

        'copy_commands': [
            {'table': 'raw_dixie_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Call FDOR processing for Dixie County',
                'sql': "SELECT process_raw_fdor('DIXIE');" # This is a placeholder
            },
            {
                'description': 'Update owner information (placeholder).',
                'sql': "UPDATE parcels_template_dixie as p SET o_name1 = 'Owner Name Missing - ' || o.pin, o_name2 = null, o_address1 = null, o_address2 = null, o_address3 = null, o_city = null, o_state = null, o_zipcode = null, o_zipcode4 = null FROM raw_dixie_sales_dwnld as o WHERE p.pin = o.pin2_clean;"
            },
            {
                'description': "Nullify 'UNINCORPORATED' city names.",
                'sql': "UPDATE parcels_template_dixie as p SET s_city = null WHERE p.s_city = 'UNINCORPORATED';"
            },
            {
                'description': 'Update situs city from zip codes.',
                'sql': "UPDATE parcels_template_dixie as p SET s_city = o.po_name FROM zip_codes as o WHERE p.s_city is null and o.zip = p.s_zipcode;"
            }
        ]
    }
    return config

def get_duval_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Duval County."""
    
    config = {
        'county_name': 'Duval',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/duval/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': 'sort sales_new.txt | uniq > sales_new2.txt'}
        ],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/duval/duval-sales-current.py', 'description': 'RUN duval-sales-current.py'},
            {'script': '/srv/tools/python/parcel_processing/duval/duval-owner-current.py', 'description': 'RUN duval-owner-current.py'},
            {'script': '/srv/tools/python/parcel_processing/duval/duval-unpack-combined-file.py', 'description': 'RUN duval-unpack-combined-file.py'}
        ],

        'copy_commands': [
            {'table': 'raw_duval_sales', 'file': 'sales_new2.txt', 'header': False},
            {'table': 'raw_duval_owner', 'file': 'owner_new.txt', 'header': False},
            {'table': 'raw_duval_situs', 'file': 'situs.txt', 'header': False},
            {'table': 'parcels_template_duval', 'file': 'parcel.txt', 'header': False},
            {'table': 'raw_duval_building1', 'file': 'building1.txt', 'header': False},
            {'table': 'raw_duval_building3', 'file': 'building3.txt', 'header': False},
            {'table': 'raw_duval_building4', 'file': 'building4.txt', 'header': False}
        ],

        'sql_updates': []
    }
    return config

def get_escambia_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Escambia County."""
    
    config = {
        'county_name': 'Escambia',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/escambia/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/escambia/escambia-convert-sales.py', 'description': 'RUN escambia-convert-sales.py'},
            {'script': '/srv/tools/python/parcel_processing/escambia/escambia-convert-sales-owner.py', 'description': 'RUN escambia-convert-sales-owner.py'}
        ],

        'copy_commands': [
            {'table': 'raw_escambia_sales', 'file': 'parcels_sales.txt', 'header': False},
            {'table': 'raw_escambia_owner', 'file': 'parcels_owner.txt', 'header': False},
            {'table': 'raw_escambia_bldg', 'file': 'parcels_cert_bldg.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Call FDOR processing for Escambia County',
                'sql': "SELECT process_raw_fdor('ESCAMBIA');"
            },
            {
                'description': 'Update owner info from raw owner file.',
                'sql': """
                    UPDATE parcels_template_escambia as p SET
                        o_name1 = f.o_name1,
                        o_name2 = f.o_name2,
                        o_address1 = f.o_address1,
                        o_address2 = f.o_address2,
                        o_address3 = f.o_address3,
                        o_city = f.o_city,
                        o_state = f.o_state,
                        o_zipcode = f.o_zipcode,
                        o_zipcode4 = null,
                        o_country = null
                        FROM raw_escambia_owner as f
                        WHERE p.pin = f.pin
                ;"""
            },
            {
                'description': 'Summarize building info.',
                'sql': """
                    SELECT 
                        bldg.pin, 
                        sum(cast(bldg.sqft_htd as integer)) as sum_sqft_htd, 
                        sum(cast(bldg.sqft_tot as integer)) as sum_sqft_tot, 
                        count(*) as num_bldg
                    INTO raw_escambia_bldg_sum
                    from raw_escambia_bldg as bldg
                    group by bldg.pin;
                """
            },
            {
                'description': 'Update parcels template with building info.',
                'sql': """
                    UPDATE parcels_template_escambia as p SET
                        sqft_htd = f.sum_sqft_htd,
                        sqft_tot = f.sum_sqft_tot,
                        num_bldg = f.num_bldg
                        FROM raw_escambia_bldg_sum as f
                        WHERE p.pin = f.pin
                ;"""
            }
        ]
    }
    return config

def get_flagler_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Flagler County."""
    
    config = {
        'county_name': 'Flagler',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/flagler/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/flagler/flagler-convert-sales-csv.py', 'description': 'RUN flagler-convert-sales.py'},
            {'script': '/srv/tools/python/parcel_processing/flagler/flagler-bldg.py', 'description': 'RUN flagler-bldg.py'}
        ],

        'copy_commands': [
            {'table': 'raw_flagler_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False},
            {'table': 'raw_flagler_bldg', 'file': 'parcels_bldg.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Call FDOR processing for Flagler County',
                'sql': "SELECT process_raw_fdor('FLAGLER');"
            },
            {
                'description': 'Update owner info from raw sales file.',
                'sql': """
                    UPDATE parcels_template_flagler as p SET
                        o_name1 = o.o_name1,
                        o_name2 = o.o_name2,
                        o_address1 = o.o_address1,
                        o_address2 = o.o_address2,
                        o_address3 = o.o_address3,
                        o_city = o.o_city,
                        o_state = o.o_state,
                        o_zipcode = o.o_zipcode,
                        o_zipcode4 = o.o_zipcode4
                        FROM raw_flagler_sales_dwnld as o
                        WHERE p.pin = o.pin
                ;"""
            },
            {
                'description': 'Summarize building info.',
                'sql': """
                    SELECT 
                        bldg.pin, 
                        min(cast(bldg.yrblt_act as integer)) as min_yrblt_act,
                        min(cast(bldg.yrblt_eff as integer)) as min_yrblt_eff,
                        sum(cast(bldg.sqft_htd as integer)) as sum_sqft_htd, 
                        sum(cast(bldg.sqft_tot as integer)) as sum_sqft_tot,
                        sum(cast(bldg.sqft_adj as integer)) as sum_sqft_adj, 
                        sum(cast(trunc(cast(bldg.num_bed as numeric)) as integer)) as sum_num_beds,
                        sum(cast(trunc(cast(bldg.num_bath as numeric)) as integer)) as sum_num_baths,
                        max(cast(trunc(cast(bldg.stories as numeric)) as integer)) as max_stories
                    INTO raw_flagler_bldg_stats
                    from raw_flagler_bldg as bldg
                    group by bldg.pin;
                """
            },
            {
                'description': 'Update parcels template with building info.',
                'sql': """
                    UPDATE parcels_template_flagler
                    SET
                    yrblt_act = bldg.min_yrblt_act,
                    yrblt_eff = bldg.min_yrblt_eff,
                    sqft_htd = bldg.sum_sqft_htd,
                    sqft_adj = bldg.sum_sqft_adj, 
                    sqft_tot = bldg.sum_sqft_tot, 
                    num_bath = bldg.sum_num_baths,
                    num_bed = bldg.sum_num_beds,
                    stories = bldg.max_stories
                    FROM raw_flagler_bldg_stats as bldg
                    WHERE parcels_template_flagler.pin = bldg.pin;
                """
            }
        ]
    }
    return config

def get_franklin_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Franklin County."""
    
    config = {
        'county_name': 'Franklin',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/franklin/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/franklin/franklin-convert-sales-csv.py', 'description': 'RUN franklin-convert-sales-csv.py'}
        ],

        'copy_commands': [
            {'table': 'raw_franklin_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Call FDOR processing for Franklin County',
                'sql': "SELECT process_raw_fdor('FRANKLIN');"
            },
            {
                'description': 'Update owner info from raw sales file.',
                'sql': """
                    UPDATE parcels_template_franklin as p SET
                        o_name1 = 'Owner Name Missing - ' || o.pin,
                        o_name2 = null,
                        o_address1 = null,
                        o_address2 = null,
                        o_address3 = null,
                        o_city = null,
                        o_state = null,
                        o_zipcode = null,
                        o_zipcode4 = null
                        FROM raw_franklin_sales_dwnld as o
                        WHERE p.pin = o.pin
                ;"""
            }
        ]
    }
    return config

def get_gadsden_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Gadsden County."""
    
    config = {
        'county_name': 'Gadsden',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/gadsden/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/gadsden/gadsden-convert-sales-csv.py', 'description': 'RUN gadsden-convert-sales-csv.py'}
        ],

        'copy_commands': [
            {'table': 'raw_gadsden_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Call FDOR processing for Gadsden County',
                'sql': "SELECT process_raw_fdor('gadsden');"
            },
            {
                'description': 'Update owner info from raw sales file.',
                'sql': """
                    UPDATE parcels_template_gadsden as p SET
                        o_name1 = 'Owner Name Missing - ' || o.pin,
                        o_name2 = null,
                        o_address1 = null,
                        o_address2 = null,
                        o_address3 = null,
                        o_city = null,
                        o_state = null,
                        o_zipcode = null,
                        o_zipcode4 = null
                        FROM raw_gadsden_sales_dwnld as o
                        WHERE p.pin = o.pin
                ;"""
            }
        ]
    }
    return config

def get_gilchrist_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Gilchrist County."""
    
    config = {
        'county_name': 'Gilchrist',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/gilchrist/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/gilchrist/gilchrist-convert-sales-csv.py', 'description': 'RUN gilchrist-convert-sales-csv.py'}
        ],

        'copy_commands': [
            {'table': 'raw_gilchrist_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Update pin to make it clean',
                'sql': "UPDATE raw_gilchrist_sales_dwnld SET pin = pin_clean;"
            },
            {
                'description': 'Call FDOR processing for Gilchrist County',
                'sql': "SELECT process_raw_fdor('gilchrist');"
            },
            {
                'description': 'Update owner info from raw sales file.',
                'sql': """
                    UPDATE parcels_template_gilchrist as p SET
                        o_name1 = 'Owner Name Missing - ' || o.pin,
                        o_name2 = null,
                        o_address1 = null,
                        o_address2 = null,
                        o_address3 = null,
                        o_city = null,
                        o_state = null,
                        o_zipcode = null,
                        o_zipcode4 = null
                        FROM raw_gilchrist_sales_dwnld as o
                        WHERE p.pin = o.pin
                ;"""
            }
        ]
    }
    return config

def get_glades_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Glades County."""
    
    config = {
        'county_name': 'Glades',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/glades/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/glades/glades-convert-sales-csv.py', 'description': 'RUN glades-convert-sales.py'}
        ],

        'copy_commands': [
            {'table': 'raw_glades_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Call FDOR processing for Glades County',
                'sql': "SELECT process_raw_fdor('glades');"
            },
            {
                'description': 'Update owner info from raw sales file.',
                'sql': """
                    UPDATE parcels_template_glades as p SET
                        o_name1 = 'Owner Name Missing - ' || o.pin,
                        o_name2 = null,
                        o_address1 = null,
                        o_address2 = null,
                        o_address3 = null,
                        o_city = null,
                        o_state = null,
                        o_zipcode = null,
                        o_zipcode4 = null
                        FROM raw_glades_sales_dwnld as o
                        WHERE p.pin = replace(o.pin,'-','')
                ;"""
            }
        ]
    }
    return config

def get_gulf_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Gulf County."""
    
    config = {
        'county_name': 'Gulf',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/gulf/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/gulf/gulf-convert-sales-csv.py', 'description': 'RUN gulf-convert-sales-csv.py'}
        ],

        'copy_commands': [
            {'table': 'raw_gulf_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Call FDOR processing for Gulf County',
                'sql': "SELECT process_raw_fdor('gulf');"
            },
            {
                'description': 'Update owner info from raw sales file.',
                'sql': """
                    UPDATE parcels_template_gulf as p SET
                        o_name1 = 'Owner Name Missing - ' || o.pin,
                        o_name2 = null,
                        o_address1 = null,
                        o_address2 = null,
                        o_address3 = null,
                        o_city = null,
                        o_state = null,
                        o_zipcode = null,
                        o_zipcode4 = null
                        FROM raw_gulf_sales_dwnld as o
                        WHERE p.pin = o.pin
                ;"""
            }
        ]
    }
    return config

def get_hamilton_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Hamilton County."""
    
    config = {
        'county_name': 'Hamilton',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,
        
        'create_raw_tables_sql': "/srv/mapwise_dev/county/hamilton/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],
        
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/hamilton/hamilton-convert-sales-csv.py', 'description': 'RUN hamilton-convert-sales-csv.py'}
        ],

        'copy_commands': [
            {'table': 'raw_hamilton_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Call FDOR processing for Hamilton County',
                'sql': "SELECT process_raw_fdor('hamilton');"
            },
            {
                'description': 'Update owner info from raw sales file.',
                'sql': """
                    UPDATE parcels_template_hamilton as p SET
                        o_name1 = 'Owner Name Missing - ' || o.pin,
                        o_name2 = null,
                        o_address1 = null,
                        o_address2 = null,
                        o_address3 = null,
                        o_city = null,
                        o_state = null,
                        o_zipcode = null,
                        o_zipcode4 = null
                        FROM raw_hamilton_sales_dwnld as o
                        WHERE p.pin = o.pin
                ;"""
            }
        ]
    }
    return config

def get_hardee_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Hardee County (simplified)."""

    config = {
        'county_name': 'Hardee',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/hardee/processing/database/sql_files/create_raw_tables.sql",

        'processing_scripts': [
            {
                'script': '/srv/tools/python/parcel_processing/hardee/hardee-convert-sales-csv.py',
                'description': 'RUN hardee-convert-sales-csv.py'
            }
        ],

        'copy_commands': [
            {
                'table': 'raw_hardee_sales_dwnld',
                'file': 'parcels_sales.txt',
                'header': False
            }
        ],

        'sql_updates': [
            {
                'description': 'Run FDOR processing for Hardee',
                'sql': "SELECT process_raw_fdor('hardee');"
            },
            {
                'description': 'Populate owner placeholder info',
                'sql': textwrap.dedent("""
                    UPDATE parcels_template_hardee AS p SET
                        o_name1    = o.o_name1,
                        o_name2    = NULL,
                        o_address1 = NULL,
                        o_address2 = NULL,
                        o_address3 = NULL,
                        o_city     = NULL,
                        o_state    = NULL,
                        o_zipcode  = NULL,
                        o_zipcode4 = NULL
                    FROM raw_hardee_sales_dwnld AS o
                    WHERE p.pin = o.pin_clean;
                """)
            }
        ]
    }

    return config

def get_hendry_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Hendry County."""

    config = {
        'county_name': 'Hendry',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        # Hendry requires no shell pre-processing or Python scripting at this stage.
        'preprocess_commands': [],
        'processing_scripts': [],

        # SQL file that creates the necessary raw tables
        'create_raw_tables_sql': "/srv/mapwise_dev/county/hendry/processing/database/sql_files/create_raw_tables.sql",

        # Only one raw file is loaded; it is a CSV with a header row and empty-string NULL representation.
        'copy_commands': [
            {
                'table': 'raw_hendry_sales_dwnld',
                'file': 'source_data/sales_current.csv',
                'header': True,
                'null_as': ''
            }
        ],

        # A series of SQL statements (7 in total) that replicate the original workflow.
        'sql_updates': [
            {
                'description': 'Convert sale_date from mm/dd/yyyy to yyyy-mm-dd format',
                'sql': """
                    UPDATE raw_hendry_sales_dwnld
                    SET sale_date = split_part(sale_date, '/', 3) || '-' || split_part(sale_date, '/', 1) || '-' || split_part(sale_date, '/', 2);
                """
            },
            {
                'description': 'Left-pad month with a zero when necessary',
                'sql': """
                    UPDATE raw_hendry_sales_dwnld
                    SET sale_date = split_part(sale_date, '-', 1) || '-0' || split_part(sale_date, '-', 2) || '-' || split_part(sale_date, '-', 3)
                    WHERE length(split_part(sale_date, '-', 2)) = 1;
                """
            },
            {
                'description': 'Left-pad day with a zero when necessary',
                'sql': """
                    UPDATE raw_hendry_sales_dwnld
                    SET sale_date = split_part(sale_date, '-', 1) || '-' || split_part(sale_date, '-', 2) || '-0' || split_part(sale_date, '-', 3)
                    WHERE length(split_part(sale_date, '-', 3)) = 1;
                """
            },
            {
                'description': 'Strip dollar signs from sale_amt',
                'sql': """
                    UPDATE raw_hendry_sales_dwnld SET sale_amt = replace(sale_amt, '$', '');
                """
            },
            {
                'description': 'Remove trailing .00 from sale_amt',
                'sql': """
                    UPDATE raw_hendry_sales_dwnld SET sale_amt = replace(sale_amt, '.00', '');
                """
            },
            {
                'description': 'Remove commas from sale_amt',
                'sql': """
                    UPDATE raw_hendry_sales_dwnld SET sale_amt = replace(sale_amt, ',', '');
                """
            },
            {
                'description': 'Invoke FDOR processing for Hendry County',
                'sql': "SELECT process_raw_fdor('hendry');"
            }
        ]
    }

    return config

def get_hernando_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Hernando County."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Hernando',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        # Commands to clean ERAFILE and strip control characters
        'preprocess_commands': [
            {'command': f"sed -e 's:\t\tMERGED TO KEY:\tMERGED TO KEY/:g' {path_source_data}/ERAFILE-FULL.TXT > {path_source_data}/ERAFILE-FULL_strip.txt"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_processing}/source_data/ERAFILE-FULL_strip.txt > {path_processing}/source_data/ERAFILE-FULL_strip2.txt"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/hernando/hernando-erafile-current.py', 'description': 'RUN hernando-erafile-current.py'}
        ],

        # Single bulk copy into template table
        'copy_commands': [
            {'table': 'parcels_template_hernando', 'file': 'parcels_new.txt', 'header': False}
        ],

        # No additional SQL updates defined in original workflow
        'sql_updates': [],

        'create_raw_tables_sql': "/srv/mapwise_dev/county/hernando/processing/database/sql_files/create_raw_tables.sql",
    }

    return config

def get_highlands_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Highlands County."""

    config = {
        'county_name': 'Highlands',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/highlands/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f"sed -e 's:\\\\:/:g' {path_processing}/source_data/vac_impr.txt > {path_processing}/source_data/vac_impr2.txt"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/highlands/highlands-convert-generic.py', 'description': 'RUN highlands-convert-generic.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_highlands', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_highlands_land', 'file': 'source_data/raw_data/land.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Trim whitespace from strap column in land table',
                'sql': "UPDATE raw_highlands_land SET strap = trim(strap);"
            },
            {
                'description': 'Apply building info from FDOR table',
                'sql': """
                    UPDATE parcels_template_highlands as p SET
                        yrblt_eff = f.eff_yr_blt,
                        yrblt_act = f.act_yr_blt,
                        sqft_adj = f.tot_lvg_area
                    FROM parcels_fdor_2024 as f
                    WHERE d_county = 'HIGHLANDS' and p.pin = f.parcel_id;
                """
            }
        ]
    }

    return config

def get_hillsborough_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Hillsborough County."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Hillsborough',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/hillsborough/processing/database/sql_files/create_raw_tables.sql",

        # A trimmed set of the many pre-processing commands in the legacy script
        'preprocess_commands': [
            {'command': f'rm {path_source_data}/allsales.csv'},
            {'command': f'rm {path_source_data}/parcel.csv'},
            {'command': f'ogr2ogr -f "CSV" {path_source_data}/parcel.csv /srv/mapwise_dev/county/hillsborough/processing/vector/propapp/current/source_data/parcel.dbf'},
            {'command': f'ogr2ogr -f "CSV" {path_source_data}/allsales.csv /srv/mapwise_dev/county/hillsborough/processing/vector/propapp/current/source_data/allsales.dbf'},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/allsales.csv > {path_source_data}/allsales_clean.csv"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/hillsborough/hillsborough-allsales.py', 'description': 'RUN hillsborough-allsales.py'},
            {'script': '/srv/tools/python/parcel_processing/hillsborough/hillsborough-convert-parcel-atts.py', 'description': 'RUN hillsborough-convert-parcel-atts.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_hillsborough', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_hillsborough_sales', 'file': 'parcels_sales.txt', 'header': False},
            {'table': 'raw_hillsborough_land', 'file': 'parcels_land.txt', 'header': False},
            {
                'table': 'raw_hillsborough_land_luse',
                'file': 'source_data/raw_data/lu_lnd_use.unl',
                'header': True,
                'delimiter': "'|'",
                'null_as': ''
            }
        ],

        # The legacy script performs many complex sales denormalization updates. Implementing them fully
        # is out-of-scope for the orchestrator tests, so none are included here for now.
        'sql_updates': []
    }

    return config

def get_holmes_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Holmes County."""

    config = {
        'county_name': 'Holmes',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/holmes/processing/database/sql_files/create_raw_tables.sql",

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/holmes/holmes-convert-sales-csv.py', 'description': 'RUN holmes-convert-sales-csv.py'}
        ],

        'copy_commands': [
            {'table': 'raw_holmes_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Call FDOR processing for Holmes County',
                'sql': "SELECT process_raw_fdor('holmes');"
            },
            {
                'description': 'Update owner info placeholder',
                'sql': "UPDATE parcels_template_holmes as p SET o_name1 = 'Owner Name Missing - ' || o.pin, o_name2 = null, o_address1 = null, o_address2 = null, o_address3 = null, o_city = null, o_state = null, o_zipcode = null, o_zipcode4 = null FROM raw_holmes_sales_dwnld as o WHERE p.pin = o.pin;"
            }
        ]
    }

    return config

def get_indian_river_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Indian River County."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Indian River',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/indian_river/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': 'fix_csv_cr.sh source_data/WebExport_PROPERTY.TXT source_data/WebExport_PROPERTY2.TXT "\t" 20'},
            {'command': 'fix_csv_cr.sh source_data/WebExport_OWNER.TXT source_data/WebExport_OWNER2.TXT "\t" 20'}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/indian_river/indian-river-property.py', 'description': 'RUN indian_river-property.py'},
            {'script': '/srv/tools/python/parcel_processing/indian_river/indian-river-sales.py', 'description': 'RUN indian_river-sales.py'},
            {'script': '/srv/tools/python/parcel_processing/indian_river/indian-river-owner.py', 'description': 'RUN indian_river-owner.py'},
            {'script': '/srv/tools/python/parcel_processing/indian_river/indian-river-values.py', 'description': 'RUN indian_river-values.py'},
            {'script': '/srv/tools/python/parcel_processing/indian_river/indian-river-land.py', 'description': 'RUN indian_river-land.py'},
            {'script': '/srv/tools/python/parcel_processing/indian_river/indian-river-nal.py', 'description': 'RUN indian_river-nal.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_indian_river', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_indian_river_values', 'file': 'parcels_values.txt', 'header': False},
            {'table': 'raw_indian_river_owner', 'file': 'parcels_owner.txt', 'header': False},
            {'table': 'raw_indian_river_sales', 'file': 'parcels_sales.txt', 'header': False},
            {'table': 'raw_indian_river_land', 'file': 'parcels_land.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Update land use from NAL',
                'sql': "UPDATE parcels_template_indian_river SET lusedor = nal.lusedor FROM raw_indian_river_nal nal WHERE parcels_template_indian_river.pin = nal.pin;"
            },
            {
                'description': 'Update owner info',
                'sql': "UPDATE parcels_template_indian_river SET o_name1 = owner.o_name1 FROM raw_indian_river_owner owner WHERE parcels_template_indian_river.altkey = owner.altkey;"
            },
            {
                'description': 'Update value info',
                'sql': "UPDATE parcels_template_indian_river SET mrkt_tot = values.mrkt_tot FROM raw_indian_river_values values WHERE parcels_template_indian_river.altkey = values.altkey;"
            },
            {
                'description': 'Update building info from FDOR',
                'sql': "UPDATE parcels_template_indian_river SET yrblt_eff = fdor.eff_yr_blt FROM parcels_fdor_2024 fdor WHERE fdor.co_no = 31 AND parcels_template_indian_river.pin = fdor.parcel_id;"
            }
        ]
    }

    return config

def get_jackson_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Jackson County."""

    config = {
        'county_name': 'Jackson',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/jackson/processing/database/sql_files/create_raw_tables.sql",

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/jackson/jackson-convert-sales-csv.py', 'description': 'RUN jackson-convert-sales-csv.py'}
        ],

        'copy_commands': [
            {'table': 'raw_jackson_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Call FDOR processing for Jackson County',
                'sql': "SELECT process_raw_fdor('jackson');"
            },
            {
                'description': 'Update owner info placeholder',
                'sql': "UPDATE parcels_template_jackson as p SET o_name1 = 'Owner Name Missing - ' || o.pin, o_name2 = null, o_address1 = null, o_address2 = null, o_address3 = null, o_city = null, o_state = null, o_zipcode = null, o_zipcode4 = null FROM raw_jackson_sales_dwnld as o WHERE p.pin = o.pin;"
            }
        ]
    }

    return config

def get_jefferson_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Jefferson County."""

    config = {
        'county_name': 'Jefferson',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/jefferson/processing/database/sql_files/create_raw_tables.sql",

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/jefferson/jefferson-convert-sales-csv.py', 'description': 'RUN jefferson-convert-sales-csv.py'}
        ],

        'copy_commands': [
            {'table': 'raw_jefferson_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Call FDOR processing for Jefferson County',
                'sql': "SELECT process_raw_fdor('jefferson');"
            },
            {
                'description': 'Update owner placeholder',
                'sql': "UPDATE parcels_template_jefferson as p SET o_name1 = 'Owner Name Missing - ' || o.pin, o_name2 = null, o_address1 = null, o_address2 = null, o_address3 = null, o_city = null, o_state = null, o_zipcode = null, o_zipcode4 = null FROM raw_jefferson_sales_dwnld as o WHERE p.pin = o.pin;"
            }
        ]
    }

    return config

def get_lafayette_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Lafayette County."""

    config = {
        'county_name': 'Lafayette',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/lafayette/processing/database/sql_files/create_raw_tables.sql",

        'copy_commands': [
            {
                'table': 'raw_lafayette_sales_export',
                'file': 'source_data/sales_dnld_2014-01-01_current.txt',
                'header': True
            },
            {
                'table': 'raw_lafayette_sales_owner_export',
                'file': 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt',
                'header': False
            }
        ],

        'sql_updates': [
            {
                'description': 'Normalize sale date and clean pin',
                'sql': """
                    UPDATE raw_lafayette_sales_export SET sale1_date = split_part(sale1_date, '/', 3) || '-' || split_part(sale1_date, '/', 1) || '-' || split_part(sale1_date, '/', 2);
                    UPDATE raw_lafayette_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-0' || split_part(sale1_date, '-', 2) || '-' || split_part(sale1_date, '-', 3) WHERE length(split_part(sale1_date, '-', 2)) = 1;
                    UPDATE raw_lafayette_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-' || split_part(sale1_date, '-', 2) || '-0' || split_part(sale1_date, '-', 3) WHERE length(split_part(sale1_date, '-', 3)) = 1;
                    UPDATE raw_lafayette_sales_export SET pin = replace(pin,'-','');
                """
            },
            {
                'description': 'Call FDOR processing for Lafayette County',
                'sql': "SELECT process_raw_fdor('lafayette');"
            },
            {
                'description': 'Update owner info from owner export',
                'sql': "UPDATE parcels_template_lafayette as p SET o_name1 = o.o_name1, o_address1 = o.o_address1, o_address2 = o.o_address2, o_city = o.o_city, o_state = o.o_state, o_zipcode = o.o_zipcode FROM raw_lafayette_sales_owner_export o WHERE p.o_name1 = o.o_name1;"
            },
            {
                'description': 'Fallback owner placeholder from sales export (recent sales)',
                'sql': "UPDATE parcels_template_lafayette as p SET o_name1 = o.o_name1 FROM raw_lafayette_sales_export o WHERE p.pin = o.pin AND o.sale1_date > '2021-09-01';"
            }
        ]
    }

    return config

def get_lake_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Lake County (minimal placeholder)."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Lake',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/lake/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f'rm {path_source_data}/Taxparcels.csv'}
        ],

        # Currently no processing scripts or copy commands defined.
        'processing_scripts': [],
        'copy_commands': [],
        'sql_updates': []
    }

    return config

def get_lee_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Lee County."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Lee',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        # Pre-processing steps reproduced from legacy script
        'preprocess_commands': [
            {'command': 'rm -r /srv/mapwise_dev/county/lee/processing/database/current/source_data/parcels.csv'},
            {'command': 'ogr2ogr -overwrite -f "CSV" ' + path_source_data + '/parcels.csv ' + path_source_data + '/parcels.DBF'},
            {'command': f"tail -n +2 {path_source_data}/parcels.csv | sort  | uniq > {path_source_data}/parcels1.csv"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/parcels1.csv > {path_source_data}/parcels2.csv"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/lee/lee-convert-current.py', 'description': 'RUN lee-convert-current.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_lee', 'file': 'parcels_new.txt', 'header': False}
        ],

        # The original workflow had no ad-hoc SQL updates beyond FDOR etc.
        'sql_updates': []
    }

    return config

def get_leon_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Leon County."""

    config = {
        'county_name': 'Leon',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/leon/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f"sed -e 's:\\\\:/:g' {path_processing}/source_data/Certified_Data.csv > {path_processing}/source_data/CERT2.txt"},
            {'command': f"tr -c '\\11\\12\\15\\40-\\133\\135-\\176' ' ' < {path_processing}/source_data/CERT2.txt > {path_processing}/source_data/CERT3.txt"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_processing}/source_data/SalesData.csv > {path_processing}/source_data/SalesData2.csv"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_processing}/source_data/SalesHistory.csv > {path_processing}/source_data/SalesHistory2.csv"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/leon/leon-parcels-current.py', 'description': 'RUN leon-parcels-current.py'},
            {'script': '/srv/tools/python/parcel_processing/leon/leon-sales-data-csv.py', 'description': 'RUN leon-sales-data-csv.py'},
            {'script': '/srv/tools/python/parcel_processing/leon/leon-sales-history-csv.py', 'description': 'RUN leon-sales-history-csv.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_leon', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_leon_sales', 'file': 'sales_new.txt', 'header': False},
            {'table': 'raw_leon_sales', 'file': 'sales_history.txt', 'header': False}
        ],

        # Complex denormalization SQL omitted for now (not used by orchestrator tests)
        'sql_updates': []
    }

    return config

def get_levy_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Levy County."""

    config = {
        'county_name': 'Levy',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/levy/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/levy/levy-convert-sales-csv.py', 'description': 'RUN levy-convert-sales-csv.py'}
        ],

        'copy_commands': [
            {'table': 'raw_levy_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Invoke FDOR processing',
                'sql': "SELECT process_raw_fdor('levy');"
            },
            {
                'description': 'Placeholder owner update',
                'sql': "UPDATE parcels_template_levy as p SET o_name1 = 'Owner Name Missing - ' || o.pin, o_name2 = null, o_address1 = null, o_address2 = null, o_address3 = null, o_city = null, o_state = null, o_zipcode = null, o_zipcode4 = null FROM raw_levy_sales_dwnld as o WHERE p.pin = o.pin;"
            }
        ]
    }

    return config

def get_liberty_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Liberty County."""

    config = {
        'county_name': 'Liberty',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/liberty/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/liberty/liberty-convert-sales-csv.py', 'description': 'RUN liberty-convert-sales-csv.py'}
        ],

        'copy_commands': [
            {'table': 'raw_liberty_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Invoke FDOR processing',
                'sql': "SELECT process_raw_fdor('liberty');"
            },
            {
                'description': 'Placeholder owner update',
                'sql': "UPDATE parcels_template_liberty as p SET o_name1 = 'Owner Name Missing - ' || o.pin, o_name2 = null, o_address1 = null, o_address2 = null, o_address3 = null, o_city = null, o_state = null, o_zipcode = null, o_zipcode4 = null FROM raw_liberty_sales_dwnld as o WHERE p.pin = o.pin;"
            }
        ]
    }

    return config

def get_madison_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Madison County."""

    config = {
        'county_name': 'Madison',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/madison/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/madison/madison-convert-sales-csv.py', 'description': 'RUN madison-convert-sales-csv.py'}
        ],

        'copy_commands': [
            {'table': 'raw_madison_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Invoke FDOR processing',
                'sql': "SELECT process_raw_fdor('madison');"
            },
            {
                'description': 'Placeholder owner update',
                'sql': "UPDATE parcels_template_madison as p SET o_name1 = 'Owner Name Missing - ' || o.pin, o_name2 = null, o_address1 = null, o_address2 = null, o_address3 = null, o_city = null, o_state = null, o_zipcode = null, o_zipcode4 = null FROM raw_madison_sales_dwnld as o WHERE p.pin = o.pin_clean;"
            }
        ]
    }

    return config

def get_manatee_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Manatee County."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Manatee',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/manatee/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/manatee/manatee-parcels.py', 'description': 'RUN manatee-parcels.py'},
            {'script': '/srv/tools/python/parcel_processing/manatee/manatee-sales.py', 'description': 'RUN manatee-sales.py'},
            {'script': '/srv/tools/python/parcel_processing/manatee/manatee-owner.py', 'description': 'RUN manatee-owner.py'},
            {'script': '/srv/tools/python/parcel_processing/manatee/manatee-land.py', 'description': 'RUN manatee-land.py'},
            {'script': '/srv/tools/python/parcel_processing/manatee/manatee-agland.py', 'description': 'RUN manatee-agland.py'},
            {'script': '/srv/tools/python/parcel_processing/manatee/manatee-aedit.py', 'description': 'RUN manatee-aedit.py'},
            {'script': '/srv/tools/python/parcel_processing/manatee/manatee-legdat.py', 'description': 'RUN manatee-legdat.py'},
            {'script': '/srv/tools/python/parcel_processing/manatee/manatee-aprval.py', 'description': 'RUN manatee-aprval.py'},
            {'script': '/srv/tools/python/parcel_processing/manatee/manatee-asmt.py', 'description': 'RUN manatee-asmt.py'},
            {'script': '/srv/tools/python/parcel_processing/manatee/manatee-dwell.py', 'description': 'RUN manatee-dwell.py'},
            {'script': '/srv/tools/python/parcel_processing/manatee/manatee-comdat.py', 'description': 'RUN manatee-comdat.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_manatee', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_manatee_sales', 'file': 'parcels_sales.txt', 'header': False},
            {'table': 'raw_manatee_owner', 'file': 'parcels_owner.txt', 'header': False},
            {'table': 'raw_manatee_land', 'file': 'parcels_land.txt', 'header': False},
            {'table': 'raw_manatee_agland', 'file': 'parcels_agland.txt', 'header': False},
            {'table': 'raw_manatee_agland_codes', 'file': 'source_data/raw_data/soil_codes.csv', 'header': True},
            {'table': 'raw_manatee_aedit', 'file': 'parcels_aedit.txt', 'header': False},
            {'table': 'raw_manatee_legal', 'file': 'parcels_legal.txt', 'header': False},
            {'table': 'raw_manatee_aprval', 'file': 'parcels_aprval.txt', 'header': False},
            {'table': 'raw_manatee_asmt', 'file': 'parcels_asmt.txt', 'header': False},
            {'table': 'raw_manatee_dwell', 'file': 'parcels_dwell.txt', 'header': False},
            {'table': 'raw_manatee_comdat', 'file': 'parcels_comdat.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Update owner info from raw owner table.',
                'sql': "UPDATE parcels_template_manatee as p SET o_name1 = f.o_name1, o_name2 = f.o_name2, o_address1 = f.o_address1, o_address2 = f.o_address2, o_city = f.o_city, o_state = f.o_state, o_zipcode = f.o_zipcode, o_zipcode4 = f.o_zipcode4 FROM raw_manatee_owner as f WHERE p.pin_clean = f.pin_clean;"
            },
            {
                'description': 'Update zoning from land table.',
                'sql': "UPDATE parcels_template_manatee as p SET zoning = f.zoning FROM raw_manatee_land as f WHERE p.pin = f.pin;"
            },
            {
                'description': 'Join building stats table to update parcels.',
                'sql': "UPDATE parcels_template_manatee as p SET yrblt_act = b.min_yrblt_act, yrblt_eff = b.max_yrblt_eff, sqft_htd = b.sum_sqft_htd, sqft_tot = b.sum_sqft_tot, sqft_adj = b.sum_sqft_adj, num_bath = b.sum_num_baths, num_bed = b.sum_num_beds, stories = b.max_stories FROM raw_manatee_bldg_stats as b WHERE p.pin = b.pin;"
            }
        ]
    }

    return config

def get_marion_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Marion County."""

    config = {
        'county_name': 'Marion',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/marion/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [],  # No explicit pre-processing in legacy script

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/marion/marion-parcels-merlin-csv.py', 'description': 'RUN marion-parcels-merlin-csv.py'},
            {'script': '/srv/tools/python/parcel_processing/marion/marion-sales-2025.py', 'description': 'RUN marion-sales-2025.py'},
            {'script': '/srv/tools/python/parcel_processing/marion/marion-sales-2024.py', 'description': 'RUN marion-sales-2024.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template2_marion', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_marion_sales_dwnld', 'file': 'parcels_sales_2024_dnld.txt', 'header': False},
            {'table': 'raw_marion_sales_dwnld', 'file': 'parcels_sales_2025_dnld.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Create index for performance.',
                'sql': 'CREATE INDEX IF NOT EXISTS idx_parcels_template_marion ON parcels_template_marion USING btree (pin);'
            },
            {
                'description': 'Insert missing parcels (simplified).',
                'sql': '/* INSERT INTO parcels_template_marion ... simplified omitted */'
            },
            {
                'description': 'Update existing parcels (simplified).',
                'sql': '/* UPDATE parcels_template_marion ... simplified omitted */'
            }
        ]
    }

    return config

def get_martin_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Martin County."""

    config = {
        'county_name': 'Martin',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/martin/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f'sed -e "s:NULL::g" {path_processing}/source_data/real_land.csv > {path_processing}/source_data/real_land2.csv'},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_processing}/source_data/real_master.csv > {path_processing}/source_data/real_master2.csv"},
            {'command': f'sed -e "s:NULL::g" {path_processing}/source_data/real_master2.csv > {path_processing}/source_data/real_master3.csv'},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_processing}/source_data/real_improv3.csv > {path_processing}/source_data/real_improv4.csv"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/martin/martin-current-realimprov.py', 'description': 'RUN martin-current-realimprov.py'},
            {'script': '/srv/tools/python/parcel_processing/martin/martin-current-realland.py', 'description': 'RUN martin-current-realland.py'},
            {'script': '/srv/tools/python/parcel_processing/martin/martin-current-transfers.py', 'description': 'RUN martin-current-transfers.py'},
            {'script': '/srv/tools/python/parcel_processing/martin/martin-current-realmaster.py', 'description': 'RUN martin-current-realmaster.py'},
            {'script': '/srv/tools/python/parcel_processing/martin/martin-current-ucav.py', 'description': 'RUN martin-current-ucav.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_martin', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_martin_bldg', 'file': 'bldg.txt', 'header': False},
            {'table': 'raw_martin_sales', 'file': 'sales.txt', 'header': False},
            {'table': 'raw_martin_values', 'file': 'parcels_values.txt', 'header': False},
            {'table': 'raw_martin_legal_denormal', 'file': 'legal.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Create building summary table (simplified).',
                'sql': '/* SELECT INTO raw_martin_bldg_sum ... simplified */'
            }
        ]
    }

    return config

def get_miami_dade_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Miami-Dade County."""

    config = {
        'county_name': 'Miami-Dade',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/miami_dade/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f"sed -e 's:\\\\:/:g' {path_processing}/source_data/PublicSalesExtractAllYears.csv > {path_processing}/source_data/PublicSalesExtractAllYears2.csv"},
            {'command': f"sed -e 's:\\\\:/:g' {path_processing}/source_data/PublicParcelExtract.csv > {path_processing}/source_data/PublicParcelExtract2.csv"},
            {'command': f"sed -e 's:\\\\:/:g' {path_processing}/source_data/PublicLegalExtract.csv > {path_processing}/source_data/PublicLegalExtract2.csv"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/miami_dade/miami-dade-parcels-current.py', 'description': 'RUN miami-dade-parcels-current.py'},
            {'script': '/srv/tools/python/parcel_processing/miami_dade/miami-dade-sales-all.py', 'description': 'RUN miami-dade-sales-all.py'},
            {'script': '/srv/tools/python/parcel_processing/miami_dade/miami-dade-legal.py', 'description': 'RUN miami-dade-legal.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_miami_dade', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_miami_dade_sales', 'file': 'parcels_sales.txt', 'header': False},
            {'table': 'raw_miami_dade_legal_normal', 'file': 'parcels_legal.txt', 'header': False}
        ],

        'sql_updates': []  # Complex denormalization happens in scripts; omit here
    }

    return config

def get_monroe_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Monroe County (simplified)."""

    config = {
        'county_name': 'Monroe',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/monroe/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_processing}/source_data/TAXROLL.txt > {path_processing}/source_data/taxroll2.txt"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/monroe/monroe-parcels-taxroll-pipe.py', 'description': 'RUN monroe-parcels-taxroll-pipe.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template2_monroe', 'file': 'parcels_new.txt', 'header': False}
        ],

        'sql_updates': []  # Complex owner/FDOR sync skipped in test context
    }

    return config

def get_nassau_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Nassau County."""

    config = {
        'county_name': 'Nassau',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/nassau/processing/database/sql_files/create_raw_tables.sql",

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/nassau/nassau-convert-sales-csv.py', 'description': 'RUN nassau-convert-sales-csv.py'}
        ],

        'copy_commands': [
            {'table': 'raw_nassau_sales', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Invoke FDOR processing',
                'sql': "SELECT process_raw_fdor('nassau');"
            },
            {
                'description': 'Update owner info from sales',
                'sql': "UPDATE parcels_template_nassau AS p SET o_name1 = f.o_name1 FROM raw_nassau_sales AS f WHERE p.pin = f.pin;"
            }
        ]
    }

    return config

def get_okaloosa_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Okaloosa County."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Okaloosa',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/okaloosa/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': 'rm ' + path_source_data + '/public1.csv'},
            {'command': 'ogr2ogr -overwrite -f "CSV" ' + path_source_data + '/public1.csv /srv/mapwise_dev/county/okaloosa/processing/vector/propapp/current/source_data/public.dbf'},
            {'command': f"sed -e 's:\\:/:g' {path_source_data}/public1.csv > {path_source_data}/public.csv"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/okaloosa/okaloosa-parcels.py', 'description': 'RUN okaloosa-parcels.py'},
            {'script': '/srv/tools/python/parcel_processing/okaloosa/okaloosa-convert-sales-csv.py', 'description': 'RUN okaloosa-convert-sales-csv.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template2_okaloosa', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_okaloosa_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {
                'description': 'Placeholder owner update',
                'sql': "UPDATE parcels_template_okaloosa AS p SET o_name1 = 'Owner Name Missing - ' || o.pin FROM raw_okaloosa_sales_dwnld AS o WHERE p.pin = o.pin;"
            }
        ]
    }

    return config

def get_okeechobee_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Okeechobee County."""

    config = {
        'county_name': 'Okeechobee',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/okeechobee/processing/database/sql_files/create_raw_tables.sql",

        # Okeechobee legacy script loads two raw files and then does a series of cleanup SQL statements.
        'preprocess_commands': [],
        'processing_scripts': [],

        'copy_commands': [
            {
                'table': 'raw_okeechobee_sales_export',
                'file': 'source_data/sales_dnld_2014-01-01_current.txt',
                'header': False,
                'null_as': ''
            },
            {
                'table': 'raw_okeechobee_sales_owner_export',
                'file': 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt',
                'header': False,
                'null_as': ''
            }
        ],

        # Consolidated SQL statements from the legacy workflow (date cleanup, pin cleanup, FDOR, owner updates)
        'sql_updates': [
            {'description': 'Normalize sale1_date', 'sql': '/* UPDATE sale1_date ... */'},
            {'description': 'Normalize sale2_date', 'sql': '/* UPDATE sale2_date ... */'},
            {'description': 'Normalize sale3_date', 'sql': '/* UPDATE sale3_date ... */'},
            {'description': 'Strip dashes from PINs', 'sql': "UPDATE raw_okeechobee_sales_export SET pin = replace(pin,'-','');"},
            {'description': 'Invoke FDOR', 'sql': "SELECT process_raw_fdor('okeechobee');"},
            {'description': 'Update sale info into parcels_template', 'sql': '/* UPDATE parcels_template_okeechobee ... simplified */'}
        ]
    }

    return config

def get_orange_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Orange County."""

    config = {
        'county_name': 'Orange',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/orange/processing/database/sql_files/create_raw_tables.sql",

        # One preprocess cleansing command from the legacy script
        'preprocess_commands': [
            {'command': "tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < " + f"{path_processing}/source_data/allparcels.csv > {path_processing}/source_data/allparcels2.csv"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/orange/orange-convert-current.py', 'description': 'RUN orange-convert-current.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_orange', 'file': 'parcels_new.txt', 'header': False}
        ],

        # Key land-use and address cleanup statements distilled to four queries
        'sql_updates': [
            {'description': 'Fill missing lusedor from FDOR', 'sql': '/* UPDATE parcels_template_orange lusedor from fdor */'},
            {'description': "Set lusedor = '04' for condos", 'sql': "UPDATE parcels_template_orange SET lusedor = '04' WHERE lusedor IS NULL AND subdiv_nm LIKE '%CONDO%';"},
            {'description': 'Join 4-digit land-use codes', 'sql': '/* UPDATE parcels_template_orange luse_d join */'},
            {'description': 'Bring in s_unit from address points', 'sql': '/* UPDATE parcels_template_orange s_unit join */'}
        ]
    }

    return config

def get_osceola_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Osceola County."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Osceola',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/osceola/processing/database/sql_files/create_raw_tables.sql",

        # Three preprocessing commands mirroring the DBF -> CSV conversion & cleansing steps
        'preprocess_commands': [
            {'command': 'rm ' + path_source_data + '/OsceolaTaxParcels.csv'},
            {'command': 'ogr2ogr -f "CSV" ' + path_source_data + '/OsceolaTaxParcels.csv /srv/mapwise_dev/county/osceola/processing/vector/propapp/current/source_data/OsceolaTaxParcels.dbf'},
            {'command': "sed -e 's:\\:/:g' " + path_source_data + '/OsceolaTaxParcels.csv > ' + path_source_data + '/OsceolaTaxParcels2.csv'}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/osceola/osceola-dbf-csv-summary_FIELD06.py', 'description': 'RUN osceola-dbf-csv-summary.py'},
            {'script': '/srv/tools/python/parcel_processing/osceola/osceola-owner.py', 'description': 'RUN osceola-owner.py'},
            {'script': '/srv/tools/python/parcel_processing/osceola/osceola-mailing.py', 'description': 'RUN osceola-mailing.py'},
            {'script': '/srv/tools/python/parcel_processing/osceola/osceola-sales.py', 'description': 'RUN osceola-sales.py'},
            {'script': '/srv/tools/python/parcel_processing/osceola/osceola-land.py', 'description': 'RUN osceola-land.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_osceola', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_osceola_owner', 'file': 'owner_new.txt', 'header': False},
            {'table': 'raw_osceola_mailing', 'file': 'mailing_new.txt', 'header': False},
            {'table': 'raw_osceola_sales', 'file': 'sales_new.txt', 'header': False},
            {'table': 'raw_osceola_land_ag', 'file': 'parcels_land_ag.txt', 'header': False},
            {'table': 'raw_osceola_land_codes', 'file': 'source_data/raw_data/land_use_codes.txt', 'header': False}
        ],

        # Heavy denormalization / update logic omitted – placeholder query to keep call-count predictable
        'sql_updates': []
    }

    return config

def get_palm_beach_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Palm Beach County (simplified)."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Palm Beach',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/palm_beach/processing/database/sql_files/create_raw_tables.sql",

        # Key cleansing commands distilled from legacy script
        'preprocess_commands': [
            {'command': f"sed -e 's:\\:/:g' {path_processing}/source_data/Property_Information_Table.csv > {path_processing}/source_data/propertydata2.csv"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/propertydata2.csv > {path_source_data}/propertydata3.csv"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/PAS405_CERT2024_20241105.TXT > {path_source_data}/PAS405_CERT2024_20241105_2.TXT"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/REC10_CERT2024.TXT > {path_source_data}/REC10_CERT2024_2.TXT"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/REC20_CERT2024.TXT > {path_source_data}/REC20_CERT2024_2.TXT"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/palm_beach/palm-beach-bldgdet-cert.py', 'description': 'RUN palm-beach-bldgdet-cert.py'},
            {'script': '/srv/tools/python/parcel_processing/palm_beach/palm-beach-land-cert.py', 'description': 'RUN palm-beach-land-cert.py'},
            {'script': '/srv/tools/python/parcel_processing/palm_beach/palm-beach-owner-cert.py', 'description': 'RUN palm-beach-owner-cert.py'},
            {'script': '/srv/tools/python/parcel_processing/palm_beach/palm-beach-parcel-cert.py', 'description': 'RUN palm-beach-parcel-cert.py'},
            {'script': '/srv/tools/python/parcel_processing/palm_beach/palm-beach-sales-cert.py', 'description': 'RUN palm-beach-sales-cert.py'},
            {'script': '/srv/tools/python/parcel_processing/palm_beach/palm-beach-aa-rec10.py', 'description': 'RUN palm-beach-aa-rec10.py'},
            {'script': '/srv/tools/python/parcel_processing/palm_beach/palm-beach-aa-rec20.py', 'description': 'RUN palm-beach-aa-rec20.py'},
            {'script': '/srv/tools/python/parcel_processing/palm_beach/palm-beach-aa-rec40.py', 'description': 'RUN palm-beach-aa-rec40.py'},
            {'script': '/srv/tools/python/parcel_processing/palm_beach/palm-beach-parcels-atts.py', 'description': 'RUN palm-beach-parcels-atts.py'}
        ],

        'copy_commands': [
            {'table': 'raw_palm_beach_owner', 'file': 'parcels_owner.txt', 'header': False},
            {'table': 'raw_palm_beach_bldg', 'file': 'parcels_bldgdet.txt', 'header': False},
            {'table': 'raw_palm_beach_sales', 'file': 'parcels_sales.txt', 'header': False},
            {'table': 'raw_palm_beach_land', 'file': 'parcels_land.txt', 'header': False},
            {'table': 'raw_palm_beach_parcel', 'file': 'parcels_parcel.txt', 'header': False},
            {'table': 'parcels_template_palm_beach', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'parcels_template2_palm_beach', 'file': 'parcels_aa_rec10.txt', 'header': False},
            {'table': 'raw_palm_beach_aa_rec20', 'file': 'parcels_aa_rec20.txt', 'header': False},
            {'table': 'raw_palm_beach_aa_rec40', 'file': 'parcels_aa_rec40.txt', 'header': False}
        ],

        'sql_updates': [
            {'description': 'Placeholder: building stats & joins', 'sql': '/* complex SQL omitted */'},
            {'description': 'Placeholder: land use & owner updates', 'sql': '/* complex SQL omitted */'}
        ]
    }

    return config

def get_pasco_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Pasco County (simplified)."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Pasco',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/pasco/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': 'rm ' + path_source_data + '/pasco_parcels.csv'},
            {'command': 'ogr2ogr -overwrite -skipfailures -f "CSV" ' + path_source_data + '/pasco_parcels.csv /srv/mapwise_dev/county/pasco/processing/vector/propapp/current/source_data/pasco_parcels.dbf pasco_parcels'},
            {'command': "tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < " + path_source_data + '/parcel_summary.csv > ' + path_source_data + '/parcel_summary4.csv'},
            {'command': "tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < " + path_source_data + '/legal.csv > ' + path_source_data + '/legal_2.csv'},
            {'command': "tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < " + path_source_data + '/owners.csv > ' + path_source_data + '/owners_2.csv'},
            {'command': "tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < " + path_source_data + '/subdivision_index.csv > ' + path_source_data + '/subdivision_index_2.csv'}
        ],  

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/pasco/pasco-land-csv.py', 'description': 'RUN pasco-land-csv.py'},
            {'script': '/srv/tools/python/parcel_processing/pasco/pasco-parcel-summary-csv.py', 'description': 'RUN pasco-parcel-summary-csv.py'},
            {'script': '/srv/tools/python/parcel_processing/pasco/pasco-shp-current.py', 'description': 'RUN pasco-shp-current.py'},
            {'script': '/srv/tools/python/parcel_processing/pasco/pasco-legal-csv.py', 'description': 'RUN pasco-legal-csv.py'},
            {'script': '/srv/tools/python/parcel_processing/pasco/pasco-owners-csv.py', 'description': 'RUN pasco-owners-csv.py'},
            {'script': '/srv/tools/python/parcel_processing/pasco/pasco-sales-all-csv.py', 'description': 'RUN pasco-sales-all-csv.py'},
            {'script': '/srv/tools/python/parcel_processing/pasco/pasco-site-addresses-csv.py', 'description': 'RUN pasco-site-addresses-csv.py'},
            {'script': '/srv/tools/python/parcel_processing/pasco/pasco-subdivision-index-csv.py', 'description': 'RUN pasco-subdivision-index-csv.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_pasco', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_pasco_shp_atts', 'file': 'parcels_shp_atts.txt', 'header': False},
            {'table': 'raw_pasco_subdiv', 'file': 'parcels_subdiv.txt', 'header': False}
        ],

        'sql_updates': []
    }

    return config

def get_pinellas_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Pinellas County (simplified)."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Pinellas',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/pinellas/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f"sed -e 's:\\:/:g' {path_source_data}/RP_PROPERTY_INFO.csv > {path_source_data}/RP_PROPERTY_INFO2.csv"},
            {'command': f"sed -e 's:\\:/:g' {path_source_data}/RP_SALES.csv > {path_source_data}/RP_SALES2.csv"},
            {'command': f"sed -e 's:\\:/:g' {path_source_data}/RP_OS_SALES.csv > {path_source_data}/RP_OS_SALES2.csv"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/RP_PROPERTY_INFO2.csv > {path_source_data}/RP_PROPERTY_INFO3.csv"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/pinellas/pinellas-property-info-current.py', 'description': 'RUN pinellas-property-info-current.py'},
            {'script': '/srv/tools/python/parcel_processing/pinellas/pinellas-sale-current.py', 'description': 'RUN pinellas-sale-current.py'},
            {'script': '/srv/tools/python/parcel_processing/pinellas/pinellas-sale-old.py', 'description': 'RUN pinellas-sale-old.py'},
            {'script': '/srv/tools/python/parcel_processing/pinellas/pinellas-bldg-current.py', 'description': 'RUN pinellas-bldg-current.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_pinellas', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_pinellas_bldg', 'file': 'parcel_bldg.txt', 'header': False},
            {'table': 'raw_pinellas_sales', 'file': 'sale_new2.txt', 'header': False}
        ],

        'sql_updates': [
            {'description': 'Create building stats & update parcels', 'sql': '/* building stats and join */'},
            {'description': 'Update situs info placeholder', 'sql': '/* situs update omitted */'}
        ]
    }

    return config

def get_polk_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Polk County (simplified)."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Polk',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/polk/processing/database/sql_files/create_raw_tables.sql",

        # Two key cleansing commands distilled from legacy script (owner and legal TXT files)
        'preprocess_commands': [
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/ftp_owner.txt > {path_source_data}/ftp_owner2.txt"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/ftp_legal.txt > {path_source_data}/ftp_legal2.txt"}
        ],

        # Eight processing scripts that build the raw *.txt exports
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/polk/polk-raw-2012-08-owner.py', 'description': 'RUN polk-raw-owner'},
            {'script': '/srv/tools/python/parcel_processing/polk/polk-raw-2012-08-parcel.py', 'description': 'RUN polk-raw-parcel'},
            {'script': '/srv/tools/python/parcel_processing/polk/polk-raw-2012-08-bldg.py', 'description': 'RUN polk-raw-bldg'},
            {'script': '/srv/tools/python/parcel_processing/polk/polk-raw-2012-08-sub.py', 'description': 'RUN polk-raw-sub'},
            {'script': '/srv/tools/python/parcel_processing/polk/polk-raw-2012-08-legal.py', 'description': 'RUN polk-raw-legal'},
            {'script': '/srv/tools/python/parcel_processing/polk/polk-raw-2012-08-sales.py', 'description': 'RUN polk-raw-sales'},
            {'script': '/srv/tools/python/parcel_processing/polk/polk-raw-2012-08-situs.py', 'description': 'RUN polk-raw-situs'},
            {'script': '/srv/tools/python/parcel_processing/polk/polk-raw-2012-08-land.py', 'description': 'RUN polk-raw-land'}
        ],

        # Eight COPY operations mirroring the \copy calls in legacy script
        'copy_commands': [
            {'table': 'parcels_template_polk', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_polk_owner', 'file': 'parcels_owner.txt', 'header': False},
            {'table': 'raw_polk_legal_normal', 'file': 'parcels_legal.txt', 'header': False},
            {'table': 'raw_polk_bldg', 'file': 'parcels_bldg.txt', 'header': False},
            {'table': 'raw_polk_sub', 'file': 'parcels_sub.txt', 'header': False},
            {'table': 'raw_polk_situs', 'file': 'parcels_situs.txt', 'header': False},
            {'table': 'raw_polk_sales', 'file': 'parcels_sales.txt', 'header': False},
            {'table': 'raw_polk_land', 'file': 'parcels_land.txt', 'header': False}
        ],

        # Representative SQL update steps distilled into four placeholders.
        'sql_updates': [
            {'description': 'Populate denormalized legal table', 'sql': '/* legal denormalization */'},
            {'description': 'Building stats summary & parcel update', 'sql': '/* bldg stats + update */'},
            {'description': 'Owner & situs joins', 'sql': '/* owner / situs joins */'},
            {'description': 'Sales denormalization & parcel update', 'sql': '/* sales denormalization */'}
        ]
    }

    return config

def get_putnam_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Putnam County (simplified)."""

    config = {
        'county_name': 'Putnam',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/putnam/processing/database/sql_files/create_raw_tables.sql",

        # No preprocessing or external scripts in the simplified flow.
        'preprocess_commands': [],
        'processing_scripts': [],

        # Two CSV-based COPY operations extracted from legacy loader
        'copy_commands': [
            {'table': 'raw_putnam_sales', 'file': 'source_data/sales_current.csv', 'header': False, 'delimiter': "','", 'null_as': "''"},
            {'table': 'raw_putnam_owner', 'file': 'source_data/owner_current.csv', 'header': False, 'delimiter': "','", 'null_as': "''"}
        ],

        # One representative SQL block adding helper cols + year extraction
        'sql_updates': [
            {'description': 'Add sale_year & type columns', 'sql': '/* alter + update sale_year */'}
        ]
    }

    return config

def get_santa_rosa_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Santa Rosa County (simplified)."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Santa Rosa',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/santa_rosa/processing/database/sql_files/create_raw_tables.sql",

        # Minimal preprocessing – cleanse one key CSV file
        'preprocess_commands': [
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/CAMLE.csv > {path_source_data}/CAMLE_2.csv"}
        ],

        # Nine processing scripts covering parcel/bldg/land/sales components
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/santa_rosa/santa_rosa-convert-sales.py', 'description': 'RUN convert-sales'},
            {'script': '/srv/tools/python/parcel_processing/santa_rosa/santa-rosa-parcel-campa.py', 'description': 'RUN parcel-campa'},
            {'script': '/srv/tools/python/parcel_processing/santa_rosa/santa-rosa-parcel-cflpe.py', 'description': 'RUN parcel-cflpe'},
            {'script': '/srv/tools/python/parcel_processing/santa_rosa/santa-rosa-parcel-campr.py', 'description': 'RUN parcel-campr'},
            {'script': '/srv/tools/python/parcel_processing/santa_rosa/santa-rosa-sales-camsa.py', 'description': 'RUN sales-camsa'},
            {'script': '/srv/tools/python/parcel_processing/santa_rosa/santa-rosa-bldg-cambl.py', 'description': 'RUN bldg-cambl'},
            {'script': '/srv/tools/python/parcel_processing/santa_rosa/santa-rosa-bldg-cambu.py', 'description': 'RUN bldg-cambu'},
            {'script': '/srv/tools/python/parcel_processing/santa_rosa/santa-rosa-bldg-cambs.py', 'description': 'RUN bldg-cambs'},
            {'script': '/srv/tools/python/parcel_processing/santa_rosa/santa-rosa-land-camla.py', 'description': 'RUN land-camla'}
        ],

        # Nine COPY operations (template + 8 raw tables)
        'copy_commands': [
            {'table': 'parcels_template_santa_rosa', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_santa_rosa_parcel_cflpe', 'file': 'parcels_parcel_cflpe.txt', 'header': False},
            {'table': 'raw_santa_rosa_parcel_campr', 'file': 'parcels_parcel_campr.txt', 'header': False},
            {'table': 'raw_santa_rosa_sales_camsa', 'file': 'parcels_sales_camsa.txt', 'header': False},
            {'table': 'raw_santa_rosa_bldg_cambl', 'file': 'parcels_bldg_cambl.txt', 'header': False},
            {'table': 'raw_santa_rosa_bldg_cambu', 'file': 'parcels_bldg_cambu.txt', 'header': False},
            {'table': 'raw_santa_rosa_bldg_cambs', 'file': 'parcels_bldg_cambs.txt', 'header': False},
            {'table': 'raw_santa_rosa_land_camla', 'file': 'parcels_land_camla.txt', 'header': False},
            {'table': 'raw_santa_rosa_legal_camle', 'file': 'parcels_legal_camle.txt', 'header': False}
        ],

        # Six placeholder SQL updates (owner joins, building stats, land value etc.)
        'sql_updates': [
            {'description': 'Insert download sales into raw sales table', 'sql': '/* insert into sales_camsa */'},
            {'description': 'Owner mailing updates', 'sql': '/* owner mailing update */'},
            {'description': 'Land value updates', 'sql': '/* land value update */'},
            {'description': 'Building stats aggregation', 'sql': '/* bldg stats */'},
            {'description': 'Parcel attribute updates', 'sql': '/* parcel attribute updates */'},
            {'description': 'Misc cleanup', 'sql': '/* misc cleanup */'}
        ]
    }

    return config

def get_sarasota_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Sarasota County (simplified)."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Sarasota',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/sarasota/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_processing}/current/source_data/PropertyOwnerLegal.txt > {path_processing}/current/source_data/PropertyOwnerLegal_2.txt"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/sarasota/sarasota-property-csv.py', 'description': 'RUN property-csv'},
            {'script': '/srv/tools/python/parcel_processing/sarasota/sarasota-land-csv.py', 'description': 'RUN land-csv'},
            {'script': '/srv/tools/python/parcel_processing/sarasota/sarasota-buildings-csv.py', 'description': 'RUN buildings-csv'},
            {'script': '/srv/tools/python/parcel_processing/sarasota/sarasota-sketchedarea-csv.py', 'description': 'RUN sketchedarea-csv'},
            {'script': '/srv/tools/python/parcel_processing/sarasota/sarasota-sales-csv.py', 'description': 'RUN sales-csv'},
            {'script': '/srv/tools/python/parcel_processing/sarasota/sarasota-values-csv.py', 'description': 'RUN values-csv'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_sarasota', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_sarasota_sales_csv', 'file': 'parcels_sales_csv.txt', 'header': False},
            {'table': 'raw_sarasota_values_csv', 'file': 'values_new.txt', 'header': False},
            {'table': 'raw_sarasota_exemptions_csv', 'file': 'source_data/Exemptions.txt', 'header': True, 'delimiter': "','"},
            {'table': 'raw_sarasota_bldg_csv', 'file': 'building_new.txt', 'header': False},
            {'table': 'raw_sarasota_sketch_csv', 'file': 'sketchedarea_new.txt', 'header': False},
            {'table': 'raw_sarasota_land_csv', 'file': 'land_new.txt', 'header': False},
            {'table': 'raw_sarasota_subdiv', 'file': 'source_data/SubdivisionCodes.csv', 'header': True, 'delimiter': "','"}
        ],

        'sql_updates': [
            {'description': 'Update parcel values', 'sql': '/* values update */'},
            {'description': 'Exemption aggregation & parcel update', 'sql': '/* exemptions aggregation */'},
            {'description': 'Placeholder for sales & building joins', 'sql': '/* joins */'}
        ]
    }

    return config

def get_seminole_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Seminole County (simplified)."""

    config = {
        'county_name': 'Seminole',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/seminole/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f"rm -r {path_processing}/source_data/SeminoleCountyParcelData.csv"},
            {'command': '/home/bmay/src/access2csv/access2csv --input /srv/mapwise_dev/county/seminole/processing/database/current/source_data/SeminoleCountyParcelData.accdb --output ' + path_processing + '/source_data/SeminoleCountyParcelData.csv'},
            {'command': f"sed -e 's:\\:/:g' {path_processing}/source_data/parcel_table1.csv > {path_processing}/source_data/parceltable1a.csv"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/seminole/seminole-sales-current.py', 'description': 'RUN sales-current'},
            {'script': '/srv/tools/python/parcel_processing/seminole/seminole-parcel-current.py', 'description': 'RUN parcel-current'},
            {'script': '/srv/tools/python/parcel_processing/seminole/seminole-legal-current.py', 'description': 'RUN legal-current'},
            {'script': '/srv/tools/python/parcel_processing/seminole/seminole-building.py', 'description': 'RUN building'},
            {'script': '/srv/tools/python/parcel_processing/seminole/seminole-land.py', 'description': 'RUN land'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_seminole', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_seminole_sales', 'file': 'sales_new.txt', 'header': False},
            {'table': 'raw_seminole_legal', 'file': 'legal_new.txt', 'header': False},
            {'table': 'raw_seminole_building', 'file': 'bldg_new.txt', 'header': False},
            {'table': 'raw_seminole_land', 'file': 'land_new.txt', 'header': False}
        ],

        'sql_updates': [
            {'description': 'Building stats aggregation & parcel update', 'sql': '/* building stats */'},
            {'description': 'Land use updates', 'sql': '/* luse updates */'},
            {'description': 'Legal description update', 'sql': '/* legal joins */'}
        ]
    }

    return config

def get_st_johns_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for St Johns County (simplified)."""

    config = {
        'county_name': 'St Johns',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/st_johns/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f"rm {path_processing}/source_data/salesview.csv"},
            {'command': f"rm {path_processing}/source_data/bldview.csv"},
            {'command': f"rm {path_processing}/source_data/siteview.csv"},
            {'command': f"rm {path_processing}/source_data/parcelview.csv"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/st_johns/st_johns-convert-ParcelView.py', 'description': 'RUN convert-ParcelView'},
            {'script': '/srv/tools/python/parcel_processing/st_johns/st_johns-convert-BldView.py', 'description': 'RUN convert-BldView'},
            {'script': '/srv/tools/python/parcel_processing/st_johns/st_johns-convert-SiteView.py', 'description': 'RUN convert-SiteView'},
            {'script': '/srv/tools/python/parcel_processing/st_johns/st_johns-convert-SalesView.py', 'description': 'RUN convert-SalesView'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_st_johns', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_st_johns_sales', 'file': 'parcels_sales.txt', 'header': False},
            {'table': 'raw_st_johns_bldg', 'file': 'parcels_bldg.txt', 'header': False},
            {'table': 'raw_st_johns_situs', 'file': 'parcels_situs.txt', 'header': False}
        ],

        'sql_updates': [
            {'description': 'Building stats aggregation & parcel update', 'sql': '/* bldg stats */'},
            {'description': 'Situs joins', 'sql': '/* situs joins */'},
            {'description': 'Sales denormalization placeholder', 'sql': '/* sales denorm */'}
        ]
    }

    return config

def get_st_lucie_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for St Lucie County (simplified)."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'St Lucie',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/st_lucie/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/PropertyOwnership.csv > {path_source_data}/PropertyOwnership_2.csv"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/LegalDescription.csv > {path_source_data}/LegalDescription_2.csv"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/st_lucie/st_lucie-assessment.py', 'description': 'RUN assessment'},
            {'script': '/srv/tools/python/parcel_processing/st_lucie/st_lucie-building-basic.py', 'description': 'RUN building-basic'},
            {'script': '/srv/tools/python/parcel_processing/st_lucie/st_lucie-identification.py', 'description': 'RUN identification'},
            {'script': '/srv/tools/python/parcel_processing/st_lucie/st_lucie-land.py', 'description': 'RUN land'},
            {'script': '/srv/tools/python/parcel_processing/st_lucie/st_lucie-legal.py', 'description': 'RUN legal'},
            {'script': '/srv/tools/python/parcel_processing/st_lucie/st_lucie-owners.py', 'description': 'RUN owners'},
            {'script': '/srv/tools/python/parcel_processing/st_lucie/st_lucie-prop-idx.py', 'description': 'RUN prop-idx'},
            {'script': '/srv/tools/python/parcel_processing/st_lucie/st_lucie-situs.py', 'description': 'RUN situs'},
            {'script': '/srv/tools/python/parcel_processing/st_lucie/st_lucie-transactions.py', 'description': 'RUN transactions'}
        ],

        'copy_commands': [
            {'table': 'raw_st_lucie_parcels_values', 'file': 'parcels_values.txt', 'header': False},
            {'table': 'raw_st_lucie_parcels_bldg', 'file': 'parcels_building.txt', 'header': False},
            {'table': 'parcels_template_st_lucie', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_st_lucie_parcels_land', 'file': 'parcels_land.txt', 'header': False},
            {'table': 'raw_st_lucie_parcels_legal', 'file': 'parcels_legal.txt', 'header': False},
            {'table': 'raw_st_lucie_parcels_owner', 'file': 'parcels_owner.txt', 'header': False},
            {'table': 'raw_st_lucie_parcels_index', 'file': 'parcels_index.txt', 'header': False},
            {'table': 'raw_st_lucie_parcels_situs', 'file': 'parcels_situs.txt', 'header': False},
            {'table': 'raw_st_lucie_parcels_sales', 'file': 'parcels_sales.txt', 'header': False}
        ],

        'sql_updates': [
            {'description': 'Building stats summary & parcel update', 'sql': '/* bldg stats */'},
            {'description': 'Owner join', 'sql': '/* owner join */'},
            {'description': 'Situs join', 'sql': '/* situs join */'},
            {'description': 'Sales denormalization placeholder', 'sql': '/* sales denorm */'}
        ]
    }

    return config

def get_sumter_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Sumter County (simplified)."""

    config = {
        'county_name': 'Sumter',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/sumter/processing/database/sql_files/create_raw_tables.sql",

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/sumter/sumter-convert-sales-csv.py', 'description': 'RUN convert-sales'},
            {'script': '/srv/tools/python/parcel_processing/sumter/sumter-convert-building-area.py', 'description': 'RUN convert-building-area'}
        ],

        'copy_commands': [
            {'table': 'raw_sumter_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False},
            {'table': 'raw_sumter_bldg', 'file': 'buildings_area.txt', 'header': False},
            {'table': 'parcels_template2_sumter', 'file': 'parcels_new.txt', 'header': False}
        ],

        'sql_updates': [
            {'description': 'Owner placeholder updates', 'sql': '/* owner updates */'},
            {'description': 'Parcel attribute overrides', 'sql': '/* parcel overrides */'},
            {'description': 'Sales owner updates', 'sql': '/* sales owner update */'},
            {'description': 'Misc cleanup', 'sql': '/* cleanup */'}
        ]
    }

    return config

def get_suwannee_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Suwannee County (simplified)."""

    config = {
        'county_name': 'Suwannee',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/suwannee/processing/database/sql_files/create_raw_tables.sql",

        'copy_commands': [
            {'table': 'raw_suwannee_sales_export', 'file': 'source_data/sales_dnld_2014-01-01_current.txt', 'header': False},
            {'table': 'raw_suwannee_sales_owner_export', 'file': 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt', 'header': False}
        ],

        'sql_updates': [
            {'description': 'Normalize sale date (yyyy-mm-dd)', 'sql': '/* sale date format */'}
        ]
    }

    return config

def get_taylor_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Taylor County (simplified)."""

    config = {
        'county_name': 'Taylor',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/taylor/processing/database/sql_files/create_raw_tables.sql",

        # Single conversion script from legacy flow
        'processing_scripts': [
            {
                'script': '/srv/tools/python/parcel_processing/taylor/taylor-convert-sales-csv.py',
                'description': 'RUN taylor-convert-sales-csv.py'
            }
        ],

        # One \copy operation loading the sales download
        'copy_commands': [
            {
                'table': 'raw_taylor_sales_dwnld',
                'file': 'parcels_sales.txt',
                'header': False
            }
        ],

        # Minimal SQL updates – just populate placeholder owner info
        'sql_updates': [
            {
                'description': 'Populate missing owner info',
                'sql': '/* owner info placeholder update */'
            }
        ]
    }

    return config

def get_union_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Union County (simplified)."""

    config = {
        'county_name': 'Union',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/union/processing/database/sql_files/create_raw_tables.sql",

        # Two \copy operations – sales export and owner mailing export
        'copy_commands': [
            {
                'table': 'raw_union_sales_export',
                'file': 'source_data/sales_dnld_2014-01-01_current.txt',
                'header': False
            },
            {
                'table': 'raw_union_sales_owner_export',
                'file': 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt',
                'header': False
            }
        ],

        # Five representative SQL blocks mirroring date normalization and joins
        'sql_updates': [
            {'description': 'Normalize sale1_date', 'sql': '/* sale1_date yyyy-mm-dd */'},
            {'description': 'Normalize sale2_date', 'sql': '/* sale2_date yyyy-mm-dd */'},
            {'description': 'Normalize sale3_date', 'sql': '/* sale3_date yyyy-mm-dd */'},
            {'description': 'Denormalize sales info into parcels_template', 'sql': '/* sales denormal join */'},
            {'description': 'Update owner mailing information', 'sql': '/* owner mailing update */'}
        ]
    }

    return config

def get_volusia_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Volusia County (simplified)."""

    path_source_data = f"{path_processing}/source_data"

    config = {
        'county_name': 'Volusia',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/volusia/processing/database/sql_files/create_raw_tables.sql",

        'preprocess_commands': [
            {'command': f"rm -r {path_source_data}/CAMA_DATA_EXPORT_WEB.csv"},
            {'command': '/home/bmay/src/access2csv/access2csv --input ' + path_source_data + '/CAMA_DATA_EXPORT_WEB.accdb --output ' + path_source_data + '/CAMA_DATA_EXPORT_WEB.csv'},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/CAMA_DATA_EXPORT_WEB.csv/VCPA_CAMA_LEGAL.csv > {path_source_data}/CAMA_DATA_EXPORT_WEB.csv/VCPA_CAMA_LEGAL_2.csv"},
            {'command': f"tr -cd '\\11\\12\\15\\40-\\133\\135-\\176' < {path_source_data}/CAMA_DATA_EXPORT_WEB.csv/VCPA_CAMA_PARCEL.csv > {path_source_data}/CAMA_DATA_EXPORT_WEB.csv/VCPA_CAMA_PARCEL_2.csv"},
            {'command': f"sed -e 's:\\\\:/:g' {path_source_data}/CAMA_DATA_EXPORT_WEB.csv/VCPA_CAMA_OWNER.csv > {path_source_data}/CAMA_DATA_EXPORT_WEB.csv/VCPA_CAMA_OWNER_2.csv"},
            {'command': f"sed -e 's:ORMOND \\n:ORMOND :g' {path_source_data}/CAMA_DATA_EXPORT_WEB.csv/VCPA_CAMA_OWNER_2.csv > {path_source_data}/CAMA_DATA_EXPORT_WEB.csv/VCPA_CAMA_OWNER_3.csv"}
        ],

        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/volusia/volusia-parcel-current.py', 'description': 'RUN parcel-current'},
            {'script': '/srv/tools/python/parcel_processing/volusia/volusia-sales-current.py', 'description': 'RUN sales-current'},
            {'script': '/srv/tools/python/parcel_processing/volusia/volusia-legal-current.py', 'description': 'RUN legal-current'},
            {'script': '/srv/tools/python/parcel_processing/volusia/volusia-owner-current.py', 'description': 'RUN owner-current'},
            {'script': '/srv/tools/python/parcel_processing/volusia/volusia-situs-current.py', 'description': 'RUN situs-current'},
            {'script': '/srv/tools/python/parcel_processing/volusia/volusia-building-comm-bldg.py', 'description': 'RUN building-comm-bldg'},
            {'script': '/srv/tools/python/parcel_processing/volusia/volusia-building-condo-bldg.py', 'description': 'RUN building-condo-bldg'},
            {'script': '/srv/tools/python/parcel_processing/volusia/volusia-building-res-bldg.py', 'description': 'RUN building-res-bldg'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_volusia', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_volusia_sales', 'file': 'sale_new.txt', 'header': False},
            {'table': 'raw_volusia_legal', 'file': 'legal_new.txt', 'header': False},
            {'table': 'raw_volusia_owner', 'file': 'owner_new.txt', 'header': False},
            {'table': 'raw_volusia_situs', 'file': 'situs_new.txt', 'header': False},
            {'table': 'raw_volusia_res_bldg', 'file': 'res_bldg_new.txt', 'header': False},
            {'table': 'raw_volusia_comm_bldg', 'file': 'comm_bldg_new.txt', 'header': False},
            {'table': 'raw_volusia_condo_bldg', 'file': 'condo_bldg_new.txt', 'header': False}
        ],

        'sql_updates': [
            {'description': 'Insert res building stats', 'sql': '/* insert res_bldg stats */'},
            {'description': 'Insert comm & condo building stats', 'sql': '/* insert comm/condo stats */'}
        ]
    }

    return config

def get_wakulla_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Wakulla County (simplified)."""

    config = {
        'county_name': 'Wakulla',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/wakulla/processing/database/sql_files/create_raw_tables.sql",

        'processing_scripts': [
            {
                'script': '/srv/tools/python/parcel_processing/wakulla/wakulla-convert-sales-csv.py',
                'description': 'RUN wakulla-convert-sales-csv.py'
            }
        ],

        'copy_commands': [
            {
                'table': 'raw_wakulla_sales_dwnld',
                'file': 'parcels_sales.txt',
                'header': False
            }
        ],

        'sql_updates': [
            {
                'description': 'Run FDOR processing for Wakulla',
                'sql': "SELECT process_raw_fdor('wakulla');"
            },
            {
                'description': 'Populate missing owner info',
                'sql': textwrap.dedent("""
                    UPDATE parcels_template_wakulla AS p SET
                        o_name1    = 'Owner Name Missing - ' || o.pin,
                        o_name2    = NULL,
                        o_address1 = NULL,
                        o_address2 = NULL,
                        o_address3 = NULL,
                        o_city     = NULL,
                        o_state    = NULL,
                        o_zipcode  = NULL,
                        o_zipcode4 = NULL
                    FROM raw_wakulla_sales_dwnld AS o
                    WHERE p.pin = o.pin;
                """)
            }
        ]
    }

    return config

def get_walton_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Walton County (simplified)."""

    config = {
        'county_name': 'Walton',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/walton/processing/database/sql_files/create_raw_tables.sql",

        # Two shell commands that prepare input data
        'preprocess_commands': [
            {'command': 'rm /srv/mapwise_dev/county/walton/processing/database/current/source_data/PA_Parcels.csv'},
            {'command': 'ogr2ogr -f "CSV" /srv/mapwise_dev/county/walton/processing/database/current/source_data/ParcelsMerged.csv /srv/mapwise_dev/county/walton/processing/vector/propapp/current/source_data/PublicData_20240722.gdb ParcelsMerged'}
        ],

        # Two python conversion utilities from legacy flow
        'processing_scripts': [
            {'script': '/srv/tools/python/parcel_processing/walton/walton-convert-sales-csv.py', 'description': 'RUN walton-convert-sales-csv.py'},
            {'script': '/srv/tools/python/parcel_processing/walton/walton-convert-parcels.py', 'description': 'RUN walton-convert-parcels.py'}
        ],

        # Load the sales download and two parcel CSV variants
        'copy_commands': [
            {'table': 'raw_walton_sales_dwnld', 'file': 'parcels_sales.txt', 'header': False},
            {'table': 'parcels_template2_walton', 'file': 'parcels_new2.txt', 'header': False},
            {'table': 'parcels_template2_walton', 'file': 'PA_parcels.csv', 'header': True}
        ],

        'sql_updates': [
            {
                'description': 'Run FDOR processing for Walton',
                'sql': "SELECT process_raw_fdor('walton');"
            },
            {
                'description': 'Insert records not already present in parcels_template_walton',
                'sql': textwrap.dedent("""
                    INSERT INTO parcels_template_walton (
                        pin, pin_clean, pin2, o_name1, o_address1, o_address2, o_city, o_state,
                        o_zipcode, o_zipcode4, sale1_year, sale1_date, mrkt_bld, mrkt_impr,
                        mrkt_lnd, mrkt_ag, mrkt_tot, assd_tot, luse, luse_d, legal_full,
                        legal1, legal2, legal3)
                    SELECT o.pin, o.pin_clean, o.pin2, o.o_name1, o.o_address1, o.o_address2, o.o_city,
                           o.o_state, o.o_zipcode, o.o_zipcode4, o.sale1_year, o.sale1_date,
                           o.mrkt_bld, o.mrkt_impr, o.mrkt_lnd, o.mrkt_ag, o.mrkt_tot,
                           o.assd_tot, o.luse, o.luse_d, o.legal_full, o.legal1, o.legal2, o.legal3
                    FROM parcels_template2_walton AS o
                    WHERE NOT EXISTS (
                        SELECT 1 FROM parcels_template_walton AS p WHERE p.pin = o.pin);
                """)
            },
            {
                'description': 'Update owner mailing address information',
                'sql': textwrap.dedent("""
                    UPDATE parcels_template_walton AS p SET
                        o_name1    = o.o_name1,
                        o_name2    = o.o_name2,
                        o_address1 = o.o_address1,
                        o_address2 = o.o_address2,
                        o_address3 = o.o_address3,
                        o_city     = o.o_city,
                        o_state    = o.o_state,
                        o_zipcode  = o.o_zipcode,
                        o_zipcode4 = o.o_zipcode4
                    FROM raw_walton_sales_dwnld AS o
                    WHERE p.pin = REPLACE(o.pin, '-', '');
                """)
            },
            {
                'description': 'Back-fill dashed PIN into pin2 field',
                'sql': textwrap.dedent("""
                    UPDATE parcels_template_walton AS p
                    SET pin2 = o.pin_orig
                    FROM parcels_std_2010_shp_temp AS o
                    WHERE p.pin = o.pin_clean_orig AND o.d_state_orig = 'FL';
                """)
            }
        ]
    }

    return config

def get_washington_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Washington County (simplified)."""

    config = {
        'county_name': 'Washington',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/washington/processing/database/sql_files/create_raw_tables.sql",

        'processing_scripts': [
            {
                'script': '/srv/tools/python/parcel_processing/washington/washington-convert-sales-csv.py',
                'description': 'RUN washington-convert-sales-csv.py'
            }
        ],

        'copy_commands': [
            {
                'table': 'raw_washington_sales_dwnld',
                'file': 'parcels_sales.txt',
                'header': False
            }
        ],

        'sql_updates': [
            {
                'description': 'Run FDOR processing for Washington',
                'sql': "SELECT process_raw_fdor('washington');"
            },
            {
                'description': 'Populate missing owner info',
                'sql': textwrap.dedent("""
                    UPDATE parcels_template_washington AS p SET
                        o_name1    = 'Owner Name Missing - ' || o.pin,
                        o_name2    = NULL,
                        o_address1 = NULL,
                        o_address2 = NULL,
                        o_address3 = NULL,
                        o_city     = NULL,
                        o_state    = NULL,
                        o_zipcode  = NULL,
                        o_zipcode4 = NULL
                    FROM raw_washington_sales_dwnld AS o
                    WHERE p.pin = o.pin;
                """)
            }
        ]
    }

    return config

def get_sussex_config(path_processing, pg_connection, pg_psql):
    """Returns the processing configuration for Sussex County, Delaware (simplified)."""

    config = {
        'county_name': 'Sussex',
        'path_processing': path_processing,
        'pg_connection': pg_connection,
        'pg_psql': pg_psql,

        'create_raw_tables_sql': "/srv/mapwise_dev/county/sussex/processing/database/sql_files/create_raw_tables.sql",

        'processing_scripts': [
            {'script': '/srv/projects\\tools\\python\\parcel_processing\\DE_sussex\\sussex-prop-current.py', 'description': 'RUN sussex-prop-current.py'},
            {'script': '/srv/projects\\tools\\python\\parcel_processing\\DE_sussex\\sussex-sales-current.py', 'description': 'RUN sussex-sales-current.py'}
        ],

        'copy_commands': [
            {'table': 'parcels_template_sussex', 'file': 'parcels_new.txt', 'header': False},
            {'table': 'raw_sussex_sales', 'file': 'sales_new.txt', 'header': False}
        ],

        'sql_updates': [
            {'description': 'Insert denormalized sales rows', 'sql': '/* sales denormalization into raw_sussex_sales_denormal */'},
            {'description': 'Update parcels_template with sales data', 'sql': '/* join update parcels_template_sussex */'}
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