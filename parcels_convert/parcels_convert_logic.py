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
            description = sql_info.get('description', '')
            if description:
                print(f"\n{description}")
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