# Grizzly

"""
BASIC PROCESSING FLOW

1. Create raw tables in postgres
2. Load raw files (sales, owners, parcels)
3. Process raw files
4. Merge into parcels_template_<county> in postgres
5. Update sale info
"""
# =======================================================================================
# GRIZZLY RAW
# - INPUT = Raw text files
# - OUTPUT = raw data tables and parcels_template_<county> postgres tables
# =======================================================================================
def process_raw_grizzly(county):
    # change working directory
    os.chdir(pathProcessing)
    print 'Current working directory: ', os.getcwd()

    county_lower = county.lower()

    # Connect to postgres and open cursor
    connection = psycopg2.connect(pg_connection)
    cursor = connection.cursor()

    # create raw tables
    sql_file = "/srv/mapwise_dev/county/{0}/processing/database/sql_files/create_raw_tables.sql".format(county_lower)
    print 'SQL COMMAND: ', sql_file
    mycmd = pg_psql + ' -f "' + sql_file + '"'
    print mycmd
    os.system(mycmd)

    # -----------------------------------------------------------------------------------------
    # LOAD RAW FILES
    # -----------------------------------------------------------------------------------------
    if county_lower == 'desoto':
        print '\nRUN desoto-convert-land-denormal.py'
        mycmd = '/srv/tools/python/parcel_processing/de_soto/desoto-convert-land-denormal.py'
        print mycmd
        os.system(mycmd)

        sql = "\\copy raw_desoto_land from 'parcels_land.txt' with delimiter as E'\\t' null as ''"
        mycmd = pg_psql + ' -c "' + sql + '"'
        print mycmd
        os.system(mycmd)

    # Load sales export
    copy_options = ''
    if county_lower == 'lafayette':
        copy_options = "WITH CSV HEADER"

    sales_export_file = 'source_data/sales_dnld_2014-01-01_current.txt'
    sql = "\\copy raw_{0}_sales_export from '{1}' {2} delimiter as E'\\t' null as ''".format(county_lower, sales_export_file, copy_options)
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)

    # Load sales owner export
    owner_export_file = 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt'
    sql = "\\copy raw_{0}_sales_owner_export from '{1}' with delimiter as E'\\t' null as ''".format(county_lower, owner_export_file)
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)

    # -----------------------------------------------------------------------------------------
    # PROCESS RAW FILES
    # -----------------------------------------------------------------------------------------

    # Date Formatting
    sales_dates_to_process = ['sale1_date']
    if county_lower in ['bradford', 'columbia', 'desoto', 'okeechobee', 'union']:
        sales_dates_to_process.extend(['sale2_date', 'sale3_date'])

    for sale_date_col in sales_dates_to_process:
        sql = """
            UPDATE raw_{0}_sales_export SET {1} = split_part({1}, '/', 3) || '-' || split_part({1}, '/', 1) || '-' || split_part({1}, '/', 2);
            UPDATE raw_{0}_sales_export SET {1} = split_part({1}, '-', 1) || '-0' || split_part({1}, '-', 2) || '-' || split_part({1}, '-', 3)
                WHERE length(split_part({1}, '-', 2)) = 1;
            UPDATE raw_{0}_sales_export SET {1} = split_part({1}, '-', 1) || '-' || split_part({1}, '-', 2) || '-0' || split_part({1}, '-', 3)
                WHERE length(split_part({1}, '-', 3)) = 1;
        """.format(county_lower, sale_date_col)
        print sql
        cursor.execute(sql)
        connection.commit()

    # PIN Cleaning
    if county_lower in ['bradford', 'desoto', 'lafayette', 'okeechobee']:
        sql = "UPDATE raw_{0}_sales_export SET pin = replace(pin,'-','');".format(county_lower)
        print sql
        cursor.execute(sql)
        connection.commit()

    # -----------------------------------------------------------------------------------------
    # process_raw_fdor - create parcels_template_<county>
    # -----------------------------------------------------------------------------------------
    process_raw_fdor(county)

    # Reconnect to postgres and open cursor
    connection = psycopg2.connect(pg_connection)
    cursor = connection.cursor()

    # County-specific pin_clean update
    if county_lower in ['desoto', 'okeechobee']:
        sql = "UPDATE parcels_template_{0} SET pin_clean = replace(pin,'-','');".format(county_lower)
        print sql
        cursor.execute(sql)
        connection.commit()

    # -----------------------------------------------------------------------------------------
    # update sale info
    # -----------------------------------------------------------------------------------------
    if county_lower not in ['lafayette', 'suwannee']:
        where_clause = ""
        if county_lower in ['bradford', 'desoto']:
            where_clause = "interim.pin = denormal.pin"
        elif county_lower in ['columbia', 'okeechobee', 'union']:
            where_clause = "interim.pin_clean = replace(denormal.pin,'-','')"

        sale_columns = """
            sale1_date = cast(denormal.sale1_date as text),
            sale1_year = CAST(split_part(denormal.sale1_date, '-', 1) as int),
            sale1_amt = denormal.sale1_amt,
            sale1_typ = denormal.sale1_typ,
            sale1_vac = denormal.sale1_vac,
            sale1_qual = denormal.sale1_qual,
            sale1_bk = denormal.sale1_bk,
            sale1_pg = denormal.sale1_pg,
            o_name1 = denormal.o_name1
        """
        if county_lower in ['bradford', 'columbia', 'desoto', 'okeechobee', 'union']:
            sale_columns += """,
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
            sale3_pg = denormal.sale3_pg
            """

        sql = """UPDATE parcels_template_{0} as interim
            SET -- all sales columns
            {1}
            FROM raw_{0}_sales_export as denormal
            WHERE {2};""".format(county_lower, sale_columns, where_clause)

        print sql
        cursor.execute(sql)
        connection.commit()

    # -----------------------------------------------------------------------------------------
    # update owner names
    # -----------------------------------------------------------------------------------------
    owner_update_set_clause = """
            o_name1 = o.o_name1,
            o_address1 = o.o_address1,
            o_address2 = o.o_address2,
            o_city = o.o_city,
            o_state = o.o_state,
            o_zipcode = o.o_zipcode
    """
    if county_lower == 'suwannee':
        owner_update_set_clause = """
            o_address1 = o.o_address1,
            o_address2 = o.o_address2,
            o_city = o.o_city,
            o_state = o.o_state,
            o_zipcode = o.o_zipcode
        """
    
    sql = """
        UPDATE parcels_template_{0} as p SET
        {1}
        FROM raw_{0}_sales_owner_export as o
        WHERE p.o_name1 = o.o_name1
    ;""".format(county_lower, owner_update_set_clause)
    print sql
    cursor.execute(sql)
    connection.commit()
    
    if county_lower == 'lafayette':
        # old logic from lafayette
        sql = """
            UPDATE parcels_template_lafayette as p SET
                o_name1 = o.o_name1,
                o_address1 = null,
                o_address2 = null,
                o_city = null,
                o_state = null,
                o_zipcode = null
                FROM raw_lafayette_sales_export as o
                WHERE p.pin = o.pin and o.sale1_date > '2021-09-01'
        ;"""
        #print sql
        #cursor.execute(sql)
        #connection.commit()

        # split out o_city, o_state, o_zipcode
        sql = """
            UPDATE parcels_template_lafayette as p SET
                o_city = trim(substring(o_address2 from 1 for 20)),
                o_state = substring(o_address2 from 21 for 2),
                o_zipcode = substring(o_address2 from 24 for 5)
                WHERE o_city = '' and o_state = ''
        ;"""
        #print sql
        #cursor.execute(sql)
        #connection.commit()
    
    if county_lower == 'suwannee':
        # 3/2017 - no need to do this now
        # split out o_city, o_state, o_zipcode
        #sql = """
        #    UPDATE parcels_template_suwannee as p SET
        #        o_city = trim(substring(o_address2 from 1 for 20)),
        #        o_state = substring(o_address2 from 21 for 2),
        #        o_zipcode = substring(o_address2 from 24 for 5)
        #        WHERE o_city = '' and o_state = ''
        #;"""
        #print sql
        #cursor.execute(sql)
        #connection.commit()
        pass

    # close communication with the database
    cursor.close()
    connection.close()

