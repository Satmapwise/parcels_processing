# Grizzly

# =======================================================================================
# BRADFORD COUNTY RAW
# - INPUT = Raw text files
# - OUTPUT = raw data tables and parcels_template_<county> postgres tables
# =======================================================================================
def process_raw_bradford() :

    # change working directory
    os.chdir(pathProcessing)
    print 'Current working directory: ',os.getcwd()

    county_upper = county.upper()
    county_lower = county.lower()

    # Connect to postgres and open cursor
    connection = psycopg2.connect(pg_connection)
    cursor = connection.cursor()    

    # create raw tables
    sql_file = "/srv/mapwise_dev/county/bradford/processing/database/sql_files/create_raw_tables.sql"
    print 'SQL COMMAND: ', sql_file
    mycmd = pg_psql + ' -f "' + sql_file + '"'
    print mycmd
    os.system(mycmd)
    
    #-----------------------------------------------------------------------------------------
    # LOAD RAW FILES
    #-----------------------------------------------------------------------------------------
    #sql = "\\copy raw_bradford_sales_export from 'source_data/sales_dnld_2013-09-01_2013-12-31.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    #mycmd = pg_psql + ' -c "' + sql + '"'
    #print mycmd
    #os.system(mycmd)
    
    # PIN has dashes
    # SHP PIN is clean
    # FDOR parce;_id is clean
    sql = "\\copy raw_bradford_sales_export from 'source_data/sales_dnld_2014-01-01_current.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)
    
    
    sql = "\\copy raw_bradford_sales_owner_export from 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)

    #exit()
    
    # make sure the sales are in yyyy-mm-dd format
    # make pin a clean version
    sql = """
        UPDATE raw_bradford_sales_export SET sale1_date = split_part(sale1_date, '/', 3) || '-' || split_part(sale1_date, '/', 1) || '-' || split_part(sale1_date, '/', 2);
        
        UPDATE raw_bradford_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-0' || split_part(sale1_date, '-', 2) || '-' || split_part(sale1_date, '-', 3)
            WHERE length(split_part(sale1_date, '-', 2)) = 1;

        UPDATE raw_bradford_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-' || split_part(sale1_date, '-', 2) || '-0' || split_part(sale1_date, '-', 3)
            WHERE length(split_part(sale1_date, '-', 3)) = 1;

        UPDATE raw_bradford_sales_export SET pin = replace(pin,'-','');            
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

    sql = """
        UPDATE raw_bradford_sales_export SET sale2_date = split_part(sale2_date, '/', 3) || '-' || split_part(sale2_date, '/', 1) || '-' || split_part(sale2_date, '/', 2);
        
        UPDATE raw_bradford_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-0' || split_part(sale2_date, '-', 2) || '-' || split_part(sale2_date, '-', 3)
            WHERE length(split_part(sale2_date, '-', 2)) = 1;

        UPDATE raw_bradford_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-' || split_part(sale2_date, '-', 2) || '-0' || split_part(sale2_date, '-', 3)
            WHERE length(split_part(sale2_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

    sql = """
        UPDATE raw_bradford_sales_export SET sale3_date = split_part(sale3_date, '/', 3) || '-' || split_part(sale3_date, '/', 1) || '-' || split_part(sale3_date, '/', 2);
        
        UPDATE raw_bradford_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-0' || split_part(sale3_date, '-', 2) || '-' || split_part(sale3_date, '-', 3)
            WHERE length(split_part(sale3_date, '-', 2)) = 1;

        UPDATE raw_bradford_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-' || split_part(sale3_date, '-', 2) || '-0' || split_part(sale3_date, '-', 3)
            WHERE length(split_part(sale3_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()     




    
    #-----------------------------------------------------------------------------------------
    # process_raw_fdor - create parcels_template_<county>
    #-----------------------------------------------------------------------------------------
    process_raw_fdor(county)


    #-----------------------------------------------------------------------------------------
    # update sale info
    #-----------------------------------------------------------------------------------------
    # bring denormalized sales info into parcels_template
    sql = """UPDATE parcels_template_bradford as interim
        SET -- all sales columns
        sale1_date = cast(denormal.sale1_date as text),
        sale1_year = CAST(split_part(denormal.sale1_date, '-', 1) as int),
        sale1_amt = denormal.sale1_amt,
        sale1_typ = denormal.sale1_typ,
        sale1_vac = denormal.sale1_vac,
        sale1_qual = denormal.sale1_qual,
        --sale1_multi = denormal.sale1_multi,
        sale1_bk = denormal.sale1_bk,
        sale1_pg = denormal.sale1_pg,
        --sale1_docnum = denormal.sale1_docnum,
        --sale1_grantor = denormal.sale1_grantor,
        --sale1_grantee = denormal.sale1_grantee,
        sale2_date = cast(denormal.sale2_date as text),
        sale2_year = CAST(split_part(denormal.sale2_date, '-', 1) as int),
        sale2_amt = denormal.sale2_amt,
        sale2_typ = denormal.sale2_typ,
        sale2_vac = denormal.sale2_vac,
        sale2_qual = denormal.sale2_qual,
        --sale2_multi = denormal.sale2_multi,
        sale2_bk = denormal.sale2_bk,
        sale2_pg = denormal.sale2_pg,
        --sale2_docnum = denormal.sale2_docnum,
        --sale2_grantor = denormal.sale2_grantor,
        --sale2_grantee = denormal.sale2_grantee,
        sale3_date = cast(denormal.sale3_date as text),
        sale3_year = CAST(split_part(denormal.sale3_date, '-', 1) as int),
        sale3_amt = denormal.sale3_amt,
        sale3_typ = denormal.sale3_typ,
        sale3_vac = denormal.sale3_vac,
        sale3_qual = denormal.sale3_qual,
        --sale3_multi = denormal.sale3_multi,
        sale3_bk = denormal.sale3_bk,
        sale3_pg = denormal.sale3_pg,
        --sale3_docnum = denormal.sale3_docnum,
        --sale3_grantor = denormal.sale3_grantor,
        --sale3_grantee = denormal.sale3_grantee,
        o_name1 = denormal.o_name1
        FROM raw_bradford_sales_export as denormal
        WHERE interim.pin = denormal.pin;""" 

    print sql
    cursor.execute(sql)
    connection.commit()    

    
    #-----------------------------------------------------------------------------------------
    # update owner names
    #-----------------------------------------------------------------------------------------
    # owner names are included in the sales excport, but not the mailing address
    # we need to do a separate mailing export to get it
    # only need updated owner info for sales that happen after the certified FDOR data
    # so if last updates are in 9/2012, get 9/2012 and later owners
    # can do one file one time for the remainder of 2012, and then 2013_current for the rest
    # PIN	Name	Address1	Address2	City	State	ZIP
    sql = """
        UPDATE parcels_template_bradford as p SET
            o_name1 = o.o_name1,
            o_address1 = o.o_address1,
            o_address2 = o.o_address2,
            --o_address3 = o.o_address3,
            o_city = o.o_city,
            o_state = o.o_state,
            o_zipcode = o.o_zipcode
            FROM raw_bradford_sales_owner_export as o
            WHERE p.o_name1 = o.o_name1
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()
    
    # close communication with the database
    cursor.close()
    connection.close()

# =======================================================================================
# COLUMBIA COUNTY RAW
# - INPUT = Raw text files
# - OUTPUT = raw data tables and parcels_template_<county> postgres tables
# =======================================================================================
def process_raw_columbia() :

    # change working directory
    os.chdir(pathProcessing)
    print 'Current working directory: ',os.getcwd()

    county_upper = county.upper()
    county_lower = county.lower()

    # Connect to postgres and open cursor
    connection = psycopg2.connect(pg_connection)
    cursor = connection.cursor()

    
    # create raw tables
    sql_file = "/srv/mapwise_dev/county/columbia/processing/database/sql_files/create_raw_tables.sql"
    print 'SQL COMMAND: ', sql_file
    mycmd = pg_psql + ' -f "' + sql_file + '"'
    print mycmd
    os.system(mycmd)
    
    #-----------------------------------------------------------------------------------------
    # LOAD RAW FILES
    #-----------------------------------------------------------------------------------------
    #sql = "\\copy raw_columbia_sales_export from 'source_data/sales_dnld_2013-09-01_2013-12-31.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    #mycmd = pg_psql + ' -c "' + sql + '"'
    #print mycmd
    #os.system(mycmd)
    
    sql = "\\copy raw_columbia_sales_export from 'source_data/sales_dnld_2014-01-01_current.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)

    #exit()

    
    # make sure the sales are in yyyy-mm-dd format
    
    sql = """
        UPDATE raw_columbia_sales_export SET sale1_date = split_part(sale1_date, '/', 3) || '-' || split_part(sale1_date, '/', 1) || '-' || split_part(sale1_date, '/', 2);
        
        UPDATE raw_columbia_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-0' || split_part(sale1_date, '-', 2) || '-' || split_part(sale1_date, '-', 3)
            WHERE length(split_part(sale1_date, '-', 2)) = 1;

        UPDATE raw_columbia_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-' || split_part(sale1_date, '-', 2) || '-0' || split_part(sale1_date, '-', 3)
            WHERE length(split_part(sale1_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

    sql = """
        UPDATE raw_columbia_sales_export SET sale2_date = split_part(sale2_date, '/', 3) || '-' || split_part(sale2_date, '/', 1) || '-' || split_part(sale2_date, '/', 2);
        
        UPDATE raw_columbia_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-0' || split_part(sale2_date, '-', 2) || '-' || split_part(sale2_date, '-', 3)
            WHERE length(split_part(sale2_date, '-', 2)) = 1;

        UPDATE raw_columbia_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-' || split_part(sale2_date, '-', 2) || '-0' || split_part(sale2_date, '-', 3)
            WHERE length(split_part(sale2_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

    sql = """
        UPDATE raw_columbia_sales_export SET sale3_date = split_part(sale3_date, '/', 3) || '-' || split_part(sale3_date, '/', 1) || '-' || split_part(sale3_date, '/', 2);
        
        UPDATE raw_columbia_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-0' || split_part(sale3_date, '-', 2) || '-' || split_part(sale3_date, '-', 3)
            WHERE length(split_part(sale3_date, '-', 2)) = 1;

        UPDATE raw_columbia_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-' || split_part(sale3_date, '-', 2) || '-0' || split_part(sale3_date, '-', 3)
            WHERE length(split_part(sale3_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()     

    # PIN	Name	Address1	Address2	City	State	ZIP
    #sql = "\\copy raw_columbia_sales_owner_export from 'source_data/sales_owner_mailing_dnld_2013-09-01_2013-12-31.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    #mycmd = pg_psql + ' -c "' + sql + '"'
    #print mycmd
    #os.system(mycmd)

    sql = "\\copy raw_columbia_sales_owner_export from 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)
    
    #exit()
        
    #-----------------------------------------------------------------------------------------
    # process_raw_fdor - create parcels_template_<county>
    #-----------------------------------------------------------------------------------------
    process_raw_fdor(county)


    # Connect to postgres and open cursor
    connection = psycopg2.connect(pg_connection)
    cursor = connection.cursor()
 
    
    #-----------------------------------------------------------------------------------------
    # update sale info
    #-----------------------------------------------------------------------------------------
    # bring denormalized sales info into parcels_template
    sql = """UPDATE parcels_template_columbia as interim
        SET -- all sales columns
        sale1_date = cast(denormal.sale1_date as text),
        sale1_year = CAST(split_part(denormal.sale1_date, '-', 1) as int),
        sale1_amt = denormal.sale1_amt,
        sale1_typ = denormal.sale1_typ,
        sale1_vac = denormal.sale1_vac,
        sale1_qual = denormal.sale1_qual,
        --sale1_multi = denormal.sale1_multi,
        sale1_bk = denormal.sale1_bk,
        sale1_pg = denormal.sale1_pg,
        --sale1_docnum = denormal.sale1_docnum,
        --sale1_grantor = denormal.sale1_grantor,
        --sale1_grantee = denormal.sale1_grantee,
        sale2_date = cast(denormal.sale2_date as text),
        sale2_year = CAST(split_part(denormal.sale2_date, '-', 1) as int),
        sale2_amt = denormal.sale2_amt,
        sale2_typ = denormal.sale2_typ,
        sale2_vac = denormal.sale2_vac,
        sale2_qual = denormal.sale2_qual,
        --sale2_multi = denormal.sale2_multi,
        sale2_bk = denormal.sale2_bk,
        sale2_pg = denormal.sale2_pg,
        --sale2_docnum = denormal.sale2_docnum,
        --sale2_grantor = denormal.sale2_grantor,
        --sale2_grantee = denormal.sale2_grantee,
        sale3_date = cast(denormal.sale3_date as text),
        sale3_year = CAST(split_part(denormal.sale3_date, '-', 1) as int),
        sale3_amt = denormal.sale3_amt,
        sale3_typ = denormal.sale3_typ,
        sale3_vac = denormal.sale3_vac,
        sale3_qual = denormal.sale3_qual,
        --sale3_multi = denormal.sale3_multi,
        sale3_bk = denormal.sale3_bk,
        sale3_pg = denormal.sale3_pg,
        --sale3_docnum = denormal.sale3_docnum,
        --sale3_grantor = denormal.sale3_grantor,
        --sale3_grantee = denormal.sale3_grantee,
        o_name1 = denormal.o_name1
        FROM raw_columbia_sales_export as denormal
        WHERE interim.pin_clean = replace(denormal.pin,'-',''); """ 

    print sql
    cursor.execute(sql)
    connection.commit()    

    
    #-----------------------------------------------------------------------------------------
    # update owner names
    #-----------------------------------------------------------------------------------------
    # owner names are included in the sales excport, but not the mailing address
    # we need to do a separate mailing export to get it
    # only need updated owner info for sales that happen after the certified FDOR data
    # so if last updates are in 9/2012, get 9/2012 and later owners
    # can do one file one time for the remainder of 2012, and then 2013_current for the rest
    # PIN	Name	Address1	Address2	City	State	ZIP
    sql = """
        UPDATE parcels_template_columbia as p SET
            o_name1 = o.o_name1,
            o_address1 = o.o_address1,
            o_address2 = o.o_address2,
            --o_address3 = o.o_address3,
            o_city = o.o_city,
            o_state = o.o_state,
            o_zipcode = o.o_zipcode
            FROM raw_columbia_sales_owner_export as o
            WHERE p.o_name1 = o.o_name1
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

    # close communication with the database
    cursor.close()
    connection.close()

    #exit()
    
# =======================================================================================
# DESOTO COUNTY RAW
# - INPUT = Raw text files
# - OUTPUT = raw data tables and parcels_template_<county> postgres tables
# =======================================================================================
def process_raw_desoto() :

    # change working directory
    os.chdir(pathProcessing)
    print 'Current working directory: ',os.getcwd()

    county_upper = county.upper()
    county_lower = county.lower()

    # Connect to postgres and open cursor
    connection = psycopg2.connect(pg_connection)
    cursor = connection.cursor()

    # NOTE: Desoto uses docnum now
    
    # create raw tables
    sql_file = "/srv/mapwise_dev/county/desoto/processing/database/sql_files/create_raw_tables.sql"
    print 'SQL COMMAND: ', sql_file
    mycmd = pg_psql + ' -f "' + sql_file + '"'
    print mycmd
    os.system(mycmd)

    #-----------------------------------------------------------------------------------------
    # PROCESS RAW FILES
    #-----------------------------------------------------------------------------------------
    print '\nRUN desoto-convert-land-denormal.py'
    mycmd = '/srv/tools/python/parcel_processing/de_soto/desoto-convert-land-denormal.py'
    print mycmd
    os.system(mycmd)
    
    
    #-----------------------------------------------------------------------------------------
    # LOAD RAW FILES
    #-----------------------------------------------------------------------------------------

    sql = "\\copy raw_desoto_land from 'parcels_land.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)
    
    # Each time you want to add sales for the current year, make a run and add/replace as necessary
    # for example, include previous month to make sure you've got all of the sales, and replace existing previous month
    #sql = "\\copy raw_desoto_sales_export from 'source_data/sales_dnld_2013-09-01_2013-12-31.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    #mycmd = pg_psql + ' -c "' + sql + '"'
    #print mycmd
    #os.system(mycmd)
    
    # Each time you want to add sales for the current year, make a run and add/replace as necessary
    # for example, include previous month to make sure you've got all of the sales, and replace existing previous month
    sql = "\\copy raw_desoto_sales_export from 'source_data/sales_dnld_2014-01-01_current.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)

    # make sure the sales are in yyyy-mm-dd format
    
    sql = """
        UPDATE raw_desoto_sales_export SET sale1_date = split_part(sale1_date, '/', 3) || '-' || split_part(sale1_date, '/', 1) || '-' || split_part(sale1_date, '/', 2);
        
        UPDATE raw_desoto_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-0' || split_part(sale1_date, '-', 2) || '-' || split_part(sale1_date, '-', 3)
            WHERE length(split_part(sale1_date, '-', 2)) = 1;

        UPDATE raw_desoto_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-' || split_part(sale1_date, '-', 2) || '-0' || split_part(sale1_date, '-', 3)
            WHERE length(split_part(sale1_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

    sql = """
        UPDATE raw_desoto_sales_export SET sale2_date = split_part(sale2_date, '/', 3) || '-' || split_part(sale2_date, '/', 1) || '-' || split_part(sale2_date, '/', 2);
        
        UPDATE raw_desoto_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-0' || split_part(sale2_date, '-', 2) || '-' || split_part(sale2_date, '-', 3)
            WHERE length(split_part(sale2_date, '-', 2)) = 1;

        UPDATE raw_desoto_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-' || split_part(sale2_date, '-', 2) || '-0' || split_part(sale2_date, '-', 3)
            WHERE length(split_part(sale2_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

    sql = """
        UPDATE raw_desoto_sales_export SET sale3_date = split_part(sale3_date, '/', 3) || '-' || split_part(sale3_date, '/', 1) || '-' || split_part(sale3_date, '/', 2);
        
        UPDATE raw_desoto_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-0' || split_part(sale3_date, '-', 2) || '-' || split_part(sale3_date, '-', 3)
            WHERE length(split_part(sale3_date, '-', 2)) = 1;

        UPDATE raw_desoto_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-' || split_part(sale3_date, '-', 2) || '-0' || split_part(sale3_date, '-', 3)
            WHERE length(split_part(sale3_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()
    
    
    # strip dashes from pin - they are now putting dashes in main pin
    sql = """
        UPDATE raw_desoto_sales_export SET pin = replace(pin,'-','');
    """
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)

    #sql = "\\copy raw_desoto_sales_owner_export from 'source_data/sales_owner_mailing_dnld_2013-09-01_2013-12-31.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    #mycmd = pg_psql + ' -c "' + sql + '"'
    #print mycmd
    #os.system(mycmd)
    
    sql = "\\copy raw_desoto_sales_owner_export from 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)

    #exit()
    
    #-----------------------------------------------------------------------------------------
    # process_raw_fdor - create parcels_template_<county>
    #-----------------------------------------------------------------------------------------
    process_raw_fdor(county)


    # Connect to postgres and open cursor
    connection = psycopg2.connect(pg_connection)
    cursor = connection.cursor()
    

    #-----------------------------------------------------------------------------------------
    # set pin_clen
    #-----------------------------------------------------------------------------------------
    sql = """
        UPDATE parcels_template_desoto
            SET pin_clean = replace(pin,'-','');
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()
    
    #-----------------------------------------------------------------------------------------
    # update sale info
    #-----------------------------------------------------------------------------------------
    # bring denormalized sales info into parcels_template
    sql = """UPDATE parcels_template_desoto as interim
        SET -- all sales columns
        sale1_date = cast(denormal.sale1_date as text),
        sale1_year = CAST(split_part(denormal.sale1_date, '-', 1) as int),
        sale1_amt = denormal.sale1_amt,
        sale1_typ = denormal.sale1_typ,
        sale1_vac = denormal.sale1_vac,
        sale1_qual = denormal.sale1_qual,
        --sale1_multi = denormal.sale1_multi,
        sale1_bk = denormal.sale1_bk,
        sale1_pg = denormal.sale1_pg,
        --sale1_docnum = denormal.sale1_docnum,
        --sale1_grantor = denormal.sale1_grantor,
        --sale1_grantee = denormal.sale1_grantee,
        sale2_date = cast(denormal.sale2_date as text),
        sale2_year = CAST(split_part(denormal.sale2_date, '-', 1) as int),
        sale2_amt = denormal.sale2_amt,
        sale2_typ = denormal.sale2_typ,
        sale2_vac = denormal.sale2_vac,
        sale2_qual = denormal.sale2_qual,
        --sale2_multi = denormal.sale2_multi,
        sale2_bk = denormal.sale2_bk,
        sale2_pg = denormal.sale2_pg,
        --sale2_docnum = denormal.sale2_docnum,
        --sale2_grantor = denormal.sale2_grantor,
        --sale2_grantee = denormal.sale2_grantee,
        sale3_date = cast(denormal.sale3_date as text),
        sale3_year = CAST(split_part(denormal.sale3_date, '-', 1) as int),
        sale3_amt = denormal.sale3_amt,
        sale3_typ = denormal.sale3_typ,
        sale3_vac = denormal.sale3_vac,
        sale3_qual = denormal.sale3_qual,
        --sale3_multi = denormal.sale3_multi,
        sale3_bk = denormal.sale3_bk,
        sale3_pg = denormal.sale3_pg,
        --sale3_docnum = denormal.sale3_docnum,
        --sale3_grantor = denormal.sale3_grantor,
        --sale3_grantee = denormal.sale3_grantee
        o_name1 = denormal.o_name1
        FROM raw_desoto_sales_export as denormal
        WHERE interim.pin = denormal.pin;""" 

    print sql
    cursor.execute(sql)
    connection.commit()    

    
    #-----------------------------------------------------------------------------------------
    # update owner names
    #-----------------------------------------------------------------------------------------
    # owner names are included in the sales excport, but not the mailing address
    # we need to do a separate mailing export to get it
    # only need updated owner info for sales that happen after the certified FDOR data
    # so if last updates are in 9/2012, get 9/2012 and later owners
    # can do one file one time for the remainder of 2012, and then 2013_current for the rest
    # PIN	Name	Address1	Address2	City	State	ZIP
    sql = """
        UPDATE parcels_template_desoto as p SET
            o_name1 = o.o_name1,
            o_address1 = o.o_address1,
            o_address2 = o.o_address2,
            --o_address3 = o.o_address3,
            o_city = o.o_city,
            o_state = o.o_state,
            o_zipcode = o.o_zipcode
            FROM raw_desoto_sales_owner_export as o
            WHERE p.o_name1 = o.o_name1
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()
    
    # close communication with the database
    cursor.close()
    connection.close()

    #exit()

# =======================================================================================
# LAFAYETTE COUNTY RAW
# - INPUT = Raw text files
# - OUTPUT = raw data tables and parcels_template_<county> postgres tables
# =======================================================================================
def process_raw_lafayette() :

    # change working directory
    os.chdir(pathProcessing)
    print 'Current working directory: ',os.getcwd()

    county_upper = county.upper()
    county_lower = county.lower()

    # Connect to postgres and open cursor
    connection = psycopg2.connect(pg_connection)
    cursor = connection.cursor()

    # create raw tables
    sql_file = "/srv/mapwise_dev/county/lafayette/processing/database/sql_files/create_raw_tables.sql"
    print 'SQL COMMAND: ', sql_file
    mycmd = pg_psql + ' -f "' + sql_file + '"'
    print mycmd
    os.system(mycmd)
    

    #-----------------------------------------------------------------------------------------
    # LOAD RAW FILES
    #-----------------------------------------------------------------------------------------
    #sql = "\\copy raw_lafayette_sales_export from 'source_data/sales_dnld_2013-09-01_2013-12-31.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    #mycmd = pg_psql + ' -c "' + sql + '"'
    #print mycmd
    #os.system(mycmd)

    # Each time you want to add sales for the current year, make a run and add/replace as necessary
    # for example, include previous month to make sure you've got all of the sales, and replace existing previous month
    sql = "\\copy raw_lafayette_sales_export from 'source_data/sales_dnld_2014-01-01_current.txt' WITH CSV HEADER delimiter as E'\\t' null as ''"
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)
    
    sql = "\\copy raw_lafayette_sales_owner_export from 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)
    
    # make sure the sales are in yyyy-mm-dd format
    # make pin a clean version
    sql = """
        UPDATE raw_lafayette_sales_export SET sale1_date = split_part(sale1_date, '/', 3) || '-' || split_part(sale1_date, '/', 1) || '-' || split_part(sale1_date, '/', 2);
        
        UPDATE raw_lafayette_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-0' || split_part(sale1_date, '-', 2) || '-' || split_part(sale1_date, '-', 3)
            WHERE length(split_part(sale1_date, '-', 2)) = 1;

        UPDATE raw_lafayette_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-' || split_part(sale1_date, '-', 2) || '-0' || split_part(sale1_date, '-', 3)
            WHERE length(split_part(sale1_date, '-', 3)) = 1;

        UPDATE raw_lafayette_sales_export SET pin = replace(pin,'-','');
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

    #exit()
    
    #-----------------------------------------------------------------------------------------
    # process_raw_fdor - create parcels_template_<county>
    #-----------------------------------------------------------------------------------------
    process_raw_fdor(county)

    #exit()
    #-----------------------------------------------------------------------------------------
    # update owner names
    #-----------------------------------------------------------------------------------------
    # owner names and mailing addresses are included in the sales export
    # only need updated owner info for sales that happen after the certified FDOR data
    # so if last updates are in 9/2012, get 9/2012 and later owners
    # can do one file one time for the remainder of 2012, and then 2013_current for the rest
    # PIN	Name	Address1	Address2	City	State	ZIP
    #
    # new 3/15/2022
    sql = """
        UPDATE parcels_template_lafayette as p SET
            o_name1 = o.o_name1,
            o_address1 = o.o_address1,
            o_address2 = o.o_address2,
            --o_address3 = o.o_address3,
            o_city = o.o_city,
            o_state = o.o_state,
            o_zipcode = o.o_zipcode
            FROM raw_lafayette_sales_owner_export as o
            WHERE p.o_name1 = o.o_name1
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()
    
    # old
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
    
    
    # close communication with the database
    cursor.close()
    connection.close()

# =======================================================================================
# OKEECHOBEE COUNTY RAW
# - INPUT = Raw text files
# - OUTPUT = raw data tables and parcels_template_<county> postgres tables
# =======================================================================================
def process_raw_okeechobee() :

    # change working directory
    os.chdir(pathProcessing)
    print 'Current working directory: ',os.getcwd()

    county_upper = county.upper()
    county_lower = county.lower()

    # Connect to postgres and open cursor
    connection = psycopg2.connect(pg_connection)
    cursor = connection.cursor()

    
    # create raw tables
    sql_file = "/srv/mapwise_dev/county/okeechobee/processing/database/sql_files/create_raw_tables.sql"
    print 'SQL COMMAND: ', sql_file
    mycmd = pg_psql + ' -f "' + sql_file + '"'
    print mycmd
    os.system(mycmd)
    
    #-----------------------------------------------------------------------------------------
    # LOAD RAW FILES
    #-----------------------------------------------------------------------------------------


    # Each time you want to add sales for the current year, make a run and add/replace as necessary
    # for example, include previous month to make sure you've got all of the sales, and replace existing previous month
    #sql = "\\copy raw_okeechobee_sales_export from 'source_data/sales_dnld_2013-09-01_2013-12-31.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    #mycmd = pg_psql + ' -c "' + sql + '"'
    #print mycmd
    #os.system(mycmd)

    # Each time you want to add sales for the current year, make a run and add/replace as necessary
    # for example, include previous month to make sure you've got all of the sales, and replace existing previous month
    sql = "\\copy raw_okeechobee_sales_export from 'source_data/sales_dnld_2014-01-01_current.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)    

    #exit()
    
    # make sure the sales are in yyyy-mm-dd format
    
    sql = """
        UPDATE raw_okeechobee_sales_export SET sale1_date = split_part(sale1_date, '/', 3) || '-' || split_part(sale1_date, '/', 1) || '-' || split_part(sale1_date, '/', 2);
        
        UPDATE raw_okeechobee_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-0' || split_part(sale1_date, '-', 2) || '-' || split_part(sale1_date, '-', 3)
            WHERE length(split_part(sale1_date, '-', 2)) = 1;

        UPDATE raw_okeechobee_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-' || split_part(sale1_date, '-', 2) || '-0' || split_part(sale1_date, '-', 3)
            WHERE length(split_part(sale1_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

    sql = """
        UPDATE raw_okeechobee_sales_export SET sale2_date = split_part(sale2_date, '/', 3) || '-' || split_part(sale2_date, '/', 1) || '-' || split_part(sale2_date, '/', 2);
        
        UPDATE raw_okeechobee_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-0' || split_part(sale2_date, '-', 2) || '-' || split_part(sale2_date, '-', 3)
            WHERE length(split_part(sale2_date, '-', 2)) = 1;

        UPDATE raw_okeechobee_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-' || split_part(sale2_date, '-', 2) || '-0' || split_part(sale2_date, '-', 3)
            WHERE length(split_part(sale2_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

    sql = """
        UPDATE raw_okeechobee_sales_export SET sale3_date = split_part(sale3_date, '/', 3) || '-' || split_part(sale3_date, '/', 1) || '-' || split_part(sale3_date, '/', 2);
        
        UPDATE raw_okeechobee_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-0' || split_part(sale3_date, '-', 2) || '-' || split_part(sale3_date, '-', 3)
            WHERE length(split_part(sale3_date, '-', 2)) = 1;

        UPDATE raw_okeechobee_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-' || split_part(sale3_date, '-', 2) || '-0' || split_part(sale3_date, '-', 3)
            WHERE length(split_part(sale3_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()   
  

    #-----------------------------------------------------------------------------------------
    # 10/2020 - pin now has dashes and main pin does not
    #-----------------------------------------------------------------------------------------
    sql = """
        UPDATE raw_okeechobee_sales_export
            SET pin = replace(pin,'-','');
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()
    
    
    # PIN	Name	Address1	Address2	City	State	ZIP
    #sql = "\\copy raw_okeechobee_sales_owner_export from 'source_data/sales_owner_mailing_dnld_2013-09-01_2013-12-31.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    #mycmd = pg_psql + ' -c "' + sql + '"'
    #print mycmd
    #os.system(mycmd)

    sql = "\\copy raw_okeechobee_sales_owner_export from 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)
    
    #-----------------------------------------------------------------------------------------
    # process_raw_fdor - create parcels_template_<county>
    #-----------------------------------------------------------------------------------------
    process_raw_fdor(county)

    
    #-----------------------------------------------------------------------------------------
    # set pin_clen
    #-----------------------------------------------------------------------------------------
    sql = """
        UPDATE parcels_template_okeechobee
            SET pin_clean = replace(pin,'-','');
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()
 
    
    #-----------------------------------------------------------------------------------------
    # update sale info
    #-----------------------------------------------------------------------------------------
    # bring denormalized sales info into parcels_template
    sql = """UPDATE parcels_template_okeechobee as interim
        SET -- all sales columns
        sale1_date = cast(denormal.sale1_date as text),
        sale1_year = CAST(split_part(denormal.sale1_date, '-', 1) as int),
        sale1_amt = denormal.sale1_amt,
        sale1_typ = denormal.sale1_typ,
        sale1_vac = denormal.sale1_vac,
        sale1_qual = denormal.sale1_qual,
        --sale1_multi = denormal.sale1_multi,
        sale1_bk = denormal.sale1_bk,
        sale1_pg = denormal.sale1_pg,
        --sale1_docnum = denormal.sale1_docnum,
        --sale1_grantor = denormal.sale1_grantor,
        --sale1_grantee = denormal.sale1_grantee,
        sale2_date = cast(denormal.sale2_date as text),
        sale2_year = CAST(split_part(denormal.sale2_date, '-', 1) as int),
        sale2_amt = denormal.sale2_amt,
        sale2_typ = denormal.sale2_typ,
        sale2_vac = denormal.sale2_vac,
        sale2_qual = denormal.sale2_qual,
        --sale2_multi = denormal.sale2_multi,
        sale2_bk = denormal.sale2_bk,
        sale2_pg = denormal.sale2_pg,
        --sale2_docnum = denormal.sale2_docnum,
        --sale2_grantor = denormal.sale2_grantor,
        --sale2_grantee = denormal.sale2_grantee,
        sale3_date = cast(denormal.sale3_date as text),
        sale3_year = CAST(split_part(denormal.sale3_date, '-', 1) as int),
        sale3_amt = denormal.sale3_amt,
        sale3_typ = denormal.sale3_typ,
        sale3_vac = denormal.sale3_vac,
        sale3_qual = denormal.sale3_qual,
        --sale3_multi = denormal.sale3_multi,
        sale3_bk = denormal.sale3_bk,
        sale3_pg = denormal.sale3_pg,
        --sale3_docnum = denormal.sale3_docnum,
        --sale3_grantor = denormal.sale3_grantor,
        --sale3_grantee = denormal.sale3_grantee,
    
        
        o_name1 = denormal.o_name1
        FROM raw_okeechobee_sales_export as denormal
        WHERE interim.pin_clean = replace(denormal.pin,'-','');""" 

    print sql
    cursor.execute(sql)
    connection.commit()    

    
    #-----------------------------------------------------------------------------------------
    # update owner names
    #-----------------------------------------------------------------------------------------
    # owner names are included in the sales excport, but not the mailing address
    # we need to do a separate mailing export to get it
    # only need updated owner info for sales that happen after the certified FDOR data
    # so if last updates are in 9/2012, get 9/2012 and later owners
    # can do one file one time for the remainder of 2012, and then 2013_current for the rest
    # PIN	Name	Address1	Address2	City	State	ZIP
    sql = """
        UPDATE parcels_template_okeechobee as p SET
            o_name1 = o.o_name1,
            o_address1 = o.o_address1,
            o_address2 = o.o_address2,
            o_city = o.o_city,
            o_state = o.o_state,
            o_zipcode = o.o_zipcode
            FROM raw_okeechobee_sales_owner_export as o
            WHERE p.o_name1 = o.o_name1
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

    # close communication with the database
    cursor.close()
    connection.close()
    #exit()

# =======================================================================================
# SUWANNEE COUNTY RAW
# - INPUT = Raw text files
# - OUTPUT = raw data tables and parcels_template_<county> postgres tables
# =======================================================================================
def process_raw_suwannee() :

    # change working directory
    os.chdir(pathProcessing)
    print 'Current working directory: ',os.getcwd()

    county_upper = county.upper()
    county_lower = county.lower()


    # Connect to postgres and open cursor
    connection = psycopg2.connect(pg_connection)
    cursor = connection.cursor()
    

    
    # create raw tables
    sql_file = "/srv/mapwise_dev/county/suwannee/processing/database/sql_files/create_raw_tables.sql"
    print 'SQL COMMAND: ', sql_file
    mycmd = pg_psql + ' -f "' + sql_file + '"'
    print mycmd
    os.system(mycmd)
    
    #-----------------------------------------------------------------------------------------
    # LOAD RAW FILES
    #-----------------------------------------------------------------------------------------

    # CHANGE THIS EVERY YEAR AT CERTIFIED TIME?
    # SaleDate	Price	V/I	Qual	OR	Book	Page	Name	Street#	StreetName	City	ZIP	PIN	Sec	Twp	Rng	Use	AssdValue	TaxValue
    # where did fdor sales leave off for 2012 certified? FDOR has 208 out 604 9/2012 sales, so get 9/2012 to current
    
    #sql = "\\copy raw_suwannee_sales_export from 'source_data/sales_dnld_2013-09-01_2013-12-31.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    #mycmd = pg_psql + ' -c "' + sql + '"'
    #print mycmd
    #os.system(mycmd)

    # Each time you want to add sales for the current year, make a run and add/replace as necessary
    # for example, include previous month to make sure you've got all of the sales, and replace existing previous month
    sql = "\\copy raw_suwannee_sales_export from 'source_data/sales_dnld_2014-01-01_current.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)

    # make sure the sales are in yyyy-mm-dd format
    
    sql = """
        UPDATE raw_suwannee_sales_export SET sale1_date = split_part(sale1_date, '/', 3) || '-' || split_part(sale1_date, '/', 1) || '-' || split_part(sale1_date, '/', 2);
        
        UPDATE raw_suwannee_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-0' || split_part(sale1_date, '-', 2) || '-' || split_part(sale1_date, '-', 3)
            WHERE length(split_part(sale1_date, '-', 2)) = 1;

        UPDATE raw_suwannee_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-' || split_part(sale1_date, '-', 2) || '-0' || split_part(sale1_date, '-', 3)
            WHERE length(split_part(sale1_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()


    sql = "\\copy raw_suwannee_sales_owner_export from 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt' with delimiter as E'\\t' null as ''"
    #print 'SQL COMMAND: ', sql
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)
    
    #exit()
    
    #-----------------------------------------------------------------------------------------
    # process_raw_fdor - create parcels_template_<county>
    #-----------------------------------------------------------------------------------------
    process_raw_fdor(county)


    #-----------------------------------------------------------------------------------------
    # update owner names
    #-----------------------------------------------------------------------------------------
    # owner names and mailing addresses are included in the sales export
    # only need updated owner info for sales that happen after the certified FDOR data
    # so if last updates are in 9/2012, get 9/2012 and later owners
    # can do one file one time for the remainder of 2012, and then 2013_current for the rest
    # PIN	Name	Address1	Address2	City	State	ZIP
    #
    # !!!! UPDATE ME FOR new fdor files in 2013 !!!
    
    sql = """
        UPDATE parcels_template_suwannee as p SET
            --o_name1 = o.o_name1,
            --o_name2 = o.o_name2,
            --o_name3 = o.o_name3,
            o_address1 = o.o_address1,
            o_address2 = o.o_address2,
            o_city = o.o_city,
            o_state = o.o_state,
            o_zipcode = o.o_zipcode
            FROM raw_suwannee_sales_owner_export as o
            WHERE p.o_name1 = o.o_name1 
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

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
    
    # exit()
    
    # close communication with the database
    cursor.close()
    connection.close()

# =======================================================================================
# UNION COUNTY RAW
# - INPUT = Raw text files
# - OUTPUT = raw data tables and parcels_template_<county> postgres tables
# =======================================================================================
def process_raw_union() :

    # change working directory
    os.chdir(pathProcessing)
    print 'Current working directory: ',os.getcwd()

    county_upper = county.upper()
    county_lower = county.lower()

    # Connect to postgres and open cursor
    connection = psycopg2.connect(pg_connection)
    cursor = connection.cursor()

    # create raw tables
    sql_file = "/srv/mapwise_dev/county/union/processing/database/sql_files/create_raw_tables.sql"
    print 'SQL COMMAND: ', sql_file
    mycmd = pg_psql + ' -f "' + sql_file + '"'
    print mycmd
    os.system(mycmd)
    
    #-----------------------------------------------------------------------------------------
    # LOAD RAW FILES
    #-----------------------------------------------------------------------------------------
    
    sql = "\\copy raw_union_sales_export from 'source_data/sales_dnld_2014-01-01_current.txt' with delimiter as E'\\t' null as ''"
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)

    # make sure the sales are in yyyy-mm-dd format
    
    sql = """
        UPDATE raw_union_sales_export SET sale1_date = split_part(sale1_date, '/', 3) || '-' || split_part(sale1_date, '/', 1) || '-' || split_part(sale1_date, '/', 2);
        
        UPDATE raw_union_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-0' || split_part(sale1_date, '-', 2) || '-' || split_part(sale1_date, '-', 3)
            WHERE length(split_part(sale1_date, '-', 2)) = 1;

        UPDATE raw_union_sales_export SET sale1_date = split_part(sale1_date, '-', 1) || '-' || split_part(sale1_date, '-', 2) || '-0' || split_part(sale1_date, '-', 3)
            WHERE length(split_part(sale1_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

    sql = """
        UPDATE raw_union_sales_export SET sale2_date = split_part(sale2_date, '/', 3) || '-' || split_part(sale2_date, '/', 1) || '-' || split_part(sale2_date, '/', 2);
        
        UPDATE raw_union_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-0' || split_part(sale2_date, '-', 2) || '-' || split_part(sale2_date, '-', 3)
            WHERE length(split_part(sale2_date, '-', 2)) = 1;

        UPDATE raw_union_sales_export SET sale2_date = split_part(sale2_date, '-', 1) || '-' || split_part(sale2_date, '-', 2) || '-0' || split_part(sale2_date, '-', 3)
            WHERE length(split_part(sale2_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

    sql = """
        UPDATE raw_union_sales_export SET sale3_date = split_part(sale3_date, '/', 3) || '-' || split_part(sale3_date, '/', 1) || '-' || split_part(sale3_date, '/', 2);
        
        UPDATE raw_union_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-0' || split_part(sale3_date, '-', 2) || '-' || split_part(sale3_date, '-', 3)
            WHERE length(split_part(sale3_date, '-', 2)) = 1;

        UPDATE raw_union_sales_export SET sale3_date = split_part(sale3_date, '-', 1) || '-' || split_part(sale3_date, '-', 2) || '-0' || split_part(sale3_date, '-', 3)
            WHERE length(split_part(sale3_date, '-', 3)) = 1;        
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()     

    # PIN	Name	Address1	Address2	City	State	ZIP

    sql = "\\copy raw_union_sales_owner_export from 'source_data/sales_owner_mailing_dnld_2014-01-01_current.txt' with delimiter as E'\\t' null as ''"
    mycmd = pg_psql + ' -c "' + sql + '"'
    print mycmd
    os.system(mycmd)

    #exit()
    
    #-----------------------------------------------------------------------------------------
    # process_raw_fdor - create parcels_template_<county>
    #-----------------------------------------------------------------------------------------
    process_raw_fdor(county)


    #-----------------------------------------------------------------------------------------
    # update sale info
    #-----------------------------------------------------------------------------------------
    # bring denormalized sales info into parcels_template
    # 2/28/2023 - QUESTION: THIS IS JUST REPLACING WHAT IS THERE - SHOULDN'T WE BE DENPORMIZING AND ADDING THIS TO THE 
    # denomalized table used in FDOR processing?
    sql = """UPDATE parcels_template_union as interim
        SET -- all sales columns
        sale1_date = cast(denormal.sale1_date as text),
        sale1_year = CAST(split_part(denormal.sale1_date, '-', 1) as int),
        sale1_amt = denormal.sale1_amt,
        sale1_typ = denormal.sale1_typ,
        sale1_vac = denormal.sale1_vac,
        sale1_qual = denormal.sale1_qual,
        --sale1_multi = denormal.sale1_multi,
        sale1_bk = denormal.sale1_bk,
        sale1_pg = denormal.sale1_pg,
        --sale1_docnum = denormal.sale1_docnum,
        --sale1_grantor = denormal.sale1_grantor,
        --sale1_grantee = denormal.sale1_grantee,
        sale2_date = cast(denormal.sale2_date as text),
        sale2_year = CAST(split_part(denormal.sale2_date, '-', 1) as int),
        sale2_amt = denormal.sale2_amt,
        sale2_typ = denormal.sale2_typ,
        sale2_vac = denormal.sale2_vac,
        sale2_qual = denormal.sale2_qual,
        --sale2_multi = denormal.sale2_multi,
        sale2_bk = denormal.sale2_bk,
        sale2_pg = denormal.sale2_pg,
        --sale2_docnum = denormal.sale2_docnum,
        --sale2_grantor = denormal.sale2_grantor,
        --sale2_grantee = denormal.sale2_grantee,
        sale3_date = cast(denormal.sale3_date as text),
        sale3_year = CAST(split_part(denormal.sale3_date, '-', 1) as int),
        sale3_amt = denormal.sale3_amt,
        sale3_typ = denormal.sale3_typ,
        sale3_vac = denormal.sale3_vac,
        sale3_qual = denormal.sale3_qual,
        --sale3_multi = denormal.sale3_multi,
        sale3_bk = denormal.sale3_bk,
        sale3_pg = denormal.sale3_pg,
        --sale3_docnum = denormal.sale3_docnum,
        --sale3_grantor = denormal.sale3_grantor,
        --sale3_grantee = denormal.sale3_grantee,
        o_name1 = denormal.o_name1
        FROM raw_union_sales_export as denormal
        WHERE interim.pin_clean = replace(denormal.pin, '-','');""" 

    print sql
    cursor.execute(sql)
    connection.commit()    

    
    #-----------------------------------------------------------------------------------------
    # update owner names
    #-----------------------------------------------------------------------------------------
    # owner names are included in the sales excport, but not the mailing address
    # we need to do a separate mailing export to get it
    # only need updated owner info for sales that happen after the certified FDOR data
    # so if last updates are in 9/2012, get 9/2012 and later owners
    # can do one file one time for the remainder of 2012, and then 2013_current for the rest
    # PIN	Name	Address1	Address2	City	State	ZIP
    sql = """
        UPDATE parcels_template_union as p SET
            o_name1 = o.o_name1,
            o_address1 = o.o_address1,
            o_address2 = o.o_address2,
            --o_address3 = o.o_address3,
            o_city = o.o_city,
            o_state = o.o_state,
            o_zipcode = o.o_zipcode
            FROM raw_union_sales_owner_export as o
            WHERE p.o_name1 = o.o_name1
    ;"""
    print sql
    cursor.execute(sql)
    connection.commit()

    
    # close communication with the database
    cursor.close()
    connection.close()

