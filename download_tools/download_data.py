#!/usr/bin/python

# download_data.py
# original code - 12/30/2016
# goal - generic script to download any data represented by a web resource
# for now, just zip files
# data must be represented in data catalog table
# overwrites file if remote file is newer
# uses wget to grab the file
#
# required metadata fields:
#   resource - API-like reference to the data, e.g.  /data/streets/palm_beach
#   src_url_file
#   sys_raw_folder
#
# optional metadata fields:
#   sys_raw_file
#   sys_raw_zip_file
#   category - category matches the categories of data under M:\docs\a_data, e.g. 05_parcels
#   sub_category - category matches the sub categories of data under M:/docs/a_data/<category>
#   distrib_comments
#   src_url


import sys,os,fileinput,string,math,psycopg2,io,datetime,subprocess
import psycopg2.extras, smtplib, textwrap


# ===============================================================================================
#  download_data
#  - 
# ===============================================================================================
def download_data(resource) :
     
    pg_connection = 'host=localhost port=5432 dbname=gisdev user=postgres password=galactic529'
    pg_psql = 'psql -h localhost -p 5432 -d gisdev -U postgres '

    # Connect to postgres and open cursor
    connection = psycopg2.connect(pg_connection)
    cursor = connection.cursor()

    # get all column names - select column_name from information_schema.columns where table_name='parcels_martin_fdor';
    sql = """ SELECT src_url_file, sys_raw_folder, sys_raw_file, category, sub_category, distrib_comments, src_url, title
        FROM fmo.m_gis_data_catalog_main
        WHERE resource = '""" + resource + """';"""
    print(sql)
    cursor.execute(sql)
    connection.commit()

    src_url_file = ''
    title = ''
	
    rows = cursor.fetchall()
    
    if len(rows) == 0:
        print("\nNo records selected.")
        print("len(rows): ",len(rows))
    
    for row in rows:
        # colname = row['column_name'] # get TypeError: tuple indices must be integers, not str error
        src_url_file = row[0]
        sys_raw_folder = row[1]
        sys_raw_file = row[2]
        category = row[3]
        sub_category = row[4]
        distrib_comments = row[5]
        src_url = row[6]
        title = row[7]
    
    
    print("title: ",title)
    
    # if there is a src_url_file, download the file to sys_raw_folder
    if len(src_url_file) > 4 :
        # set working directory to sys_raw_folder
        if os.path.isdir(sys_raw_folder) :
            os.chdir(sys_raw_folder)
        else :
            print("sys_raw_folder: '", sys_raw_folder,"' does not exist")
            return

        # use subprocess.run to capture wget output
        mycmd = """wget -N """ + src_url_file
        print(mycmd)
        
        # Run wget and capture output
        result = subprocess.run(mycmd, shell=True, capture_output=True, text=True)
        
        # Print the output so parent script can parse it
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        
        # Check if no new data was available
        output_lower = (result.stdout + result.stderr).lower()
        if ('304 not modified' in output_lower or 
            'not modified on server' in output_lower or 
            'omitting download' in output_lower):
            print("No new data available from server")
            return 1  # Return exit code 1 to indicate no new data
    else :
        print("src_url_file: '", src_url_file,"' does not exist")
        return
        

# -----------------------------------------------------------------------------
# END FUNCTIONS
# -----------------------------------------------------------------------------

# define all text messages here
#
msgUsage = "Usage: download_data <resource> "
msgInvalidResource = "Resource example: /data/streets/palm_beach    "

#
# retrieve the required parameters
#
try:
    resource = sys.argv[1]

    
    #try:
    #    extraArgs = sys.argv[6]
    #except:
    #    pass
    
except:
    print(msgUsage)
    print(msgInvalidResource)
    sys.exit(0)


exit_code = download_data(resource)
if exit_code:
    sys.exit(exit_code)

