#!/usr/bin/python
# ags_extract_data.py
# 
# History:
#   original code: 2016 time frame?
# 
# USAGE: ./ags_extract_data.py <layer_table_name> {delete|nodelete} {delay in seconds} {field_names}
#
# PREREQUISITE:
#
#  Update m_gis_data_catalog_main:
#   - Add / update record for the layer to be downloaded.
#   - Fill in required fields (review previously entered county for example).
# 
#

# Import needed modules
import sys,os,fileinput,string,math,psycopg2,StringIO,datetime,urllib,urllib2,json,shutil,time
import psycopg2.extras

#
# retrieve the required parameters
#
msgUsage = 'ags_extract_data.py <layer_table_name> {delete|nodelete} {delay in seconds} {field_names}'

try:
    layer = sys.argv[1].lower()
    
    try:
        delete_existing = sys.argv[2].upper()
    except:
        delete_existing = 'FALSE'
    try:
        delay = float(sys.argv[3])
    except:
        delay = 15
    try:
        field_names = sys.argv[4].upper()
    except:
        field_names = '*'
    
except:
    print msgUsage
    sys.exit(0)


pg_connection = 'host=localhost port=5432 dbname=gisdev user=postgres password=galactic529'
pg_psql = 'psql -h localhost -p 5432 -d gisdev -U postgres -c '


# connect to database
connection = psycopg2.connect(pg_connection)

# arg to cursor allows selection by column name
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

# pump up the memory
sql = """set work_mem = '2000MB';"""
#print sql    
cursor.execute(sql)
connection.commit()

print " "
print " START layer extract from AGS \n"

#-----------------------------------------------------------------------------------------
# Get data info from m_gis_data_catalog_main table
#-----------------------------------------------------------------------------------------    
sql = """SELECT * FROM m_gis_data_catalog_main WHERE table_name = '""" + layer + """';  """
#print " "
#print sql
cursor.execute(sql)
connection.commit()

# initialize variables that have info about the SHP from parcel_shp_fields
rows = cursor.fetchall()
for row in rows:
    #shp_name = row['sys_raw_file']
    #print 'shp_name:',shp_name
    layer_url = row['src_url_file']
    temp_table_name = row['table_name']
    sys_raw_folder = row['sys_raw_folder']
    #columns_transform = row['fields_obj_transform']
    srs_epsg = str(row['srs_epsg']) 
    
    #shp_date = str(row['shp_date'])
    #shp_pin_clean = row['shp_pin_clean']
    #import_fields = row['import_fields']

# close communication with the database
cursor.close()
connection.close()


# Set working directory
    
os.chdir(sys_raw_folder)
print 'CWD: ', os.getcwd()


#-----------------------------------------------------------------------------------------
# 
#-----------------------------------------------------------------------------------------

url = layer_url
out_file_file = temp_table_name
out_file = out_file_file + ".geojson"

# esri2geojson http://cookviewer1.cookcountyil.gov/ArcGIS/rest/services/cookVwrDynmc/MapServer/11 cookcounty.geojson
# esri2geojson https://gis.lakecountyfl.gov/lakegis/rest/services/LocalGov/CityFLU/MapServer/9 flu_minneola.geojson

#if delete_existing == 'DELETE' and not (os.path.isfile(out_file)):

# Get the data
print '\n Grabbing GeoJson data from AGS - ' + layer_url + ' ... \n'
myCmd = 'esri2geojson ' + layer_url + ' ' + out_file 
print myCmd
os.system(myCmd)
print '\n Finished Grabbing Data. \n'



  
# Convert json to shp
# Note: esri2geojson is always going to spit out 4326 projection, because it follows the geojson standard.
print '\n Converting json to shapefile... \n'
myCmd = 'ogr2ogr -overwrite -a_srs "EPSG:4326" -F "ESRI Shapefile" ' + out_file_file + '.shp ' + out_file_file + '.geojson ' 
print myCmd
os.system(myCmd)
print '\n Finished converting json to shapefile... \n'
    
    


