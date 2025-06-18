#!/usr/bin/python
# Load raw zoning
#
# Use a table to store needed info to process raw zoning into standardized zoning
#

# Import needed modules
import sys, os, fileinput, string, math, psycopg2, StringIO, datetime
import psycopg2.extras

# Helper functions to reduce repetitive code
def execute_sql_command(cmd, execute=True):
    """Execute a SQL command and print it"""
    print " "
    print cmd
    if execute:
        os.system(cmd)

def delete_city_polygons(county, city, temp_table, condition):
    """Delete city polygons based on a condition"""
    if county_upper == county and city_name == city:
        mycmd = pg_psql + '"' + "DELETE FROM " + temp_table + " WHERE " + condition + ";" + '"'
        execute_sql_command(mycmd)

def dissolve_and_explode(county, city, temp_table_name, select_col_list):
    """Dissolve polygons and explode to single part"""
    if county_upper == county and city_name == city:
        sql = """
        --set work_mem = '2000MB';
        set enable_hashagg to off;    
        -- st_union parcels based on zon_code
        DROP TABLE IF EXISTS {0}_dissolve;
        CREATE TABLE {0}_dissolve AS
        SELECT 
            ST_UNION(wkb_geometry) as wkb_geometry,  
            {1}
        FROM 
            {0}
        GROUP BY 
            {1};
        """.format(temp_table_name, select_col_list)

        print sql
        cursor.execute(sql)
        connection.commit()    
        
        # expand multi-polys to single-polys
        sql = """
        TRUNCATE {0};
        INSERT INTO {0} (wkb_geometry,{1})
        SELECT (ST_DUMP(wkb_geometry)).geom, {1}
        FROM {0}_dissolve;
        """.format(temp_table_name, select_col_list)

        print sql
        cursor.execute(sql)
        connection.commit()

#
# retrieve the required parameters
#
try:
    county = sys.argv[1].lower()
    city = sys.argv[2].lower()
except:
    print "Usage: python update_zoning_v2.py <county> <city>"
    sys.exit(0)

county_upper = county.upper()
county_lower = county.lower()

city_upper = city.upper()
city_lower = city.lower()

pg_connection = 'host=localhost port=5432 dbname=gisdev user=postgres password=galactic529'
pg_psql = 'psql -p 5432 -d gisdev -U postgres -c '

# connect to database
connection = psycopg2.connect(pg_connection)

# arg to cursor allows selection by column name
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

print " "
print " LOAD ZONING "

#-----------------------------------------------------------------------------------------
# Get ZONING transform info from zoning_transform table
#-----------------------------------------------------------------------------------------    
sql = """SELECT * FROM zoning_transform WHERE county = '""" + county_upper + """' and city_name = '""" + city_upper + """';  """
print " "
print sql
cursor.execute(sql)
connection.commit()

# initialize variables that have info about the ZONING SHP from zoning_transform
rows = cursor.fetchall()
for row in rows:
    print "city_name = ", row['city_name']
    city_name = row['city_name']
    city_name_path = city_name.lower()

        
    shp_name = row['shp_name']
    temp_table_name = row['temp_table_name']
    zon_code_col = row['zon_code_col']
    zon_code2_col = row['zon_code2_col']
    zon_desc_col = row['zon_desc_col']
    zon_gen_col = row['zon_gen_col']
    notes_col = row['notes_col']
    ord_num_col = row['ord_num_col']
    
    srs_epsg = str(row['srs_epsg']) 

    #shp_date = str(row['shp_date'])
    #shp_epsg = str(row['shp_epsg'])    
    #shp_pin_clean = row['shp_pin_clean']
    #import_fields = row['import_fields']


# Set working directory
top_level_dir = '/srv/datascrub/08_Land_Use_and_Zoning/zoning/florida/county/'
path_src = top_level_dir + county_lower + '/current/source_data/' + city_name_path
os.chdir(path_src)

# repair shapefile
mycmd = 'L:/projects/tools/python/lib/gp_repair_geometry.py ' + shp_name
execute_sql_command(mycmd, execute=False)  # Commented out in original

# Delete existing pg table
execute_sql_command(pg_psql + '"' + 'DROP TABLE ' + temp_table_name + ';"')
execute_sql_command(pg_psql + '"' + 'DROP TABLE ' + temp_table_name + '_2;"')

# Load raw shp into postgres
# build list of columns to load
select_col_list = zon_code_col

if (zon_code2_col is not None):
    select_col_list += ',' + zon_code2_col
else:
    zon_code2_col = 'Null'
    
if (zon_desc_col is not None):
    select_col_list += ',' + zon_desc_col
else:
    zon_desc_col = 'Null'
    
if (zon_gen_col is not None):
    select_col_list += ',' + zon_gen_col
else:
    zon_gen_col = 'Null'
    
if (notes_col is not None):
    select_col_list += ',' + notes_col
else:
    notes_col = 'Null'
    
if (ord_num_col is not None):
    select_col_list += ',' + ord_num_col
else:
    ord_num_col = 'Null'


print " "
print "Columns to load: ",select_col_list

# load the data into temp table
#mycmd = 'ogr2ogr -skipfailures -select "' + select_col_list + '" -t_srs "EPSG:43000" -nlt GEOMETRY -f "PostgreSQL" PG:"user=postgres dbname=gisdev host=localhost port=5432 password=galactic529" -nln ' + temp_table_name + ' ' + shp_name
mycmd = 'ogr2ogr -skipfailures  -t_srs "EPSG:32767" -s_srs "EPSG:' + srs_epsg + '" -select "' + select_col_list + '" -nlt GEOMETRY -f "PostgreSQL" PG:"user=postgres dbname=gisdev host=localhost port=5432 password=galactic529" -nln ' + temp_table_name + ' ' + shp_name
execute_sql_command(mycmd)

# TODO: How to read signal info from shell if ogr2ogr bails?
# we want to end the script at that error instead of powering through nothing.

# Updates the SRID of all features in a geometry column, geometry_columns metadata and srid table constraint
mycmd = pg_psql + '"' + "SELECT UpdateGeometrySRID('" + temp_table_name + "', 'wkb_geometry', 32767)" +  ';"'
execute_sql_command(mycmd)


# Fix any invalid polygons
mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + " SET wkb_geometry =  ST_MakeValid(wkb_geometry) WHERE st_isvalid(wkb_geometry) is false;" + '"'
#mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + " SET wkb_geometry =  ST_BuildArea(ST_Boundary(wkb_geometry)) WHERE st_isvalid(wkb_geometry) is false;"
execute_sql_command(mycmd)

# -----------------------------------------
# Special processing per county/city

# Dictionary of county/city pairs and their delete conditions
delete_conditions = {
    ('BAKER', 'UNINCORPORATED'): "zone_code = 'CITY'",
    ('CHARLOTTE', 'UNINCORPORATED'): "zone_ = 'CITY'",
    ('CITRUS', 'UNINCORPORATED'): "zoning IN ('CITY')",
    ('CLAY', 'UNINCORPORATED'): "zoning IN ('GCSMUNI','KHMUNI','OPMUNI','PFMUNI')",
    ('COLLIER', 'UNINCORPORATED'): "zoning IN ('CITY OF NAPLES','CITY OF MARCO ISLAND')",
    ('DESOTO', 'UNINCORPORATED'): "zoning IN ('CITY','')",
    ('HERNANDO', 'UNINCORPORATED'): "zoning IN ('CITY','')",
    ('INDIAN_RIVER', 'UNINCORPORATED'): "zoning IN ('MUNI','')",
    ('MANATEE', 'UNINCORPORATED'): "zonelabel IN ('CITY','')",
    ('MARTIN', 'UNINCORPORATED'): "zoning in ('STUART','JUPITER', 'OCEAN BREE', 'SEWALL')",
    ('MIAMI-DADE', 'INCORPORATED'): "zone = 'NONE'",
    ('NASSAU', 'UNINCORPORATED'): "zoning = 'INCORPORATED'",
    ('OKALOOSA', 'UNINCORPORATED'): "zngpy_zone IN ('CRESTVIEW','DESTIN','EGLIN AFB','FWB','LAUREL HILL','MARY ESTHER','NICEVILLE','SHALIMAR','VALPARAISO')",
    ('ORANGE', 'UNINCORPORATED'): "zoning = 'CITY'",
    ('ORANGE', 'ORLANDO'): "zoning = 'UNINCORPORATED'",
    ('OSCEOLA', 'UNINCORPORATED'): "prim_zon = 'INCORP'",
    ('PASCO', 'UNINCORPORATED'): "zn_type in ('NPR','DC','PR','ZH','SA')",
    ('PUTNAM', 'UNINCORPORATED'): "zoning = 'SA'",
    ('SANTA_ROSA', 'UNINCORPORATED'): "district IN ('CITY')",
    ('SARASOTA', 'UNINCORPORATED'): "zoningcode IN ('LONGBOAT','NORTH PORT','SARASOTA','VENICE')",
    ('ST_JOHNS', 'UNINCORPORATED'): "zoning = 'SA'",
    ('VOLUSIA', 'UNINCORPORATED'): "zoncode = '999'",
    ('WALTON', 'UNINCORPORATED'): "zone_class = 'Municipal'"
}

# Apply delete conditions for matching county/city pairs
for (county, city), condition in delete_conditions.items():
    delete_city_polygons(county, city, temp_table_name, condition)

# Special case for BROWARD county
if (county_upper == 'BROWARD' and city_name == 'A_PROPERTY_APPRAISER_UNIFIED'):
    print " UPDATE BROWARD CITY NAME BASED ON OVERLAY WITH CITIES"
    print "No command specified in original script"

# Counties that need dissolve and explode operations
dissolve_counties = [
    ('LAKE', 'UNINCORPORATED'),
    ('OSCEOLA', 'ST_CLOUD'),
    ('SEMINOLE', 'CASSELBERRY'),
    ('SEMINOLE', 'LONGWOOD'),
    ('SEMINOLE', 'OVIEDO1'),
    ('SEMINOLE', 'SANFORD'),
    ('SEMINOLE', 'WINTER_SPRINGS'),
    ('ST_LUCIE', 'UNINCORPORATED11')
]

# Apply dissolve and explode for matching counties
for county, city in dissolve_counties:
    dissolve_and_explode(county, city, temp_table_name, select_col_list)

# Create standardized table
execute_sql_command(pg_psql + '"CREATE TABLE ' + temp_table_name + '_2 (zon_code text, zon_code2 text, zon_desc text, zon_gen text, ord_num text, city_name text, county_name text, notes text, the_geom geometry) ;"')

# Load raw data into standard table
mycmd = pg_psql + '"INSERT INTO ' + temp_table_name + '_2 (zon_code, zon_code2, zon_desc, zon_gen, ord_num, city_name, county_name, notes, the_geom) SELECT ' + zon_code_col + ',' + zon_code2_col + ',' + zon_desc_col + ',' + zon_gen_col + ',' + ord_num_col + ",'" + city_name + "','" + county_upper + "'," + notes_col + ', wkb_geometry FROM ' + temp_table_name + ';"'
execute_sql_command(mycmd)

# Bay special processing
if (county_upper == 'BAY'):
    mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + "_2 SET city_name = 'CALLAWAY' WHERE zon_code2 = '2';" + '"'
    execute_sql_command(mycmd)   

    mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + "_2 SET city_name = 'LYNN HAVEN' WHERE zon_code2 = '3';" + '"'
    execute_sql_command(mycmd)

    mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + "_2 SET city_name = 'MEXICO BEACH' WHERE zon_code2 = '4';" + '"'
    execute_sql_command(mycmd)
    
    mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + "_2 SET city_name = 'PANAMA CITY' WHERE zon_code2 = '5';" + '"'
    execute_sql_command(mycmd)
    
    mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + "_2 SET city_name = 'PANAMA CITY BEACH' WHERE zon_code2 = '6';" + '"'
    execute_sql_command(mycmd)
    
    mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + "_2 SET city_name = 'PARKER' WHERE zon_code2 = '7';" + '"'
    execute_sql_command(mycmd)
    
    mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + "_2 SET city_name = 'SPRINGFIELD' WHERE zon_code2 = '8';" + '"'
    execute_sql_command(mycmd)

    #UPDATE temp_table_name SET city_name = 'UNINCORPORATED' WHERE county_name = 'BAY' and zon_code2 = '1';


# Miami-dade special processing
if (county_upper == 'MIAMI-DADE' and city_name == 'INCORPORATED'):
    
    # move city name to proper column
    mycmd = pg_psql + '"' + "UPDATE zoning SET city_name = zon_code2 WHERE county_name = 'MIAMI-DADE' and city_name != 'UNINCORPORATED';" + '"'
    execute_sql_command(mycmd)

    # clear city name from old column
    mycmd = pg_psql + '"' + "UPDATE zoning SET zon_code2 = null WHERE county_name = 'MIAMI-DADE' and city_name != 'UNINCORPORATED';" + '"'
    execute_sql_command(mycmd) 

# Delete existing data from zoning
mycmd = pg_psql + '" DELETE FROM zoning WHERE city_name = ' + "'" + city_name + "' and county_name = '" + county_upper + "'" + ';"'
execute_sql_command(mycmd)

# Bay county special processing
if (county_upper == 'BAY'):
    mycmd = pg_psql + '" DELETE FROM zoning WHERE city_name IN (\'CALLAWAY\',\'LYNN HAVEN\',\'MEXICO BEACH\',\'PANAMA CITY\',\'PANAMA CITY BEACH\',\'PARKER\',\'SPRINGFIELD\') and county_name = \'BAY\';"'
    execute_sql_command(mycmd)

# Load raw data into standard table
mycmd = pg_psql + '"INSERT INTO zoning (zon_code, zon_code2, zon_desc, zon_gen, ord_num, city_name, county_name, notes, the_geom) \
    SELECT zon_code, zon_code2, zon_desc, zon_gen, ord_num, city_name, county_name, notes, the_geom FROM ' + temp_table_name + '_2;"'
execute_sql_command(mycmd)



# =========================================
# DUMP TO BACKUP FILE
# =========================================
mycmd = 'pg_dump --port 5432 --username postgres --format custom --verbose --file "/var/www/apps/mapwise/htdocs/x342/' + temp_table_name + '.backup" --table "\\"temp\\".\\"' + temp_table_name + '_2\\"" gisdev'
execute_sql_command(mycmd)


# =========================================
# RESTORE BACKUP FILE ON SAUNDERS SERVER
# =========================================
print " "
print "----- SCRIPT to update on server -----"
print " "
print " "
print 'pg_restore -p 5432 -U postgres -d gislib -v "/home/bmay/incoming/' + temp_table_name + '.backup"'
# C:\"Program Files (x86)"\PostgreSQL\9.1\bin\pg_restore.exe -h localhost -p 5433 -U postgres -d postgis -v "C:\ftp_filezilla\ols\incoming\raw_flu_charlotte_unincorp.backup
print " "

# Delete existing data from zoning
print 'psql -p 5432 -U postgres -d gislib -c "DELETE FROM zoning WHERE city_name = ' + "'" + city_name + "' and county_name = '" + county_upper + "'" + ';"'
# Miami-Dade special case
if (county_upper == 'MIAMI-DADE' and city_name == 'INCORPORATED'):
    print 'psql -p 5432 -U postgres -d gislib -c "DELETE FROM zoning WHERE county_name = \'MIAMI-DADE\';"'
print " "

# Load raw data into standard table and project geometry (transform)

print """psql -d gislib -U postgres -p 5432 -c "INSERT INTO zoning (zon_code,zon_code2,zon_desc,zon_gen,ord_num,city_name,county_name,notes,the_geom) 
SELECT zon_code, zon_code2, zon_desc, zon_gen, ord_num, city_name, county_name, notes, the_geom FROM """ + temp_table_name + """_2;" """
print " "

#print 'psql -d gislib -U postgres -p 5432 -c "INSERT INTO zoning (zon_code,zon_code2,zon_desc,zon_gen,ord_num,city_name,county_name,notes,the_geom) SELECT ' + zon_code_col + ',' + zon_code2_col + ',' + zon_desc_col + ',' + zon_gen_col + ',' + ord_num_col + ",'" + city_name + "','" + county_upper + "'," + notes_col + ', wkb_geometry FROM ' + temp_table_name + ';"'


# Miami-Dade special case
if (county_upper == 'MIAMI-DADE' and city_name == 'INCORPORATED'):
    
    # move city name to proper column
    mycmd = 'psql -p 5432 -U postgres -d gislib -c "UPDATE zoning SET city_name = zon_code2 WHERE county_name = \'MIAMI-DADE\' and city_name != \'UNINCORPORATED\';"'
    execute_sql_command(mycmd)

    # clear city name from old column
    mycmd = pg_psql + '"' + "UPDATE zoning SET zon_code2 = null WHERE county_name = 'MIAMI-DADE' and city_name != 'UNINCORPORATED';"
    execute_sql_command(mycmd)
print " "

print 'psql -d gislib -U postgres -p 5432 -c "DROP TABLE ' + temp_table_name + '_2;"'
# C:\"Program Files (x86)"\PostgreSQL\9.1\bin\psql -d postgis -U postgres -c "DROP TABLE raw_flu_charlotte_unified;"
print " "
print " "
print "----- END SCRIPT to update on server -----"
print " "

#==============================================================================
# Write batch file to run update commands
#==============================================================================
file_batch = '/var/www/apps/mapwise/htdocs/x342/' + temp_table_name + '.bat'
with open(file_batch,'w') as f1:
    commands = [
        'pg_restore -h -U postgres -d gislib -v "/home/bmay/incoming/' + temp_table_name + '.backup"',
        'psql -p 5432 -U postgres -d gislib -c "DELETE FROM zoning WHERE city_name = \'' + city_name + '\' and county_name = \'' + county_upper + '\';"'
    ]
    
    # Miami-Dade special case
    if (county_upper == 'MIAMI-DADE' and city_name == 'INCORPORATED'):
        commands.append('psql -d gislib -U postgres -p 5432 -c "DELETE FROM zoning WHERE county_name = \'MIAMI-DADE\';"')
    
    commands.append('psql -d gislib -U postgres -p 5432 -c "INSERT INTO zoning (zon_code,zon_code2,zon_desc,zon_gen,ord_num,city_name,county_name,notes,the_geom) SELECT ' + 
                   zon_code_col + ',' + zon_code2_col + ',' + zon_desc_col + ',' + zon_gen_col + ',' + ord_num_col + 
                   ",'" + city_name + "','" + county_upper + "'," + notes_col + ', wkb_geometry FROM ' + temp_table_name + '_2;"')
    
    # Miami-Dade special case
    if (county_upper == 'MIAMI-DADE' and city_name == 'INCORPORATED'):
        commands.append('psql -d gislib -U postgres -p 5432 -c "UPDATE zoning SET city_name = zon_code2 WHERE county_name = \'MIAMI-DADE\' and city_name != \'UNINCORPORATED\';"')
        commands.append('psql -d gislib -U postgres -p 5432 -c "UPDATE zoning SET zon_code2 = null WHERE county_name = \'MIAMI-DADE\' and city_name != \'UNINCORPORATED\';"')
    
    commands.append('psql -d gislib -U postgres -p 5432 -c "DROP TABLE ' + temp_table_name + '_2;"')
    
    for cmd in commands:
        f1.write(cmd + '\n')
