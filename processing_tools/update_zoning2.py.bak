#!/usr/bin/python
# Load raw zoning
#
# Use a table to store needed info to process raw zoning into standardized zoning
#

# Import needed modules
import sys,os,fileinput,string,math,psycopg2,StringIO,datetime
import psycopg2.extras

#
# retrieve the required parameters
#
try:
    county = sys.argv[1].lower()
    city = sys.argv[2].lower()
    
    #try:
    #    extraArgs = sys.argv[6]
    #except:
    #    pass
    
except:
    print msgUsage
    sys.exit(0)


county_upper = county.upper()
county_lower = county.lower()

city_upper = city.upper()
city_lower = city.lower()


pg_connection = 'host=localhost port=5432 dbname=gisdev user=postgres password=galactic529'
pg_psql = 'psql -p 5432 -d gisdev -U postgres -c '

    
# populate variables from data transform table

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
print " "
print mycmd
#os.system(mycmd)

# Delete existing pg table
mycmd = pg_psql + '"' + 'DROP TABLE ' + temp_table_name + ';"'
print " "
print mycmd
os.system(mycmd)

mycmd = pg_psql + '"' + 'DROP TABLE ' + temp_table_name + '_2;"'
print " "
print mycmd
os.system(mycmd)

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
print " "
print mycmd
os.system(mycmd)

# TODO: How to read signal info from shell if ogr2ogr bails?
# we want to end the script at that error instead of powering through nothing.

# Updates the SRID of all features in a geometry column, geometry_columns metadata and srid table constraint
mycmd = pg_psql + '"' + "SELECT UpdateGeometrySRID('" + temp_table_name + "', 'wkb_geometry', 32767)" +  ';"'
print " "
print mycmd
os.system(mycmd)


# Fix any invalid polygons
mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + " SET wkb_geometry =  ST_MakeValid(wkb_geometry) WHERE st_isvalid(wkb_geometry) is false;" + '"'
#mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + " SET wkb_geometry =  ST_BuildArea(ST_Boundary(wkb_geometry)) WHERE st_isvalid(wkb_geometry) is false;"
print " "
print mycmd
os.system(mycmd)

# -----------------------------------------
# Special processing per county/city

if (county_upper == 'BAKER' and city_name == 'UNINCORPORATED'):
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zone_code = 'CITY';" + '"'
    print " "
    print mycmd
    os.system(mycmd)


    
if (county_upper == 'BROWARD' and city_name == 'A_PROPERTY_APPRAISER_UNIFIED'):
    # Delete city polygons
    #mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zone_ = 'CITY';"
    print " UPDATE BROWARD CITY NAME BASED ON OVERLAY WITH CITIES"
    print mycmd
    os.system(mycmd)
    
if (county_upper == 'CHARLOTTE' and city_name == 'UNINCORPORATED'):
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zone_ = 'CITY';" + '"'
    print " "
    print mycmd
    os.system(mycmd)

if (county_upper == 'CITRUS' and city_name == 'UNINCORPORATED'):
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoning IN ('CITY');" + '"'
    print " "
    print mycmd
    os.system(mycmd)
    
if (county_upper == 'CLAY' and city_name == 'UNINCORPORATED'):
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoning IN ('GCSMUNI','KHMUNI','OPMUNI','PFMUNI');" + '"'
    print " "
    print mycmd
    os.system(mycmd)

if (county_upper == 'COLLIER' and city_name == 'UNINCORPORATED'):
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoning IN ('CITY OF NAPLES','CITY OF MARCO ISLAND');" + '"'
    print " "
    print mycmd
    os.system(mycmd)
    
if (county_upper == 'DESOTO' and city_name == 'UNINCORPORATED'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoning IN ('CITY','');" + '"'
    print " "
    print mycmd
    os.system(mycmd)
    
if (county_upper == 'HERNANDO' and city_name == 'UNINCORPORATED'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoning IN ('CITY','');" + '"'
    print " "
    print mycmd
    os.system(mycmd)

if (county_upper == 'INDIAN_RIVER' and city_name == 'UNINCORPORATED'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoning IN ('MUNI','');" + '"'
    print " "
    print mycmd
    os.system(mycmd)
    
if (county_upper == 'LAKE' and city_name == 'UNINCORPORATED'):
    
# adding dissolve and explode to single part functionality
    sql = """
    --set work_mem = '2000MB';
    set enable_hashagg to off;    
    -- st_union parcels based on zon_code
    DROP TABLE IF EXISTS """ + temp_table_name + """_dissolve;
    CREATE TABLE """ + temp_table_name + """_dissolve AS
    SELECT 
        ST_UNION(wkb_geometry) as wkb_geometry,  
        """ + select_col_list + """
    FROM 
        """ + temp_table_name + """
    GROUP BY 
        """ + select_col_list + """;
    """

    print sql
    cursor.execute(sql)
    connection.commit()    
    
    # expand multi-polys to single-polys
    sql = """
    TRUNCATE """ + temp_table_name + """;
    INSERT INTO """ + temp_table_name + """ (wkb_geometry,""" + select_col_list + """)
    SELECT (ST_DUMP(wkb_geometry)).geom, """ + select_col_list + """
    FROM """ + temp_table_name + """_dissolve;
    """

    print sql
    cursor.execute(sql)
    connection.commit() 
    
if (county_upper == 'LEON' and city_name == 'UNINCORPORATED'):

    # ZONING IS UNIFIED
    pass
    # Delete city polygons
    #mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoning IN ('CITY','');"
    #print " "
    #print mycmd
    #os.system(mycmd)    

if (county_upper == 'MANATEE' and city_name == 'UNINCORPORATED'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zonelabel IN ('CITY','');" + '"'
    print " "
    print mycmd
    os.system(mycmd)
    
if (county_upper == 'MARTIN' and city_name == 'UNINCORPORATED'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoning in ('STUART','JUPITER', 'OCEAN BREE', 'SEWALL');" + '"'
    print " "
    print mycmd
    os.system(mycmd)

if (county_upper == 'MIAMI-DADE' and city_name == 'INCORPORATED'):

    # miami-dade incorporated special case - go ahead and delete all zoning polygons
    
    # Delete unincorporated polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zone = 'NONE';" + '"'
    print " "
    print mycmd
    os.system(mycmd)
    
    
# Miami-dade incorporated unified zoning layer has one of the best layouts andmost detailed info
# it includesminlotwidth, maxheight, density, etc
   
    
if (county_upper == 'NASSAU' and city_name == 'UNINCORPORATED'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoning = 'INCORPORATED';" + '"'
    print " "
    print mycmd
    os.system(mycmd)


if (county_upper == 'OKALOOSA' and city_name == 'UNINCORPORATED'):
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zngpy_zone IN ('CRESTVIEW','DESTIN','EGLIN AFB','FWB','LAUREL HILL','MARY ESTHER','NICEVILLE','SHALIMAR','VALPARAISO');" + '"'
    print " "
    print mycmd
    os.system(mycmd)
    
if (county_upper == 'ORANGE' and city_name == 'UNINCORPORATED'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoning = 'CITY';" + '"'
    print " "
    print mycmd
    os.system(mycmd)

if (county_upper == 'ORANGE' and city_name == 'ORLANDO'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoning = 'UNINCORPORATED';" + '"'
    print " "
    print mycmd
    os.system(mycmd)     

if (county_upper == 'OSCEOLA' and city_name == 'ST_CLOUD'):
    
# adding dissolve and explode to single part functionality
    sql = """
    --set work_mem = '2000MB';
    set enable_hashagg to off;    
    -- st_union parcels based on zon_code
    DROP TABLE IF EXISTS """ + temp_table_name + """_dissolve;
    CREATE TABLE """ + temp_table_name + """_dissolve AS
    SELECT 
        ST_UNION(wkb_geometry) as wkb_geometry,  
        """ + select_col_list + """
    FROM 
        """ + temp_table_name + """
    GROUP BY 
        """ + select_col_list + """;
    """

    print sql
    cursor.execute(sql)
    connection.commit()    
    
    # expand multi-polys to single-polys
    sql = """
    TRUNCATE """ + temp_table_name + """;
    INSERT INTO """ + temp_table_name + """ (wkb_geometry,""" + select_col_list + """)
    SELECT (ST_DUMP(wkb_geometry)).geom, """ + select_col_list + """
    FROM """ + temp_table_name + """_dissolve;
    """

    print sql
    cursor.execute(sql)
    connection.commit()  
    
    
if (county_upper == 'OSCEOLA' and city_name == 'UNINCORPORATED'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE prim_zon = 'INCORP';" + '"'
    print " "
    print mycmd
    os.system(mycmd)

if (county_upper == 'PASCO' and city_name == 'UNINCORPORATED'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zn_type in ('NPR','DC','PR','ZH','SA');" + '"'
    print " "
    print mycmd
    os.system(mycmd)
    
# Putnam raw data can be pulled from AGS Server
# M:\datascrub\08_Land_Use_and_Zoning\zoning\florida\county\putnam\current\a_export.txt
if (county_upper == 'PUTNAM' and city_name == 'UNINCORPORATED'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoning = 'SA';" + '"'
    print " "
    print mycmd
    os.system(mycmd)

if (county_upper == 'SANTA_ROSA' and city_name == 'UNINCORPORATED'):
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE district IN ('CITY');" + '"'
    print " "
    print mycmd
    os.system(mycmd)
    
if (county_upper == 'SARASOTA' and city_name == 'UNINCORPORATED'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoningcode IN ('LONGBOAT','NORTH PORT','SARASOTA','VENICE');" + '"'
    print " "
    print mycmd
    os.system(mycmd)
    
if (county_upper == 'SEMINOLE' and city_name == 'CASSELBERRY'):
# adding dissolve and explode to single part functionality
    sql = """
    --set work_mem = '2000MB';
    set enable_hashagg to off;    
    -- st_union parcels based on zon_code
    DROP TABLE IF EXISTS """ + temp_table_name + """_dissolve;
    CREATE TABLE """ + temp_table_name + """_dissolve AS
    SELECT 
        ST_UNION(wkb_geometry) as wkb_geometry,  
        """ + select_col_list + """
    FROM 
        """ + temp_table_name + """
    GROUP BY 
        """ + select_col_list + """;
    """

    print sql
    cursor.execute(sql)
    connection.commit()    
    
    # expand multi-polys to single-polys
    sql = """
    TRUNCATE """ + temp_table_name + """;
    INSERT INTO """ + temp_table_name + """ (wkb_geometry,""" + select_col_list + """)
    SELECT (ST_DUMP(wkb_geometry)).geom, """ + select_col_list + """
    FROM """ + temp_table_name + """_dissolve;
    """

    print sql
    cursor.execute(sql)
    connection.commit()   


if (county_upper == 'SEMINOLE' and city_name == 'LONGWOOD'):
# adding dissolve and explode to single part functionality
    sql = """
    --set work_mem = '2000MB';
    set enable_hashagg to off;    
    -- st_union parcels based on zon_code
    DROP TABLE IF EXISTS """ + temp_table_name + """_dissolve;
    CREATE TABLE """ + temp_table_name + """_dissolve AS
    SELECT 
        ST_UNION(wkb_geometry) as wkb_geometry,  
        """ + select_col_list + """
    FROM 
        """ + temp_table_name + """
    GROUP BY 
        """ + select_col_list + """;
    """

    print sql
    cursor.execute(sql)
    connection.commit()    
    
    # expand multi-polys to single-polys
    sql = """
    TRUNCATE """ + temp_table_name + """;
    INSERT INTO """ + temp_table_name + """ (wkb_geometry,""" + select_col_list + """)
    SELECT (ST_DUMP(wkb_geometry)).geom, """ + select_col_list + """
    FROM """ + temp_table_name + """_dissolve;
    """

    print sql
    cursor.execute(sql)
    connection.commit()  

# This produces a lot of slivers - take another look to see if we can eliminate that
if (county_upper == 'SEMINOLE' and city_name == 'OVIEDO1'):
# adding dissolve and explode to single part functionality
    sql = """
    --set work_mem = '2000MB';
    set enable_hashagg to off;    
    -- st_union parcels based on zon_code
    DROP TABLE IF EXISTS """ + temp_table_name + """_dissolve;
    CREATE TABLE """ + temp_table_name + """_dissolve AS
    SELECT 
        ST_UNION(wkb_geometry) as wkb_geometry,  
        """ + select_col_list + """
    FROM 
        """ + temp_table_name + """
    GROUP BY 
        """ + select_col_list + """;
    """

    print sql
    cursor.execute(sql)
    connection.commit()    
    
    # expand multi-polys to single-polys
    sql = """
    TRUNCATE """ + temp_table_name + """;
    INSERT INTO """ + temp_table_name + """ (wkb_geometry,""" + select_col_list + """)
    SELECT (ST_DUMP(wkb_geometry)).geom, """ + select_col_list + """
    FROM """ + temp_table_name + """_dissolve;
    """

    print sql
    cursor.execute(sql)
    connection.commit() 
    
if (county_upper == 'SEMINOLE' and city_name == 'SANFORD'):
# adding dissolve and explode to single part functionality
    sql = """
    --set work_mem = '2000MB';
    set enable_hashagg to off;    
    -- st_union parcels based on zon_code
    DROP TABLE IF EXISTS """ + temp_table_name + """_dissolve;
    CREATE TABLE """ + temp_table_name + """_dissolve AS
    SELECT 
        ST_UNION(wkb_geometry) as wkb_geometry,  
        """ + select_col_list + """
    FROM 
        """ + temp_table_name + """
    GROUP BY 
        """ + select_col_list + """;
    """

    print sql
    cursor.execute(sql)
    connection.commit()    
    
    # expand multi-polys to single-polys
    sql = """
    TRUNCATE """ + temp_table_name + """;
    INSERT INTO """ + temp_table_name + """ (wkb_geometry,""" + select_col_list + """)
    SELECT (ST_DUMP(wkb_geometry)).geom, """ + select_col_list + """
    FROM """ + temp_table_name + """_dissolve;
    """

    print sql
    cursor.execute(sql)
    connection.commit() 
    
# This produces a lot of slivers - take another look to see if we can eliminate that
if (county_upper == 'SEMINOLE' and city_name == 'WINTER_SPRINGS'):
# adding dissolve and explode to single part functionality
    sql = """
    --set work_mem = '2000MB';
    set enable_hashagg to off;    
    -- st_union parcels based on zon_code
    DROP TABLE IF EXISTS """ + temp_table_name + """_dissolve;
    CREATE TABLE """ + temp_table_name + """_dissolve AS
    SELECT 
        ST_UNION(wkb_geometry) as wkb_geometry,  
        """ + select_col_list + """
    FROM 
        """ + temp_table_name + """
    GROUP BY 
        """ + select_col_list + """;
    """

    print sql
    cursor.execute(sql)
    connection.commit()    
    
    # expand multi-polys to single-polys
    sql = """
    TRUNCATE """ + temp_table_name + """;
    INSERT INTO """ + temp_table_name + """ (wkb_geometry,""" + select_col_list + """)
    SELECT (ST_DUMP(wkb_geometry)).geom, """ + select_col_list + """
    FROM """ + temp_table_name + """_dissolve;
    """

    print sql
    cursor.execute(sql)
    connection.commit() 
    
if (county_upper == 'ST_JOHNS' and city_name == 'UNINCORPORATED'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoning = 'SA';" + '"'
    print " "
    print mycmd
    os.system(mycmd)

# This produces a TONNN of slivers - take another look to see if we can eliminate that
if (county_upper == 'ST_LUCIE' and city_name == 'UNINCORPORATED11'):
# adding dissolve and explode to single part functionality
    sql = """
    --set work_mem = '2000MB';
    set enable_hashagg to off;    
    -- st_union parcels based on zon_code
    DROP TABLE IF EXISTS """ + temp_table_name + """_dissolve;
    CREATE TABLE """ + temp_table_name + """_dissolve AS
    SELECT 
        ST_UNION(wkb_geometry) as wkb_geometry,  
        """ + select_col_list + """
    FROM 
        """ + temp_table_name + """
    GROUP BY 
        """ + select_col_list + """;
    """

    print sql
    cursor.execute(sql)
    connection.commit()    
    
    # expand multi-polys to single-polys
    sql = """
    TRUNCATE """ + temp_table_name + """;
    INSERT INTO """ + temp_table_name + """ (wkb_geometry,""" + select_col_list + """)
    SELECT (ST_DUMP(wkb_geometry)).geom, """ + select_col_list + """
    FROM """ + temp_table_name + """_dissolve;
    """

    print sql
    cursor.execute(sql)
    connection.commit() 
    
if (county_upper == 'VOLUSIA' and city_name == 'UNINCORPORATED'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zoncode = '999';" + '"'
    print " "
    print mycmd
    os.system(mycmd)  

if (county_upper == 'WALTON' and city_name == 'UNINCORPORATED'):
    
    # Delete city polygons
    mycmd = pg_psql + '"' + "DELETE FROM " + temp_table_name + " WHERE zone_class = 'Municipal';" + '"'
    print " "
    print mycmd
    os.system(mycmd)     
    
# -----------------------------------------

mycmd = pg_psql + '"CREATE TABLE ' + temp_table_name + '_2 (zon_code text, zon_code2 text, zon_desc text, zon_gen text, ord_num text, city_name text, county_name text, \
    notes text, the_geom geometry) ;"'
print " "
print mycmd
os.system(mycmd)


# Load raw data into standard table
mycmd = pg_psql + '"INSERT INTO ' + temp_table_name + '_2 (zon_code, zon_code2, zon_desc, zon_gen, ord_num, city_name, county_name, notes, the_geom) SELECT ' + zon_code_col + ',' + zon_code2_col + ',' + zon_desc_col + ',' + zon_gen_col + ',' + ord_num_col + ",'" + city_name + "','" + county_upper + "'," + notes_col + ', wkb_geometry FROM ' + temp_table_name + ';"'
print " "
print mycmd
os.system(mycmd)

# Bay special processing
if (county_upper == 'BAY'):
    mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + "_2 SET city_name = 'CALLAWAY' WHERE zon_code2 = '2';" + '"'
    print mycmd
    os.system(mycmd)   

    mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + "_2 SET city_name = 'LYNN HAVEN' WHERE zon_code2 = '3';" + '"'
    print mycmd
    os.system(mycmd)

    mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + "_2 SET city_name = 'MEXICO BEACH' WHERE zon_code2 = '4';" + '"'
    print mycmd
    os.system(mycmd)
    
    mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + "_2 SET city_name = 'PANAMA CITY' WHERE zon_code2 = '5';" + '"'
    print mycmd
    os.system(mycmd)
    
    mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + "_2 SET city_name = 'PANAMA CITY BEACH' WHERE zon_code2 = '6';" + '"'
    print mycmd
    os.system(mycmd)
    
    mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + "_2 SET city_name = 'PARKER' WHERE zon_code2 = '7';" + '"'
    print mycmd
    os.system(mycmd)
    
    mycmd = pg_psql + '"' + "UPDATE " + temp_table_name + "_2 SET city_name = 'SPRINGFIELD' WHERE zon_code2 = '8';" + '"'
    print mycmd
    os.system(mycmd)

    #UPDATE temp_table_name SET city_name = 'UNINCORPORATED' WHERE county_name = 'BAY' and zon_code2 = '1';


# Miami-dade special processing
if (county_upper == 'MIAMI-DADE' and city_name == 'INCORPORATED'):
    
    # move city name to proper column
    mycmd = pg_psql + '"' + "UPDATE zoning SET city_name = zon_code2 WHERE county_name = 'MIAMI-DADE' and city_name != 'UNINCORPORATED';" + '"'
    print " "
    print mycmd
    os.system(mycmd)

    # clear city name from old column
    mycmd = pg_psql + '"' + "UPDATE zoning SET zon_code2 = null WHERE county_name = 'MIAMI-DADE' and city_name != 'UNINCORPORATED';" + '"'
    print " "
    print mycmd
    os.system(mycmd) 

# Delete existing data from zoning
mycmd = pg_psql + '" DELETE FROM zoning WHERE city_name = ' + "'" + city_name + "' and county_name = '" + county_upper + "'" + ';"'
print " "
print mycmd
os.system(mycmd)

# Bay county special processing
if (county_upper == 'BAY'):
    mycmd = pg_psql + '" DELETE FROM zoning WHERE city_name IN (\'CALLAWAY\',\'LYNN HAVEN\',\'MEXICO BEACH\',\'PANAMA CITY\',\'PANAMA CITY BEACH\',\'PARKER\',\'SPRINGFIELD\') and county_name = \'BAY\';"'
    print " "
    print mycmd
    os.system(mycmd)

# Load raw data into standard table
mycmd = pg_psql + '"INSERT INTO zoning (zon_code, zon_code2, zon_desc, zon_gen, ord_num, city_name, county_name, notes, the_geom) \
    SELECT zon_code, zon_code2, zon_desc, zon_gen, ord_num, city_name, county_name, notes, the_geom FROM ' + temp_table_name + '_2;"'
print " "
print mycmd
os.system(mycmd)



# =========================================
# DUMP TO BACKUP FILE
# =========================================
mycmd = 'pg_dump --port 5432 --username postgres --format custom --verbose --file "/var/www/apps/mapwise/htdocs/x342/' + temp_table_name + '.backup" --table "\\"temp\\".\\"' + temp_table_name + '_2\\"" gisdev'
print " "
print mycmd
os.system(mycmd)


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
    print " "
    print mycmd

    # clear city name from old column
    mycmd = 'psql -p 5432 -U postgres -d gislib -c "UPDATE zoning SET zon_code2 = null WHERE county_name = \'MIAMI-DADE\' and city_name != \'UNINCORPORATED\';"'
    print " "
    print mycmd
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
f1 = open(file_batch,'w')

string = 'pg_restore -h -U postgres -d gislib -v "/home/bmay/incoming/' + temp_table_name + '.backup"'
f1.write(string + '\n')

string = 'psql -p 5432 -U postgres -d gislib -c "DELETE FROM zoning WHERE city_name = ' + "'" + city_name + "' and county_name = '" + county_upper + "'" + ';"'
f1.write(string + '\n')

# Miami-Dade special case
if (county_upper == 'MIAMI-DADE' and city_name == 'INCORPORATED'):
    string = 'psql -d gislib -U postgres -p 5432 -c "DELETE FROM zoning WHERE county_name = \'MIAMI-DADE\';"'
    f1.write(string + '\n')
    
string = 'psql -d gislib -U postgres -p 5432 -c "INSERT INTO zoning (zon_code,zon_code2,zon_desc,zon_gen,ord_num,city_name,county_name,notes,the_geom) SELECT ' + zon_code_col + ',' + zon_code2_col + ',' + zon_desc_col + ',' + zon_gen_col + ',' + ord_num_col + ",'" + city_name + "','" + county_upper + "'," + notes_col + ', wkb_geometry FROM ' + temp_table_name + '_2;"'
f1.write(string + '\n')

# Miami-Dade special case
if (county_upper == 'MIAMI-DADE' and city_name == 'INCORPORATED'):    
    # move city name to proper column
    string = 'psql -d gislib -U postgres -p 5432 -c "UPDATE zoning SET city_name = zon_code2 WHERE county_name = \'MIAMI-DADE\' and city_name != \'UNINCORPORATED\';"'
    f1.write(string + '\n')

    # clear city name from old column
    mycmd = pg_psql + '"' + "UPDATE zoning SET zon_code2 = null WHERE county_name = 'MIAMI-DADE' and city_name != 'UNINCORPORATED';"
    string = 'psql -d gislib -U postgres -p 5432 -c "UPDATE zoning SET zon_code2 = null WHERE county_name = \'MIAMI-DADE\' and city_name != \'UNINCORPORATED\';"'
    f1.write(string + '\n')
    
string = 'psql -d gislib -U postgres -p 5432 -c "DROP TABLE ' + temp_table_name + '_2;"'
f1.write(string + '\n')

f1.close()
