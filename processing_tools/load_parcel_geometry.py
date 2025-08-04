#!/usr/bin/python3
#
# Copyright Brian May 2011
#
#
# The main ideas are:
#   - load parcel geometry into postgres
#   - do things only one or two ways, i.e. re-use best practices
#   - compartmentalize / modularize processing
#   - Issue SQL statements directly from python
#   - provide for more flow control, error handling, error & status reporting, etc.
#
#   load_parcel_geometry.py <state> <county> <data_subdir> <data_date>
#
#   Example:
#   load_parcel_geometry_georgia.py ga worth parcels-ga-c348bd4a-shp 2022-07-01
#

# Import needed modules
import sys,os,fileinput,string,math,psycopg2,io,datetime
import psycopg2.extras, smtplib, textwrap



# ===============================================================================================
#  PROCESS SHAPEFILE
#  - DEPENDENCIES
#       - raw_<county>_parcels must exist in gidev on plato
#       - values must be current and valid in parcel_shp_fields table
#
# ===============================================================================================
def load_parcel_geometry(state, county, repair) :

    print("*********************************")
    print("NOW IN update_parcel_shapefile.py")
    print(" ")
    print("----------------------------------------------------------------------")
    print("  FUNCTION process_shapefile(county, repair)")
    print("----------------------------------------------------------------------")

    # connect to database
    connection = psycopg2.connect(pg_connection)
    
    # arg to cursor allows selection by column name
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    county_upper = county.upper()
    county_lower = county.lower()
    state_upper = state.upper()
    state_lower = state.lower()
    
    # replace county_lower with this because we are including state identifiers in the table name
    state_county_lower = state_lower + '_' + county_lower
    
    if county_upper == 'MIAMI_DADE' :
        county_upper = 'MIAMI-DADE'
        county_lower = 'miami_dade'
        
    #path =  '/srv/mapwise_dev/county/' + county_lower + '/processing/vector/propapp/current'
    #print 'pathSourceData: ',pathSourceData
    os.chdir(pathSourceData)    

    #-----------------------------------------------------------------------------------------
    # Get SHP info from parcel_shp_fields table
    #-----------------------------------------------------------------------------------------    
    sql = """SELECT * FROM parcel_shp_fields WHERE county = '""" + county_upper + """' AND state = '""" + state_upper + """';  """
    print(sql)
    cursor.execute(sql)
    connection.commit()

    # initialize variables that have info about the SHP from parcel_shp_fields
    rows = cursor.fetchall()
    for row in rows:
        print("COUNTY = ", row['county'])
        print("STATE = ", row['state'])
        print("import_fields = ", row['import_fields'])
        shp_name = row['shp_name']
        shp_date = str(row['shp_date'])
        shp_epsg = str(row['shp_epsg'])
        
        pin_insert_cols_arr = []
        pin_select_cols_arr = []
         
        shp_pin = row['shp_pin']
        #if len(shp_pin) > 0:
        if shp_pin is not None:
            pin_insert_cols_arr.append('pin_orig') 
            pin_select_cols_arr.append(shp_pin)
            
        shp_pin_clean = row['shp_pin_clean']
        if shp_pin_clean is not None:
            pin_insert_cols_arr.append('pin_clean_orig') 
            pin_select_cols_arr.append(shp_pin_clean)
            
        shp_pin2 = row['shp_pin2']
        if shp_pin2 is not None:
            pin_insert_cols_arr.append('pin2_orig') 
            pin_select_cols_arr.append(shp_pin2) 
            
        #shp_pin2_clean = row['shp_pin2_clean']
        #if shp_pin2_clean is not None:
        #    pin_insert_cols_arr.append('pin2_clean_orig') 
        #    pin_select_cols_arr.append(shp_pin2_clean)
            
        shp_altkey = row['shp_altkey']
        if shp_altkey is not None:
            pin_insert_cols_arr.append('altkey_orig') 
            pin_select_cols_arr.append(shp_altkey)
            
        
        
        
        condo_key = row['condo_key']
        
        import_fields = row['import_fields']
        
        
        
        pin_insert_cols = ','.join(pin_insert_cols_arr)
        
        pin_select_cols = ','.join(pin_select_cols_arr)

        #print "pin_insert_cols_arr: ", pin_insert_cols_arr
        #print "pin_insert_cols: ", pin_insert_cols
        
        #print "pin_select_cols_arr: ", pin_select_cols_arr
        #print "pin_select_cols: ", pin_select_cols
        
        #exit()
        


    #-----------------------------------------------------------------------------------------
    # Repair the shapefile
    #-----------------------------------------------------------------------------------------
    # 11/2022 - this needs to be removed - we don;t do this anymore
    repair = 'TRUE'
    repair = 'FALSE'
    if (repair == 'TRUE') :
        # repair raw shp -- avoid some invalid geom and loading problems
        mycmd = """/srv/projects/tools/python/lib/gp_repair_geometry.py source_data/""" + shp_name
        print('Executing: ', mycmd)
        os.system(mycmd)
    
    # create a spatial index on raw shp -- for use in QA
    #mycmd = """/srv/projects/tools/python/lib/add_shapefile_index.py source_data/""" + shp_name
    #print 'Executing: ', mycmd
    #os.system(mycmd)

    mycmd = pg_psql + """  -c "DROP TABLE raw_""" + state_county_lower + """_parcels"  """
    print('Executing: ', mycmd)
    os.system(mycmd)

    # handle counties not in shapefile format
    #if (county_upper == 'BROWARD') :
    #    mycmd = 'ogr2ogr -nlt GEOMETRY -a_srs "EPSG:' + shp_epsg + '" -skipfailures -select "' + import_fields + '" -f "PostgreSQL" PG:"' + pg_connection + '" -nln raw_' + county_lower + '_parcels source_data/' + shp_name
    #    print 'Executing: ', mycmd
    #    os.system(mycmd)
    #else: 


    # load raw parcels shapefile 
    # NOTE: FGDB can also be loaded, just enter the FGDB name and the layer name, e.g. ParcelHosted.gdb ParcelHosted
    mycmd = 'ogr2ogr -nlt GEOMETRY -a_srs "EPSG:' + shp_epsg + '" -skipfailures -select "' + import_fields + '" -f "PostgreSQL" PG:"' + pg_connection + '" -nln raw_' + state_county_lower + '_parcels ' + shp_name
    print('Executing: ', mycmd)
    os.system(mycmd)


    sql = """UPDATE raw_""" + state_county_lower + """_parcels SET wkb_geometry = ST_Multi(wkb_geometry);"""
    print(sql)
    cursor.execute(sql)
    connection.commit()

    #=====================
    # fix invalid polygons
    
    # CHECK validity
    # SELECT st_isvalid(wkb_geometry), st_isvalidReason(wkb_geometry)  FROM raw_hillsborough_parcels  WHERE st_isvalid(wkb_geometry) is false;

    # Fix polygons that are geometrycollections - combo of polygon and line
    sql = """
        UPDATE raw_""" + state_county_lower + """_parcels 
            SET wkb_geometry = ST_Buffer(wkb_geometry, 0.0)
            WHERE  geometrytype(wkb_geometry) = 'GEOMETRYCOLLECTION';
        """
    print(sql)
    cursor.execute(sql)
    connection.commit()
        
    # Use ST_MakeValid(wkb_geometry) whenever possible - if too slow, kill it and use older technique
    if (county_upper in ['HAMILTON','POLK'] and state_upper == 'FL') :

        sql = """
            UPDATE raw_""" + state_county_lower + """_parcels 
                SET wkb_geometry = ST_Buffer(wkb_geometry, 0.0)
                WHERE  ST_IsValid(wkb_geometry) = false and ST_IsValidReason(wkb_geometry) like '%Ring Self-intersection%';
            """
        print(sql)
        cursor.execute(sql)
        connection.commit()
        
        #fix a few polygons that weren't fixed by above
        #fix_counties = ['BAY','BREVARD','BROWARD','PALM_BEACH','SUMTER']
        sql = """
            UPDATE raw_""" + state_county_lower + """_parcels 
                SET wkb_geometry = ST_Buffer(wkb_geometry, 0.0)
                WHERE ST_IsValid(wkb_geometry) is false;
            """
        print(sql)
        cursor.execute(sql)
        connection.commit()

        # fix a few more by county, where the county is still having problems
        sql = """
            UPDATE raw_""" + state_county_lower + """_parcels 
                SET wkb_geometry = ST_Buffer(wkb_geometry, 0.0)
                WHERE  ST_IsValid(wkb_geometry) = false and ST_IsValidReason(wkb_geometry) like '%Ring Self-intersection%';
            """
        print(sql)
        cursor.execute(sql)
        connection.commit()
    
    else :
        sql = """
            UPDATE raw_""" + state_county_lower + """_parcels 
                SET wkb_geometry = ST_MakeValid(wkb_geometry) WHERE ST_IsValid(wkb_geometry) is false;
            """
        print(sql)
        cursor.execute(sql)
        connection.commit()

    #-----------------------------------------------------------------------------------------
    # UPDATE BREVARD PIN - pin is not set and must be built using the following query
    #-----------------------------------------------------------------------------------------        
    if (county_upper == 'BREVARD' and state_upper == 'FL') :

        sql = """

            -- Add pin field
            ALTER TABLE raw_fl_brevard_parcels ADD COLUMN pin text;
            
            -- Add township field - in 8/31/2016 version of data, township is not there
            -- ALTER TABLE raw_fl_brevard_parcels ADD COLUMN township text;

            -- calc township column
            -- UPDATE raw_fl_brevard_parcels SET township = substring(name,1,3);
            
            -- if there IS NOT a decimal in BOTH subblock and lot
            -- 174124
            UPDATE raw_fl_brevard_parcels 
                SET pin = township || '-' || range || '-' || section  || '-' || submoniker  || '-' || lpad(subblock,5,'0') || '.0-' || lpad(lot,4,'0') || '.00'
                WHERE strpos(subblock,'.') = 0 and strpos(lot,'.') = 0;
                
                
            -- if there IS NOT a decimal in subblock and IS a decimal in lot
            -- 5206
            UPDATE raw_fl_brevard_parcels
                SET pin = township || '-' || range || '-' || section  || '-' || submoniker  || '-' || lpad(subblock,5,'0') || '.0-' || lpad(lot,7,'0')
                WHERE strpos(subblock,'.') = 0 and strpos(lot,'.') > 0;
                
                
            -- if there IS a decimal in subblock and IS NOT a decimal in lot
            -- 653
            UPDATE raw_fl_brevard_parcels
                SET pin = township || '-' || range || '-' || section  || '-' || submoniker  || '-' || lpad(subblock,7,'0') || '-' || lpad(lot,4,'0') || '.00'
                WHERE strpos(subblock,'.') > 0 and strpos(lot,'.') = 0;
                

            -- if there IS a decimal in BOTH subblock and lot
            -- 591
            UPDATE raw_fl_brevard_parcels
                SET pin = township || '-' || range || '-' || section  || '-' || submoniker  || '-' || lpad(subblock,7,'0') || '-' || lpad(lot,7,'0') 
                WHERE strpos(subblock,'.') > 0 and strpos(lot,'.') > 0;


            -- BOTH subblock and lot are null
            -- 2
            UPDATE raw_fl_brevard_parcels 
                SET pin = township || '-' || range || '-' || section  || '-' || submoniker  || '-00000.0-0000.00'
                WHERE subblock is null and lot is null;
                
            -- subblock IS null and lot has decimal
            -- 5541
            UPDATE raw_fl_brevard_parcels 
                SET pin = township || '-' || range || '-' || section  || '-' || submoniker  || '-00000.0-' || lpad(lot,7,'0') 
                WHERE subblock is null and strpos(lot,'.') > 0;
                
            -- subblock IS null and lot has NO decimal
            -- 66836
            UPDATE raw_fl_brevard_parcels 
                SET pin = township || '-' || range || '-' || section  || '-' || submoniker  || '-00000.0-' || lpad(lot,4,'0') || '.00'
                WHERE subblock is null and strpos(lot,'.') = 0;
                
            -- subblock has decimal and lot IS NULL
            -- 5183
            UPDATE raw_fl_brevard_parcels 
                SET pin = township || '-' || range || '-' || section  || '-' || submoniker  || '-' || lpad(subblock,7,'0') || '-0000.00'
                WHERE strpos(subblock,'.') > 0 and lot is null;
                
            -- subblock has no decimal and lot IS NULL
            -- 32798
            UPDATE raw_fl_brevard_parcels 
                SET pin = township || '-' || range || '-' || section  || '-' || submoniker  || '-' || lpad(subblock,5,'0') || '.0-0000.00'
                WHERE strpos(subblock,'.') = 0 and lot is null;

        """    

    print(sql)
    cursor.execute(sql)
    connection.commit()

    #-----------------------------------------------------------------------------------------
    # UPDATE BROWARD - remove water polygons
    #-----------------------------------------------------------------------------------------
    # FOR FDOR ONLY!
    #if (county_upper == 'BROWARD' and state_upper == 'FL') :
    #    sql = """DELETE FROM raw_broward_parcels WHERE parcelno is null or parcelno = '' """
    #    print sql
    #    cursor.execute(sql)
    #    connection.commit()
        
    #-----------------------------------------------------------------------------------------
    # UPDATE COLLIER - remove empty polygons (water, mangroves)
    #-----------------------------------------------------------------------------------------        
    if (county_upper == 'COLLIER' and state_upper == 'FL') :
        sql = """DELETE FROM raw_fl_collier_parcels WHERE fln is null """
        print(sql)
        cursor.execute(sql)
        connection.commit()
        
    #-----------------------------------------------------------------------------------------
    # UPDATE MARTIN - remove water polygons
    #-----------------------------------------------------------------------------------------        
    if (county_upper == 'MARTIN' and state_upper == 'FL') :
        sql = """DELETE FROM raw_fl_martin_parcels WHERE pcn in ('LAKE OKEECHOBEE', 'WATERWAYS', 'WATERWAY', 'OCEAN') """
        print(sql)
        cursor.execute(sql)
        connection.commit()

    #-----------------------------------------------------------------------------------------
    # UPDATE POLK - remove road polygons
    #-----------------------------------------------------------------------------------------        
    if (county_upper == 'POLK' and state_upper == 'FL') :
        sql = """DELETE FROM raw_fl_polk_parcels WHERE parcelid in ('ROAD') """
        print(sql)
        cursor.execute(sql)
        connection.commit()

    #-----------------------------------------------------------------------------------------
    # UPDATE SANTA_ROSA - remove ROW polygons
    #-----------------------------------------------------------------------------------------        
    if (county_upper == 'SANTA_ROSA' and state_upper == 'FL') :
        sql = """DELETE FROM raw_fl_santa_rosa_parcels WHERE feat_type in ('ROAD') """
        print(sql)
        cursor.execute(sql)
        connection.commit()
        
        
    #-----------------------------------------------------------------------------------------
    # UPDATE ST_JOHNS - remove ROW polygons
    #-----------------------------------------------------------------------------------------        
    if (county_upper == 'ST_JOHNS' and state_upper == 'FL') :
        sql = """DELETE FROM raw_fl_st_johns_parcels WHERE strap in ('4444444444') """
        print(sql)
        cursor.execute(sql)
        connection.commit()
        
    #-----------------------------------------------------------------------------------------
    # UPDATE VOLUSIA
    #-----------------------------------------------------------------------------------------  
    # altkey os treated as real in shapefile, so it comes in like 123456.00000000
    if (county_upper == 'VOLUSIA' and state_upper == 'FL') :
        # we are doing this because the altkey field comes in a numeric
        # we could probably cast to int as well
        sql = """
            ALTER TABLE raw_fl_volusia_parcels ALTER COLUMN altkey SET DATA TYPE text;
            UPDATE raw_fl_volusia_parcels SET altkey = split_part(altkey, '.', 1);
            """
        print(sql)
        cursor.execute(sql)
        connection.commit()
        
    #-----------------------------------------------------------------------------------------
    # UPDATE WALTON - remove road row and other polygons
    #-----------------------------------------------------------------------------------------        
    if (county_upper == 'WALTON' and state_upper == 'FL') :
        sql = """DELETE FROM raw_fl_walton_parcels WHERE parcelno in ('ROAD ROW') """
        print(sql)
        cursor.execute(sql)
        connection.commit()

        sql = """DELETE FROM raw_fl_walton_parcels WHERE parcelno is null """
        print(sql)
        cursor.execute(sql)
        connection.commit()        


    #-----------------------------------------------------------------------------------------
    # UPDATE WASHINGTON
    #-----------------------------------------------------------------------------------------  
    # UNBELIEVABLE - mixed PIN formats in the shapefile parcelno field
    # Some are like '00000000-00-5171-0000'  and others like '000000000051710000'
    # most are clean (no dashes), an that is what we are expecting 
    if (county_upper == 'WASHINGTON' and state_upper == 'FL') :
        sql = """UPDATE raw_fl_washington_parcels SET parcelno = replace(parcelno,'-','')"""
        print(sql)
        cursor.execute(sql)
        connection.commit()
    
    # Transform (dissolve) to multi-poly at the same time
    sql = """
        DELETE FROM parcels_std_2010_shp_temp WHERE d_state_orig = '""" + state_upper + """' AND d_county_orig = '""" + county_upper + """';
        INSERT INTO parcels_std_2010_shp_temp (wkb_geometry, """ + pin_insert_cols + """,d_date_orig, d_county_orig, d_state_orig)
            SELECT 
                ST_Multi(ST_Transform(ST_Union(raw.wkb_geometry), 32767)) as wkb_geometry, 
                """ + pin_select_cols + """,
                '""" + shp_date + """',
                '""" + county_upper + """',
                '""" + state_upper + """'
                FROM raw_""" + state_lower + """_""" + county_lower + """_parcels as raw
                GROUP BY """ + import_fields + """;
        """
    print(sql)
    cursor.execute(sql)
    connection.commit()
 

    # If the county keeps dissapearing after running this code, then THIS IS THE CULPRIT.
    # Volusia parcels kept vanishing and this was the cause!
    # Empty PINs get deleted!
    # Shouldn't this be happening to Martin as well?
    # Basically, any geometry with missing shp_pin value should be included here, right?
    # Make sure there are no empty polygons.
    # BE CAREFUL WITH THIS ONE - ESPECIALLY in COUNTIES WHERE altkey is the main key
    # DON'T do this for counties that use altkey
    if (county_upper not in ['BREVARD', 'CITRUS','CLAY','MARTIN','VOLUSIA'] and state_upper == 'FL') :
        sql = """
            DELETE FROM parcels_std_2010_shp_temp
                WHERE d_state_orig = '""" + state_upper + """' AND d_county_orig = '""" + county_upper + """' and (pin_orig is null or pin_orig = '' or pin_orig = '0');
            """
        print(sql)
        cursor.execute(sql)
        connection.commit()
   

    # HENDRY
    if (county_upper in ['HENDRY'] and state_upper == 'FL') :
        sql = """UPDATE parcels_std_2010_shp_temp SET pin2_orig = replace(pin_orig,' ','') WHERE d_county_orig = 'HENDRY' AND d_state_orig = 'FL'"""
        print(sql)
        cursor.execute(sql)
        connection.commit()

        sql = """UPDATE parcels_std_2010_shp_temp SET pin2_orig = replace(pin2_orig,'-','') WHERE d_county_orig = 'HENDRY' AND d_state_orig = 'FL'"""
        print(sql)
        cursor.execute(sql)
        connection.commit()

        sql = """UPDATE parcels_std_2010_shp_temp SET pin2_orig = replace(pin2_orig,'.','') WHERE d_county_orig = 'HENDRY' AND d_state_orig = 'FL'"""
        print(sql)
        cursor.execute(sql)
        connection.commit()        

    # HERNANDO
    if (county_upper in ['HERNANDO'] and state_upper == 'FL') :
        sql = """UPDATE parcels_std_2010_shp_temp SET pin2_orig = replace(pin_orig,' ','') WHERE d_county_orig = 'HERNANDO' AND d_state_orig = 'FL'"""
        print(sql)
        cursor.execute(sql)
        connection.commit()        

    # MARION
    #if (county_upper in ['MARION']) :
    #    sql = """UPDATE parcels_std_2010_shp_temp SET pin2_orig = replace(replace(pin_orig,'-',''),'+','') WHERE d_county_orig = 'MARION'"""
    #    print sql
    #    cursor.execute(sql)
    #    connection.commit()

    # PINELLAS specific update 
    #if (county_upper in ['MARION']) :
    #    sql = """UPDATE parcels_std_2010_shp_temp SET pin_clean_orig = replace(pin_orig,' ','') WHERE d_county_orig = 'PINELLAS'"""
    #    print sql
    #    cursor.execute(sql)
    #    connection.commit()  
       
        
    # TAYLOR specific update for matching how PA likes PINs on their website
    # No longer valid - 8/2016
    #if (county_upper in ['TAYLOR']) :
    #    sql = """
    #        UPDATE parcels_std_2010_shp_temp
    #            SET altkey_orig = substr(pin_orig,7,5) || '-' || substr(pin_orig,12,3)
    #            WHERE d_county_orig = 'TAYLOR';
    #    """
    #    print sql
    #    cursor.execute(sql)
    #    connection.commit()

    # Fix polygons that are geometrycollections - combo of polygon and line - created from st_union
    sql = """
        UPDATE parcels_std_2010_shp_temp 
            SET wkb_geometry = ST_Buffer(wkb_geometry, 0.0)
            WHERE  geometrytype(wkb_geometry) = 'GEOMETRYCOLLECTION' and  d_county_orig = '""" + county_upper + """';
        """
    print(sql)
    cursor.execute(sql)
    connection.commit()
    

    # FIX invalid polys created from st_union
    # Use ST_MakeValid(wkb_geometry) whenever possible - if too slow, kill it and use older technique
    if (county_upper in ['HAMILTON','POLK'] and state_upper == 'FL') :

        sql = """
            UPDATE parcels_std_2010_shp_temp 
                SET wkb_geometry = ST_Buffer(wkb_geometry, 0.0)
                WHERE  ST_IsValid(wkb_geometry) = false and ST_IsValidReason(wkb_geometry) like '%Ring Self-intersection%' and d_state_orig = '""" + state_upper + """' AND d_county_orig = '""" + county_upper + """';
            """
        print(sql)
        cursor.execute(sql)
        connection.commit()
        
        #fix a few polygons that weren't fixed by above
        #fix_counties = ['BAY','BREVARD','BROWARD','PALM_BEACH','SUMTER']
        sql = """
            UPDATE parcels_std_2010_shp_temp 
                SET wkb_geometry = ST_Buffer(wkb_geometry, 0.0)
                WHERE ST_IsValid(wkb_geometry) is false and  d_county_orig = '""" + county_upper + """';
            """
        print(sql)
        cursor.execute(sql)
        connection.commit()

        # fix a few more by county, where the county is still having problems
        sql = """
            UPDATE parcels_std_2010_shp_temp 
                SET wkb_geometry = ST_Buffer(wkb_geometry, 0.0)
                WHERE  ST_IsValid(wkb_geometry) = false and ST_IsValidReason(wkb_geometry) like '%Ring Self-intersection%' and d_state_orig = '""" + state_upper + """' AND d_county_orig = '""" + county_upper + """';
            """
        print(sql)
        cursor.execute(sql)
        connection.commit()
    
    else :
        sql = """
            UPDATE parcels_std_2010_shp_temp
                SET wkb_geometry = ST_MakeValid(wkb_geometry) WHERE ST_IsValid(wkb_geometry) is false and d_state_orig = '""" + state_upper + """' AND d_county_orig = '""" + county_upper + """';
            """
        print(sql)
        cursor.execute(sql)
        connection.commit()    


    # Fix polygons that are geometrycollections - combo of polygon and line - created from st_union
    sql = """
        UPDATE parcels_std_2010_shp_temp 
            SET wkb_geometry = ST_Buffer(wkb_geometry, 0.0)
            WHERE  geometrytype(wkb_geometry) = 'GEOMETRYCOLLECTION' and d_state_orig = '""" + state_upper + """' AND d_county_orig = '""" + county_upper + """';
        """
    print(sql)
    cursor.execute(sql)
    connection.commit()

    
    # close communication with the database
    cursor.close()
    connection.close()
    return()   

#-- END FUNCTIONS
#--####################################################################################

print('\n-------------------------------\n')
# DEBUG
debug = 'True'
#debug = 'False'

# define all text messages here
# load_parcel_geometry.py baker current process_shapefile xx gisdev
#
msgUsage = "Usage: load_parcel_geometry <state> <county> <date_stamp> <data_date>"
msgInvalidCounty = "Invalid county -- "
msgInvalidPath = "Path does not exist -- "
msgInvalidFile = "File does not exist  -- "

#
# retrieve the required parameters
#
try:
    state = sys.argv[1].lower()
    county = sys.argv[2].lower()
    date_stamp = sys.argv[3]
    #myFunction = sys.argv[3]
    data_date = sys.argv[4]
    #server = sys.argv[4].upper()
    
    #try:
    #    extraArgs = sys.argv[6]
    #except:
    #    pass
    
except:
    print(msgUsage)
    sys.exit(0)

server = 'GISDEV'
print(server)

if server == 'GISDEV' :
    pg_connection = 'host=localhost port=5432 dbname=gisdev user=postgres password=galactic529'
    pg_psql = 'psql -p 5432 -d gisdev -U postgres '
    
elif server == 'GISLIB' :
    pg_connection = 'host=localhost port=5432 dbname=gislib user=postgres password=galactic529'
    pg_psql = 'psql -p 5432 -d gislib -U postgres '
    
else :
    print("must set server name")
    sys.exit(0)

county_upper = county.upper()
state_upper = state.upper()

# Check for miami-dade special handling of dash vs. underscore required
if (county_upper == 'MIAMI-DADE') :
    county = 'miami_dade'
    county_upper = 'MIAMI-DADE'


# Georgia shapefiles
# /srv/mapwise_dev/county/a_GA_attom/parcels-ga-c348bd4a-shp
#
# check existance of proc_dir
# if it doesn't exist, create it
#pathTopDir = ''.join(['/srv/mapwise_dev/county/a_GA_attom/'])
#pathTopDir = ''.join(['/srv/mapwise_dev/county/',county,'/processing/database'])
#print 'pathTopDir: ',pathTopDir
# pathProcessing = ''.join(['/srv/mapwise_dev/county/a_GA_attom/',date_stamp])
pathProcessing = ''.join(['/srv/mapwise_dev/county/',county,'/processing/vector/propapp/',date_stamp])
print('pathProcessing: ',pathProcessing)
pathSourceData = ''.join([pathProcessing,'/source_data'])
pathSourceData = pathSourceData
print('pathProcessing: ',pathProcessing)
print('pathSourceData: ',pathSourceData)

if debug == 'True' :
    print("dirname:",os.path.dirname(pathSourceData))
    print("isdir:",os.path.isdir(pathSourceData))

#if not os.path.isdir(pathSourceData) and myFunction != 'archive_parcel_data':
if not os.path.isdir(pathSourceData):
    print(msgInvalidPath,pathSourceData)
    sys.exit(0)
    #os.makedirs(pathSourceData)





load_parcel_geometry(state,county,'TRUE')


