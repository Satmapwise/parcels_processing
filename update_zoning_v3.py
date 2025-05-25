#!/usr/bin/env python3
# Load raw zoning and process into standardized zoning

import sys
import os
import argparse
import datetime
from io import StringIO
import psycopg2
import psycopg2.extras
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Config:
    """Configuration settings for zoning processing"""
    def __init__(self):
        # Database configuration - use environment variables for security
        self.pg_user = os.environ.get('PG_USER', 'postgres')
        self.pg_password = os.environ.get('PG_PASSWORD', 'galactic529')  # Default for backward compatibility
        self.pg_host = os.environ.get('PG_HOST', 'localhost')
        self.pg_port = os.environ.get('PG_PORT', '5432')
        self.pg_dbname = os.environ.get('PG_DBNAME', 'gisdev')
        
        # File paths
        self.top_level_dir = '/srv/datascrub/08_Land_Use_and_Zoning/zoning/florida/county/'
        self.backup_dir = '/var/www/apps/mapwise/htdocs/x342/'
        
        # Connection strings
        self.pg_connection = f"host={self.pg_host} port={self.pg_port} dbname={self.pg_dbname} user={self.pg_user} password={self.pg_password}"
        self.pg_psql = f'psql -p {self.pg_port} -d {self.pg_dbname} -U {self.pg_user} -c '

class DatabaseManager:
    """Handles database operations"""
    def __init__(self, config):
        self.config = config
        self.connection = psycopg2.connect(config.pg_connection)
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    def execute_query(self, query, params=None, commit=True):
        """Execute SQL query and optionally commit"""
        logger.info(f"Executing SQL: {query}")
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)
        
        if commit:
            self.connection.commit()
        
        return self.cursor
    
    def execute_command(self, cmd):
        """Execute shell command"""
        logger.info(f"Executing command: {cmd}")
        return os.system(cmd)
    
    def fetch_all(self):
        """Fetch all rows from last query"""
        return self.cursor.fetchall()
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()

class CountyRulesManager:
    """Manages county-specific rules"""
    def __init__(self):
        # Dictionary of county/city pairs and their delete conditions
        self.delete_conditions = {
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
        
        # Counties that need dissolve and explode operations
        self.dissolve_counties = [
            ('LAKE', 'UNINCORPORATED'),
            ('OSCEOLA', 'ST_CLOUD'),
            ('SEMINOLE', 'CASSELBERRY'),
            ('SEMINOLE', 'LONGWOOD'),
            ('SEMINOLE', 'OVIEDO1'),
            ('SEMINOLE', 'SANFORD'),
            ('SEMINOLE', 'WINTER_SPRINGS'),
            ('ST_LUCIE', 'UNINCORPORATED11')
        ]
    
    def get_delete_condition(self, county, city):
        """Get delete condition for county/city if it exists"""
        return self.delete_conditions.get((county, city))
    
    def needs_dissolve(self, county, city):
        """Check if county/city needs dissolve and explode operations"""
        return (county, city) in self.dissolve_counties

class ShapefileProcessor:
    """Handles shapefile processing operations"""
    def __init__(self, db_manager, config):
        self.db = db_manager
        self.config = config
    
    def repair_shapefile(self, shp_name):
        """Repair shapefile"""
        repair_cmd = f'L:/projects/tools/python/lib/gp_repair_geometry.py {shp_name}'
        logger.info(f"Repairing shapefile (skipped): {repair_cmd}")
        # Commented out in original: self.db.execute_command(repair_cmd)
    
    def load_shapefile(self, shp_name, temp_table, columns, srs_epsg):
        """Load shapefile into PostgreSQL"""
        # Drop existing tables
        self.db.execute_command(
            f'{self.config.pg_psql} "DROP TABLE IF EXISTS {temp_table};"')
        self.db.execute_command(
            f'{self.config.pg_psql} "DROP TABLE IF EXISTS {temp_table}_2;"')
        
        # Build ogr2ogr command
        ogr_cmd = (f'ogr2ogr -skipfailures -t_srs "EPSG:32767" -s_srs "EPSG:{srs_epsg}" '
                  f'-select "{columns}" -nlt GEOMETRY -f "PostgreSQL" '
                  f'PG:"user={self.config.pg_user} dbname={self.config.pg_dbname} '
                  f'host={self.config.pg_host} port={self.config.pg_port} '
                  f'password={self.config.pg_password}" -nln {temp_table} {shp_name}')
        
        self.db.execute_command(ogr_cmd)
        
        # Update SRID and fix geometries
        self.update_srid(temp_table)
        self.fix_invalid_geometries(temp_table)
    
    def update_srid(self, table_name):
        """Update SRID of geometry column"""
        srid_cmd = (f'{self.config.pg_psql} "SELECT UpdateGeometrySRID('
                   f"'{table_name}', 'wkb_geometry', 32767)\"")
        self.db.execute_command(srid_cmd)
    
    def fix_invalid_geometries(self, table_name):
        """Fix invalid geometries"""
        fix_cmd = (f'{self.config.pg_psql} "UPDATE {table_name} SET wkb_geometry = '
                  f'ST_MakeValid(wkb_geometry) WHERE st_isvalid(wkb_geometry) is false;"')
        self.db.execute_command(fix_cmd)
    
    def dissolve_and_explode(self, temp_table, select_col_list):
        """Dissolve polygons and explode to single parts"""
        dissolve_sql = f"""
        set enable_hashagg to off;    
        DROP TABLE IF EXISTS {temp_table}_dissolve;
        CREATE TABLE {temp_table}_dissolve AS
        SELECT 
            ST_UNION(wkb_geometry) as wkb_geometry,  
            {select_col_list}
        FROM 
            {temp_table}
        GROUP BY 
            {select_col_list};
        """
        
        self.db.execute_query(dissolve_sql)
        
        # Expand multi-polys to single-polys
        explode_sql = f"""
        TRUNCATE {temp_table};
        INSERT INTO {temp_table} (wkb_geometry,{select_col_list})
        SELECT (ST_DUMP(wkb_geometry)).geom, {select_col_list}
        FROM {temp_table}_dissolve;
        """
        
        self.db.execute_query(explode_sql)

class ZoningProcessor:
    """Main class for processing zoning data"""
    def __init__(self, county, city, config=None):
        if config is None:
            config = Config()
        
        self.config = config
        self.county = county.lower()
        self.city = city.lower()
        self.county_upper = county.upper()
        self.county_lower = county.lower()
        self.city_upper = city.upper()
        self.city_lower = city.lower()
        
        self.db = DatabaseManager(config)
        self.rules = CountyRulesManager()
        self.shapefile = ShapefileProcessor(self.db, config)
        
        # Variables to be populated from transform table
        self.city_name = None
        self.city_name_path = None
        self.shp_name = None
        self.temp_table_name = None
        self.zon_code_col = None
        self.zon_code2_col = None
        self.zon_desc_col = None
        self.zon_gen_col = None
        self.notes_col = None
        self.ord_num_col = None
        self.srs_epsg = None
        self.select_col_list = None
    
    def get_transform_info(self):
        """Get zoning transform info from database"""
        logger.info("LOAD ZONING")
        
        sql = f"SELECT * FROM zoning_transform WHERE county = '{self.county_upper}' AND city_name = '{self.city_upper}';"
        
        self.db.execute_query(sql)
        rows = self.db.fetch_all()
        
        if not rows:
            logger.error(f"No transform information found for {self.county_upper}, {self.city_upper}")
            sys.exit(1)
        
        for row in rows:
            logger.info(f"city_name = {row['city_name']}")
            self.city_name = row['city_name']
            self.city_name_path = self.city_name.lower()
            
            self.shp_name = row['shp_name']
            self.temp_table_name = row['temp_table_name']
            self.zon_code_col = row['zon_code_col']
            self.zon_code2_col = row['zon_code2_col']
            self.zon_desc_col = row['zon_desc_col']
            self.zon_gen_col = row['zon_gen_col']
            self.notes_col = row['notes_col']
            self.ord_num_col = row['ord_num_col']
            
            self.srs_epsg = str(row['srs_epsg'])
    
    def build_column_list(self):
        """Build column list for import"""
        self.select_col_list = self.zon_code_col
        
        # Add optional columns with fallbacks
        if self.zon_code2_col is not None:
            self.select_col_list += f',{self.zon_code2_col}'
        else:
            self.zon_code2_col = 'Null'
        
        if self.zon_desc_col is not None:
            self.select_col_list += f',{self.zon_desc_col}'
        else:
            self.zon_desc_col = 'Null'
        
        if self.zon_gen_col is not None:
            self.select_col_list += f',{self.zon_gen_col}'
        else:
            self.zon_gen_col = 'Null'
        
        if self.notes_col is not None:
            self.select_col_list += f',{self.notes_col}'
        else:
            self.notes_col = 'Null'
        
        if self.ord_num_col is not None:
            self.select_col_list += f',{self.ord_num_col}'
        else:
            self.ord_num_col = 'Null'
        
        logger.info(f"Columns to load: {self.select_col_list}")
    
    def set_working_directory(self):
        """Set working directory"""
        path_src = os.path.join(
            self.config.top_level_dir, 
            self.county_lower, 
            'current/source_data/', 
            self.city_name_path
        )
        logger.info(f"Setting working directory: {path_src}")
        os.chdir(path_src)
        return path_src
    
    def apply_county_rules(self):
        """Apply county-specific rules"""
        # Apply delete conditions
        delete_condition = self.rules.get_delete_condition(self.county_upper, self.city_name)
        if delete_condition:
            delete_cmd = f'{self.config.pg_psql} "DELETE FROM {self.temp_table_name} WHERE {delete_condition};"'
            self.db.execute_command(delete_cmd)
        
        # Special case for BROWARD county
        if self.county_upper == 'BROWARD' and self.city_name == 'A_PROPERTY_APPRAISER_UNIFIED':
            logger.info("UPDATE BROWARD CITY NAME BASED ON OVERLAY WITH CITIES")
            logger.info("No command specified in original script")
        
        # Apply dissolve and explode
        if self.rules.needs_dissolve(self.county_upper, self.city_name):
            self.shapefile.dissolve_and_explode(self.temp_table_name, self.select_col_list)
    
    def create_standardized_table(self):
        """Create and populate standardized table"""
        # Create table
        create_cmd = (f'{self.config.pg_psql} "CREATE TABLE {self.temp_table_name}_2 '
                     f'(zon_code text, zon_code2 text, zon_desc text, zon_gen text, '
                     f'ord_num text, city_name text, county_name text, notes text, the_geom geometry);"')
        self.db.execute_command(create_cmd)
        
        # Insert data
        insert_cmd = (f'{self.config.pg_psql} "INSERT INTO {self.temp_table_name}_2 '
                     f'(zon_code, zon_code2, zon_desc, zon_gen, ord_num, city_name, county_name, notes, the_geom) '
                     f'SELECT {self.zon_code_col}, {self.zon_code2_col}, {self.zon_desc_col}, '
                     f"{self.zon_gen_col}, {self.ord_num_col}, '{self.city_name}', '{self.county_upper}', "
                     f'{self.notes_col}, wkb_geometry FROM {self.temp_table_name};"')
        self.db.execute_command(insert_cmd)
        
        # Apply Bay county processing
        if self.county_upper == 'BAY':
            self._apply_bay_county_processing()
    
    def _apply_bay_county_processing(self):
        """Special processing for Bay county"""
        city_mappings = {
            'CALLAWAY': '2',
            'LYNN HAVEN': '3',
            'MEXICO BEACH': '4',
            'PANAMA CITY': '5',
            'PANAMA CITY BEACH': '6',
            'PARKER': '7',
            'SPRINGFIELD': '8'
        }
        
        for city, code in city_mappings.items():
            update_cmd = (f'{self.config.pg_psql} "UPDATE {self.temp_table_name}_2 '
                         f"SET city_name = '{city}' WHERE zon_code2 = '{code}';"")
            self.db.execute_command(update_cmd)
    
    def update_zoning_table(self):
        """Update main zoning table"""
        # Delete existing data
        delete_cmd = (f'{self.config.pg_psql} "DELETE FROM zoning WHERE city_name = '
                     f"'{self.city_name}' AND county_name = '{self.county_upper}';"")
        self.db.execute_command(delete_cmd)
        
        # Special case for Bay county
        if self.county_upper == 'BAY':
            bay_delete_cmd = (f'{self.config.pg_psql} "DELETE FROM zoning WHERE '
                            f"city_name IN ('CALLAWAY','LYNN HAVEN','MEXICO BEACH',"
                            f"'PANAMA CITY','PANAMA CITY BEACH','PARKER','SPRINGFIELD') "
                            f"AND county_name = 'BAY';"")
            self.db.execute_command(bay_delete_cmd)
        
        # Insert data into zoning table
        insert_cmd = (f'{self.config.pg_psql} "INSERT INTO zoning '
                     f'(zon_code, zon_code2, zon_desc, zon_gen, ord_num, city_name, county_name, notes, the_geom) '
                     f'SELECT zon_code, zon_code2, zon_desc, zon_gen, ord_num, city_name, county_name, notes, the_geom '
                     f'FROM {self.temp_table_name}_2;"')
        self.db.execute_command(insert_cmd)
        
        # Special processing for Miami-Dade
        if self.county_upper == 'MIAMI-DADE' and self.city_name == 'INCORPORATED':
            # Move city name to proper column
            update_cmd1 = (f'{self.config.pg_psql} "UPDATE zoning SET city_name = zon_code2 '
                         f"WHERE county_name = 'MIAMI-DADE' AND city_name != 'UNINCORPORATED';"")
            self.db.execute_command(update_cmd1)
            
            # Clear city name from old column
            update_cmd2 = (f'{self.config.pg_psql} "UPDATE zoning SET zon_code2 = NULL '
                         f"WHERE county_name = 'MIAMI-DADE' AND city_name != 'UNINCORPORATED';"")
            self.db.execute_command(update_cmd2)
    
    def generate_backup(self):
        """Generate backup and server scripts"""
        # Create backup file
        backup_path = os.path.join(self.config.backup_dir, f"{self.temp_table_name}.backup")
        backup_cmd = (f'pg_dump --port {self.config.pg_port} --username {self.config.pg_user} '
                     f'--format custom --verbose --file "{backup_path}" '
                     f'--table "\\"temp\\".\\""{self.temp_table_name}_2\\"" {self.config.pg_dbname}')
        self.db.execute_command(backup_cmd)
        
        # Print server update instructions
        self._print_server_update_instructions()
        
        # Generate batch file
        self._generate_server_batch_file()
    
    def _print_server_update_instructions(self):
        """Print server update instructions"""
        logger.info("----- SCRIPT to update on server -----")
        
        print(" ")
        print(f'pg_restore -p 5432 -U postgres -d gislib -v "/home/bmay/incoming/{self.temp_table_name}.backup"')
        print(" ")
        
        # Delete from zoning table
        print(f'psql -p 5432 -U postgres -d gislib -c "DELETE FROM zoning WHERE city_name = '
             f"'{self.city_name}' AND county_name = '{self.county_upper}';"")
        
        # Special case for Miami-Dade
        if self.county_upper == 'MIAMI-DADE' and self.city_name == 'INCORPORATED':
            print('psql -p 5432 -U postgres -d gislib -c "DELETE FROM zoning WHERE county_name = \'MIAMI-DADE\';"')
        print(" ")
        
        # Insert into zoning table
        print(f'psql -d gislib -U postgres -p 5432 -c "INSERT INTO zoning '
             f'(zon_code, zon_code2, zon_desc, zon_gen, ord_num, city_name, county_name, notes, the_geom) '
             f'SELECT zon_code, zon_code2, zon_desc, zon_gen, ord_num, city_name, county_name, notes, the_geom '
             f'FROM {self.temp_table_name}_2;"')
        print(" ")
        
        # Special case for Miami-Dade
        if self.county_upper == 'MIAMI-DADE' and self.city_name == 'INCORPORATED':
            print('psql -p 5432 -U postgres -d gislib -c "UPDATE zoning SET city_name = zon_code2 '
                 f"WHERE county_name = 'MIAMI-DADE' AND city_name != 'UNINCORPORATED';"")
            print(" ")
            print('psql -p 5432 -U postgres -d gislib -c "UPDATE zoning SET zon_code2 = NULL '
                 f"WHERE county_name = 'MIAMI-DADE' AND city_name != 'UNINCORPORATED';"")
        print(" ")
        
        # Drop temp table
        print(f'psql -d gislib -U postgres -p 5432 -c "DROP TABLE {self.temp_table_name}_2;"')
        print(" ")
        print(" ")
        print("----- END SCRIPT to update on server -----")
    
    def _generate_server_batch_file(self):
        """Generate batch file for server update"""
        batch_file = os.path.join(self.config.backup_dir, f"{self.temp_table_name}.bat")
        
        commands = [
            f'pg_restore -h -U postgres -d gislib -v "/home/bmay/incoming/{self.temp_table_name}.backup"',
            f'psql -p 5432 -U postgres -d gislib -c "DELETE FROM zoning WHERE city_name = \'{self.city_name}\' AND county_name = \'{self.county_upper}\';"'
        ]
        
        # Special case for Miami-Dade
        if self.county_upper == 'MIAMI-DADE' and self.city_name == 'INCORPORATED':
            commands.append('psql -d gislib -U postgres -p 5432 -c "DELETE FROM zoning WHERE county_name = \'MIAMI-DADE\';"')
        
        # Insert into zoning
        commands.append(f'psql -d gislib -U postgres -p 5432 -c "INSERT INTO zoning '
                        f'(zon_code, zon_code2, zon_desc, zon_gen, ord_num, city_name, county_name, notes, the_geom) '
                        f'SELECT {self.zon_code_col}, {self.zon_code2_col}, {self.zon_desc_col}, '
                        f"{self.zon_gen_col}, {self.ord_num_col}, '{self.city_name}', '{self.county_upper}', "
                        f'{self.notes_col}, wkb_geometry FROM {self.temp_table_name}_2;"')
        
        # Special case for Miami-Dade
        if self.county_upper == 'MIAMI-DADE' and self.city_name == 'INCORPORATED':
            commands.append('psql -d gislib -U postgres -p 5432 -c "UPDATE zoning SET city_name = zon_code2 '
                           f"WHERE county_name = 'MIAMI-DADE' AND city_name != 'UNINCORPORATED';"")
            commands.append('psql -d gislib -U postgres -p 5432 -c "UPDATE zoning SET zon_code2 = NULL '
                           f"WHERE county_name = 'MIAMI-DADE' AND city_name != 'UNINCORPORATED';"")
        
        # Drop temp table
        commands.append(f'psql -d gislib -U postgres -p 5432 -c "DROP TABLE {self.temp_table_name}_2;"')
        
        # Write the commands to the file
        with open(batch_file, 'w') as f:
            for cmd in commands:
                f.write(f"{cmd}\n")
    
    def process(self):
        """Run full zoning process workflow"""
        try:
            # Get transform info
            self.get_transform_info()
            
            # Build column list
            self.build_column_list()
            
            # Set working directory
            self.set_working_directory()
            
            # Repair shapefile (commented out in original)
            self.shapefile.repair_shapefile(self.shp_name)
            
            # Load shapefile
            self.shapefile.load_shapefile(
                self.shp_name, 
                self.temp_table_name, 
                self.select_col_list, 
                self.srs_epsg
            )
            
            # Apply county rules
            self.apply_county_rules()
            
            # Create standardized table
            self.create_standardized_table()
            
            # Update zoning table
            self.update_zoning_table()
            
            # Generate backup and scripts
            self.generate_backup()
            
            logger.info("Zoning processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error processing zoning data: {str(e)}")
            raise
        finally:
            # Close database connection
            self.db.close()

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Process zoning data")
    parser.add_argument("county", help="County name")
    parser.add_argument("city", help="City name")
    args = parser.parse_args()
    
    # Create and run processor
    processor = ZoningProcessor(args.county, args.city)
    processor.process()

if __name__ == "__main__":
    main()
