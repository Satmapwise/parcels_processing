#!/usr/bin/env python3
"""
Layer Database Standardization Script

This script standardizes the database and manifest files for Florida GIS layers.
It ensures consistency between layer_manifest.json, m_gis_data_catalog_main table,
and transform tables (zoning_transform, flu_transform).

Usage:
    python layer_standardize_database.py <layer> <county> <city> [options]
    python layer_standardize_database.py all [options]
    python layer_standardize_database.py --check <layer> [options]
    python layer_standardize_database.py --create <layer> <county> <city> [options]
    python layer_standardize_database.py --manual-fill [options]

Examples:
    python layer_standardize_database.py zoning alachua gainesville
    python layer_standardize_database.py flu all
    python layer_standardize_database.py --check zoning
    python layer_standardize_database.py --create zoning new_county new_city
    python layer_standardize_database.py --manual-fill
"""

import sys
import os
import json
import argparse
import logging
import csv
import re
import requests
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from urllib.parse import urlparse
import psycopg2
import psycopg2.extras

# Configuration variables
optional_conditions = True
generate_CSV = True
debug = False
test_mode = True

# Database connection
PG_CONNECTION = 'host=localhost port=5432 dbname=gisdev user=postgres password=galactic529'

# Layer mappings
LAYER_GROUPS = {
    'zoning': 'flu_zoning',
    'flu': 'flu_zoning',
    'future_land_use': 'flu_zoning'
}

LAYER_CATEGORIES = {
    'zoning': '08_Land_Use_and_Zoning',
    'flu': '08_Land_Use_and_Zoning',
    'future_land_use': '08_Land_Use_and_Zoning'
}

# Layer to transform table mapping
TRANSFORM_TABLES = {
    'zoning': 'zoning_transform',
    'flu': 'flu_transform',
    'future_land_use': 'flu_transform'
}

# Layer to temp table prefix mapping
TEMP_TABLE_PREFIXES = {
    'zoning': 'raw_zon',
    'flu': 'raw_flu',
    'future_land_use': 'raw_flu'
}

# Layer display names
LAYER_DISPLAY_NAMES = {
    'zoning': 'Zoning',
    'flu': 'Future Land Use',
    'future_land_use': 'Future Land Use'
}

# Setup logging
logging.basicConfig(
    level=logging.DEBUG if debug else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Handles database operations"""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.connect()
    
    def connect(self):
        """Connect to database"""
        try:
            self.connection = psycopg2.connect(PG_CONNECTION)
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict]:
        """Execute query and return results"""
        try:
            if self.cursor is None:
                raise Exception("Database cursor is not initialized")
            
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            if query.strip().upper().startswith('SELECT'):
                results = self.cursor.fetchall()
                # Convert DictRow objects to regular dictionaries
                return [dict(row) for row in results]
            else:
                if self.connection:
                    self.connection.commit()
                return []
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


class ManifestManager:
    """Handles manifest file operations"""
    
    def __init__(self, manifest_file: str = 'layer_manifest.json'):
        self.manifest_file = manifest_file
        self.manifest = self.load_manifest()
    
    def load_manifest(self) -> Dict:
        """Load manifest file"""
        try:
            with open(self.manifest_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load manifest: {e}")
            raise
    
    def get_entities(self, layer: str) -> List[str]:
        """Get all entities for a layer"""
        if layer not in self.manifest:
            return []
        return list(self.manifest[layer]['entities'].keys())
    
    def get_entity_commands(self, layer: str, entity: str) -> List:
        """Get commands for an entity"""
        if layer not in self.manifest or entity not in self.manifest[layer]['entities']:
            return []
        return self.manifest[layer]['entities'][entity]
    
    def is_ags_download(self, commands: List) -> bool:
        """Check if entity uses AGS download"""
        if not commands:
            return False
        first_command = commands[0]
        if isinstance(first_command, list) and len(first_command) > 1:
            return 'ags_extract_data2.py' in first_command[1]
        return False
    
    def get_target_city(self, commands: List, entity_name: str) -> str:
        """Extract target city from commands"""
        # Default to entity city name
        entity_parts = entity_name.split('_', 1)
        if len(entity_parts) > 1:
            default_city = entity_parts[1]
        else:
            default_city = entity_parts[0]
        
        # Look for target city in update commands
        for command in commands:
            if isinstance(command, list) and len(command) > 2:
                if 'update_zoning2.py' in command[1] or 'update_flu.py' in command[1]:
                    if len(command) > 3:
                        return command[3]  # city parameter
        return default_city


class LayerStandardizer:
    """Main class for standardizing layer data"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.manifest = ManifestManager()
        self.missing_fields = {}
        self.duplicates = []
    
    def get_format(self, src_url_file: str) -> str:
        """Determine format from source URL"""
        if not src_url_file:
            return 'UNKNOWN'
        
        url_lower = src_url_file.lower()
        if '.shp' in url_lower or 'shapefile' in url_lower:
            return 'SHP'
        elif '.zip' in url_lower:
            return 'ZIP'
        elif 'ags' in url_lower or 'arcgis' in url_lower:
            return 'AGS'
        elif '.geojson' in url_lower:
            return 'GEOJSON'
        elif '.kml' in url_lower:
            return 'KML'
        else:
            return 'UNKNOWN'
    
    def validate_url(self, url: str) -> bool:
        """Validate if URL is accessible"""
        if not url:
            return False
        try:
            response = requests.head(url, timeout=10)
            return response.status_code == 200
        except:
            return False
    
    def format_title(self, layer: str, county: str, city: str, entity_type: str) -> str:
        """Format title based on entity type"""
        layer_display = LAYER_DISPLAY_NAMES.get(layer, layer.title())
        
        if entity_type in ['unincorporated', 'unified']:
            return f"{layer_display} - {county.title()} {entity_type.title()}"
        else:
            return f"{layer_display} - City of {city.title()}"
    
    def format_table_name(self, layer: str, county: str, city: str, entity_type: str) -> str:
        """Format table name based on entity type"""
        if entity_type in ['unincorporated', 'unified']:
            return f"{layer}_{county}_{entity_type}"
        else:
            return f"{layer}_{city}"
    
    def format_temp_table_name(self, layer: str, county: str, city: str) -> str:
        """Format temp table name"""
        prefix = TEMP_TABLE_PREFIXES.get(layer, f"raw_{layer}")
        return f"{prefix}_{county}_{city}"
    
    def format_sys_raw_folder(self, layer_group: str, layer: str, county: str, city: str) -> str:
        """Format system raw folder path"""
        return f"/srv/datascrub/{layer_group}/{layer}/florida/county/{county}/current/source_data/{city}"
    
    def ensure_directory(self, path: str):
        """Ensure directory exists"""
        if not test_mode:
            os.makedirs(path, exist_ok=True)
            logger.info(f"Created directory: {path}")
    
    def find_catalog_records(self, layer: str, county: str, target_city: str) -> List[Dict]:
        """Find records in m_gis_data_catalog_main"""
        layer_display = LAYER_DISPLAY_NAMES.get(layer, layer.title())
        
        # Search by title containing layer name, county, and city
        # This ensures we only find records for the specific county
        query = """
        SELECT *, table_name as id FROM m_gis_data_catalog_main 
        WHERE LOWER(title) LIKE LOWER(%s) 
        AND LOWER(title) LIKE LOWER(%s)
        AND (LOWER(title) LIKE LOWER(%s) OR LOWER(city) LIKE LOWER(%s))
        """
        params = (f"%{layer_display}%", f"%{county}%", f"%{target_city}%", f"%{target_city}%")
        
        return self.db.execute_query(query, params)
    
    def find_transform_record(self, layer: str, county: str, city: str) -> Optional[Dict]:
        """Find record in transform table"""
        transform_table = TRANSFORM_TABLES.get(layer)
        if not transform_table:
            return None
        
        query = f"""
        SELECT * FROM {transform_table} 
        WHERE LOWER(county) = LOWER(%s) AND LOWER(city_name) = LOWER(%s)
        """
        params = (county, city)
        
        results = self.db.execute_query(query, params)
        return results[0] if results else None
    
    def update_catalog_record(self, record: Dict, layer: str, county: str, city: str, 
                            entity_type: str, is_ags: bool, target_city: str):
        """Update catalog record"""
        # Use table_name as the identifier since it should be unique
        table_name_identifier = record.get('table_name', record.get('id'))
        if not table_name_identifier:
            logger.error(f"No identifier found for record: {record}")
            return
        
        # Prepare update data
        title = self.format_title(layer, county, city, entity_type)
        table_name = self.format_table_name(layer, county, city, entity_type)
        layer_group = LAYER_GROUPS.get(layer, layer)
        category = LAYER_CATEGORIES.get(layer, layer)
        sys_raw_folder = self.format_sys_raw_folder(layer_group, layer, county, city)
        
        # Ensure directory exists
        self.ensure_directory(sys_raw_folder)
        
        # Build update query
        update_fields = []
        update_params = []
        
        update_fields.extend([
            "title = %s", "county = %s", "city = %s", "format = %s", 
            "download = %s", "layer_group = %s", "category = %s", 
            "sys_raw_folder = %s", "table_name = %s"
        ])
        update_params.extend([
            title, county.upper(), city.upper(), self.get_format(record.get('src_url_file', '')),
            'AUTO', layer_group, category, sys_raw_folder, table_name
        ])
        
        # Handle non-AGS resource format
        if not is_ags:
            expected_resource = f"/data/{layer}/{county}/{city}"
            if record.get('resource') != expected_resource:
                logger.warning(f"Resource mismatch for {layer}_{county}_{city}: expected {expected_resource}, got {record.get('resource')}")
        
        # Handle fields_obj_transform
        if not record.get('fields_obj_transform'):
            self.missing_fields[f"{layer}_{county}_{city}"] = {
                'fields_obj_transform': 'MISSING'
            }
        else:
            update_fields.append("fields_obj_transform = %s")
            update_params.append(record['fields_obj_transform'])
        
        # Handle src_url_file
        if not record.get('src_url_file'):
            if f"{layer}_{county}_{city}" not in self.missing_fields:
                self.missing_fields[f"{layer}_{county}_{city}"] = {}
            self.missing_fields[f"{layer}_{county}_{city}"]['src_url_file'] = 'MISSING'
        else:
            update_fields.append("src_url_file = %s")
            update_params.append(record['src_url_file'])
        
        # Optional conditions
        if optional_conditions:
            if not record.get('source_org'):
                if f"{layer}_{county}_{city}" not in self.missing_fields:
                    self.missing_fields[f"{layer}_{county}_{city}"] = {}
                self.missing_fields[f"{layer}_{county}_{city}"]['source_org'] = 'MISSING'
            else:
                update_fields.append("source_org = %s")
                update_params.append(record['source_org'])
        
        # Execute update
        if not test_mode:
            update_params.append(table_name_identifier)
            query = f"UPDATE m_gis_data_catalog_main SET {', '.join(update_fields)} WHERE table_name = %s"
            self.db.execute_query(query, tuple(update_params))
            logger.info(f"Updated catalog record for {layer}_{county}_{city}")
        else:
            logger.info(f"TEST MODE: Would update catalog record for {layer}_{county}_{city}")
    
    def update_transform_record(self, record: Dict, layer: str, county: str, city: str):
        """Update transform record"""
        transform_table = TRANSFORM_TABLES.get(layer)
        if not transform_table:
            return
        
        record_id = record['id']
        temp_table_name = self.format_temp_table_name(layer, county, city)
        
        if not test_mode:
            query = f"""
            UPDATE {transform_table} 
            SET city_name = %s, temp_table_name = %s 
            WHERE id = %s
            """
            self.db.execute_query(query, (city.upper(), temp_table_name, record_id))
            logger.info(f"Updated transform record for {layer}_{county}_{city}")
        else:
            logger.info(f"TEST MODE: Would update transform record for {layer}_{county}_{city}")
    
    def process_entity(self, layer: str, entity: str):
        """Process a single entity"""
        logger.info(f"Processing {layer} entity: {entity}")
        
        # Parse entity name
        entity_parts = entity.split('_', 1)
        if len(entity_parts) != 2:
            logger.warning(f"Invalid entity format: {entity}")
            return
        
        county, city = entity_parts
        
        # Get entity commands
        commands = self.manifest.get_entity_commands(layer, entity)
        if not commands:
            logger.warning(f"No commands found for entity: {entity}")
            return
        
        # Determine download type and target city
        is_ags = self.manifest.is_ags_download(commands)
        target_city = self.manifest.get_target_city(commands, entity)
        
        # Determine entity type
        entity_type = 'city'
        if city in ['unincorporated', 'unified']:
            entity_type = city
        
        # Find catalog records
        catalog_records = self.find_catalog_records(layer, county, target_city)
        
        if len(catalog_records) > 1:
            self.duplicates.append({
                'layer': layer,
                'county': county,
                'city': city,
                'target_city': target_city,
                'records': catalog_records
            })
            logger.warning(f"Multiple catalog records found for {layer}_{county}_{city}")
            return
        
        if not catalog_records:
            logger.warning(f"No catalog record found for {layer}_{county}_{city}")
            return
        
        # Update catalog record
        self.update_catalog_record(
            catalog_records[0], layer, county, city, entity_type, is_ags, target_city
        )
        
        # Update transform record if applicable
        if layer in TRANSFORM_TABLES:
            transform_record = self.find_transform_record(layer, county, target_city)
            if transform_record:
                self.update_transform_record(transform_record, layer, county, city)
            else:
                logger.warning(f"No transform record found for {layer}_{county}_{city}")
    
    def process_layer(self, layer: str, county: Optional[str] = None, city: Optional[str] = None):
        """Process entities for a layer"""
        entities = self.manifest.get_entities(layer)
        
        if county and city:
            if city.lower() == 'all':
                # Process all entities for specific county
                county_entities = [entity for entity in entities if entity.startswith(f"{county}_")]
                if county_entities:
                    for entity in county_entities:
                        self.process_entity(layer, entity)
                else:
                    logger.error(f"No entities found for county {county} in {layer} manifest")
            else:
                # Process specific entity
                entity = f"{county}_{city}"
                if entity in entities:
                    self.process_entity(layer, entity)
                else:
                    logger.error(f"Entity {entity} not found in {layer} manifest")
        elif county and county.lower() == 'all':
            # Process all entities for layer
            for entity in entities:
                self.process_entity(layer, entity)
        else:
            # Process all entities
            for entity in entities:
                self.process_entity(layer, entity)
    
    def check_orphaned_records(self, layer: str) -> List[Dict]:
        """Find records in database without manifest entries"""
        orphaned = []
        
        # Check catalog records
        layer_display = LAYER_DISPLAY_NAMES.get(layer, layer.title())
        query = """
        SELECT * FROM m_gis_data_catalog_main 
        WHERE LOWER(title) LIKE LOWER(%s)
        """
        catalog_records = self.db.execute_query(query, (f"%{layer_display}%",))
        
        for record in catalog_records:
            # Extract county and city from title
            title = record['title']
            # This is a simplified check - you might need more sophisticated parsing
            if not any(entity in title.lower() for entity in self.manifest.get_entities(layer)):
                orphaned.append({
                    'table': 'm_gis_data_catalog_main',
                    'record': record
                })
        
        # Check transform records
        if layer in TRANSFORM_TABLES:
            transform_table = TRANSFORM_TABLES[layer]
            query = f"SELECT * FROM {transform_table}"
            transform_records = self.db.execute_query(query)
            
            for record in transform_records:
                entity = f"{record['county'].lower()}_{record['city_name'].lower()}"
                if entity not in self.manifest.get_entities(layer):
                    orphaned.append({
                        'table': transform_table,
                        'record': record
                    })
        
        return orphaned
    
    def generate_csv_report(self, layer: str, check_results: List[Dict]):
        """Generate CSV report for check mode"""
        if not generate_CSV:
            return
        
        timestamp = datetime.now().strftime('%Y-%m-%d')
        filename = f"{layer}_database_check_{timestamp}.csv"
        
        # Check if file exists to append or create new
        file_exists = os.path.exists(filename)
        
        with open(filename, 'a', newline='') as csvfile:
            fieldnames = [
                'layer', 'county', 'city', 'entity_type', 'download_type',
                'catalog_title', 'catalog_county', 'catalog_city', 'catalog_format',
                'catalog_download', 'catalog_resource', 'catalog_layer_group',
                'catalog_category', 'catalog_sys_raw_folder', 'catalog_table_name',
                'catalog_src_url_file', 'catalog_fields_obj_transform',
                'catalog_source_org', 'catalog_layer_subgroup', 'catalog_sub_category',
                'catalog_format_subtype'
            ]
            
            # Add transform table columns if applicable
            if layer in TRANSFORM_TABLES:
                fieldnames.extend([
                    'transform_county', 'transform_city_name', 'transform_temp_table_name',
                    'transform_shp_name', 'transform_srs_epsg', 'transform_data_date',
                    'transform_update_date'
                ])
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            # Write data rows
            for result in check_results:
                row = {
                    'layer': result.get('layer', ''),
                    'county': result.get('county', ''),
                    'city': result.get('city', ''),
                    'entity_type': result.get('entity_type', ''),
                    'download_type': result.get('download_type', ''),
                }
                
                # Catalog fields
                catalog = result.get('catalog', {})
                for field in fieldnames[5:15]:  # catalog fields
                    field_name = field.replace('catalog_', '')
                    row[field] = catalog.get(field_name, 'MISSING')
                
                # Transform fields
                if layer in TRANSFORM_TABLES:
                    transform = result.get('transform', {})
                    for field in fieldnames[15:]:  # transform fields
                        field_name = field.replace('transform_', '')
                        row[field] = transform.get(field_name, 'MISSING')
                
                writer.writerow(row)
            
            # Write summary row
            summary_row = {field: '' for field in fieldnames}
            summary_row['layer'] = 'SUMMARY'
            summary_row['county'] = f"Total records: {len(check_results)}"
            summary_row['city'] = f"Missing fields: {sum(1 for r in check_results if 'MISSING' in str(r.values()))}"
            writer.writerow(summary_row)
        
        logger.info(f"CSV report generated: {filename}")
    
    def save_missing_fields(self):
        """Save missing fields to JSON file"""
        if self.missing_fields:
            filename = f"missing_fields_{datetime.now().strftime('%Y-%m-%d')}.json"
            with open(filename, 'w') as f:
                json.dump(self.missing_fields, f, indent=2)
            logger.info(f"Missing fields saved to: {filename}")
    
    def load_manual_fill_data(self, filename: str) -> Dict:
        """Load manual fill data from JSON file"""
        try:
            with open(filename, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load manual fill data: {e}")
            return {}
    
    def manual_fill(self, filename: str):
        """Fill in missing fields with manual data"""
        manual_data = self.load_manual_fill_data(filename)
        
        for entity, fields in manual_data.items():
            # Parse entity
            parts = entity.split('_', 2)
            if len(parts) < 3:
                continue
            
            layer, county, city = parts[0], parts[1], '_'.join(parts[2:])
            
            # Find and update catalog record
            catalog_records = self.find_catalog_records(layer, county, city)
            if catalog_records:
                record = catalog_records[0]
                update_fields = []
                update_params = []
                
                for field, value in fields.items():
                    if field in ['src_url_file', 'fields_obj_transform', 'source_org']:
                        update_fields.append(f"{field} = %s")
                        update_params.append(value)
                
                if update_fields and not test_mode:
                    table_name_identifier = record.get('table_name', record.get('id'))
                    update_params.append(table_name_identifier)
                    query = f"UPDATE m_gis_data_catalog_main SET {', '.join(update_fields)} WHERE table_name = %s"
                    self.db.execute_query(query, tuple(update_params))
                    logger.info(f"Manually filled fields for {entity}")
                elif test_mode:
                    logger.info(f"TEST MODE: Would manually fill fields for {entity}")
    
    def create_entity(self, layer: str, county: str, city: str, manual_info: Dict):
        """Create new entity in all tables"""
        logger.info(f"Creating new entity: {layer}_{county}_{city}")
        
        # Determine entity type
        entity_type = 'city'
        if city in ['unincorporated', 'unified']:
            entity_type = city
        
        # Create catalog record
        title = self.format_title(layer, county, city, entity_type)
        table_name = self.format_table_name(layer, county, city, entity_type)
        layer_group = LAYER_GROUPS.get(layer, layer)
        category = LAYER_CATEGORIES.get(layer, layer)
        sys_raw_folder = self.format_sys_raw_folder(layer_group, layer, county, city)
        
        if not test_mode:
            query = """
            INSERT INTO m_gis_data_catalog_main 
            (title, county, city, format, download, layer_group, category, 
             sys_raw_folder, table_name, src_url_file, fields_obj_transform, source_org)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                title, county.upper(), city.upper(), 'UNKNOWN', 'AUTO',
                layer_group, category, sys_raw_folder, table_name,
                manual_info.get('src_url_file', ''),
                manual_info.get('fields_obj_transform', ''),
                manual_info.get('source_org', '')
            )
            self.db.execute_query(query, params)
            logger.info(f"Created catalog record for {layer}_{county}_{city}")
        else:
            logger.info(f"TEST MODE: Would create catalog record for {layer}_{county}_{city}")
        
        # Create transform record if applicable
        if layer in TRANSFORM_TABLES:
            temp_table_name = self.format_temp_table_name(layer, county, city)
            transform_table = TRANSFORM_TABLES[layer]
            
            if not test_mode:
                query = f"""
                INSERT INTO {transform_table} 
                (county, city_name, temp_table_name)
                VALUES (%s, %s, %s)
                """
                params = (county.upper(), city.upper(), temp_table_name)
                self.db.execute_query(query, params)
                logger.info(f"Created transform record for {layer}_{county}_{city}")
            else:
                logger.info(f"TEST MODE: Would create transform record for {layer}_{county}_{city}")
    
    def run_check_mode(self, layer: str) -> List[Dict]:
        """Run check mode to validate all records"""
        logger.info(f"Running check mode for {layer}")
        
        check_results = []
        entities = self.manifest.get_entities(layer)
        
        for entity in entities:
            entity_parts = entity.split('_', 1)
            if len(entity_parts) != 2:
                continue
            
            county, city = entity_parts
            commands = self.manifest.get_entity_commands(layer, entity)
            is_ags = self.manifest.is_ags_download(commands)
            target_city = self.manifest.get_target_city(commands, entity)
            
            entity_type = 'city'
            if city in ['unincorporated', 'unified']:
                entity_type = city
            
            # Get catalog record
            catalog_records = self.find_catalog_records(layer, county, target_city)
            catalog = catalog_records[0] if catalog_records else {}
            
            # Get transform record
            transform = {}
            if layer in TRANSFORM_TABLES:
                transform_record = self.find_transform_record(layer, county, target_city)
                if transform_record:
                    transform = dict(transform_record)
            
            check_results.append({
                'layer': layer,
                'county': county,
                'city': city,
                'entity_type': entity_type,
                'download_type': 'AGS' if is_ags else 'NON-AGS',
                'catalog': catalog,
                'transform': transform
            })
        
        # Find orphaned records
        orphaned = self.check_orphaned_records(layer)
        for orphan in orphaned:
            check_results.append({
                'layer': layer,
                'county': 'ORPHANED',
                'city': 'ORPHANED',
                'entity_type': 'ORPHANED',
                'download_type': 'ORPHANED',
                'catalog': dict(orphan['record']) if orphan['table'] == 'm_gis_data_catalog_main' else {},
                'transform': dict(orphan['record']) if orphan['table'] != 'm_gis_data_catalog_main' else {}
            })
        
        # Generate CSV report
        self.generate_csv_report(layer, check_results)
        
        return check_results
    
    def print_summary(self):
        """Print summary of operations"""
        logger.info("=== OPERATION SUMMARY ===")
        logger.info(f"Missing fields: {len(self.missing_fields)}")
        logger.info(f"Duplicates found: {len(self.duplicates)}")
        
        if self.missing_fields:
            logger.info("Missing fields by entity:")
            for entity, fields in self.missing_fields.items():
                logger.info(f"  {entity}: {', '.join(fields.keys())}")
        
        if self.duplicates:
            logger.info("Duplicates found:")
            for dup in self.duplicates:
                logger.info(f"  {dup['layer']}_{dup['county']}_{dup['city']}: {len(dup['records'])} records")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Standardize layer database and manifest files')
    parser.add_argument('layer', help='Layer name (zoning, flu, etc.) or "all"')
    parser.add_argument('county', nargs='?', help='County name or "all"')
    parser.add_argument('city', nargs='?', help='City name')
    parser.add_argument('--check', action='store_true', help='Check mode - validate without making changes')
    parser.add_argument('--create', action='store_true', help='Create mode - create new entity')
    parser.add_argument('--manual-fill', action='store_true', help='Manual fill mode - fill missing fields')
    parser.add_argument('--manual-file', help='Manual fill data file')
    parser.add_argument('--test-mode', action='store_true', help='Test mode - show what would be done')
    
    args = parser.parse_args()
    
    # Set test mode
    global test_mode
    test_mode = args.test_mode
    
    standardizer = LayerStandardizer()
    
    try:
        if args.check:
            # Check mode
            if args.layer.lower() == 'all':
                for layer in ['zoning', 'flu']:
                    standardizer.run_check_mode(layer)
            else:
                standardizer.run_check_mode(args.layer)
        
        elif args.create:
            # Create mode
            if not all([args.layer, args.county, args.city]):
                logger.error("Create mode requires layer, county, and city arguments")
                return
            
            manual_info = {}
            if args.manual_file:
                manual_info = standardizer.load_manual_fill_data(args.manual_file)
            
            standardizer.create_entity(args.layer, args.county, args.city, manual_info)
        
        elif args.manual_fill:
            # Manual fill mode
            filename = args.manual_file or f"missing_fields_{datetime.now().strftime('%Y-%m-%d')}.json"
            standardizer.manual_fill(filename)
        
        else:
            # Standard mode
            if args.layer.lower() == 'all':
                for layer in ['zoning', 'flu']:
                    standardizer.process_layer(layer)
            else:
                standardizer.process_layer(args.layer, args.county, args.city)
            
            # Save missing fields and print summary
            standardizer.save_missing_fields()
            standardizer.print_summary()
    
    except Exception as e:
        logger.error(f"Operation failed: {e}")
        raise
    finally:
        standardizer.db.close()


if __name__ == '__main__':
    main()
