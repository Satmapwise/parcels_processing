#!/usr/bin/env python3
"""
Transform Table to Object Script

This script queries the database for field mappings from support tables and converts them
to fields_obj_transform format for missing_values.json.

The three tables to process are:
- support.zoning_transform
- support.flu_transform  
- support.parcel_shp_fields

Each record's field mappings will be converted to the format:
<column header>:<column value> separated by commas
"""

import os
import json
import psycopg2
import psycopg2.extras
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path


# ---------------------------------------------------------------------------
# Database Configuration
# ---------------------------------------------------------------------------

def load_environment():
    """Load environment variables from .env file if available."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        # dotenv not available, try manual loading
        env_path = Path('.env')
        if env_path.exists():
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip().strip('\'"')

# Load environment on import
load_environment()

# Database connection
PG_CONNECTION = os.getenv("PG_CONNECTION")


# ---------------------------------------------------------------------------
# Database Helper Class
# ---------------------------------------------------------------------------

class DB:
    """Thin wrapper around psycopg2 connection with dict cursors."""

    def __init__(self, conn_str: str):
        print(f"[DEBUG] Attempting to connect to database...")
        try:
            self.conn = psycopg2.connect(conn_str, connect_timeout=10)
            print(f"[DEBUG] Database connection successful")
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            raise

    def fetchone(self, sql: str, params: Tuple[Any, ...] | None = None):
        self.cur.execute(sql, params)
        row = self.cur.fetchone()
        return dict(row) if row else None

    def fetchall(self, sql: str, params: Tuple[Any, ...] | None = None):
        self.cur.execute(sql, params)
        return self.cur.fetchall()

    def execute(self, sql: str, params: Tuple[Any, ...] | None = None):
        self.cur.execute(sql, params)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()


# ---------------------------------------------------------------------------
# Main Transform Class
# ---------------------------------------------------------------------------

class TransformTableToObject:
    """Main class to handle the transformation of database table mappings to object format."""
    
    def __init__(self):
        if not PG_CONNECTION:
            raise ValueError("PG_CONNECTION not found in environment. Please set it in .env file.")
        
        self.db = DB(PG_CONNECTION)
        self.missing_fields_file = "missing_fields.json"
        self.missing_fields_data = {}
        
    def run(self):
        """Main execution method."""
        print("[INFO] Starting transform table to object process...")
        
        try:
            # Load existing missing_fields.json if it exists
            self._load_missing_fields()
            
            # Process each table type
            self._process_zoning_transforms()
            self._process_flu_transforms() 
            self._process_parcel_shp_fields()
            
            # Save updated missing_fields.json
            self._save_missing_fields()
            
            print("[INFO] Transform process completed successfully!")
            
        except Exception as e:
            print(f"[ERROR] Transform process failed: {e}")
            raise
        finally:
            self.db.close()
    
    def _load_missing_fields(self):
        """Load existing missing_fields.json file if it exists."""
        if Path(self.missing_fields_file).exists():
            with open(self.missing_fields_file, 'r') as f:
                self.missing_fields_data = json.load(f)
            print(f"[INFO] Loaded existing {self.missing_fields_file}")
        else:
            raise FileNotFoundError(f"{self.missing_fields_file} not found. This file must exist.")
    
    def _process_zoning_transforms(self):
        """Process zoning transform table records."""
        print("[INFO] Processing zoning transforms...")
        
        try:
            # Query all zoning transform records
            sql = """
                SELECT id, county, city_name, temp_table_name, shp_name,
                       zon_code_col, zon_code2_col, zon_desc_col, zon_gen_col, 
                       notes_col, ord_num_col, data_date, update_date, srs_epsg
                FROM support.zoning_transform
                ORDER BY county, city_name
            """
            
            records = self.db.fetchall(sql)
            print(f"[INFO] Found {len(records)} zoning transform records")
            
            for record in records:
                entity = self._get_zoning_entity_from_record(record)
                if entity:
                    field_mappings = self._extract_zoning_field_mappings(record)
                    if field_mappings:
                        transform_string = self._convert_field_mappings_to_transform(field_mappings)
                        self._add_to_missing_fields(entity, "fields_obj_transform", transform_string)
                        print(f"[INFO] Added transform for {entity}: {transform_string}")
                        
        except Exception as e:
            print(f"[ERROR] Error processing zoning transforms: {e}")
            raise
        
    def _process_flu_transforms(self):
        """Process FLU transform table records."""
        print("[INFO] Processing FLU transforms...")
        
        try:
            # Query all FLU transform records
            sql = """
                SELECT id, county, city_name, temp_table_name, shp_name,
                       flu_code_col, flu_code2_col, flu_desc_col, flu_gen_col, 
                       notes_col, dca_num_col, data_date, update_date, srs_epsg
                FROM support.flu_transform
                ORDER BY county, city_name
            """
            
            records = self.db.fetchall(sql)
            print(f"[INFO] Found {len(records)} FLU transform records")
            
            for record in records:
                entity = self._get_flu_entity_from_record(record)
                if entity:
                    field_mappings = self._extract_flu_field_mappings(record)
                    if field_mappings:
                        transform_string = self._convert_field_mappings_to_transform(field_mappings)
                        self._add_to_missing_fields(entity, "fields_obj_transform", transform_string)
                        print(f"[INFO] Added transform for {entity}: {transform_string}")
                        
        except Exception as e:
            print(f"[ERROR] Error processing FLU transforms: {e}")
            raise
        
    def _process_parcel_shp_fields(self):
        """Process parcel shapefile fields table records."""
        print("[INFO] Processing parcel shapefile fields...")
        
        try:
            # Query all parcel shapefile field records
            sql = """
                SELECT county, shp_name, shp_date, shp_epsg, shp_pin, shp_pin_clean,
                       shp_pin2, shp_pin2_clean, shp_altkey, fdor, column_notes,
                       geom_notes, import_fields, condo_key, state, ogc_fid
                FROM support.parcel_shp_fields
                ORDER BY county, state
            """
            
            records = self.db.fetchall(sql)
            print(f"[INFO] Found {len(records)} parcel shapefile field records")
            
            for record in records:
                entity = self._get_parcel_entity_from_record(record)
                if entity:
                    field_mappings = self._extract_parcel_field_mappings(record)
                    if field_mappings:
                        transform_string = self._convert_field_mappings_to_transform(field_mappings)
                        self._add_to_missing_fields(entity, "fields_obj_transform", transform_string)
                        print(f"[INFO] Added transform for {entity}: {transform_string}")
                        
        except Exception as e:
            print(f"[ERROR] Error processing parcel shapefile fields: {e}")
            raise
        
    def _strip_col_suffix(self, column_name: str) -> str:
        """Strip '_col' suffix from column names if present.
        
        Args:
            column_name: Original column name from database
            
        Returns:
            Column name with '_col' suffix removed
        """
        if column_name and column_name.endswith('_col'):
            return column_name[:-4]  # Remove last 4 characters ('_col')
        return column_name
    
    def _convert_field_mappings_to_transform(self, field_mappings: Dict) -> str:
        """Convert field mappings dictionary to fields_obj_transform string format.
        
        Args:
            field_mappings: Dictionary of field mappings from database
            
        Returns:
            String in format "column_header:column_value, column_header:column_value, ..."
        """
        if not field_mappings:
            return ""
        
        # Convert dictionary to "column_header:column_value" format, separated by commas
        transform_parts = []
        for source_col, target_field in field_mappings.items():
            if source_col and target_field:  # Skip None/empty values
                # Strip '_col' suffix from source column name to get the header
                clean_header = self._strip_col_suffix(source_col)
                # The target_field is the standardized field name, source_col is the actual column value
                transform_parts.append(f"{clean_header}:{target_field}")
        
        return ", ".join(transform_parts)
    
    def _get_zoning_entity_from_record(self, record: Dict) -> Optional[str]:
        """Extract entity name from zoning transform record.
        
        Args:
            record: Database record from support.zoning_transform
            
        Returns:
            Entity name string (e.g., "zoning_fl_alachua_gainesville") or None
        """
        county = record.get('county', '').lower() if record.get('county') else None
        city_name = record.get('city_name', '').lower() if record.get('city_name') else None
        
        if not county:
            return None
            
        # Handle different city name formats
        if city_name and city_name != 'none':
            # Convert city name to entity format (e.g., "FORT_MYERS_BEACH" -> "fort_myers_beach")
            city_entity = city_name.replace('_', '_').lower()
            return f"zoning_fl_{county}_{city_entity}"
        else:
            # Unincorporated county
            return f"zoning_fl_{county}_unincorporated"
    
    def _get_flu_entity_from_record(self, record: Dict) -> Optional[str]:
        """Extract entity name from FLU transform record.
        
        Args:
            record: Database record from support.flu_transform
            
        Returns:
            Entity name string (e.g., "flu_fl_alachua_gainesville") or None
        """
        county = record.get('county', '').lower() if record.get('county') else None
        city_name = record.get('city_name', '').lower() if record.get('city_name') else None
        
        if not county:
            return None
            
        # Handle different city name formats
        if city_name and city_name != 'none':
            # Convert city name to entity format
            city_entity = city_name.replace('_', '_').lower()
            return f"flu_fl_{county}_{city_entity}"
        else:
            # Unincorporated county
            return f"flu_fl_{county}_unincorporated"
    
    def _get_parcel_entity_from_record(self, record: Dict) -> Optional[str]:
        """Extract entity name from parcel shapefile fields record.
        
        Args:
            record: Database record from support.parcel_shp_fields
            
        Returns:
            Entity name string (e.g., "parcel_geo_fl_citrus") or None
        """
        county = record.get('county', '').lower() if record.get('county') else None
        state = record.get('state', '').lower() if record.get('state') else None
        
        if not county or not state:
            return None
            
        return f"parcel_geo_{state}_{county}"
    
    def _extract_zoning_field_mappings(self, record: Dict) -> Dict[str, str]:
        """Extract field mappings from zoning transform record.
        
        Args:
            record: Database record from support.zoning_transform
            
        Returns:
            Dictionary mapping standardized field names to actual column values
        """
        mappings = {}
        
        # Map standardized field names to actual column values from database
        if record.get('zon_code_col'):
            mappings['zon_code'] = record['zon_code_col']
        
        if record.get('zon_code2_col'):
            mappings['zon_code2'] = record['zon_code2_col']
            
        if record.get('zon_desc_col'):
            mappings['zon_desc'] = record['zon_desc_col']
            
        if record.get('zon_gen_col'):
            mappings['zon_gen'] = record['zon_gen_col']
            
        if record.get('notes_col'):
            mappings['notes'] = record['notes_col']
            
        if record.get('ord_num_col'):
            mappings['ord_num'] = record['ord_num_col']
        
        # Always include OBJECTID mapping if we have any other mappings
        if mappings:
            mappings['id'] = 'OBJECTID'
        
        return mappings
    
    def _extract_flu_field_mappings(self, record: Dict) -> Dict[str, str]:
        """Extract field mappings from FLU transform record.
        
        Args:
            record: Database record from support.flu_transform
            
        Returns:
            Dictionary mapping standardized field names to actual column values
        """
        mappings = {}
        
        # Map standardized field names to actual column values from database
        if record.get('flu_code_col'):
            mappings['flu_code'] = record['flu_code_col']
        
        if record.get('flu_code2_col'):
            mappings['flu_code2'] = record['flu_code2_col']
            
        if record.get('flu_desc_col'):
            mappings['flu_desc'] = record['flu_desc_col']
            
        if record.get('flu_gen_col'):
            mappings['flu_gen'] = record['flu_gen_col']
            
        if record.get('notes_col'):
            mappings['notes'] = record['notes_col']
            
        if record.get('dca_num_col'):
            mappings['dca_num'] = record['dca_num_col']
        
        # Always include OBJECTID mapping if we have any other mappings
        if mappings:
            mappings['id'] = 'OBJECTID'
        
        return mappings
    
    def _extract_parcel_field_mappings(self, record: Dict) -> Dict[str, str]:
        """Extract field mappings from parcel shapefile fields record.
        
        Args:
            record: Database record from support.parcel_shp_fields
            
        Returns:
            Dictionary mapping standardized field names to actual column values
        """
        mappings = {}
        
        # Map standardized field names to actual column values from database
        if record.get('shp_pin'):
            mappings['parcel_id'] = record['shp_pin']
        
        if record.get('shp_pin_clean'):
            mappings['parcel_id_clean'] = record['shp_pin_clean']
            
        if record.get('shp_pin2'):
            mappings['parcel_id2'] = record['shp_pin2']
            
        if record.get('shp_pin2_clean'):
            mappings['parcel_id2_clean'] = record['shp_pin2_clean']
            
        if record.get('shp_altkey'):
            mappings['parcel_alt_key'] = record['shp_altkey']
            
        if record.get('condo_key'):
            mappings['condo_key'] = record['condo_key']
        
        # Always include OBJECTID mapping if we have any other mappings
        if mappings:
            mappings['id'] = 'OBJECTID'
        
        return mappings
    
    def _add_to_missing_fields(self, entity: str, field: str, value: str):
        """Add or update a field value in missing_fields_data.
        
        Args:
            entity: Entity name (e.g., "zoning_fl_alachua_gainesville")
            field: Field name (e.g., "fields_obj_transform")
            value: Field value
        """
        if entity not in self.missing_fields_data:
            raise ValueError(f"Entity '{entity}' not found in {self.missing_fields_file}. This entity needs to be added to the data catalog.")
        
        self.missing_fields_data[entity][field] = value
        
    def _save_missing_fields(self):
        """Save updated missing_fields_data to JSON file."""
        with open(self.missing_fields_file, 'w') as f:
            json.dump(self.missing_fields_data, f, indent=2)
        print(f"[INFO] Saved updated {self.missing_fields_file}")


# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------

def main():
    """Main execution function."""
    try:
        transformer = TransformTableToObject()
        transformer.run()
    except Exception as e:
        print(f"[ERROR] Script execution failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main()) 