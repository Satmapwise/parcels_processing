#!/usr/bin/env python3
"""
Find Missing Entities Script

This script compares entities from the database with those in missing_fields.json
to identify which entities need to be added to the data catalog.
"""

import os
import json
import psycopg2
import psycopg2.extras
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path


# ---------------------------------------------------------------------------
# Environment Loading
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
                        os.environ[key] = value

# Load environment
load_environment()

# Database connection string
PG_CONNECTION = os.getenv('PG_CONNECTION')


# ---------------------------------------------------------------------------
# Database Helper Class
# ---------------------------------------------------------------------------

class DB:
    """Database wrapper class."""
    
    def __init__(self, conn_str: str):
        self.conn_str = conn_str
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Connect to database."""
        try:
            self.conn = psycopg2.connect(self.conn_str)
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            print("[DEBUG] Database connection successful")
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            raise
    
    def fetchone(self, sql: str, params: Tuple[Any, ...] | None = None):
        """Fetch one record."""
        self.cursor.execute(sql, params)
        return self.cursor.fetchone()
    
    def fetchall(self, sql: str, params: Tuple[Any, ...] | None = None):
        """Fetch all records."""
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()
    
    def execute(self, sql: str, params: Tuple[Any, ...] | None = None):
        """Execute SQL."""
        self.cursor.execute(sql, params)
    
    def commit(self):
        """Commit transaction."""
        self.conn.commit()
    
    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


# ---------------------------------------------------------------------------
# Main Script
# ---------------------------------------------------------------------------

def get_zoning_entity_from_record(record: Dict) -> Optional[str]:
    """Extract entity name from zoning transform record."""
    county = record.get('county', '').lower() if record.get('county') else None
    city_name = record.get('city_name', '').lower() if record.get('city_name') else None
    
    if not county:
        return None
        
    # Handle different city name formats
    if city_name and city_name != 'none':
        # Convert city name to entity format
        city_entity = city_name.replace('_', '_').lower()
        return f"zoning_fl_{county}_{city_entity}"
    else:
        # Unincorporated county
        return f"zoning_fl_{county}_unincorporated"

def get_flu_entity_from_record(record: Dict) -> Optional[str]:
    """Extract entity name from FLU transform record."""
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

def get_parcel_entity_from_record(record: Dict) -> Optional[str]:
    """Extract entity name from parcel shapefile fields record."""
    county = record.get('county', '').lower() if record.get('county') else None
    state = record.get('state', '').lower() if record.get('state') else None
    
    if not county or not state:
        return None
    
    return f"parcel_geo_{state}_{county}"

def main():
    """Main execution function."""
    if not PG_CONNECTION:
        raise ValueError("PG_CONNECTION not found in environment. Please set it in .env file.")
    
    db = DB(PG_CONNECTION)
    db.connect()
    
    # Load missing_fields.json
    missing_fields_file = "missing_fields.json"
    with open(missing_fields_file, 'r') as f:
        missing_fields_data = json.load(f)
    
    existing_entities = set(missing_fields_data.keys())
    db_entities = set()
    
    try:
        # Get zoning entities
        sql = "SELECT county, city_name FROM support.zoning_transform ORDER BY county, city_name"
        records = db.fetchall(sql)
        for record in records:
            entity = get_zoning_entity_from_record(record)
            if entity:
                db_entities.add(entity)
        
        # Get FLU entities
        sql = "SELECT county, city_name FROM support.flu_transform ORDER BY county, city_name"
        records = db.fetchall(sql)
        for record in records:
            entity = get_flu_entity_from_record(record)
            if entity:
                db_entities.add(entity)
        
        # Get parcel entities
        sql = "SELECT county, state FROM support.parcel_shp_fields ORDER BY county, state"
        records = db.fetchall(sql)
        for record in records:
            entity = get_parcel_entity_from_record(record)
            if entity:
                db_entities.add(entity)
        
        # Find missing entities
        missing_entities = db_entities - existing_entities
        
        print(f"[INFO] Found {len(existing_entities)} existing entities in {missing_fields_file}")
        print(f"[INFO] Found {len(db_entities)} entities in database")
        print(f"[INFO] Found {len(missing_entities)} missing entities:")
        
        for entity in sorted(missing_entities):
            print(f"  - {entity}")
        
    except Exception as e:
        print(f"[ERROR] Script execution failed: {e}")
        return 1
    finally:
        db.close()
    
    return 0


if __name__ == "__main__":
    exit(main()) 