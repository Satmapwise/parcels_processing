#!/usr/bin/env python3

"""Debug script to test buildings detection logic step by step."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from layers_prescrape import LayersPrescrape, Config
from layers_helpers import format_name

def debug_buildings_detection():
    """Test buildings detection with detailed debugging."""
    
    print("=== DEBUGGING BUILDINGS DETECTION ===")
    
    # Create config exactly like the main script
    cfg = Config(layer='buildings', mode='detect', debug=True)
    
    # Create LayersPrescrape instance
    prescraper = LayersPrescrape(cfg)
    
    # Test format_name function
    layer_internal = format_name(cfg.layer, 'layer', external=False)
    layer_external = format_name(cfg.layer, 'layer', external=True)
    
    print(f"Layer: {cfg.layer}")
    print(f"Internal format: '{layer_internal}'")
    print(f"External format: '{layer_external}'")
    print()
    
    # Test the exact SQL query from _run_detect_mode
    sql = """
        SELECT * FROM m_gis_data_catalog_main 
        WHERE status IS DISTINCT FROM 'DELETE' 
        AND (lower(title) LIKE %s OR lower(title) LIKE %s)
        ORDER BY title
    """
    
    params = (f'%{layer_internal}%', f'%{layer_external.lower()}%')
    print(f"SQL parameters: {params}")
    print()
    
    # Execute the query
    records = prescraper.db.fetchall(sql, params)
    
    print(f"Found {len(records)} total records")
    print()
    
    # Show all records
    print("All records found:")
    for i, record in enumerate(records, 1):
        print(f"  {i:2d}. '{record['title']}'")
    
    print()
    
    # Test entity generation for each record
    print("Testing entity generation:")
    valid_entities = set()
    duplicate_entities = set()
    error_entities = set()
    
    for i, record in enumerate(records, 1):
        try:
            entity = prescraper._generate_entity_from_record(record)
            print(f"  {i:2d}. '{record['title']}' -> '{entity}'")
            
            if entity == 'ERROR':
                error_entities.add(entity)
            elif entity in valid_entities:
                duplicate_entities.add(entity)
            else:
                valid_entities.add(entity)
                
        except Exception as e:
            print(f"  {i:2d}. '{record['title']}' -> ERROR: {e}")
            error_entities.add('ERROR')
    
    print()
    print(f"Summary:")
    print(f"  Valid entities: {len(valid_entities)}")
    print(f"  Duplicate entities: {len(duplicate_entities)}")
    print(f"  Error entities: {len(error_entities)}")
    
    print()
    print("Valid entities:")
    for entity in sorted(valid_entities):
        print(f"  {entity}")
    
    # Clean up
    prescraper.db.close()

if __name__ == "__main__":
    debug_buildings_detection()
