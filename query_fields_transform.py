#!/usr/bin/env python3
"""
Query script to examine existing fields_obj_transform values.
"""

import sys
import os
sys.path.append('/srv/tools/python/layers_scraping')

from layers_helpers import PG_CONNECTION
import psycopg2
import psycopg2.extras

def query_fields_transform():
    """Query all existing fields_obj_transform values with titles."""
    
    try:
        # Connect to database
        conn = psycopg2.connect(PG_CONNECTION, connect_timeout=10)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Query for fields_obj_transform values
        sql = """
            SELECT title, fields_obj_transform, field_names, layer_subgroup, state, county, city
            FROM m_gis_data_catalog_main 
            WHERE status IS DISTINCT FROM 'DELETE' 
            AND fields_obj_transform IS NOT NULL 
            AND fields_obj_transform != '' 
            ORDER BY layer_subgroup, title
        """
        
        cur.execute(sql)
        results = cur.fetchall()
        
        print(f"Found {len(results)} records with fields_obj_transform values:")
        print("=" * 80)
        
        # Group by layer_subgroup for better analysis
        by_layer = {}
        for row in results:
            layer = row['layer_subgroup'] or 'unknown'
            if layer not in by_layer:
                by_layer[layer] = []
            by_layer[layer].append(row)
        
        # Display results grouped by layer
        for layer in sorted(by_layer.keys()):
            print(f"\n=== {layer.upper()} LAYER ===")
            print(f"Records: {len(by_layer[layer])}")
            print("-" * 60)
            
            for row in by_layer[layer]:
                title = row['title']
                transform = row['fields_obj_transform']
                field_names = row['field_names']
                state = row['state'] or 'N/A'
                county = row['county'] or 'N/A'
                city = row['city'] or 'N/A'
                
                print(f"Title: {title}")
                print(f"Location: {state}, {county}, {city}")
                print(f"Transform: {transform}")
                print(f"Field Names: {field_names}")
                print("-" * 40)
        
        # Summary statistics
        print(f"\n=== SUMMARY ===")
        print(f"Total records with transforms: {len(results)}")
        print(f"Layers represented: {len(by_layer)}")
        for layer, records in by_layer.items():
            print(f"  {layer}: {len(records)} records")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error querying database: {e}")
        return

if __name__ == "__main__":
    query_fields_transform() 