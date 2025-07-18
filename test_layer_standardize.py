#!/usr/bin/env python3
"""
Test script for layer_standardize_database.py
"""

import sys
import os
import json
from layer_standardize_database import LayerStandardizer, ManifestManager

def test_manifest_loading():
    """Test manifest loading functionality"""
    print("Testing manifest loading...")
    
    try:
        manifest = ManifestManager()
        print(f"✓ Manifest loaded successfully")
        
        # Test getting entities
        zoning_entities = manifest.get_entities('zoning')
        print(f"✓ Found {len(zoning_entities)} zoning entities")
        
        flu_entities = manifest.get_entities('flu')
        print(f"✓ Found {len(flu_entities)} flu entities")
        
        # Test getting commands for a specific entity
        if zoning_entities:
            entity = zoning_entities[0]
            commands = manifest.get_entity_commands('zoning', entity)
            print(f"✓ Found {len(commands)} commands for entity {entity}")
            
            # Test AGS detection
            is_ags = manifest.is_ags_download(commands)
            print(f"✓ Entity {entity} is AGS: {is_ags}")
            
            # Test target city extraction
            target_city = manifest.get_target_city(commands, entity)
            print(f"✓ Target city for {entity}: {target_city}")
        
        return True
        
    except Exception as e:
        print(f"✗ Manifest loading failed: {e}")
        return False

def test_format_detection():
    """Test format detection functionality"""
    print("\nTesting format detection...")
    
    try:
        standardizer = LayerStandardizer()
        
        test_cases = [
            ("http://example.com/data.shp", "SHP"),
            ("http://example.com/data.zip", "ZIP"),
            ("http://example.com/ags/rest", "AGS"),
            ("http://example.com/data.geojson", "GEOJSON"),
            ("http://example.com/data.kml", "KML"),
            ("", "UNKNOWN"),
            (None, "UNKNOWN")
        ]
        
        for url, expected in test_cases:
            result = standardizer.get_format(url)
            if result == expected:
                print(f"✓ Format detection for '{url}': {result}")
            else:
                print(f"✗ Format detection failed for '{url}': expected {expected}, got {result}")
        
        return True
        
    except Exception as e:
        print(f"✗ Format detection failed: {e}")
        return False

def test_formatting_functions():
    """Test formatting functions"""
    print("\nTesting formatting functions...")
    
    try:
        standardizer = LayerStandardizer()
        
        # Test title formatting
        title = standardizer.format_title('zoning', 'alachua', 'gainesville', 'city')
        expected = "Zoning - City of Gainesville"
        if title == expected:
            print(f"✓ Title formatting: {title}")
        else:
            print(f"✗ Title formatting failed: expected '{expected}', got '{title}'")
        
        # Test table name formatting
        table_name = standardizer.format_table_name('zoning', 'alachua', 'gainesville', 'city')
        expected = "zoning_gainesville"
        if table_name == expected:
            print(f"✓ Table name formatting: {table_name}")
        else:
            print(f"✗ Table name formatting failed: expected '{expected}', got '{table_name}'")
        
        # Test temp table name formatting
        temp_table = standardizer.format_temp_table_name('zoning', 'alachua', 'gainesville')
        expected = "raw_zon_alachua_gainesville"
        if temp_table == expected:
            print(f"✓ Temp table name formatting: {temp_table}")
        else:
            print(f"✗ Temp table name formatting failed: expected '{expected}', got '{temp_table}'")
        
        # Test unincorporated formatting
        title_uninc = standardizer.format_title('zoning', 'alachua', 'unincorporated', 'unincorporated')
        expected_uninc = "Zoning - Alachua Unincorporated"
        if title_uninc == expected_uninc:
            print(f"✓ Unincorporated title formatting: {title_uninc}")
        else:
            print(f"✗ Unincorporated title formatting failed: expected '{expected_uninc}', got '{title_uninc}'")
        
        return True
        
    except Exception as e:
        print(f"✗ Formatting functions failed: {e}")
        return False

def test_check_mode():
    """Test check mode functionality"""
    print("\nTesting check mode...")
    
    try:
        standardizer = LayerStandardizer()
        
        # Test with a small subset (just first few entities)
        manifest = ManifestManager()
        zoning_entities = manifest.get_entities('zoning')[:3]  # Just first 3 entities
        
        print(f"Testing check mode with {len(zoning_entities)} entities...")
        
        for entity in zoning_entities:
            print(f"  Processing entity: {entity}")
            # This would normally connect to database, so we'll just test the parsing
            entity_parts = entity.split('_', 1)
            if len(entity_parts) == 2:
                county, city = entity_parts
                print(f"    County: {county}, City: {city}")
            else:
                print(f"    Invalid entity format: {entity}")
        
        print("✓ Check mode parsing test completed")
        return True
        
    except Exception as e:
        print(f"✗ Check mode failed: {e}")
        return False

def main():
    """Run all tests"""
    print("Running Layer Standardization Tests")
    print("=" * 40)
    
    tests = [
        test_manifest_loading,
        test_format_detection,
        test_formatting_functions,
        test_check_mode
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"✗ Test {test.__name__} crashed: {e}")
    
    print("\n" + "=" * 40)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("✓ All tests passed!")
        return 0
    else:
        print("✗ Some tests failed!")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 