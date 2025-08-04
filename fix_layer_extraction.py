#!/usr/bin/env python3
"""
Test script to debug the layer extraction issue.
"""

import fnmatch
from layers_helpers import LAYER_CONFIGS

def test_extract_layers_from_patterns(include_patterns):
    """Test the current layer extraction logic."""
    layers = set()
    
    print(f"Testing patterns: {include_patterns}")
    print(f"Available layers: {list(LAYER_CONFIGS.keys())}")
    print()
    
    for pattern in include_patterns:
        print(f"Processing pattern: '{pattern}'")
        
        # First check if the pattern itself is a layer name
        if pattern in LAYER_CONFIGS:
            print(f"  ✓ Direct layer match: {pattern}")
            layers.add(pattern)
        elif '_' in pattern:
            print(f"  - Pattern contains '_', checking for layer prefix...")
            # Entity format is layer_state_county_city, so layer is first component
            # Try to find the longest matching layer name
            found_layer = None
            for layer_name in LAYER_CONFIGS.keys():
                if pattern.startswith(layer_name + '_'):
                    found_layer = layer_name
                    break
            
            if found_layer:
                print(f"  ✓ Found layer prefix: {found_layer}")
                layers.add(found_layer)
            else:
                print(f"  ✗ No layer prefix found")
        else:
            print(f"  - Pattern has no '_', checking wildcard match...")
            # Check if pattern is a wildcard that matches any layer name
            for layer_name in LAYER_CONFIGS.keys():
                if fnmatch.fnmatch(layer_name, pattern):
                    print(f"  ✓ Wildcard match: {layer_name}")
                    layers.add(layer_name)
    
    print(f"\nFinal layers: {sorted(layers)}")
    return sorted(layers)

def test_fixed_extract_layers_from_patterns(include_patterns):
    """Test the fixed layer extraction logic."""
    layers = set()
    
    print(f"Testing FIXED patterns: {include_patterns}")
    print()
    
    for pattern in include_patterns:
        print(f"Processing pattern: '{pattern}'")
        
        # First check if the pattern itself is a layer name
        if pattern in LAYER_CONFIGS:
            print(f"  ✓ Direct layer match: {pattern}")
            layers.add(pattern)
        else:
            # Check if pattern is a wildcard that matches any layer name FIRST
            print(f"  - Checking wildcard match...")
            wildcard_match = False
            for layer_name in LAYER_CONFIGS.keys():
                if fnmatch.fnmatch(layer_name, pattern):
                    print(f"  ✓ Wildcard match: {layer_name}")
                    layers.add(layer_name)
                    wildcard_match = True
            
            if not wildcard_match and '_' in pattern:
                print(f"  - No wildcard match, checking for layer prefix...")
                # Entity format is layer_state_county_city, so layer is first component
                # Try to find the longest matching layer name
                found_layer = None
                for layer_name in LAYER_CONFIGS.keys():
                    if pattern.startswith(layer_name + '_'):
                        found_layer = layer_name
                        break
                
                if found_layer:
                    print(f"  ✓ Found layer prefix: {found_layer}")
                    layers.add(found_layer)
                else:
                    print(f"  ✗ No layer prefix found")
    
    print(f"\nFinal layers: {sorted(layers)}")
    return sorted(layers)

if __name__ == "__main__":
    print("=== CURRENT LOGIC ===")
    test_extract_layers_from_patterns(["parcel_geo*"])
    
    print("\n=== FIXED LOGIC ===")
    test_fixed_extract_layers_from_patterns(["parcel_geo*"]) 