#!/usr/bin/env python3
"""
Quick test script to debug the requests-based extraction method.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from opendata_detector import extract_via_requests_method

def test_extraction():
    """Test the requests-based extraction on a known URL."""
    
    test_url = "https://ocgis-datahub-ocfl.hub.arcgis.com/datasets/ocfl::orange-county-zoning/explore"
    layer_keywords = ["zoning"]
    
    print(f"Testing extraction on: {test_url}")
    print("=" * 60)
    
    try:
        results = extract_via_requests_method(test_url, layer_keywords)
        
        print(f"Extraction completed successfully!")
        print(f"Found {len(results)} ArcGIS URLs:")
        
        for i, (url, score) in enumerate(results[:5], 1):  # Show top 5
            print(f"{i}. {url} (score: {score:.2f})")
            
    except Exception as e:
        print(f"Extraction failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_extraction()
