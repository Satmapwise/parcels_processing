#!/usr/bin/env python3
"""
Test the full pipeline from opendata URL to ArcGIS service URLs.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from opendata_detector import extract_arcgis_urls_from_opendata

def test_full_pipeline():
    """Test the complete extraction pipeline."""
    
    test_url = "https://ocgis-datahub-ocfl.hub.arcgis.com/datasets/ocfl::orange-county-zoning/explore"
    layer_keywords = ["zoning"]
    
    print(f"Testing full pipeline on: {test_url}")
    print(f"Keywords: {layer_keywords}")
    print("=" * 80)
    
    try:
        # Use the main extraction function (same as opendata-to-AGS tool uses)
        results = extract_arcgis_urls_from_opendata(test_url, layer_keywords)
        
        print(f"Pipeline completed successfully!")
        print(f"Found {len(results)} ArcGIS URLs:")
        
        for i, (url, score) in enumerate(results[:5], 1):  # Show top 5
            print(f"{i}. {url}")
            print(f"   Score: {score:.2f}")
            print()
            
        if results:
            print(f"🎯 Best URL: {results[0][0]} (score: {results[0][1]:.2f})")
        else:
            print("❌ No URLs found")
            
    except Exception as e:
        print(f"Pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_full_pipeline()
