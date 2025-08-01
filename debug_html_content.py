#!/usr/bin/env python3
"""
Debug script to examine the actual HTML content from opendata portals.
"""

import urllib.request
import ssl
import re

def debug_html_content():
    """Debug what HTML content we're actually getting."""
    
    url = "https://ocgis-datahub-ocfl.hub.arcgis.com/datasets/ocfl::orange-county-zoning/explore"
    
    print(f"Fetching HTML content from: {url}")
    print("=" * 80)
    
    try:
        # Set up request with headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        request = urllib.request.Request(url, headers=headers)
        
        # Handle SSL
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Fetch content
        response = urllib.request.urlopen(request, timeout=15, context=ssl_context)
        content = response.read().decode('utf-8', errors='ignore')
        
        print(f"Response code: {response.getcode()}")
        print(f"Content length: {len(content)} characters")
        print(f"Final URL: {response.geturl()}")
        print()
        
        # Look for any mentions of "rest/services" or "arcgis"
        rest_services_count = content.lower().count('rest/services')
        arcgis_count = content.lower().count('arcgis')
        
        print(f"Occurrences of 'rest/services': {rest_services_count}")
        print(f"Occurrences of 'arcgis': {arcgis_count}")
        print()
        
        # Show first 2000 characters of content
        print("First 2000 characters of HTML content:")
        print("-" * 50)
        print(content[:2000])
        print("-" * 50)
        
        # Look for any script tags that might contain configuration
        script_pattern = r'<script[^>]*>(.*?)</script>'
        scripts = re.findall(script_pattern, content, re.DOTALL | re.IGNORECASE)
        
        print(f"\nFound {len(scripts)} script tags")
        
        # Check if any scripts contain service URLs
        for i, script in enumerate(scripts[:3]):  # Check first 3 scripts
            if 'rest/services' in script or 'arcgis' in script:
                print(f"\nScript {i+1} contains ArcGIS references:")
                print(script[:500] + "..." if len(script) > 500 else script)
        
        # Look for specific patterns that might indicate dynamic loading
        dynamic_indicators = [
            'react', 'angular', 'vue', 'ember', 'backbone',
            'ajax', 'xhr', 'fetch', 'async',
            'spa', 'single-page', 'client-side'
        ]
        
        found_indicators = []
        for indicator in dynamic_indicators:
            if indicator in content.lower():
                found_indicators.append(indicator)
        
        if found_indicators:
            print(f"\nDynamic loading indicators found: {', '.join(found_indicators)}")
        else:
            print("\nNo obvious dynamic loading indicators found")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_html_content()
