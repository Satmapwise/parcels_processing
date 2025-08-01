#!/usr/bin/env python3
"""
Detailed debug script to trace the config extraction process step by step.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import urllib.request
import urllib.parse
import ssl
import re
import json
from opendata_detector import extract_from_encoded_config

def debug_config_extraction():
    """Debug the config extraction process step by step."""
    
    url = "https://ocgis-datahub-ocfl.hub.arcgis.com/datasets/ocfl::orange-county-zoning/explore"
    layer_keywords = ["zoning"]
    
    print(f"Debugging config extraction for: {url}")
    print(f"Layer keywords: {layer_keywords}")
    print("=" * 80)
    
    try:
        # Fetch HTML content
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        request = urllib.request.Request(url, headers=headers)
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        response = urllib.request.urlopen(request, timeout=15, context=ssl_context)
        html_content = response.read().decode('utf-8', errors='ignore')
        
        print(f"HTML content length: {len(html_content)} characters")
        print()
        
        # Test our regex patterns directly
        config_patterns = [
            r'window\.__SITE\s*=\s*["\']([^"\'\']+)["\']',
            r'window\.__CONFIG\s*=\s*["\']([^"\'\']+)["\']',
            r'window\.__DATA\s*=\s*["\']([^"\'\']+)["\']',
            r'window\.__INITIAL_STATE\s*=\s*["\']([^"\'\']+)["\']',
            r'__SITE__\s*=\s*["\']([^"\'\']+)["\']',
            r'hubConfig\s*=\s*["\']([^"\'\']+)["\']'
        ]
        
        print("Testing regex patterns:")
        for i, pattern in enumerate(config_patterns, 1):
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            print(f"{i}. Pattern '{pattern}': {len(matches)} matches")
            if matches:
                print(f"   First match length: {len(matches[0])} characters")
                print(f"   First 100 chars: {matches[0][:100]}...")
        print()
        
        # Test the actual extraction function
        print("Testing extract_from_encoded_config function:")
        print("-" * 50)
        
        result = extract_from_encoded_config(html_content, layer_keywords)
        
        print(f"Function returned {len(result)} URLs:")
        for i, (url, score) in enumerate(result, 1):
            print(f"{i}. {url} (score: {score:.2f})")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_config_extraction()
