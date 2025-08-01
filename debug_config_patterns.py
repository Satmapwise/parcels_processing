#!/usr/bin/env python3
"""
Debug script to find and decode configuration patterns in ArcGIS Hub pages.
"""

import urllib.request
import urllib.parse
import ssl
import re
import json

def debug_config_patterns():
    """Debug what configuration patterns exist in the HTML."""
    
    url = "https://ocgis-datahub-ocfl.hub.arcgis.com/datasets/ocfl::orange-county-zoning/explore"
    
    print(f"Debugging configuration patterns in: {url}")
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
        
        # Look for all window.__ patterns
        window_patterns = re.findall(r'window\.__[A-Z_]+\s*=\s*["\']([^"\']+)["\']', html_content, re.IGNORECASE)
        print(f"Found {len(window_patterns)} window.__ patterns")
        
        for i, pattern in enumerate(window_patterns[:3]):  # Show first 3
            print(f"Pattern {i+1}: {pattern[:100]}...")
        print()
        
        # Look for the specific __SITE pattern we saw earlier
        site_pattern = r'window\.__SITE\s*=\s*"([^"]+)"'
        site_matches = re.findall(site_pattern, html_content)
        
        if site_matches:
            print(f"Found __SITE pattern with {len(site_matches)} matches")
            encoded_data = site_matches[0]
            print(f"Encoded data length: {len(encoded_data)} characters")
            print(f"First 200 chars: {encoded_data[:200]}...")
            
            try:
                # URL decode the data
                decoded_data = urllib.parse.unquote(encoded_data)
                print(f"Decoded data length: {len(decoded_data)} characters")
                print(f"First 500 chars of decoded data:")
                print("-" * 50)
                print(decoded_data[:500])
                print("-" * 50)
                
                # Try to parse as JSON
                try:
                    config_json = json.loads(decoded_data)
                    print("✅ Successfully parsed as JSON!")
                    
                    # Look for interesting keys
                    if isinstance(config_json, dict):
                        print(f"Top-level keys: {list(config_json.keys())}")
                        
                        # Look for site/item information
                        if 'site' in config_json:
                            site_info = config_json['site']
                            if isinstance(site_info, dict):
                                print(f"Site keys: {list(site_info.keys())}")
                                
                                if 'item' in site_info:
                                    item_info = site_info['item']
                                    if isinstance(item_info, dict):
                                        print(f"Item keys: {list(item_info.keys())}")
                                        
                                        # Show relevant item information
                                        for key in ['id', 'orgId', 'title', 'type']:
                                            if key in item_info:
                                                print(f"  {key}: {item_info[key]}")
                        
                        # Look for any service URLs in the JSON
                        def find_service_urls(data, path=""):
                            urls = []
                            if isinstance(data, dict):
                                for key, value in data.items():
                                    current_path = f"{path}.{key}" if path else key
                                    if isinstance(value, str) and 'rest/services' in value:
                                        urls.append((current_path, value))
                                    elif isinstance(value, (dict, list)):
                                        urls.extend(find_service_urls(value, current_path))
                            elif isinstance(data, list):
                                for i, item in enumerate(data):
                                    current_path = f"{path}[{i}]"
                                    urls.extend(find_service_urls(item, current_path))
                            return urls
                        
                        service_urls = find_service_urls(config_json)
                        if service_urls:
                            print(f"\n🎯 Found {len(service_urls)} service URLs in JSON:")
                            for path, url in service_urls:
                                print(f"  {path}: {url}")
                        else:
                            print("\n❌ No service URLs found in JSON")
                            
                            # Look for item ID to construct potential URLs
                            if 'site' in config_json and 'item' in config_json['site']:
                                item = config_json['site']['item']
                                if 'id' in item and 'orgId' in item:
                                    item_id = item['id']
                                    org_id = item['orgId']
                                    print(f"\n💡 Could construct URLs from:")
                                    print(f"  Item ID: {item_id}")
                                    print(f"  Org ID: {org_id}")
                                    
                                    potential_urls = [
                                        f"https://services.arcgis.com/{org_id}/arcgis/rest/services/{item_id}/FeatureServer/0",
                                        f"https://services.arcgis.com/{org_id}/arcgis/rest/services/{item_id}/MapServer/0"
                                    ]
                                    
                                    print("  Potential service URLs:")
                                    for url in potential_urls:
                                        print(f"    {url}")
                    
                except json.JSONDecodeError as je:
                    print(f"❌ JSON parsing failed: {je}")
                    print("Raw decoded data (first 1000 chars):")
                    print(decoded_data[:1000])
                
            except Exception as decode_error:
                print(f"❌ URL decoding failed: {decode_error}")
        else:
            print("❌ No __SITE pattern found")
            
            # Look for other potential patterns
            all_patterns = [
                r'window\.__[A-Z_]+\s*=\s*"([^"]+)"',
                r'__[A-Z_]+__\s*=\s*"([^"]+)"',
                r'hubConfig\s*=\s*"([^"]+)"'
            ]
            
            for pattern_name, pattern in zip(['window.__*', '__*__', 'hubConfig'], all_patterns):
                matches = re.findall(pattern, html_content)
                if matches:
                    print(f"Found {len(matches)} matches for pattern '{pattern_name}'")
                    for i, match in enumerate(matches[:2]):
                        print(f"  Match {i+1}: {match[:100]}...")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_config_patterns()
