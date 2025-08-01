#!/usr/bin/env python3

"""
Prototype for detecting opendata portal URLs and extracting ArcGIS service URLs.

This module provides functions to:
1. Detect if a URL points to an opendata portal
2. Extract ArcGIS REST service URLs from opendata portal pages
3. Validate and rank extracted ArcGIS URLs by relevance
"""

import re
import urllib.request
import urllib.parse
from typing import List, Tuple, Optional
import json
import html.parser

# Common opendata portal URL patterns
OPENDATA_PATTERNS = [
    # Major opendata platforms
    'data.gov',
    'opendata.arcgis.com',
    'hub.arcgis.com',
    'data.cityof',
    'opendata.',
    'data.',
    'gis.cityof',
    'gis-open',
    'open-data',
    'geoportal',
    
    # Florida-specific patterns
    'floridagio.gov',
    'data.florida.gov',
    'gis.doh.state.fl.us',
    
    # County/city specific patterns (examples)
    'miamidade.gov/gis',
    'broward.org/gis',
    'pinellascounty.org/gis',
    'data.cityof',
    'gis.county',
    'opendata.county'
]

# ArcGIS REST service URL patterns
ARCGIS_URL_PATTERNS = [
    r'https?://[^/\s"\']+/arcgis/rest/services/[^/\s"\']+/[^/\s"\']+/(?:FeatureServer|MapServer)(?:/\d+)?',
    r'https?://services\d*\.arcgis\.com/[^/\s"\']+/arcgis/rest/services/[^/\s"\']+/[^/\s"\']+/(?:FeatureServer|MapServer)(?:/\d+)?',
    r'https?://[^/\s"\']+/server/rest/services/[^/\s"\']+/[^/\s"\']+/(?:FeatureServer|MapServer)(?:/\d+)?'
]

def is_opendata_portal(url: str) -> bool:
    """
    Check if a URL points to an opendata portal.
    
    Args:
        url: URL to check
        
    Returns:
        True if URL appears to be an opendata portal
    """
    if not url:
        return False
    
    url_lower = url.lower()
    
    # Check against known opendata patterns
    return any(pattern in url_lower for pattern in OPENDATA_PATTERNS)

def is_arcgis_service_url(url: str) -> bool:
    """
    Check if a URL is an ArcGIS REST service URL.
    
    Args:
        url: URL to check
        
    Returns:
        True if the URL appears to be an ArcGIS service URL
    """
    if not url or not isinstance(url, str):
        return False
        
    url_lower = url.lower()
    
    # Check against ArcGIS service URL patterns
    arcgis_indicators = [
        '/rest/services/',
        '/mapserver',
        '/featureserver',
        '/imageserver',
        '/geocodeserver',
        '/geometryserver',
        '/geoprocessingserver'
    ]
    
    return any(indicator in url_lower for indicator in arcgis_indicators)


def calculate_relevance_score(url: str, keywords: List[str]) -> float:
    """
    Calculate relevance score for an ArcGIS service URL based on keywords.
    
    Args:
        url: ArcGIS service URL
        keywords: List of keywords to match against
        
    Returns:
        Relevance score (higher is better)
    """
    if not url or not keywords:
        return 0.0
        
    url_lower = url.lower()
    score = 0.0
    
    # Base score for being an ArcGIS service
    if is_arcgis_service_url(url):
        score += 1.0
    
    # Bonus for FeatureServer (preferred over MapServer)
    if '/featureserver' in url_lower:
        score += 0.5
    
    # Score based on keyword matches in URL
    for keyword in keywords:
        if keyword.lower() in url_lower:
            score += 2.0  # High bonus for keyword match
    
    # Bonus for hosted services (usually more reliable)
    if '/hosted/' in url_lower:
        score += 0.3
    
    return score

class SimpleHTMLParser(html.parser.HTMLParser):
    """Simple HTML parser to extract URLs without external dependencies."""
    
    def __init__(self):
        super().__init__()
        self.urls = set()
        self.script_content = []
        self.in_script = False
    
    def handle_starttag(self, tag, attrs):
        if tag == 'script':
            self.in_script = True
        elif tag == 'a':
            for attr_name, attr_value in attrs:
                if attr_name == 'href' and attr_value:
                    # Check if href contains ArcGIS URL patterns
                    for pattern in ARCGIS_URL_PATTERNS:
                        if re.search(pattern, attr_value, re.IGNORECASE):
                            self.urls.add(attr_value)
    
    def handle_endtag(self, tag):
        if tag == 'script':
            self.in_script = False
    
    def handle_data(self, data):
        if self.in_script:
            self.script_content.append(data)

def extract_arcgis_urls_from_html(html_content: str, base_url: str = None) -> List[str]:
    """
    Extract ArcGIS REST service URLs from HTML content using only standard library.
    
    Args:
        html_content: HTML content to parse
        base_url: Base URL for resolving relative URLs
        
    Returns:
        List of found ArcGIS service URLs
    """
    found_urls = set()
    
    # Method 1: Direct regex search in HTML content
    for pattern in ARCGIS_URL_PATTERNS:
        matches = re.findall(pattern, html_content, re.IGNORECASE)
        found_urls.update(matches)
    
    # Method 2: Simple HTML parsing using standard library
    try:
        parser = SimpleHTMLParser()
        parser.feed(html_content)
        
        # Add URLs found in HTML tags
        found_urls.update(parser.urls)
        
        # Search in JavaScript content
        for script_content in parser.script_content:
            for pattern in ARCGIS_URL_PATTERNS:
                matches = re.findall(pattern, script_content, re.IGNORECASE)
                found_urls.update(matches)
        
        # Resolve relative URLs if base_url provided
        if base_url:
            resolved_urls = set()
            for url in found_urls:
                if not url.startswith('http'):
                    url = urllib.parse.urljoin(base_url, url)
                resolved_urls.add(url)
            found_urls = resolved_urls
                
    except Exception as e:
        print(f"[WARNING] HTML parsing failed: {e}")
    
    return list(found_urls)

def validate_arcgis_url(url: str) -> Tuple[bool, str, dict]:
    """
    Validate an ArcGIS service URL and get metadata.
    
    Args:
        url: ArcGIS service URL to validate
        
    Returns:
        Tuple of (is_valid, reason, metadata_dict)
    """
    try:
        # Add metadata endpoint
        metadata_url = url.rstrip('/') + '?f=json'
        
        req = urllib.request.Request(metadata_url)
        req.add_header('User-Agent', 'Mozilla/5.0 (compatible; OpendataDetector/1.0)')
        
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.getcode() == 200:
                content = response.read().decode('utf-8', errors='ignore')
                
                try:
                    metadata = json.loads(content)
                    
                    # Check for errors
                    if 'error' in metadata:
                        return False, "SERVICE_ERROR", {}
                    
                    # Check for valid service metadata
                    if any(key in metadata for key in ['name', 'type', 'geometryType', 'fields']):
                        return True, "OK", metadata
                    else:
                        return False, "INVALID_METADATA", {}
                        
                except json.JSONDecodeError:
                    return False, "INVALID_JSON", {}
            else:
                return False, f"HTTP_{response.getcode()}", {}
                
    except Exception as e:
        return False, f"ERROR_{type(e).__name__}", {}

def rank_arcgis_urls_by_relevance(urls: List[str], target_keywords: List[str] = None) -> List[Tuple[str, float]]:
    """
    Rank ArcGIS URLs by relevance to target keywords.
    
    Args:
        urls: List of ArcGIS URLs to rank
        target_keywords: Keywords to match against (e.g., ['zoning', 'parcels'])
        
    Returns:
        List of (url, score) tuples sorted by relevance score
    """
    if not target_keywords:
        target_keywords = []
    
    scored_urls = []
    
    for url in urls:
        score = 0.0
        url_lower = url.lower()
        
        # Base score for being a valid ArcGIS URL
        score += 1.0
        
        # Bonus for FeatureServer vs MapServer
        if 'featureserver' in url_lower:
            score += 0.5
        
        # Bonus for matching target keywords
        for keyword in target_keywords:
            if keyword.lower() in url_lower:
                score += 2.0
        
        # Bonus for being a layer endpoint (ends with /0, /1, etc.)
        if re.search(r'/\d+$', url):
            score += 0.3
        
        scored_urls.append((url, score))
    

def extract_arcgis_from_opendata(url: str, target_keywords: List[str] = None) -> List[Tuple[str, float, dict]]:
    """
    Main function to extract and rank ArcGIS URLs from an opendata portal.
    
    Args:
        url: Opendata portal URL
        target_keywords: Keywords to match against
        
    Returns:
        List of (arcgis_url, relevance_score, metadata) tuples
    """
    if not is_opendata_portal(url):
        return []
    
    try:
        # Fetch the opendata page
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0 (compatible; OpendataDetector/1.0)')
        
        with urllib.request.urlopen(req, timeout=15) as response:
            if response.getcode() != 200:
                return []
            
            html_content = response.read().decode('utf-8', errors='ignore')
        
        # Extract ArcGIS URLs
        arcgis_urls = extract_arcgis_urls_from_html(html_content, url)
        
        if not arcgis_urls:
            return []
        
        # Rank URLs by relevance
        ranked_urls = rank_arcgis_urls_by_relevance(arcgis_urls, target_keywords)
        
        # Validate top URLs and get metadata
        results = []
        for arcgis_url, score in ranked_urls[:5]:  # Limit to top 5
            is_valid, reason, metadata = validate_arcgis_url(arcgis_url)
            if is_valid:
                results.append((arcgis_url, score, metadata))
        
        return results
        
    except Exception as e:
        print(f"[ERROR] Failed to extract from {url}: {e}")
        return []

def extract_arcgis_urls_from_opendata(url, layer_keywords=None):
    """
    Extract ArcGIS REST service URLs from opendata portal pages.
    Uses robust requests-based approach with redirect following and comprehensive HTML parsing.
    
    Args:
        url (str): URL of the opendata portal page
        layer_keywords (list): Keywords to help identify relevant services
        
    Returns:
        list: List of tuples (arcgis_url, relevance_score)
    """
    if layer_keywords is None:
        layer_keywords = []
        
    return extract_via_requests_method(url, layer_keywords)


def extract_via_api(url, layer_keywords):
    """
    Try to extract ArcGIS URLs using various API approaches.
    """
    try:
        # ArcGIS Hub API approach
        if 'opendata.arcgis.com' in url or '.hub.arcgis.com' in url:
            return extract_from_arcgis_hub_api(url, layer_keywords)
            
        # Socrata API approach
        if any(domain in url for domain in ['data.', 'opendata.']):
            return extract_from_socrata_api(url, layer_keywords)
            
        # CKAN API approach
        if 'ckan' in url.lower():
            return extract_from_ckan_api(url, layer_keywords)
            
    except Exception as e:
        print(f"API extraction failed for {url}: {e}")
        
    return []


def extract_from_arcgis_hub_api(url, layer_keywords):
    """
    Extract from ArcGIS Hub using their API.
    """
    # Extract dataset ID from URL patterns like:
    # https://data-slc.opendata.arcgis.com/datasets/7f739a7c042b476f840be6c4104aeb6b_0
    # https://ocgis-datahub-ocfl.hub.arcgis.com/datasets/ocfl::orange-county-zoning
    
    dataset_id = None
    
    # Pattern 1: /datasets/{id}
    match = re.search(r'/datasets/([a-f0-9\-_]+)', url)
    if match:
        dataset_id = match.group(1)
        
    # Pattern 2: /datasets/{org}::{name}
    match = re.search(r'/datasets/([^/]+::[^/]+)', url)
    if match:
        dataset_id = match.group(1)
        
    if not dataset_id:
        return []
        
    try:
        # Try ArcGIS Hub API v3
        api_url = f"https://opendata.arcgis.com/api/v3/datasets/{dataset_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        
        response = urllib.request.urlopen(
            urllib.request.Request(api_url, headers=headers), 
            timeout=10
        )
        
        if response.getcode() != 200:
            return []
            
        data = json.loads(response.read().decode('utf-8'))
        
        # Extract service URLs from API response
        service_urls = []
        
        # Check for server info
        if 'data' in data and 'server' in data['data']:
            server_info = data['data']['server']
            if 'url' in server_info:
                service_url = server_info['url']
                if is_arcgis_service_url(service_url):
                    score = calculate_relevance_score(service_url, layer_keywords)
                    service_urls.append((service_url, score))
                    
        # Check for layer info
        if 'data' in data and 'layer' in data['data']:
            layer_info = data['data']['layer']
            if 'url' in layer_info:
                layer_url = layer_info['url']
                if is_arcgis_service_url(layer_url):
                    score = calculate_relevance_score(layer_url, layer_keywords)
                    service_urls.append((layer_url, score))
                    
        # Check attributes for service references
        if 'attributes' in data:
            attrs = data['attributes']
            for key, value in attrs.items():
                if isinstance(value, str) and 'rest/services' in value:
                    if is_arcgis_service_url(value):
                        score = calculate_relevance_score(value, layer_keywords)
                        service_urls.append((value, score))
        
        return sorted(service_urls, key=lambda x: x[1], reverse=True)
        
    except Exception as e:
        print(f"ArcGIS Hub API extraction failed: {e}")
        return []


def extract_from_socrata_api(url, layer_keywords):
    """
    Extract from Socrata-based opendata portals.
    """
    # Extract dataset ID from Socrata URLs
    match = re.search(r'/([a-z0-9]{4}-[a-z0-9]{4})(?:/|$)', url)
    if not match:
        return []
        
    dataset_id = match.group(1)
    
    try:
        # Parse domain from URL
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # Try Socrata API
        api_url = f"https://{domain}/api/views/{dataset_id}.json"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        
        response = urllib.request.urlopen(
            urllib.request.Request(api_url, headers=headers), 
            timeout=10
        )
        
        if response.getcode() != 200:
            return []
            
        data = json.loads(response.read().decode('utf-8'))
        
        # Look for ArcGIS service references in metadata
        service_urls = []
        
        # Check metadata fields
        for field in ['description', 'attribution', 'metadata']:
            if field in data and isinstance(data[field], str):
                urls = re.findall(r'https?://[^\s<>"]+/rest/services/[^\s<>"]+', data[field])
                for service_url in urls:
                    if is_arcgis_service_url(service_url):
                        score = calculate_relevance_score(service_url, layer_keywords)
                        service_urls.append((service_url, score))
        
        return sorted(service_urls, key=lambda x: x[1], reverse=True)
        
    except Exception as e:
        print(f"Socrata API extraction failed: {e}")
        return []


def extract_from_ckan_api(url, layer_keywords):
    """
    Extract from CKAN-based opendata portals.
    """
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        # Extract dataset name from URL
        match = re.search(r'/dataset/([^/]+)', url)
        if not match:
            return []
            
        dataset_name = match.group(1)
        
        # Try CKAN API
        api_url = f"{base_url}/api/3/action/package_show?id={dataset_name}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        
        response = urllib.request.urlopen(
            urllib.request.Request(api_url, headers=headers), 
            timeout=10
        )
        
        if response.getcode() != 200:
            return []
            
        data = json.loads(response.read().decode('utf-8'))
        
        service_urls = []
        
        # Check resources for ArcGIS service URLs
        if 'result' in data and 'resources' in data['result']:
            for resource in data['result']['resources']:
                if 'url' in resource:
                    resource_url = resource['url']
                    if is_arcgis_service_url(resource_url):
                        score = calculate_relevance_score(resource_url, layer_keywords)
                        service_urls.append((resource_url, score))
        
        return sorted(service_urls, key=lambda x: x[1], reverse=True)
        
    except Exception as e:
        print(f"CKAN API extraction failed: {e}")
        return []


def extract_via_requests_method(url, layer_keywords):
    """
    Robust requests-based extraction with redirect following and comprehensive HTML parsing.
    """
    try:
        import urllib.request
        import urllib.parse
        import urllib.error
        from urllib.request import HTTPRedirectHandler, HTTPError
        import ssl
        
        # Create a custom opener that handles redirects and cookies
        opener = urllib.request.build_opener(HTTPRedirectHandler)
        
        # Set comprehensive headers to mimic a real browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        
        # Create request with headers
        request = urllib.request.Request(url, headers=headers)
        
        # Handle SSL context for HTTPS
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Open URL with timeout and SSL context
        try:
            response = urllib.request.urlopen(request, timeout=15, context=ssl_context)
        except HTTPError as e:
            if e.code == 403:
                print(f"Access forbidden (403) for {url} - trying alternative approach")
                # Try with minimal headers
                minimal_request = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                response = urllib.request.urlopen(minimal_request, timeout=15, context=ssl_context)
            else:
                raise
        
        # Check if we got a successful response
        if response.getcode() != 200:
            print(f"HTTP {response.getcode()} for {url}")
            return []
        
        # Get the final URL after redirects
        final_url = response.geturl()
        print(f"Following redirects: {url} → {final_url}")
        
        # Read and decode content
        content = response.read()
        
        # Handle gzip/deflate compression
        content_encoding = response.getheader('Content-Encoding', '').lower()
        if content_encoding == 'gzip':
            import gzip
            content = gzip.decompress(content)
        elif content_encoding == 'deflate':
            import zlib
            content = zlib.decompress(content)
        elif content_encoding == 'br':
            try:
                import brotli
                content = brotli.decompress(content)
            except ImportError:
                print("Warning: brotli compression detected but brotli module not available")
        
        # Handle different encodings
        try:
            # Try UTF-8 first
            html_content = content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                # Fall back to latin-1
                html_content = content.decode('latin-1')
            except UnicodeDecodeError:
                # Last resort - ignore errors
                html_content = content.decode('utf-8', errors='ignore')
        
        # Extract ArcGIS URLs using multiple methods
        arcgis_urls = []
        
        # Method 1: Direct regex search for ArcGIS service URLs
        service_patterns = [
            r'https?://[^\s<>"\'\']+/rest/services/[^\s<>"\'\']+/(?:MapServer|FeatureServer)(?:/\d+)?',
            r'https?://services\d*\.arcgis\.com/[^\s<>"\'\']+/arcgis/rest/services/[^\s<>"\'\']+',
            r'https?://[^\s<>"\'\']+\.arcgis\.com/[^\s<>"\'\']+/rest/services/[^\s<>"\'\']+'
        ]
        
        for pattern in service_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            for match in matches:
                # Clean up the URL (remove trailing punctuation)
                clean_url = re.sub(r'[,;\)\]}>"\'\']+$', '', match)
                if is_arcgis_service_url(clean_url):
                    score = calculate_relevance_score(clean_url, layer_keywords)
                    arcgis_urls.append((clean_url, score))
        
        # Method 2: Parse HTML and extract from specific elements
        try:
            parser = EnhancedHTMLParser()
            parser.feed(html_content)
            
            # Extract from links
            for link_url in parser.links:
                if is_arcgis_service_url(link_url):
                    score = calculate_relevance_score(link_url, layer_keywords)
                    arcgis_urls.append((link_url, score))
            
            # Extract from script content
            for script_url in parser.script_urls:
                if is_arcgis_service_url(script_url):
                    score = calculate_relevance_score(script_url, layer_keywords)
                    arcgis_urls.append((script_url, score))
                    
        except Exception as parse_error:
            print(f"HTML parsing error: {parse_error}")
        
        # Method 3: Look for JSON-LD structured data
        json_ld_pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>([^<]+)</script>'
        json_matches = re.findall(json_ld_pattern, html_content, re.IGNORECASE | re.DOTALL)
        
        for json_content in json_matches:
            try:
                data = json.loads(json_content)
                # Recursively search for URLs in JSON data
                json_urls = extract_urls_from_json(data)
                for json_url in json_urls:
                    if is_arcgis_service_url(json_url):
                        score = calculate_relevance_score(json_url, layer_keywords)
                        arcgis_urls.append((json_url, score))
            except json.JSONDecodeError:
                continue
        
        # Method 4: Decode URL-encoded configuration data
        print(f"DEBUG: HTML content length: {len(html_content)} characters")
        print(f"DEBUG: First 200 chars: {html_content[:200]}")
        print(f"DEBUG: About to call extract_from_encoded_config with {len(layer_keywords)} keywords")
        config_urls = extract_from_encoded_config(html_content, layer_keywords)
        print(f"DEBUG: extract_from_encoded_config returned {len(config_urls)} URLs")
        arcgis_urls.extend(config_urls)
        
        # Method 5: Look for data attributes and configuration objects
        config_patterns = [
            r'["\']?(?:service|layer|feature)Url["\']?\s*[:=]\s*["\']([^"\'\']+)["\']',
            r'["\']?url["\']?\s*[:=]\s*["\']([^"\'\']*rest/services[^"\'\']+)["\']',
            r'data-[^=]*url[^=]*=["\']([^"\'\']*rest/services[^"\'\']+)["\']'
        ]
        
        for pattern in config_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            for match in matches:
                if is_arcgis_service_url(match):
                    score = calculate_relevance_score(match, layer_keywords)
                    arcgis_urls.append((match, score))
        
        # Remove duplicates and sort by relevance
        unique_urls = {}
        for url_tuple in arcgis_urls:
            url, score = url_tuple
            if url not in unique_urls or unique_urls[url] < score:
                unique_urls[url] = score
        
        # Convert back to list of tuples and sort
        final_urls = [(url, score) for url, score in unique_urls.items()]
        final_urls.sort(key=lambda x: x[1], reverse=True)
        
        print(f"Found {len(final_urls)} unique ArcGIS URLs for {url}")
        return final_urls
        
    except Exception as e:
        print(f"Requests-based extraction failed for {url}: {e}")
        return []


def extract_from_encoded_config(html_content, layer_keywords):
    """
    Extract ArcGIS URLs from URL-encoded configuration data in script tags.
    """
    import urllib.parse
    
    arcgis_urls = []
    
    try:
        # Look for common configuration patterns in ArcGIS Hub sites
        # Use the EXACT patterns that worked in our debug script
        config_patterns = [
            r'window\.__SITE\s*=\s*["\']([^"\'\']+)["\']',  # This is the pattern that works!
            r'window\.__CONFIG\s*=\s*["\']([^"\'\']+)["\']',
            r'window\.__DATA\s*=\s*["\']([^"\'\']+)["\']',
            r'window\.__INITIAL_STATE\s*=\s*["\']([^"\'\']+)["\']',
            r'__SITE__\s*=\s*["\']([^"\'\']+)["\']',
            r'hubConfig\s*=\s*["\']([^"\'\']+)["\']'
        ]
        
        for i, pattern in enumerate(config_patterns, 1):
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            print(f"DEBUG: Pattern {i} '{pattern}': {len(matches)} matches")
            
            for encoded_data in matches:
                try:
                    # URL decode the data
                    decoded_data = urllib.parse.unquote(encoded_data)
                    print(f"Decoded config data length: {len(decoded_data)} characters")
                    
                    # Try to parse as JSON
                    try:
                        config_json = json.loads(decoded_data)
                        
                        # Extract URLs from the decoded JSON
                        json_urls = extract_urls_from_json(config_json)
                        
                        for url in json_urls:
                            if is_arcgis_service_url(url):
                                score = calculate_relevance_score(url, layer_keywords)
                                arcgis_urls.append((url, score))
                                print(f"Found ArcGIS URL in config: {url}")
                        
                        # Look for specific dataset/item information
                        dataset_urls = extract_dataset_service_urls(config_json, layer_keywords)
                        arcgis_urls.extend(dataset_urls)
                        
                    except json.JSONDecodeError as je:
                        print(f"JSON decode error: {je}")
                        # If not valid JSON, search for URLs in the decoded string
                        url_matches = re.findall(r'https?://[^\s<>"\'\']+/rest/services/[^\s<>"\'\']+', decoded_data)
                        for url in url_matches:
                            clean_url = re.sub(r'[,;\)\]}>"\'\']+$', '', url)
                            if is_arcgis_service_url(clean_url):
                                score = calculate_relevance_score(clean_url, layer_keywords)
                                arcgis_urls.append((clean_url, score))
                                print(f"Found ArcGIS URL in decoded string: {clean_url}")
                        
                except Exception as decode_error:
                    print(f"Error decoding config data: {decode_error}")
                    continue
        
        print(f"Extracted {len(arcgis_urls)} URLs from encoded config data")
        return arcgis_urls
        
    except Exception as e:
        print(f"Error extracting from encoded config: {e}")
        return []


def extract_dataset_service_urls(config_json, layer_keywords):
    """
    Extract service URLs from ArcGIS Hub dataset configuration.
    """
    urls = []
    
    try:
        # Look for common ArcGIS Hub dataset patterns
        if isinstance(config_json, dict):
            # Method 1: Check for dataset item information in Hub site config
            if 'site' in config_json and 'item' in config_json['site']:
                item = config_json['site']['item']
                if 'id' in item and 'orgId' in item:
                    item_id = item['id']
                    org_id = item['orgId']
                    
                    print(f"Found Hub site item - ID: {item_id}, Org: {org_id}")
                    
                    # This is the Hub site itself, not the dataset
                    # We need to look for the actual dataset information
                    
            # Method 2: Look for dataset-specific patterns in the URL
            # The URL pattern suggests the dataset ID might be different
            # Extract from URL: /datasets/ocfl::orange-county-zoning
            
            # Method 3: Look for embedded dataset configuration
            # Check if there's dataset info in site.data or other locations
            if 'site' in config_json and 'data' in config_json['site']:
                site_data = config_json['site']['data']
                
                # Look for dataset references in various locations
                dataset_patterns = [
                    'datasets', 'layers', 'services', 'items', 'content'
                ]
                
                for pattern in dataset_patterns:
                    if pattern in site_data:
                        dataset_info = site_data[pattern]
                        print(f"Found {pattern} in site data: {type(dataset_info)}")
                        
                        # Extract URLs from dataset info
                        dataset_urls = extract_urls_from_json(dataset_info)
                        for url in dataset_urls:
                            if is_arcgis_service_url(url):
                                score = calculate_relevance_score(url, layer_keywords)
                                urls.append((url, score))
                                print(f"Found dataset service URL: {url}")
            
            # Method 4: Look for direct service references anywhere in the config
            service_keys = ['serviceUrl', 'url', 'layerUrl', 'featureServiceUrl', 'mapServiceUrl', 'itemUrl']
            
            def search_for_service_urls(data, path=""):
                """Recursively search for service URLs in nested data."""
                found_urls = []
                
                if isinstance(data, dict):
                    for key, value in data.items():
                        current_path = f"{path}.{key}" if path else key
                        
                        # Check if this key matches our service key patterns
                        if key.lower() in [k.lower() for k in service_keys]:
                            if isinstance(value, str) and is_arcgis_service_url(value):
                                score = calculate_relevance_score(value, layer_keywords)
                                found_urls.append((value, score))
                                print(f"Found service URL at {current_path}: {value}")
                        
                        # Recursively search nested structures
                        elif isinstance(value, (dict, list)):
                            found_urls.extend(search_for_service_urls(value, current_path))
                            
                elif isinstance(data, list):
                    for i, item in enumerate(data):
                        current_path = f"{path}[{i}]" if path else f"[{i}]"
                        found_urls.extend(search_for_service_urls(item, current_path))
                
                return found_urls
            
            # Search the entire config for service URLs
            found_service_urls = search_for_service_urls(config_json)
            urls.extend(found_service_urls)
            
            # Method 5: Construct URLs based on common ArcGIS Hub patterns
            # Many Hub datasets follow predictable URL patterns
            if 'site' in config_json and 'item' in config_json['site']:
                item = config_json['site']['item']
                if 'orgId' in item:
                    org_id = item['orgId']
                    
                    # Try to extract dataset name from common patterns
                    dataset_names = []
                    
                    # Pattern 1: From layer keywords
                    if layer_keywords:
                        dataset_names.extend(layer_keywords)
                    
                    # Pattern 2: Common naming patterns for the layer type
                    common_patterns = {
                        'zoning': ['Zoning', 'zoning', 'ZoningDistricts', 'Zoning_Districts'],
                        'building': ['Buildings', 'BuildingFootprints', 'Building_Footprints'],
                        'parcel': ['Parcels', 'PropertyParcels', 'Property_Parcels'],
                        'address': ['AddressPoints', 'Address_Points', 'SiteAddresses']
                    }
                    
                    for keyword in layer_keywords:
                        if keyword.lower() in common_patterns:
                            dataset_names.extend(common_patterns[keyword.lower()])
                    
                    # Generate potential service URLs
                    for dataset_name in dataset_names[:3]:  # Limit to avoid too many URLs
                        potential_urls = [
                            f"https://services.arcgis.com/{org_id}/arcgis/rest/services/{dataset_name}/FeatureServer/0",
                            f"https://services.arcgis.com/{org_id}/arcgis/rest/services/{dataset_name}/MapServer/0",
                            f"https://services.arcgis.com/{org_id}/arcgis/rest/services/Hosted/{dataset_name}/FeatureServer/0"
                        ]
                        
                        for url in potential_urls:
                            score = calculate_relevance_score(url, layer_keywords) + 1.0  # Boost constructed URLs
                            urls.append((url, score))
                            print(f"Generated potential service URL: {url}")
        
    except Exception as e:
        print(f"Error extracting dataset service URLs: {e}")
    
    return urls


def extract_urls_from_json(data, urls=None):
    """
    Recursively extract URLs from JSON data structure.
    """
    if urls is None:
        urls = []
    
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, str) and ('rest/services' in value or 'arcgis.com' in value):
                urls.append(value)
            elif isinstance(value, (dict, list)):
                extract_urls_from_json(value, urls)
    elif isinstance(data, list):
        for item in data:
            extract_urls_from_json(item, urls)
    
    return urls


class EnhancedHTMLParser(html.parser.HTMLParser):
    """
    Enhanced HTML parser that extracts links and script URLs more comprehensively.
    """
    def __init__(self):
        super().__init__()
        self.links = []
        self.script_urls = []
        self.current_script = ''
        self.in_script = False
    
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        
        # Extract from href attributes
        if tag == 'a' and 'href' in attrs_dict:
            self.links.append(attrs_dict['href'])
        
        # Extract from src attributes
        if 'src' in attrs_dict:
            self.links.append(attrs_dict['src'])
        
        # Extract from data attributes
        for attr_name, attr_value in attrs:
            if attr_name.startswith('data-') and attr_value:
                if 'rest/services' in attr_value or 'arcgis.com' in attr_value:
                    self.links.append(attr_value)
        
        # Track script tags
        if tag == 'script':
            self.in_script = True
            self.current_script = ''
    
    def handle_endtag(self, tag):
        if tag == 'script' and self.in_script:
            self.in_script = False
            # Extract URLs from script content
            script_urls = re.findall(r'["\']([^"\'\']*rest/services[^"\'\']+)["\']', self.current_script)
            self.script_urls.extend(script_urls)
    
    def handle_data(self, data):
        if self.in_script:
            self.current_script += data


def extract_via_html_parsing(url, layer_keywords):
    """
    Legacy HTML parsing approach (kept for compatibility).
    """
    return extract_via_requests_method(url, layer_keywords)

# Example usage and testing
if __name__ == "__main__":
    # Test opendata detection
    test_urls = [
        "https://opendata.arcgis.com/datasets/some-dataset",
        "https://data.cityofgainesville.org/datasets/zoning",
        "https://services1.arcgis.com/direct/service/FeatureServer/0",
        "https://example.com/regular-download.zip"
    ]
    
    for test_url in test_urls:
        is_opendata = is_opendata_portal(test_url)
        print(f"{test_url}: {'OPENDATA' if is_opendata else 'REGULAR'}")
    
    # Test ArcGIS URL extraction (would need real HTML content)
    sample_html = '''
    <html>
        <body>
            <a href="https://services1.arcgis.com/example/arcgis/rest/services/Zoning/FeatureServer/0">Zoning Data</a>
            <script>
                var serviceUrl = "https://gis.county.gov/arcgis/rest/services/Parcels/MapServer/1";
            </script>
        </body>
    </html>
    '''
    
    extracted_urls = extract_arcgis_urls_from_html(sample_html)
    print(f"\nExtracted URLs: {extracted_urls}")
