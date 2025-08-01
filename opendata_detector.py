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
    
    # Sort by score descending
    return sorted(scored_urls, key=lambda x: x[1], reverse=True)

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
