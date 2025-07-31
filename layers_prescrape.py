#!/usr/bin/env python3
"""
Database preparation utility for layers_scrape.py

This script prepares the m_gis_data_catalog_main table to ensure it meets the requirements
for layers_scrape.py. It operates on existing database records and provides tools to:

1. DETECT mode: Find malformed records and missing fields, output CSV + JSON reports
2. FILL mode: Apply manual corrections and auto-derivable fields from JSON
3. CREATE mode: Create new records based on layer/county/city + manual info

All modes support entity filtering - specify entity names to focus on specific records.
The script relies on title-based matching to find corresponding records and uses minimal
manifest integration only for extracting preprocessing commands.
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not available, try manual loading
    env_path = Path('.env')
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip().strip('\'"')

# ---------------------------------------------------------------------------
# Name Formatting Utilities
# ---------------------------------------------------------------------------

def format_name(name: str, name_type: str, external: bool = False) -> str:
    """Convert between internal and external name formats.
    
    Args:
        name: The name to format
        name_type: Type of name - 'layer', 'county', or 'city'
        external: If True, convert to external (human-readable) format.
                 If False, convert to internal (code-friendly) format.
    
    Internal format: lowercase, underscores, abbreviations (miami_dade, st_lucie, flu)
    External format: title case, spaces/hyphens, periods for abbreviations (Miami-Dade, St. Lucie, Future Land Use)
    
    Examples:
        format_name("miami_dade", "county", external=True) -> "Miami-Dade"
        format_name("Miami-Dade", "county", external=False) -> "miami_dade"
        format_name("st_lucie", "county", external=True) -> "St. Lucie"
        format_name("howey_in_the_hills", "city", external=True) -> "Howey-in-the-Hills"
        format_name("flu", "layer", external=True) -> "Future Land Use"
    """
    if not name or not name.strip():
        return ""
    
    name = name.strip()
    
    # Special mappings for layers
    layer_mappings = {
        # internal -> external
        'flu': 'Future Land Use',
        'addr_pnts': 'Address Points',
        'bldg_ftpr': 'Building Footprints',
        'parcel_geo': 'Parcel Geometry',
        'flood_zones': 'Flood Zones',
        'subdiv': 'Subdivisions',
        'streets': 'Streets',
        'zoning': 'Zoning'
    }
    
    # Reverse mapping for external -> internal
    layer_mappings_reverse = {v.lower(): k for k, v in layer_mappings.items()}
    
    # Special county mappings (internal -> external)
    county_special = {
        'miami_dade': 'Miami-Dade',
        'desoto': 'DeSoto',
        'st_johns': 'St. Johns',
        'st_lucie': 'St. Lucie',
        'santa_rosa': 'Santa Rosa',
        'indian_river': 'Indian River',
        'palm_beach': 'Palm Beach'
    }
    
    # Reverse mapping for counties (external -> internal)
    county_special_reverse = {v.lower(): k for k, v in county_special.items()}
    
    if external:
        # Convert to external format
        if name_type == 'layer':
            return layer_mappings.get(name.lower(), name.title())
        
        elif name_type == 'county':
            name_lower = name.lower()
            # Check special cases first
            if name_lower in county_special:
                return county_special[name_lower]
            # Handle regular cases
            return _to_external_format(name)
        
        elif name_type == 'city':
            # Handle special city cases
            if name.lower() in ['unincorporated', 'incorporated', 'unified', 'countywide']:
                return name.title()
            # Handle hyphenated city names
            return _to_external_format(name)
    
    else:
        # Convert to internal format
        if name_type == 'layer':
            name_lower = name.lower()
            return layer_mappings_reverse.get(name_lower, name_lower.replace(' ', '_').replace('-', '_'))
        
        elif name_type == 'county':
            name_lower = name.lower()
            # Check special reverse mappings first
            if name_lower in county_special_reverse:
                return county_special_reverse[name_lower]
            # Handle regular cases
            return _to_internal_format(name)
        
        elif name_type == 'city':
            return _to_internal_format(name)
    
    return name

def _to_external_format(name: str) -> str:
    """Convert internal format to external format."""
    # Replace underscores with spaces/hyphens
    # First handle special abbreviations
    result = name.replace('_', ' ')
    
    # Split into words and apply title case rules
    words = result.split()
    formatted_words = []
    
    stop_words = {'of', 'and', 'in', 'the', 'on', 'at', 'by', 'for', 'with'}
    abbrev_map = {'st': 'St.', 'ft': 'Ft.', 'mt': 'Mt.'}
    
    for i, word in enumerate(words):
        word_lower = word.lower()
        is_first = i == 0
        
        if word_lower in abbrev_map:
            formatted_words.append(abbrev_map[word_lower])
        elif is_first or word_lower not in stop_words:
            formatted_words.append(word.capitalize())
        else:
            formatted_words.append(word_lower)
    
    result = ' '.join(formatted_words)
    
    # Handle hyphenated multi-word place names
    if any(phrase in result.lower() for phrase in ['in the', 'on the', 'by the']):
        # Convert spaces to hyphens for compound place names like "howey in the hills"
        result = result.replace(' ', '-')
    
    return result

def _to_internal_format(name: str) -> str:
    """Convert external format to internal format."""
    # Remove periods and convert to lowercase
    result = name.lower()
    
    # Handle special abbreviations
    result = result.replace('st.', 'st').replace('ft.', 'ft').replace('mt.', 'mt')
    
    # Convert spaces and hyphens to underscores
    result = re.sub(r'[^a-z0-9]+', '_', result)
    
    # Remove leading/trailing underscores and collapse multiple underscores
    result = re.sub(r'_+', '_', result).strip('_')
    
    return result

# ---------------------------------------------------------------------------
# Configuration and Constants
# ---------------------------------------------------------------------------

# Database connection - should match layers_scrape.py  
PG_CONNECTION = os.getenv("PG_CONNECTION")

# Output directories
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

# File paths
MISSING_FIELDS_JSON = Path("missing_fields.json")
MANIFEST_PATH = Path("test/layer_manifest.json")

# Florida counties for entity parsing (from original script)
FL_COUNTIES = {
    "alachua","baker","bay","bradford","brevard","broward","calhoun","charlotte","citrus","clay",
    "collier","columbia","desoto","dixie","duval","escambia","flagler","franklin","gadsden","gilchrist",
    "glades","gulf","hamilton","hardee","hendry","hernando","highlands","hillsborough","holmes",
    "indian_river","jackson","jefferson","lafayette","lake","lee","leon","levy","liberty","madison",
    "manatee","marion","martin","miami_dade","monroe","nassau","okaloosa","okeechobee","orange","osceola",
    "palm_beach","pasco","pinellas","polk","putnam","santa_rosa","sarasota","seminole","st_johns",
    "st_lucie","sumter","suwannee","taylor","union","volusia","wakulla","walton","washington",
}

# Layer configurations
LAYER_CONFIGS = {
    'zoning': {
        'category': '08_Land_Use_and_Zoning',
        'layer_group': 'flu_zoning',
        'level': 'state_county_city',
    },
    'flu': {
        'category': '08_Land_Use_and_Zoning', 
        'layer_group': 'flu_zoning',
        'level': 'state_county_city',
    },
    'flood_zones': {
        'category': '12_Hazards',
        'layer_group': 'hazards',
        'level': 'national',
    },
    'parcel_geo': {
        'category': '05_Parcels',
        'layer_group': 'parcels',
        'level': 'state_county',
    },
    'streets': {
        'category': '03_Transportation',
        'layer_group': 'base_map_overlay',
        'level': 'state_county',
    },
    'addr_pnts': {
        'category': '05_Parcels',
        'layer_group': 'parcels',
        'level': 'state_county',
    },
    'subdiv': {
        'category': '05_Parcels',
        'layer_group': 'parcels',
        'level': 'state_county',
    },
    'bldg_ftpr': {
        'category': '05_Parcels',
        'layer_group': 'parcels',
        'level': 'state_county',
    },
    'fdot_tc': {
        'category': '03_Transportation',
        'layer_group': 'base_map_overlay',
        'level': 'state',
    },
    'sunbiz': {
        'category': '21_Misc',
        'layer_group': 'parcels',
        'level': 'state',
    }
}

# Note: Layer configurations are now centralized in LAYER_CONFIGS above
# No need for separate LAYER_GROUP_MAP and CATEGORY_MAP dictionaries

# ---------------------------------------------------------------------------
# Utility Functions (preserved from layer_standardize_database.py)
# ---------------------------------------------------------------------------

def safe_catalog_val(val: Any) -> str:
    """Return value or **MISSING** if val is falsy/None."""
    if val in (None, "", "NULL", "null"):
        return "**MISSING**"
    return str(val)

def get_today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

def split_entity(entity: str) -> Tuple[str, str]:
    """Split manifest entity into (county, city).
    
    Handles multi-word counties like 'miami_dade_unincorporated' or 'st_lucie_port_st_lucie'.
    Strategy:
    1. If the last token is a known suffix (unincorporated/unified/incorporated/countywide) treat it as the city.
    2. Otherwise, iterate from longest possible county prefix to shortest until we find a match in FL_COUNTIES.
       The remainder is considered the city. Fallback is original heuristic (first token as county).
    """
    tokens = entity.split("_")
    if len(tokens) < 2:
        raise ValueError(f"Invalid entity format: {entity}")

    suffixes = {"unincorporated", "incorporated", "unified", "countywide"}
    if tokens[-1] in suffixes:
        county = "_".join(tokens[:-1])
        city = tokens[-1]
        return county, city

    # Try to recognise multi-word counties by longest-prefix match
    for i in range(len(tokens), 1, -1):  # from longest possible down to 2 tokens
        candidate_county = "_".join(tokens[:i])
        if candidate_county in FL_COUNTIES:
            county = candidate_county
            city = "_".join(tokens[i:])
            if not city:  # edge case – entity only county
                raise ValueError(f"Could not determine city part in entity: {entity}")
            return county, city

    # Fallback to original simple split
    county = tokens[0]
    city = "_".join(tokens[1:])
    return county, city

# ---------------------------------------------------------------------------
# Database Utilities
# ---------------------------------------------------------------------------

class DB:
    """Thin wrapper around psycopg2 connection with dict cursors."""

    def __init__(self, conn_str: str):
        print(f"[DEBUG] Attempting to connect to database...")
        try:
            self.conn = psycopg2.connect(conn_str, connect_timeout=10)
            print(f"[DEBUG] Database connection successful")
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            raise

    def fetchone(self, sql: str, params: Tuple[Any, ...] | None = None):
        self.cur.execute(sql, params)
        row = self.cur.fetchone()
        return dict(row) if row else None

    def fetchall(self, sql: str, params: Tuple[Any, ...] | None = None):
        self.cur.execute(sql, params)
        return self.cur.fetchall()

    def execute(self, sql: str, params: Tuple[Any, ...] | None = None):
        self.cur.execute(sql, params)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()

# ---------------------------------------------------------------------------
# Title-Based Record Matching
# ---------------------------------------------------------------------------

def parse_title_to_entity(title: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Parse database title to extract layer, county, city, and entity_type.
    
    Examples:
      "Zoning - City of Gainesville"        -> ("zoning", "alachua", "gainesville", "city")
      "Zoning - Alachua Unincorporated"     -> ("zoning", "alachua", "unincorporated", "unincorporated")
      "Future Land Use - Duval Unified"     -> ("flu", "duval", "unified", "unified")
    
    Returns lowercase values; if parsing fails returns (None, None, None, None).
    """
    # Split the string into layer part and the remainder
    try:
        layer_part, rest = title.split(" - ", 1)
    except ValueError:
        return (None, None, None, None)

    # Normalize layer name using format_name
    layer_norm = format_name(layer_part.strip(), 'layer', external=False)
    
    # If format_name didn't recognize it, try basic normalization
    if not layer_norm or layer_norm == layer_part.strip().lower():
        layer_norm = layer_part.strip().lower()
        layer_norm = layer_norm.replace("future land use", "flu")  # Preferred short name

    # The remainder may contain multiple " - " separated pieces
    rest_parts = rest.split(" - ")
    # If last token is a descriptor like AGS/PDF/SHP we drop it
    descriptors = {"ags", "pdf", "shp", "zip"}
    if len(rest_parts) > 1 and rest_parts[-1].lower() in descriptors:
        rest_parts = rest_parts[:-1]
    rest_main = " ".join(rest_parts).strip()
    
    # Remove the word "County" if it appears immediately before a suffix word
    rest_main = re.sub(r"\s+County\s+(?=(unincorporated|incorporated|unified|countywide)$)", " ", rest_main, flags=re.I)

    # Regex patterns for different entity types
    city_re = re.compile(r"^(?:city|town|village) of\s+(.+)$", re.I)
    county_suffix_re = re.compile(r"^([A-Za-z\s\-\.]+?)\s+(unincorporated|incorporated|unified|countywide)$", re.I)
    county_only_re = re.compile(r"^([A-Za-z\s\-\.]+?)\s+county$", re.I)

    m_city = city_re.match(rest_main)
    if m_city:
        city = format_name(m_city.group(1).strip(), 'city', external=False)
        return (layer_norm, None, city, "city")

    m_cnty = county_suffix_re.match(rest_main)
    if m_cnty:
        county = format_name(m_cnty.group(1).strip(), 'county', external=False)
        suffix = m_cnty.group(2).strip().lower()
        return (layer_norm, county, suffix, suffix)

    m_cnty_only = county_only_re.match(rest_main)
    if m_cnty_only:
        county = format_name(m_cnty_only.group(1).strip(), 'county', external=False)
        return (layer_norm, county, None, None)

    # Fallback: cannot parse
    return (None, None, None, None)

def entity_from_title_parse(layer: str, county_from_title: str, city_from_title: str, entity_type: str) -> str:
    """Convert parsed title components to entity name format (county_city)."""
    if county_from_title and city_from_title:
        # Normalize county and city names to match entity format (internal format)
        county_internal = format_name(county_from_title, 'county', external=False)
        if entity_type in {"unincorporated", "unified", "incorporated", "countywide"}:
            entity = f"{county_internal}_{entity_type}"
        else:
            city_internal = format_name(city_from_title, 'city', external=False)
            entity = f"{county_internal}_{city_internal}"
    elif county_from_title and not city_from_title:
        # County-only title (e.g., "Zoning - Walton County") -> treat as unincorporated
        county_internal = format_name(county_from_title, 'county', external=False)
        entity = f"{county_internal}_unincorporated"
    else:
        raise ValueError(f"Cannot construct entity from title components: layer={layer}, county={county_from_title}, city={city_from_title}, type={entity_type}")
    
    return entity

# ---------------------------------------------------------------------------
# Placeholder Helper Functions for Fill Mode
# ---------------------------------------------------------------------------

def validate_url_batch(urls: list[str], max_workers: int = 10) -> dict[str, tuple[bool, str]]:
    """Validate multiple URLs concurrently for better performance.
    
    Args:
        urls: List of URLs to validate
        max_workers: Maximum number of concurrent validation threads
        
    Returns:
        dict: {url: (is_valid, status_reason)} for each URL
    """
    import concurrent.futures
    
    if not urls:
        return {}
    
    results = {}
    
    # Use ThreadPoolExecutor for concurrent URL validation
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all validation tasks
        future_to_url = {executor.submit(validate_url, url): url for url in urls}
        
        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                is_valid, reason = future.result(timeout=15)  # 15 second timeout per URL
                results[url] = (is_valid, reason)
            except Exception as e:
                # If validation fails completely, mark as deprecated
                results[url] = (False, "DEPRECATED")
    
    return results

def validate_url(url: str) -> tuple[bool, str]:
    """Check if URL still serves accessible, fresh geospatial data.
    
    Focus on real-world issues that affect data freshness/accessibility:
    - Authentication now required (401/403)
    - Data moved/redirected (301/302 chains)
    - Site abandoned/dead (404/410)
    - Service discontinued (error responses)
    - ArcGIS services returning error metadata
    
    Returns:
        tuple[bool, str]: (is_valid, status_reason)
        - (True, "OK") if URL serves accessible data
        - (False, "MISSING") if URL is empty/None
        - (False, "DEPRECATED") if URL has accessibility/freshness issues
    """
    import urllib.parse
    import urllib.request
    import json
    import socket
    import ssl
    from urllib.error import HTTPError, URLError
    
    if not url or not url.strip():
        return False, "MISSING"
    
    url = url.strip()
    
    try:
        # Create request with reasonable timeout and proper user agent
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0 (compatible; LayersPrescrape/1.0; +http://gis-data-scraper)')
        
        # For ArcGIS services, check metadata endpoint for service health
        if any(x in url.lower() for x in ['arcgis', 'featureserver', 'mapserver']):
            # Try to get service metadata to check if service is healthy
            metadata_url = url.rstrip('/') + '?f=json'
            try:
                metadata_req = urllib.request.Request(metadata_url)
                metadata_req.add_header('User-Agent', 'Mozilla/5.0 (compatible; LayersPrescrape/1.0)')
                
                with urllib.request.urlopen(metadata_req, timeout=5) as response:
                    if response.getcode() == 200:
                        # Read more content for ArcGIS services (they can be large)
                        content = response.read(20480).decode('utf-8', errors='ignore')  # 20KB should be enough
                        
                        # Quick check for obvious issues before JSON parsing
                        content_lower = content.lower()
                        if 'authentication' in content_lower or 'login required' in content_lower:
                            return False, "DEPRECATED"  # Now requires auth
                        
                        # Check for error patterns in raw content (handles truncated JSON)
                        if '"error"' in content and 'code' in content:
                            return False, "DEPRECATED"  # Service returns error
                        
                        # Look for positive indicators that suggest a working service
                        positive_indicators = [
                            '"name":', '"type":', '"geometryType":', '"fields":', 
                            '"currentVersion":', '"serviceItemId":', '"defaultVisibility":'
                        ]
                        
                        if any(indicator in content for indicator in positive_indicators):
                            try:
                                # Try to parse JSON if it looks promising
                                metadata = json.loads(content)
                                
                                # Double-check for errors in parsed JSON
                                if 'error' in metadata:
                                    return False, "DEPRECATED"  # Service returns error
                                
                                # Check if service has valid geospatial metadata  
                                if any(key in metadata for key in ['name', 'type', 'geometryType', 'fields', 'extent']):
                                    return True, "OK"  # Looks like healthy service
                                else:
                                    return False, "DEPRECATED"  # Missing expected metadata
                                    
                            except json.JSONDecodeError:
                                # JSON truncated or malformed, but has positive indicators
                                # This is likely a valid service with large metadata
                                return True, "OK"
                        else:
                            return False, "DEPRECATED"  # No positive indicators found
                    else:
                        return False, "DEPRECATED"
            except HTTPError as e:
                if e.code in [401, 403]:
                    return False, "DEPRECATED"  # Authentication required
                else:
                    pass  # Fall through to regular request
            except:
                pass  # Fall through to regular HEAD request
        
        # For non-ArcGIS URLs or if ArcGIS metadata failed, check basic accessibility
        req.get_method = lambda: 'HEAD'
        
        with urllib.request.urlopen(req, timeout=5) as response:
            status_code = response.getcode()
            
            if status_code == 200:
                # Check for reasonable file size (avoid tiny error pages)
                content_length = response.headers.get('content-length')
                if content_length and int(content_length) < 50:
                    return False, "DEPRECATED"  # Suspiciously small file
                
                # Check last-modified to detect stale data (optional)
                last_modified = response.headers.get('last-modified')
                # TODO: Could add staleness detection here if needed
                
                return True, "OK"
            
            elif status_code in [301, 302, 303, 307, 308]:
                # Data has moved - follow redirect once to check final destination
                location = response.headers.get('location')
                if location and location != url:  # Avoid infinite loops
                    return validate_url(location)
                else:
                    return False, "DEPRECATED"  # Redirect without location
            
            elif status_code in [404, 410]:
                return False, "DEPRECATED"  # Resource not found/gone
            
            elif status_code in [401, 403]:
                return False, "DEPRECATED"  # Authentication/authorization required
            
            else:
                return False, "DEPRECATED"  # Other HTTP errors
    
    except HTTPError as e:
        # Handle specific HTTP errors that indicate accessibility issues
        if e.code == 401:
            return False, "DEPRECATED"  # Authentication required
        elif e.code == 403:
            return False, "DEPRECATED"  # Access denied (new restrictions)
        elif e.code in [404, 410]:
            return False, "DEPRECATED"  # Resource not found/gone
        elif e.code == 429:
            return False, "DEPRECATED"  # Rate limited (might be blocked)
        else:
            return False, "DEPRECATED"  # Other server errors
    
    except (URLError, socket.timeout, ssl.SSLError, ConnectionError) as e:
        # Network/connectivity issues - likely service problems
        return False, "DEPRECATED"
    
    except Exception as e:
        # Any other error - treat as deprecated
        return False, "DEPRECATED"

def get_format_from_url(url: str) -> str:
    """Determine format from URL patterns, focusing on AGS vs non-AGS distinction.
    
    Args:
        url: Source URL to analyze
        
    Returns:
        Detected format string - prioritizes AGS detection, defaults to SHP for most downloads
    """
    if not url:
        return ""
    
    url_lower = url.lower()
    
    # ArcGIS Services (highest priority - most reliable indicator)
    # This is the critical distinction for layers_scrape.py tool selection
    if any(pattern in url_lower for pattern in [
        '/mapserver/', '/featureserver/', '/imageserver/', '/geoprocessingserver/',
        'arcgis/rest/services', 'server/rest/services'
    ]):
        return "AGS"
    
    # WMS/WMTS Services (also web services, not file downloads)
    if any(pattern in url_lower for pattern in ['wms', 'wmts', 'getcapabilities']):
        if 'wmts' in url_lower:
            return "WMTS"
        else:
            return "WMS"
    
    # Specific file formats that are clearly identifiable
    if url_lower.endswith('.csv'):
        return "CSV"
    elif url_lower.endswith('.kml') or url_lower.endswith('.kmz'):
        return "KML"
    elif url_lower.endswith('.pdf'):
        return "PDF"
    elif url_lower.endswith('.mdb') or url_lower.endswith('.accdb'):
        return "ACCDB" if url_lower.endswith('.accdb') else "MDB"
    elif url_lower.endswith('.xls') or url_lower.endswith('.xlsx'):
        return "XLS"
    elif url_lower.endswith('.txt'):
        return "TXT"
    elif url_lower.endswith('.tif') or url_lower.endswith('.tiff'):
        return "GeoTIFF"
    
    # Everything else (ZIP files, direct SHP, GDB, etc.) defaults to SHP
    # Most geospatial downloads are shapefiles, often delivered via ZIP
    # This ensures layers_scrape.py uses download_data.py (not ags_extract_data2.py)
    else:
        return "SHP"

def get_format_from_files(work_dir: str) -> str:
    """Determine format from files in a directory, focusing on AGS vs non-AGS distinction.
    
    Args:
        work_dir: Directory path to analyze
        
    Returns:
        Detected format string - AGS if GeoJSON present, otherwise SHP for most cases
    """
    if not work_dir or not os.path.exists(work_dir):
        return ""
    
    try:
        files = os.listdir(work_dir)
        if not files:
            return ""
        
        file_extensions = set()
        for file in files:
            if '.' in file:
                ext = file.split('.')[-1].lower()
                file_extensions.add(ext)
        
        # AGS detection: GeoJSON files indicate AGS extraction
        if 'geojson' in file_extensions:
            return "AGS"
        
        # Prioritize geospatial data formats over documentation
        # Check for shapefiles first (before PDF to avoid false positives from disclaimer docs)
        elif any(ext in file_extensions for ext in ['shp', 'gdb']):
            return "SHP"
        
        # Other specific identifiable formats
        elif 'csv' in file_extensions:
            return "CSV"
        elif 'kml' in file_extensions:
            return "KML"
        elif any(ext in file_extensions for ext in ['mdb', 'accdb']):
            return "ACCDB" if 'accdb' in file_extensions else "MDB"
        elif any(ext in file_extensions for ext in ['xls', 'xlsx']):
            return "XLS"
        elif any(ext in file_extensions for ext in ['tif', 'tiff']):
            return "GeoTIFF"
        elif 'txt' in file_extensions:
            return "TXT"
        elif 'pdf' in file_extensions:
            return "PDF"
        
        # Everything else defaults to SHP (ZIP files, etc.)
        # This covers the majority of geospatial downloads
        else:
            return "SHP"
            
    except Exception:
        return ""

def _get_best_format_detection(url: str, work_dir: str) -> str:
    """Determine the best format with focus on AGS vs non-AGS distinction.
    
    Simplified logic that prioritizes the critical AGS detection needed for
    layers_scrape.py tool selection (ags_extract_data2.py vs download_data.py).
    
    Args:
        url: Source URL to analyze
        work_dir: Directory containing downloaded files
        
    Returns:
        Best format detection - AGS for services, SHP for most downloads, specific formats when clear
    """
    url_format = get_format_from_url(url)
    file_format = get_format_from_files(work_dir)
    
    # Critical rule: AGS URLs are authoritative
    # If URL indicates AGS service, it should use ags_extract_data2.py
    if url_format == "AGS":
        return "AGS"
    
    # If files exist and show AGS (GeoJSON), trust that
    if file_format == "AGS":
        return "AGS"
    
    # For specific file formats, prefer file-based detection
    if file_format in ["CSV", "KML", "PDF", "GeoTIFF", "TXT", "MDB", "ACCDB", "XLS"]:
        return file_format
    
    # Default to URL detection, or SHP if URL detection fails
    return url_format if url_format else "SHP"

# ---------------------------------------------------------------------------
# Minimal Manifest Integration (for preprocessing commands only)
# ---------------------------------------------------------------------------

def extract_manifest_commands(layer: str, entity: str) -> tuple[str, str]:
    """Extract pre-metadata and post-metadata commands from manifest.
    
    Returns:
        tuple[str, str]: (source_comments, processing_comments)
        - source_comments: commands between download and ogrinfo 
        - processing_comments: commands between ogrinfo and update
    """
    try:
        if not MANIFEST_PATH.exists():
            return "", ""
            
        with open(MANIFEST_PATH, 'r') as f:
            manifest_data = json.load(f)
            
        if layer not in manifest_data or 'entities' not in manifest_data[layer]:
            return "", ""
            
        if entity not in manifest_data[layer]['entities']:
            return "", ""
            
        commands = manifest_data[layer]['entities'][entity]
        if not isinstance(commands, list):
            return "", ""
            
        # Find commands in different phases
        pre_metadata = []  # Between download_data.py and ogrinfo (source_comments)
        post_metadata = []  # Between ogrinfo and update_zoning2.py (processing_comments)
        
        phase = "before_download"
        
        for cmd in commands:
            # Handle both list commands and string commands
            if isinstance(cmd, list) and len(cmd) > 0:
                cmd_str = ' '.join(str(x) for x in cmd)
            elif isinstance(cmd, str):
                cmd_str = cmd
            else:
                continue  # Skip invalid commands
                
            # Track phases based on command types
            if any(x in cmd_str for x in ['ags_extract_data2.py', 'download_data.py']):
                phase = "source_comments"  # Commands after download, before ogrinfo
                continue
            elif cmd == "ogrinfo" or 'ogrinfo' in cmd_str:
                phase = "processing_comments"  # Commands after ogrinfo, before update
                continue
            elif any(x in cmd_str for x in ['update_data_catalog', 'psql']) or 'update_' in cmd_str and '.py' in cmd_str:
                # Skip layer-specific update scripts as they're handled automatically by layer_processing()
                phase = "after_update"
                break
                
            # Collect commands based on current phase
            if phase == "source_comments":
                # Extract command for source_comments (pre-metadata processing)
                if isinstance(cmd, list) and cmd[0] == 'python3' and len(cmd) > 1:
                    script_name = Path(cmd[1]).name
                    pre_metadata.append(script_name)
                elif isinstance(cmd, list):
                    # Non-python command - join as shell command
                    pre_metadata.append(' '.join(str(x) for x in cmd))
                else:
                    pre_metadata.append(str(cmd))
            elif phase == "processing_comments":
                # Extract command for processing_comments (post-metadata processing)
                if isinstance(cmd, list) and cmd[0] == 'python3' and len(cmd) > 1:
                    script_name = Path(cmd[1]).name
                    post_metadata.append(script_name)
                elif isinstance(cmd, list):
                    # Non-python command - join as shell command
                    post_metadata.append(' '.join(str(x) for x in cmd))
                else:
                    post_metadata.append(str(cmd))
        
        # Format commands in bracketed format for readability
        source_comments = " ".join(f"[{cmd}]" for cmd in pre_metadata) if pre_metadata else ""
        processing_comments = " ".join(f"[{cmd}]" for cmd in post_metadata) if post_metadata else ""
        
        return source_comments, processing_comments
        
    except Exception:
        return "", ""  # If any error occurs, return empty strings

def extract_preprocessing_commands(layer: str, entity: str) -> str:
    """Extract preprocessing commands from manifest (between download and ogrinfo commands).
    
    DEPRECATED: Use extract_manifest_commands instead.
    Returns empty string if manifest is missing/invalid or no preprocessing found.
    """
    _, processing_comments = extract_manifest_commands(layer, entity)
    return processing_comments

# ---------------------------------------------------------------------------
# Validation Logic for layers_scrape.py Compatibility
# ---------------------------------------------------------------------------

# Validation functions removed - moved to fill mode only

def generate_expected_values(layer: str, county: str, city: str, entity_type: str) -> Dict[str, Any]:
    """Generate expected values for a database record based on layer/county/city."""
    config = LAYER_CONFIGS.get(layer, {})
    
    # Determine entity type if not provided
    if not entity_type:
        entity_type = "city" if city not in {"unincorporated", "unified", "incorporated", "countywide"} else city
    
    # Handle countywide alias
    if entity_type == "countywide":
        entity_type = "unified"
        city_std = "unified"
    else:
        city_std = city
    
    # Generate title using format_name for proper external format
    layer_external = format_name(layer, 'layer', external=True)
    county_external = format_name(county, 'county', external=True)
    city_external = format_name(city_std, 'city', external=True)
    
    if entity_type == "city":
        # Default to "City of" but this will be refined in health check based on original title
        title = f"{layer_external} - City of {city_external}"
    elif entity_type in ["unincorporated", "unified", "incorporated"]:
        title = f"{layer_external} - {county_external} {entity_type.capitalize()}"
    else:
        title = f"{layer_external} - {city_external}"
    
    # Generate table name (internal format)
    layer_internal = format_name(layer, 'layer', external=False)
    city_internal = format_name(city_std, 'city', external=False)
    county_internal = format_name(county, 'county', external=False)
    
    if entity_type == "city":
        table_name = f"{layer_internal}_{city_internal}"
    elif entity_type in ["unincorporated", "unified", "incorporated"]:
        table_name = f"{layer_internal}_{county_internal}_{entity_type}"
    else:
        table_name = f"{layer_internal}_{city_internal}"
    
    # Generate sys_raw_folder (internal format for paths)
    category = config.get('category', '08_Land_Use_and_Zoning')
    sys_raw_folder = f"/srv/datascrub/{category}/{layer_internal}/florida/county/{county_internal}/current/source_data/{city_internal}"
    
    return {
        'title': title,
        'county': format_name(county, 'county', external=True),  # external format for database
        'city': format_name(city_std, 'city', external=True),   # external format for database
        'layer_subgroup': layer_internal,
        'layer_group': config.get('layer_group', 'flu_zoning'),
        'category': category,
        'table_name': table_name,
        'sys_raw_folder': sys_raw_folder,
    }

# ---------------------------------------------------------------------------
# Configuration Class
# ---------------------------------------------------------------------------

@dataclass
class Config:
    layer: str
    include_entities: List[str] | None = None
    exclude_entities: List[str] | None = None
    mode: str = "detect"  # detect | fill | create
    debug: bool = False
    generate_csv: bool = True
    apply_changes: bool = False  # Apply auto-generated changes
    apply_manual: bool = False  # Apply manual field changes  
    manual_file: str = "missing_fields.json"
    fill_all: bool = False  # Include optional conditions in fill mode

# ---------------------------------------------------------------------------
# Main Processing Class
# ---------------------------------------------------------------------------

class LayersPrescrape:
    """Main engine for database preparation and validation."""
    
    def __init__(self, cfg: Config):
        self.cfg = cfg
        
        self.logger = logging.getLogger("LayersPrescrape")
        self.logger.setLevel(logging.DEBUG if cfg.debug else logging.INFO)
        
        # Only add handler if logger doesn't already have one (prevents duplicates)
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            self.logger.addHandler(handler)
        
        print(f"[DEBUG] Initializing LayersPrescrape for layer '{cfg.layer}' in mode '{cfg.mode}'")
        
        # Database connection
        try:
            self.db = DB(PG_CONNECTION)
        except Exception as e:
            print(f"[ERROR] Failed to initialize database connection: {e}")
            sys.exit(1)
        
        # Track missing fields across entities
        self.missing_fields: Dict[str, Dict[str, str]] = defaultdict(dict)
        
        # Track distrib_comments updates for preservation logic
        self.distrib_comments_updates: Dict[str, str] = {}
        
        # URL validation cache for performance
        self.url_validation_cache: Dict[str, tuple[bool, str]] = {}
    
    def run(self):
        """Execute the configured mode."""
        if self.cfg.mode == "detect":
            self._run_detect_mode()
        elif self.cfg.mode == "fill":
            self._run_fill_mode()
        elif self.cfg.mode == "create":
            self._run_create_mode()
        else:
            raise ValueError(f"Unknown mode: {self.cfg.mode}")
        
        # Write missing fields JSON if any issues found (fill mode only)
        if self.missing_fields and self.cfg.mode in {"fill", "create"}:
            self.logger.info(f"Writing missing field report → {self.cfg.manual_file}")
            with open(self.cfg.manual_file, "w", encoding="utf-8") as fh:
                json.dump(self.missing_fields, fh, indent=2)
        
        # Commit or rollback database changes
        if self.db:
            if self.cfg.mode in {"fill", "create"} and (self.cfg.apply_changes or self.cfg.apply_manual):
                self.db.commit()
                self.logger.info("Database changes committed.")
            else:
                self.db.conn.rollback()
                if not (self.cfg.apply_changes or self.cfg.apply_manual) and self.cfg.mode in {"fill", "create"}:
                    self.logger.info("Apply flags not set - no database changes made.")
            self.db.close()
    
    def _run_detect_mode(self):
        """Find all records containing the layer and output their data in CSV format."""
        self.logger.info(f"Running DETECT mode - finding all records for layer '{self.cfg.layer}'.")
        
        # Find all records containing the layer (check both internal and external formats)
        layer_internal = format_name(self.cfg.layer, 'layer', external=False)
        layer_external = format_name(self.cfg.layer, 'layer', external=True)
        
        sql = """
            SELECT * FROM m_gis_data_catalog_main 
            WHERE status IS DISTINCT FROM 'DELETE' 
            AND (lower(title) LIKE %s OR lower(title) LIKE %s)
            ORDER BY title
        """
        
        records = self.db.fetchall(sql, (f'%{layer_internal}%', f'%{layer_external.lower()}%'))
        
        if not records:
            self.logger.warning(f"No records found for layer '{self.cfg.layer}'")
            return
        
        self.logger.info(f"Found {len(records)} total records for layer '{self.cfg.layer}'")
        
        # Filter by include/exclude entities if provided
        if self.cfg.include_entities or self.cfg.exclude_entities:
            filtered_records = []
            
            for record in records:
                entity = self._generate_entity_from_record(record)
                if self._should_include_entity(entity):
                    filtered_records.append(record)
            
            filter_desc = self._get_entity_filter_description()
            records = filtered_records
            self.logger.info(f"Filtered to {len(records)} records matching entity filters {filter_desc}")
            
            if not records:
                self.logger.warning(f"No records found matching entity filters {filter_desc}")
                return
        
        # CSV headers - specific fields requested by user
        headers = [
            "entity", "title", "county", "city", "source_org", "src_url_file", 
            "format", "format_subtype", "download", "resource", "layer_group", 
            "layer_subgroup", "category", "sub_category", "sys_raw_folder", 
            "table_name", "fields_obj_transform", "source_comments", "processing_comments"
        ]
        
        # Group records by entity to detect duplicates
        entity_groups = defaultdict(list)
        field_counts = {field: 0 for field in headers[1:]}  # Skip 'entity' for counting
        total_records = len(records)
        
        for record in records:
            # Generate entity from title
            entity = self._generate_entity_from_record(record)
            
            # Extract field values
            row_values = [entity]
            for field in headers[1:]:
                value = record.get(field)
                if value is not None and str(value).strip() and str(value).strip().upper() not in ('NULL', 'NONE'):
                    field_counts[field] += 1
                    row_values.append(str(value))
                else:
                    row_values.append("")
            
            entity_groups[entity].append(row_values)
        
        # Separate unique records, duplicates, and errors
        csv_rows = [headers]
        duplicate_groups = []
        error_records = []
        unique_entities = 0
        duplicate_entities = 0
        error_entities = 0
        
        for entity in sorted(entity_groups.keys()):
            records_for_entity = entity_groups[entity]
            
            if entity == "ERROR":
                # Handle ERROR entities separately
                error_records = records_for_entity
                error_entities = len(records_for_entity)
            elif len(records_for_entity) == 1:
                # Single record for this entity
                csv_rows.append(records_for_entity[0])
                unique_entities += 1
            else:
                # Multiple records (duplicates)
                duplicate_row = [entity] + ["DUPLICATE"] * (len(headers) - 1)
                csv_rows.append(duplicate_row)
                duplicate_groups.append((entity, records_for_entity))
                duplicate_entities += 1
        
        # Add summary row showing field completion rates and duplicate info
        csv_rows.append([])
        summary_row = ["SUMMARY"]
        for field in headers[1:]:
            count = field_counts[field]
            summary_row.append(f"{count}/{total_records}")
        csv_rows.append(summary_row)
        
        # Add entity count summary (split across multiple rows for readability)
        csv_rows.append([])
        csv_rows.append(["=== ENTITY SUMMARY ==="] + [""] * (len(headers) - 1))
        csv_rows.append([f"UNIQUE ENTITIES: {unique_entities}"] + [""] * (len(headers) - 1))
        csv_rows.append([f"DUPLICATE ENTITIES: {duplicate_entities}"] + [""] * (len(headers) - 1))
        csv_rows.append([f"ERROR RECORDS: {error_entities}"] + [""] * (len(headers) - 1))
        csv_rows.append([f"TOTAL RECORDS: {total_records}"] + [""] * (len(headers) - 1))
        
        # Add ERROR section if any exist
        if error_records:
            csv_rows.append([])
            csv_rows.append(["=== ERROR SECTION ==="] + [""] * (len(headers) - 1))
            csv_rows.append([])
            csv_rows.append(["UNPARSEABLE RECORDS:"] + [""] * (len(headers) - 1))
            csv_rows.append(headers)  # Header row for error records
            
            # Sort error records alphabetically by title
            error_records.sort(key=lambda row: row[1])  # Sort by title column
            
            for record_row in error_records:
                csv_rows.append(record_row)
            
            csv_rows.append([])  # Empty row after error section
        
        # Add duplicates section if any exist
        if duplicate_groups:
            csv_rows.append([])
            csv_rows.append(["=== DUPLICATES SECTION ==="] + [""] * (len(headers) - 1))
            csv_rows.append([])
            
            for entity, duplicate_records in duplicate_groups:
                # Add separator for each duplicate group
                csv_rows.append([f"DUPLICATES FOR: {entity}"] + [""] * (len(headers) - 1))
                csv_rows.append(headers)  # Header row for this duplicate group
                
                # Sort duplicate records alphabetically by title
                duplicate_records.sort(key=lambda row: row[1])  # Sort by title column
                
                for record_row in duplicate_records:
                    csv_rows.append(record_row)
                
                csv_rows.append([])  # Empty row between duplicate groups
        
        # Write CSV report
        if self.cfg.generate_csv:
            csv_path = REPORTS_DIR / f"{self.cfg.layer}_prescrape_detect.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(csv_rows)
            self.logger.info(f"Detection report written → {csv_path}")
        
        # Log summary
        summary_parts = [f"{total_records} records found", f"{unique_entities} unique entities"]
        if duplicate_entities > 0:
            summary_parts.append(f"{duplicate_entities} duplicate entities")
        if error_entities > 0:
            summary_parts.append(f"{error_entities} error records")
        
        self.logger.info(f"Detection complete: {', '.join(summary_parts)}")
    
    def _run_fill_mode(self):
        """Conduct health checks on records and generate corrections."""
        mode_desc = "with optional conditions" if self.cfg.fill_all else "core conditions only"
        self.logger.info(f"Running FILL mode - health checking all records ({mode_desc}).")
        
        # Find all records using same logic as detect mode
        layer_internal = format_name(self.cfg.layer, 'layer', external=False)
        layer_external = format_name(self.cfg.layer, 'layer', external=True)
        
        sql = """
            SELECT * FROM m_gis_data_catalog_main 
            WHERE status IS DISTINCT FROM 'DELETE' 
            AND (lower(title) LIKE %s OR lower(title) LIKE %s)
            ORDER BY title
        """
        
        records = self.db.fetchall(sql, (f'%{layer_internal}%', f'%{layer_external.lower()}%'))
        
        if not records:
            self.logger.warning(f"No records found for layer '{self.cfg.layer}'")
            return
        
        self.logger.info(f"Found {len(records)} total records for layer '{self.cfg.layer}'")
        
        # Filter by include/exclude entities if provided
        if self.cfg.include_entities or self.cfg.exclude_entities:
            filtered_records = []
            
            for record in records:
                entity = self._generate_entity_from_record(record)
                if self._should_include_entity(entity):
                    filtered_records.append(record)
            
            filter_desc = self._get_entity_filter_description()
            records = filtered_records
            self.logger.info(f"Filtered to {len(records)} records matching entity filters {filter_desc}")
            
            if not records:
                self.logger.warning(f"No records found matching entity filters {filter_desc}")
                return
        
        # Group records by entity and filter out duplicates/errors
        entity_groups = defaultdict(list)
        for record in records:
            entity = self._generate_entity_from_record(record)
            entity_groups[entity].append(record)
        
        # Filter out duplicates and errors
        valid_records = []
        skipped_count = 0
        
        for entity, records_for_entity in entity_groups.items():
            if entity == "ERROR":
                self.logger.debug(f"Skipping ERROR entity with {len(records_for_entity)} unparseable records")
                skipped_count += len(records_for_entity)
            elif len(records_for_entity) > 1:
                self.logger.debug(f"Skipping duplicate entity '{entity}' with {len(records_for_entity)} records")
                skipped_count += len(records_for_entity)
            else:
                valid_records.append((entity, records_for_entity[0]))
        
        self.logger.info(f"Processing {len(valid_records)} valid records, skipped {skipped_count} (duplicates/errors)")
        
        # Define CSV headers - core conditions + optional conditions
        core_headers = [
            "entity", "og_title", "new_title", "county", "city", "src_url_file", "format", "download", 
            "resource", "layer_group", "category", "sys_raw_folder", "table_name", 
            "fields_obj_transform", "layer_subgroup", "source_comments", "processing_comments", "distrib_comments"
        ]
        
        optional_headers = []
        if self.cfg.fill_all:
            optional_headers = ["sub_category", "source_org", "format_subtype"]
        
        headers = core_headers + optional_headers
        
        # Pre-validate all URLs in batch for better performance
        self._batch_validate_urls(valid_records)
        
        # Process each valid record
        csv_rows = [headers]
        healthy_counts = {field: 0 for field in headers[1:]}  # Skip 'entity' 
        total_records = len(valid_records)
        
        for entity, record in valid_records:
            # Conduct health checks and generate corrections
            row_values = [entity]
            
            for field in headers[1:]:
                if field == "og_title":
                    # Show original title from database
                    row_values.append(record.get('title') or '')
                    # og_title is always "healthy" since it's just showing original data
                    healthy_counts[field] += 1
                else:
                    correction = self._check_field_health(record, entity, field)
                    row_values.append(correction)
                    
                    # Count as healthy if no correction needed (empty cell)
                    if not correction:
                        healthy_counts[field] += 1
            
            csv_rows.append(row_values)
        
        # Add summary row
        csv_rows.append([])
        summary_row = ["SUMMARY"]
        for field in headers[1:]:
            healthy = healthy_counts[field]
            summary_row.append(f"{healthy}/{total_records}")
        csv_rows.append(summary_row)
        
        # Write CSV report
        if self.cfg.generate_csv:
            csv_path = REPORTS_DIR / f"{self.cfg.layer}_prescrape_fill.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(csv_rows)
            self.logger.info(f"Fill report written → {csv_path}")
        
        # Apply database changes if requested
        if self.cfg.apply_changes or self.cfg.apply_manual:
            self.logger.info("Applying database changes...")
            applied_auto = 0
            applied_manual = 0
            skipped_auto = 0
            skipped_manual = 0
            
            for entity, record in valid_records:
                updates = {}
                
                for field in headers[1:]:
                    if field == "og_title":  # Skip the original title display field
                        continue
                        
                    correction = self._check_field_health(record, entity, field)
                    if correction and not correction.startswith("***"):  # Has correction and not a manual marker
                        is_manual = self._is_manual_field(field)
                        
                        if is_manual and self.cfg.apply_manual:
                            updates[field] = correction
                            applied_manual += 1
                        elif not is_manual and self.cfg.apply_changes:
                            updates[field] = correction  
                            applied_auto += 1
                        elif is_manual and not self.cfg.apply_manual:
                            skipped_manual += 1
                        elif not is_manual and not self.cfg.apply_changes:
                            skipped_auto += 1
                
                # Apply updates to this record
                if updates:
                    self._update_record(record, updates)
            
            # Log what was applied
            if self.cfg.apply_changes:
                self.logger.info(f"Applied {applied_auto} auto-generated changes")
                if skipped_manual > 0:
                    self.logger.info(f"Skipped {skipped_manual} manual changes (use --apply-manual to apply)")
            if self.cfg.apply_manual:
                self.logger.info(f"Applied {applied_manual} manual field changes")
                if skipped_auto > 0:
                    self.logger.info(f"Skipped {skipped_auto} auto changes (use --apply to apply)")
        
        # Log summary
        total_issues = sum(total_records - healthy_counts[field] for field in headers[1:])
        self.logger.info(f"Fill complete: {len(valid_records)} records checked, {total_issues} total issues found")
    
    def _run_create_mode(self):
        """Create new records based on layer/county/city + manual info."""
        self.logger.info("Running CREATE mode - creating new database records.")
        
        if not self.cfg.include_entities:
            self.logger.error("CREATE mode requires --include entities to be specified")
            return
        
        # Load manual data if available
        manual_data = {}
        if Path(self.cfg.manual_file).exists():
            with open(self.cfg.manual_file, 'r') as f:
                manual_data = json.load(f)
        
        created_records = []
        
        for entity in self.cfg.include_entities:
            try:
                county, city = split_entity(entity)
            except ValueError as e:
                self.logger.error(f"Invalid entity format '{entity}': {e}")
                continue
            
            # Check if record already exists
            existing = self._find_record_by_entity(entity)
            if existing:
                self.logger.warning(f"Record for {entity} already exists - skipping")
                continue
            
            # Generate base record
            entity_type = "city" if city not in {"unincorporated", "unified", "incorporated", "countywide"} else city
            expected = generate_expected_values(self.cfg.layer, county, city, entity_type)
            
            # Apply manual overrides
            if entity in manual_data:
                expected.update(manual_data[entity])
            
            # Check for required manual fields
            required_manual = []
            if not expected.get('format') or expected['format'] == "MANUAL_REQUIRED":
                required_manual.append('format')
            
            fmt = (expected.get('format') or '').lower()
            if fmt in ['ags', 'arcgis', 'esri', 'ags_extract']:
                if not expected.get('table_name') or expected['table_name'] == "MANUAL_REQUIRED":
                    required_manual.append('table_name')
            else:
                if not (expected.get('resource') or expected.get('src_url_file')):
                    required_manual.append('resource')
            
            if required_manual:
                self.logger.error(f"Cannot create {entity} - missing required manual fields: {required_manual}")
                self.missing_fields[entity] = {field: "MANUAL_REQUIRED" for field in required_manual}
                continue
            
            # Create the record
            if self.cfg.apply_changes or self.cfg.apply_manual:
                self._create_record(expected)
                self.logger.info(f"Created record for {entity}")
            else:
                self.logger.info(f"Would create record for {entity}")
            
            created_records.append({
                'entity': entity,
                'record': expected
            })
        
        # Generate CSV report
        if self.cfg.generate_csv and created_records:
            headers = ["entity", "field", "value", "status"]
            csv_rows = [headers]
            
            for creation in created_records:
                entity = creation['entity']
                record = creation['record']
                status = "CREATED" if (self.cfg.apply_changes or self.cfg.apply_manual) else "PENDING"
                
                for field, value in record.items():
                    csv_rows.append([entity, field, str(value), status])
            
            csv_path = REPORTS_DIR / f"{self.cfg.layer}_prescrape_create.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(csv_rows)
            self.logger.info(f"Creation report written → {csv_path}")
        
        self.logger.info(f"Create complete: {len(created_records)} records processed")
    
    # Helper methods
    
    def _batch_validate_urls(self, valid_records: list[tuple[str, dict]]) -> None:
        """Pre-validate all URLs in batch for better performance."""
        # Collect all unique URLs from records
        urls_to_validate = set()
        for entity, record in valid_records:
            url = record.get('src_url_file')
            if url and url.strip() and url not in self.url_validation_cache:
                urls_to_validate.add(url.strip())
        
        if urls_to_validate:
            self.logger.info(f"Batch validating {len(urls_to_validate)} URLs...")
            # Validate all URLs concurrently
            # Use conservative thread count (max 4 on smaller systems, 8 on larger)
            import os
            cpu_count = os.cpu_count() or 4
            max_workers = min(8, max(2, cpu_count // 2))  # Use half of available cores, max 8
            results = validate_url_batch(list(urls_to_validate), max_workers=max_workers)
            # Cache the results
            self.url_validation_cache.update(results)
            self.logger.info(f"URL validation complete")
    
    def _should_include_entity(self, entity: str) -> bool:
        """Check if entity should be included based on include/exclude filters."""
        entity_lower = entity.lower()
        
        # If include filters are specified, entity must match at least one
        if self.cfg.include_entities:
            include_match = any(pattern.lower() in entity_lower for pattern in self.cfg.include_entities)
            if not include_match:
                return False
        
        # If exclude filters are specified, entity must not match any
        if self.cfg.exclude_entities:
            exclude_match = any(pattern.lower() in entity_lower for pattern in self.cfg.exclude_entities)
            if exclude_match:
                return False
        
        return True
    
    def _get_entity_filter_description(self) -> str:
        """Get description of current entity filters for logging."""
        parts = []
        if self.cfg.include_entities:
            parts.append(f"include: {', '.join(self.cfg.include_entities)}")
        if self.cfg.exclude_entities:
            parts.append(f"exclude: {', '.join(self.cfg.exclude_entities)}")
        return f"({'; '.join(parts)})" if parts else ""

    def _is_manual_field(self, field: str) -> bool:
        """Check if a field requires manual input."""
        manual_fields = {
            'src_url_file',      # URL validation can mark as MANUAL_REQUIRED or URL_DEPRECATED
            'fields_obj_transform',  # Always manual
            'source_org',        # Always manual (optional)
        }
        return field in manual_fields
    
    def _check_field_health(self, record: Dict[str, Any], entity: str, field: str) -> str:
        """Check field health and return correction value or empty string if healthy.
        
        Returns:
            - Empty string if field is healthy
            - Correction value if field needs fixing (auto-correctable)
            - "***MISSING***" if field requires manual input
        """
        current_value = record.get(field) or ''
        
        try:
            # Parse entity components
            county, city = split_entity(entity)
            entity_type = "city" if city not in {"unincorporated", "unified", "incorporated", "countywide"} else city
            
            # Handle countywide alias
            if entity_type == "countywide":
                entity_type = "unified"
                city_std = "unified"
            else:
                city_std = city
            
            # Generate expected values
            layer_internal = format_name(self.cfg.layer, 'layer', external=False)
            layer_external = format_name(self.cfg.layer, 'layer', external=True)
            county_external = format_name(county, 'county', external=True)
            county_internal = format_name(county, 'county', external=False)
            city_external = format_name(city_std, 'city', external=True)
            city_internal = format_name(city_std, 'city', external=False)
            
        except ValueError:
            # If entity parsing fails, can't validate most fields
            if field in ["src_url_file", "fields_obj_transform", "source_org"]:
                return "***MISSING***" if not current_value else ""
            return ""
        
        # Field-specific health checks
        if field == "new_title":
            # Check title format: "<layer> - City/Town/Village of <city>" or "<layer> - <county> Unincorporated/Unified/Incorporated"
            # Get the actual title value from the database for comparison
            actual_title = record.get('title') or ''
            
            if entity_type == "city":
                # Determine the correct prefix (City/Town/Village) from the original title
                title_prefix = "City"
                if actual_title:
                    if "Town of" in actual_title:
                        title_prefix = "Town"
                    elif "Village of" in actual_title:
                        title_prefix = "Village"
                
                expected = f"{layer_external} - {title_prefix} of {city_external}"
            elif entity_type in ["unincorporated", "unified", "incorporated"]:
                expected = f"{layer_external} - {county_external} {entity_type.capitalize()}"
            else:
                expected = f"{layer_external} - {city_external}"
            
            return expected if actual_title != expected else ""
        
        elif field == "county":
            # Check county field contains proper county in external format
            expected = county_external
            return expected if current_value != expected else ""
        
        elif field == "city":
            # Check city field contains proper city in external format
            expected = city_external
            return expected if current_value != expected else ""
        
        elif field == "src_url_file":
            # Check URL exists and is valid using cached results
            if not current_value:
                self.missing_fields[entity]["src_url_file"] = "MANUAL_REQUIRED"
                return "***MISSING***"
            
            # Use cached validation result if available, otherwise validate on-demand
            if current_value in self.url_validation_cache:
                is_valid, status_reason = self.url_validation_cache[current_value]
            else:
                is_valid, status_reason = validate_url(current_value)
                self.url_validation_cache[current_value] = (is_valid, status_reason)
            
            if not is_valid:
                if status_reason == "MISSING":
                    self.missing_fields[entity]["src_url_file"] = "MANUAL_REQUIRED"
                    return "***MISSING***"
                elif status_reason == "DEPRECATED":
                    self.missing_fields[entity]["src_url_file"] = "URL_DEPRECATED"
                    return "***DEPRECATED***"
            return ""
        
        elif field == "format":
            # Check format is correct using URL analysis and file analysis
            if not current_value:
                expected = get_format_from_url(record.get('src_url_file') or '')
                return expected if expected else "***MISSING***"
            
            # Validate current format makes sense using intelligent format detection
            url = record.get('src_url_file') or ''
            expected = _get_best_format_detection(url, record.get('sys_raw_folder') or '')
            if expected and str(current_value).upper() != expected.upper():
                return expected
            return ""
        
        elif field == "download":
            # Check download is set to "AUTO"
            expected = "AUTO"
            return expected if current_value != expected else ""
        
        elif field == "resource":
            # For non-AGS, check resource matches pattern "/data/<layer>/<county>/<city>"
            fmt = record.get('format') or ''
            if str(fmt).upper() != 'AGS':
                expected = f"/data/{layer_internal}/{county_internal}/{city_internal}"
                return expected if current_value != expected else ""
            return ""  # AGS doesn't need resource field
        
        elif field == "layer_group":
            # Check layer_group is correct using mapping
            config = LAYER_CONFIGS.get(self.cfg.layer, {})
            expected = config.get('layer_group', '')
            return expected if current_value != expected else ""
        
        elif field == "category":
            # Check category is correct using mapping
            config = LAYER_CONFIGS.get(self.cfg.layer, {})
            expected = config.get('category', '')
            return expected if current_value != expected else ""
        
        elif field == "sys_raw_folder":
            # Check sys_raw_folder matches pattern and create directory
            config = LAYER_CONFIGS.get(self.cfg.layer, {})
            category = config.get('category', '08_Land_Use_and_Zoning')
            expected = f"/srv/datascrub/{category}/{layer_internal}/florida/county/{county_internal}/current/source_data/{city_internal}"
            
            # Create directory if it doesn't exist
            try:
                Path(expected).mkdir(parents=True, exist_ok=True)
            except Exception as e:
                self.logger.debug(f"Could not create directory {expected}: {e}")
            
            return expected if current_value != expected else ""
        
        elif field == "table_name":
            # Check table_name matches pattern
            if entity_type == "city":
                expected = f"{layer_internal}_{city_internal}"
            elif entity_type in ["unincorporated", "unified", "incorporated"]:
                expected = f"{layer_internal}_{county_internal}_{entity_type}"
            else:
                expected = f"{layer_internal}_{city_internal}"
            
            return expected if current_value != expected else ""
        
        elif field == "fields_obj_transform":
            # Check fields_obj_transform exists and matches pattern (MANUAL)
            if not current_value or not self._is_valid_transform_pattern(current_value):
                self.missing_fields[entity]["fields_obj_transform"] = "MANUAL_REQUIRED"
                return "***MISSING***"
            return ""
        
        elif field == "layer_subgroup":
            # Check layer_subgroup has layer in internal format
            expected = layer_internal
            return expected if current_value != expected else ""
        
        elif field == "source_comments":
            # Always overwrite source_comments from manifest, preserving existing values to distrib_comments
            if self.cfg.layer.lower() in ['zoning', 'flu']:
                expected_source, _ = extract_manifest_commands(self.cfg.layer, entity)
                # Preserve existing value to distrib_comments if it exists
                if current_value and current_value.strip():
                    self._preserve_to_distrib_comments(record, entity, 'SOURCE COMMENTS', current_value)
                # Always return manifest value
                return expected_source
            return ""
        
        elif field == "processing_comments":
            # Always overwrite processing_comments from manifest, preserving existing values to distrib_comments
            if self.cfg.layer.lower() in ['zoning', 'flu']:
                _, expected_processing = extract_manifest_commands(self.cfg.layer, entity)
                # Preserve existing value to distrib_comments if it exists
                if current_value and current_value.strip():
                    self._preserve_to_distrib_comments(record, entity, 'PROCESSING COMMENTS', current_value)
                # Always return manifest value
                return expected_processing
            return ""
        
        elif field == "distrib_comments":
            # Show the updated distrib_comments with preserved values
            return self._get_updated_distrib_comments(record, entity)
        
        # Optional conditions (only checked with --fill-all)
        elif field == "sub_category":
            # TODO: Implement sub_category pattern checking
            return ""  # Placeholder
        
        elif field == "source_org":
            # Check source_org has value (MANUAL)
            if not current_value:
                self.missing_fields[entity]["source_org"] = "MANUAL_REQUIRED"
                return "***MISSING***"
            return ""
        
        elif field == "format_subtype":
            # TODO: Implement format_subtype pattern checking
            return ""  # Placeholder
        
        else:
            # Unknown field
            return ""
    
    def _is_valid_transform_pattern(self, value: str) -> bool:
        """Check if fields_obj_transform matches expected pattern: '<key>: <value>'"""
        if not value or not value.strip():
            return False
        
        # Simple pattern check: should contain at least one colon
        return ':' in value
    
    def _generate_entity_from_record(self, record: Dict[str, Any]) -> str:
        """Generate entity name from database record title and fields.
        
        For flu/zoning: returns county_city format
        For other layers: returns just county name
        If parsing fails: returns "ERROR"
        """
        title = record.get('title', '')
        
        # Try to parse the title first
        layer_parsed, county_parsed, city_parsed, entity_type = parse_title_to_entity(title)
        
        if layer_parsed and county_parsed and city_parsed:
            try:
                # Successfully parsed both county and city from title
                entity = entity_from_title_parse(layer_parsed, county_parsed, city_parsed, entity_type)
                return entity
            except (ValueError, TypeError):
                pass
        
        # Partial title parsing success - try hybrid approach
        if layer_parsed and city_parsed and entity_type:
            # We got city from title but not county - use county from database
            county_db = record.get('county', '')
            if county_db:
                try:
                    county_internal = format_name(county_db, 'county', external=False)
                    entity = entity_from_title_parse(layer_parsed, county_internal, city_parsed, entity_type)
                    return entity
                except (ValueError, TypeError):
                    pass
        
        # Title parsing failed or incomplete, try fallback with county/city fields
        county_db = record.get('county', '')
        city_db = record.get('city', '')
        
        if county_db:
            county_internal = format_name(county_db, 'county', external=False)
            
            if city_db and self.cfg.layer.lower() in ['flu', 'zoning']:
                city_internal = format_name(city_db, 'city', external=False)
                if city_internal in {"unincorporated", "unified", "incorporated", "countywide"}:
                    entity = f"{county_internal}_{city_internal}"
                else:
                    entity = f"{county_internal}_{city_internal}"
            else:
                # No city or not flu/zoning layer
                if self.cfg.layer.lower() in ['flu', 'zoning']:
                    entity = f"{county_internal}_unincorporated"
                else:
                    # Most other layers are county-level only
                    entity = county_internal
            
            return entity
        
        # Complete failure
        return "ERROR"
    
    def _discover_entities_from_db(self) -> List[str]:
        """Discover entities for the layer by examining database titles."""
        sql = """
            SELECT title, county, city 
            FROM m_gis_data_catalog_main 
            WHERE status IS DISTINCT FROM 'DELETE'
        """
        rows = self.db.fetchall(sql) or []
        
        entities = set()
        
        for row in rows:
            title = row.get('title', '')
            
            # Parse title to get layer/county/city
            layer_parsed, county_parsed, city_parsed, entity_type = parse_title_to_entity(title)
            
            if layer_parsed == self.cfg.layer:
                try:
                    entity = entity_from_title_parse(self.cfg.layer, county_parsed, city_parsed, entity_type)
                    entities.add(entity)
                except (ValueError, TypeError):
                    # If title parsing fails, try fallback with DB fields
                    county_db = row.get('county', '')
                    city_db = row.get('city', '')
                    
                    if county_db and city_db:
                        county_internal = format_name(county_db, 'county', external=False)
                        city_internal = format_name(city_db, 'city', external=False)
                        if city_internal in {"unincorporated", "unified", "incorporated", "countywide"}:
                            entity = f"{county_internal}_{city_internal}"
                        else:
                            entity = f"{county_internal}_{city_internal}"
                        entities.add(entity)
        
        return sorted(entities)
    
    def _find_record_by_entity(self, entity: str) -> Optional[Dict[str, Any]]:
        """Find database record by matching entity to expected title."""
        try:
            county, city = split_entity(entity)
        except ValueError:
            return None
        
        # Generate expected title patterns for this entity
        entity_type = "city" if city not in {"unincorporated", "unified", "incorporated", "countywide"} else city
        expected = generate_expected_values(self.cfg.layer, county, city, entity_type)
        expected_title = expected['title']
        
        # Search for record with matching title
        sql = """
            SELECT * FROM m_gis_data_catalog_main 
            WHERE status IS DISTINCT FROM 'DELETE' 
            AND lower(title) = %s
        """
        
        record = self.db.fetchone(sql, (expected_title.lower(),))
        return record
    
    def _update_record(self, record: Dict[str, Any], updates: Dict[str, Any]):
        """Update database record with new values."""
        if not updates:
            return
        
        set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
        params = tuple(updates.values())
        
        # Use primary key if available
        gid = record.get("gid")
        record_id = record.get("id")
        
        if gid is not None:
            sql = f"UPDATE m_gis_data_catalog_main SET {set_clause} WHERE gid = %s"
            params += (gid,)
        elif record_id is not None:
            sql = f"UPDATE m_gis_data_catalog_main SET {set_clause} WHERE id = %s"
            params += (record_id,)
        else:
            # Fallback to title-based update
            sql = f"UPDATE m_gis_data_catalog_main SET {set_clause} WHERE lower(title) = %s"
            params += (str(record.get("title", "")).lower(),)
        
        self.db.execute(sql, params)
    
    def _preserve_to_distrib_comments(self, record: Dict[str, Any], entity: str, comment_type: str, value: str):
        """Preserve existing source/processing comments to distrib_comments field."""
        # Start with existing distrib_comments from database (only once per entity)
        if entity not in self.distrib_comments_updates:
            # First time for this entity - initialize with existing distrib_comments
            self.distrib_comments_updates[entity] = record.get('distrib_comments', '') or ''
        
        current_distrib = self.distrib_comments_updates[entity]
        
        # Add the new preserved comment
        if current_distrib and current_distrib.strip():
            # Existing content - add newline separator
            new_distrib = f"{current_distrib}\n\n{comment_type}:\n{value}"
        else:
            # No existing content - start fresh
            new_distrib = f"{comment_type}:\n{value}"
        
        # Store the updated distrib_comments for this entity
        self.distrib_comments_updates[entity] = new_distrib
    
    def _get_updated_distrib_comments(self, record: Dict[str, Any], entity: str) -> str:
        """Get the updated distrib_comments with any preserved values."""
        # Check if we have preserved comments for this entity
        if entity in self.distrib_comments_updates:
            return self.distrib_comments_updates[entity]
        
        # No preserved comments - return empty (no changes needed)
        return ""
    
    def _create_record(self, record_data: Dict[str, Any]):
        """Create new database record."""
        # Add default fields
        record_data.update({
            'publish_date': get_today_str(),
            'download': 'AUTO',
            'status': 'ACTIVE'
        })
        
        # Build INSERT statement
        fields = list(record_data.keys())
        placeholders = ', '.join(['%s'] * len(fields))
        field_names = ', '.join(fields)
        
        sql = f"INSERT INTO m_gis_data_catalog_main ({field_names}) VALUES ({placeholders})"
        params = tuple(record_data.values())
        
        self.db.execute(sql, params)

# ---------------------------------------------------------------------------
# CLI Interface
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare database for layers_scrape.py by detecting and fixing malformed records"
    )
    
    parser.add_argument("layer", help="Layer name (zoning, flu, flood_zones, parcel_geo, streets, addr_pnts, subdiv, bldg_ftpr, fdot_tc, sunbiz, all)")
    
    # Entity filtering options
    parser.add_argument("--include", nargs="*", metavar="ENTITY", 
                       help="Include only entities matching these patterns (required for CREATE mode)")
    parser.add_argument("--exclude", nargs="*", metavar="ENTITY",
                       help="Exclude entities matching these patterns")
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--detect", action="store_true", 
                           help="Detect malformed records and missing fields (default)")
    mode_group.add_argument("--fill", action="store_true", 
                           help="Fill missing fields from manual JSON file")
    mode_group.add_argument("--create", action="store_true", 
                           help="Create new records for specified entities")
    
    # Options
    parser.add_argument("--apply", action="store_true", 
                       help="Apply auto-generated changes to database (for FILL/CREATE modes)")
    parser.add_argument("--apply-manual", action="store_true", 
                       help="Apply manual field changes to database (for FILL/CREATE modes)")
    parser.add_argument("--manual-file", default="missing_fields.json",
                       help="Path to manual fields JSON file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--fill-all", action="store_true",
                       help="Include optional conditions in FILL mode")
    parser.add_argument("--no-csv", dest="generate_csv", action="store_false",
                       help="Skip CSV report generation")
    
    return parser

def main():
    parser = build_arg_parser()
    args = parser.parse_args()
    
    # Determine mode
    if args.fill:
        mode = "fill"
    elif args.create:
        mode = "create"
    else:
        mode = "detect"  # default
    
    # Validate CREATE mode requirements
    if mode == "create" and not args.include:
        print("[ERROR] CREATE mode requires --include entities to be specified")
        sys.exit(1)
    
    # Validate layer argument
    layer_input = args.layer.lower()
    if layer_input == "all":
        # Process all configured layers
        valid_layers = list(LAYER_CONFIGS.keys())
        print(f"[INFO] Processing all layers: {', '.join(valid_layers)}")
        
        for layer in valid_layers:
            print(f"\n[INFO] ==================== Processing layer: {layer.upper()} ====================")
            
            # Create config for this layer
            cfg = Config(
                layer=layer,
                include_entities=[e.lower() for e in args.include] if args.include else None,
                exclude_entities=[e.lower() for e in args.exclude] if args.exclude else None,
                mode=mode,
                debug=args.debug,
                generate_csv=args.generate_csv,
                apply_changes=args.apply,
                apply_manual=args.apply_manual,
                manual_file=args.manual_file,
                fill_all=args.fill_all
            )
            
            # Run the processor for this layer
            processor = LayersPrescrape(cfg)
            processor.run()
        
        print(f"\n[INFO] ==================== Completed processing all {len(valid_layers)} layers ====================")
        
    else:
        # Single layer - validate it exists
        if layer_input not in LAYER_CONFIGS:
            valid_layers = ', '.join(sorted(LAYER_CONFIGS.keys()))
            print(f"[ERROR] Invalid layer '{layer_input}'. Valid layers are: {valid_layers}, all")
            sys.exit(1)
        
        # Create config for single layer
        cfg = Config(
            layer=layer_input,
            include_entities=[e.lower() for e in args.include] if args.include else None,
            exclude_entities=[e.lower() for e in args.exclude] if args.exclude else None,
            mode=mode,
            debug=args.debug,
            generate_csv=args.generate_csv,
            apply_changes=args.apply,
            apply_manual=args.apply_manual,
            manual_file=args.manual_file,
            fill_all=args.fill_all
        )
        
        # Run the processor
        processor = LayersPrescrape(cfg)
        processor.run()

if __name__ == "__main__":
    main()