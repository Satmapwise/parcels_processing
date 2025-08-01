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

# Import shared utilities and constants
from layers_helpers import (
    PG_CONNECTION, VALID_STATES, FL_COUNTIES, LAYERS, LAYER_CONFIGS,
    format_name, parse_entity_pattern, safe_catalog_val, validate_state_abbreviation,
    resolve_layer_name, resolve_layer_directory
)

# ---------------------------------------------------------------------------
# Configuration and Constants
# ---------------------------------------------------------------------------

# Database connection now imported from layers_helpers.py

# Output directories
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

# File paths
MISSING_FIELDS_JSON = Path("missing_fields.json")
MANIFEST_PATH = Path("test/layer_manifest.json")

# Constants now imported from layers_helpers.py

# Note: Layer configurations are now centralized in LAYER_CONFIGS above
# No need for separate LAYER_GROUP_MAP and CATEGORY_MAP dictionaries

# ---------------------------------------------------------------------------
# Utility Functions (preserved from layer_standardize_database.py)
# ---------------------------------------------------------------------------

# safe_catalog_val now imported from layers_helpers.py

def get_today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

def split_entity(entity: str) -> Tuple[str, str, str]:
    """Split entity into (state, county, city).
    
    Handles both old 2-part (county_city) and new 3-part (state_county_city) formats.
    For multi-word counties like 'fl_miami_dade_unincorporated' or 'fl_st_lucie_port_st_lucie'.
    
    Strategy:
    1. Check if first token is a valid state - if so, use 3-part format
    2. If not, assume old 2-part format and default state to 'fl'
    3. For county detection, use known suffixes and FL_COUNTIES lookup
    """
    tokens = entity.split("_")
    if len(tokens) < 2:
        raise ValueError(f"Invalid entity format: {entity}")

    # Check if first token is a valid state
    first_token = tokens[0].lower()
    if first_token in VALID_STATES:
        # New 3-part format: state_county_city
        if len(tokens) < 3:
            raise ValueError(f"Invalid 3-part entity format: {entity}")
        
        state = first_token
        county_city_tokens = tokens[1:]
    else:
        # Old 2-part format: county_city (infer state from county)
        county_city_tokens = tokens

    # Parse county_city portion
    suffixes = {"unincorporated", "incorporated", "unified", "countywide"}
    if county_city_tokens[-1] in suffixes:
        county = "_".join(county_city_tokens[:-1])
        city = county_city_tokens[-1]
    else:
        # Try to recognize multi-word counties by longest-prefix match
        county = None
        city = None
        for i in range(len(county_city_tokens), 1, -1):  # from longest possible down to 2 tokens
            candidate_county = "_".join(county_city_tokens[:i])
            if candidate_county in FL_COUNTIES:
                county = candidate_county
                city = "_".join(county_city_tokens[i:])
                if not city:  # edge case – entity only county
                    raise ValueError(f"Could not determine city part in entity: {entity}")
                break
        
        # Fallback to simple split if no FL county match
        if county is None:
            if len(county_city_tokens) < 2:
                raise ValueError(f"Could not parse county_city from entity: {entity}")
            county = county_city_tokens[0]
            city = "_".join(county_city_tokens[1:])

    # Infer state from county if not already determined
    if 'state' not in locals():
        if county in FL_COUNTIES:
            state = 'fl'
        else:
            raise ValueError(f"County '{county}' not found in FL_COUNTIES. Cannot determine state for entity: {entity}")
    
    return state, county, city

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
      "Traffic Counts FDOT"                 -> ("fdot_tc", None, None, "state")
      "SunBiz"                              -> ("sunbiz", None, None, "state")
      "Flood Zones"                         -> ("flood_zones", None, None, "national")
    
    Returns lowercase values; if parsing fails returns (None, None, None, None).
    """
    # Handle special cases for layers without standard "Layer - Location" format
    title_lower = title.lower().strip()
    
    # Special state/national level layers
    if "traffic counts fdot" in title_lower or title_lower == "traffic counts fdot":
        return ("fdot_tc", None, None, "state")
    
    if "sunbiz" in title_lower:
        return ("sunbiz", None, None, "state") 
        
    if "fema flood zones" in title_lower or "flood zones" in title_lower:
        return ("flood_zones", None, None, "national")
    
    # Standard format: "Layer - Location"
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

def entity_from_title_parse(layer: str, county_from_title: str, city_from_title: str, entity_type: str, state: str = 'fl') -> str:
    """Convert parsed title components to entity name format."""
    # Special handling for state-level and national-level layers
    if entity_type == "state":
        # State-level layers (fdot_tc, sunbiz) - simple format: just state
        return state
    elif entity_type == "national":
        # National-level layers (flood_zones) - no state suffix
        return ""
    
    # Standard county/city-level layers (layer_state_county_city format)
    if county_from_title and city_from_title:
        # Normalize county and city names to match entity format (internal format)
        county_internal = format_name(county_from_title, 'county', external=False)
        if entity_type in {"unincorporated", "unified", "incorporated", "countywide"}:
            entity = f"{state}_{county_internal}_{entity_type}"
        else:
            city_internal = format_name(city_from_title, 'city', external=False)
            entity = f"{state}_{county_internal}_{city_internal}"
    elif county_from_title and not city_from_title:
        # County-only title (e.g., "Zoning - Walton County") -> treat as unincorporated
        county_internal = format_name(county_from_title, 'county', external=False)
        entity = f"{state}_{county_internal}_unincorporated"
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
    
    Args:
        layer: Layer name (e.g., 'zoning', 'flu')
        entity: Entity in new format (layer_state_county_city) or old format (county_city)
    
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
        
        # Convert new entity format to old format for manifest lookup
        manifest_entity = entity
        if entity.startswith(f"{layer}_"):
            # New format: layer_state_county_city -> convert to county_city
            parts = entity.split("_")
            if len(parts) >= 4:  # layer_state_county_city
                # Extract state_county_city part
                state_county_city = "_".join(parts[1:])
                try:
                    state, county, city = split_entity(state_county_city)
                    # For backwards compatibility, manifest uses county_city format
                    manifest_entity = f"{county}_{city}"
                except ValueError:
                    # If parsing fails, try as-is
                    pass
            elif len(parts) >= 3:  # Assume layer_county_city (old 3-part)
                # Extract county_city part
                manifest_entity = "_".join(parts[1:])
        
        if manifest_entity not in manifest_data[layer]['entities']:
            return "", ""
            
        commands = manifest_data[layer]['entities'][manifest_entity]
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

def generate_expected_values(layer: str, state: str, county: str, city: str, entity_type: str) -> Dict[str, Any]:
    """Generate expected values for a database record based on layer/state/county/city."""
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
    state_abbrev = validate_state_abbreviation(state) or 'FL'
    
    if entity_type == "city":
        # City-level: "Future Land Use - City of Gainesville FL"
        title = f"{layer_external} - City of {city_external} {state_abbrev}"
    elif entity_type in ["unincorporated", "unified", "incorporated"]:
        # County-level with special suffixes: "Zoning - Broward County Unincorporated FL"
        title = f"{layer_external} - {county_external} County {entity_type.capitalize()} {state_abbrev}"
    else:
        # Standard county-level: "Streets - Broward County FL"
        title = f"{layer_external} - {county_external} County {state_abbrev}"
    
    # Generate table name (internal format)
    layer_internal = format_name(layer, 'layer', external=False)
    city_internal = format_name(city_std, 'city', external=False)
    county_internal = format_name(county, 'county', external=False)
    state_internal = state.lower()
    
    if entity_type == "city":
        table_name = f"{layer_internal}_{city_internal}"
    elif entity_type in ["unincorporated", "unified", "incorporated"]:
        table_name = f"{layer_internal}_{county_internal}_{entity_type}"
    else:
        table_name = f"{layer_internal}_{city_internal}"
    
    # Generate sys_raw_folder using layer directory pattern
    sys_raw_folder = resolve_layer_directory(layer, state_internal, county_internal, city_internal)
    
    return {
        'title': title,
        'state': validate_state_abbreviation(state) or 'FL',  # external format for database
        'county': format_name(county, 'county', external=True),  # external format for database
        'city': format_name(city_std, 'city', external=True),   # external format for database
        'layer_subgroup': layer_internal,
        'layer_group': config.get('layer_group', 'flu_zoning'),
        'category': config.get('category', ''),
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
        elif self.cfg.mode == "detect_and_fill":
            self._run_detect_and_fill_mode()
        else:
            raise ValueError(f"Unknown mode: {self.cfg.mode}")
        
        # Write missing fields JSON if any issues found (fill modes only)
        if self.missing_fields and self.cfg.mode in {"fill", "create", "detect_and_fill"}:
            self.logger.info(f"Writing missing field report → {self.cfg.manual_file}")
            with open(self.cfg.manual_file, "w", encoding="utf-8") as fh:
                json.dump(self.missing_fields, fh, indent=2)
        
        # Commit or rollback database changes
        if self.db:
            if self.cfg.mode in {"fill", "create", "detect_and_fill"} and (self.cfg.apply_changes or self.cfg.apply_manual):
                self.db.commit()
                self.logger.info("Database changes committed.")
            else:
                self.db.conn.rollback()
                if not (self.cfg.apply_changes or self.cfg.apply_manual) and self.cfg.mode in {"fill", "create", "detect_and_fill"}:
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
            "entity", "title", "state", "county", "city", "source_org", "data_date", "publish_date", "src_url_file", 
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
            "entity", "og_title", "new_title", "state", "county", "city", "src_url_file", "format", "download", 
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
                state, county, city = split_entity(entity)
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
            expected = generate_expected_values(self.cfg.layer, state, county, city, entity_type)
            
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
    
    def _run_detect_and_fill_mode(self):
        """Run detect mode first, then fill mode on the same entities."""
        self.logger.info("Running DETECT+FILL mode - detecting records first, then filling on same entities")
        
        # Step 1: Run detect mode to identify entities and generate detect CSV
        self.logger.info("Step 1: Running DETECT phase...")
        self._run_detect_mode()
        
        # Step 2: Run fill mode on the same entities
        self.logger.info("Step 2: Running FILL phase on same entities...")
        self._run_fill_mode()
        
        self.logger.info("DETECT+FILL mode complete")
    
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
        
        # Initialize all variables with default values to prevent UnboundLocalError
        state = None
        county = None
        city = None
        entity_type = "unknown"
        layer_internal = format_name(self.cfg.layer, 'layer', external=False)
        layer_external = format_name(self.cfg.layer, 'layer', external=True)
        county_external = ""
        county_internal = ""
        city_external = ""
        city_internal = ""
        city_std = None
        
        # Special handling for state-level and national-level layers
        if self.cfg.layer in ['fdot_tc', 'sunbiz']:
            # State-level layers: simple format like "fdot_tc_fl"
            state = 'fl'
            county = None
            city = None
            entity_type = 'state'
        elif self.cfg.layer == 'flood_zones':
            # National-level layers: simple format like "flood_zones"
            state = None
            county = None  
            city = None
            entity_type = 'national'
        else:
            # Standard county/city-level layers: parse entity components using robust logic
            try:
                parsed_layer, state, county, city = parse_entity_pattern(entity)
                
                # Determine entity type based on what was parsed
                if city:
                    entity_type = "city" if city not in {"unincorporated", "unified", "incorporated", "countywide"} else city
                elif county:
                    # County-level entity (3 parts)
                    entity_type = "county"
                else:
                    # State-level entity (2 parts)
                    entity_type = "state"
                    
            except Exception as e:
                # Fallback for problematic entities
                logging.warning(f"Could not parse entity '{entity}': {e}")
                state, county, city = None, None, None
                entity_type = "unknown"
            
        # Handle countywide alias for all layer types
        if entity_type == "countywide":
            entity_type = "unified"
            city_std = "unified"
        else:
            city_std = city
        
        # Generate expected values for all layer types
        layer_internal = format_name(self.cfg.layer, 'layer', external=False)
        layer_external = format_name(self.cfg.layer, 'layer', external=True)
        county_external = format_name(county, 'county', external=True)
        county_internal = format_name(county, 'county', external=False)
        city_external = format_name(city_std, 'city', external=True)
        city_internal = format_name(city_std, 'city', external=False)
        
        # Field-specific health checks
        if field == "new_title":
            # Check title format with state abbreviation and "County" suffix
            # Get the actual title value from the database for comparison
            actual_title = record.get('title') or ''
            state_abbrev = validate_state_abbreviation(state) or 'FL'
            
            if entity_type == "city":
                # Determine the correct prefix (City/Town/Village) from the original title
                title_prefix = "City"
                if actual_title:
                    if "Town of" in actual_title:
                        title_prefix = "Town"
                    elif "Village of" in actual_title:
                        title_prefix = "Village"
                
                # City-level: "Future Land Use - City of Gainesville FL"
                expected = f"{layer_external} - {title_prefix} of {city_external} {state_abbrev}"
            elif entity_type in ["unincorporated", "unified", "incorporated"]:
                # County-level with special suffixes: "Zoning - Broward County Unincorporated FL"
                expected = f"{layer_external} - {county_external} County {entity_type.capitalize()} {state_abbrev}"
            elif entity_type == "county" or self.cfg.layer in ['streets', 'address_points', 'subdivisions', 'buildings']:
                # Standard county-level: "Streets - Broward County FL"
                expected = f"{layer_external} - {county_external} County {state_abbrev}"
            else:
                # Fallback - if we have county but no city, use county format
                if county_external and not city_external:
                    expected = f"{layer_external} - {county_external} County {state_abbrev}"
                else:
                    expected = f"{layer_external} - {city_external} {state_abbrev}"
            
            return expected if actual_title != expected else ""
        
        elif field == "state":
            # Special handling for state-level and national-level layers
            if self.cfg.layer in ['fdot_tc', 'sunbiz']:
                # State-level layers default to FL
                expected_state = 'FL'
                return expected_state if current_value != expected_state else ""
            elif self.cfg.layer == 'flood_zones':
                # National layer - state should be null/empty
                return "" if not current_value else ""
            else:
                # Standard county/city level layers
                expected_state = validate_state_abbreviation(state)
                if expected_state:
                    return expected_state if current_value != expected_state else ""
                else:
                    # Invalid state - mark as manual field
                    self.missing_fields[entity]["state"] = "MANUAL_REQUIRED"
                    return "***MISSING***"
        
        elif field == "county":
            # Special handling for state-level and national-level layers
            if self.cfg.layer in ['fdot_tc', 'sunbiz', 'flood_zones']:
                # State/national level layers should have null county
                return "" if not current_value else ""
            else:
                # Standard county/city level layers
                expected = county_external
                return expected if current_value != expected else ""
        
        elif field == "city":
            # Special handling for state-level and national-level layers
            if self.cfg.layer in ['fdot_tc', 'sunbiz', 'flood_zones']:
                # State/national level layers should have null city
                return "" if not current_value else ""
            elif self.cfg.layer in ['streets', 'address_points', 'subdivisions', 'buildings'] or entity_type == "county":
                # County-level layers should have null city
                return "" if current_value else ""
            else:
                # City-level layers (zoning, flu, etc.)
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
            # For non-AGS, check resource matches pattern based on layer type
            fmt = record.get('format') or ''
            if str(fmt).upper() != 'AGS':
                if self.cfg.layer in ['fdot_tc', 'sunbiz']:
                    # State-level layers: "/data/<layer>"
                    expected = f"/data/{layer_internal}"
                elif self.cfg.layer == 'flood_zones':
                    # National-level layers: "/data/<layer>"
                    expected = f"/data/{layer_internal}"
                elif self.cfg.layer in ['streets', 'address_points', 'subdivisions', 'buildings'] or entity_type == "county":
                    # County-level layers: "/data/<layer>/<county>"
                    expected = f"/data/{layer_internal}/{county_internal}"
                else:
                    # City-level layers: "/data/<layer>/<county>/<city>"
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
            expected = resolve_layer_directory(self.cfg.layer, state, county_internal, city_internal)
            
            # Create directory if it doesn't exist
            try:
                Path(expected).mkdir(parents=True, exist_ok=True)
            except Exception as e:
                self.logger.debug(f"Could not create directory {expected}: {e}")
            
            return expected if current_value != expected else ""
        
        elif field == "table_name":
            # Check table_name matches pattern - now includes state for better uniqueness
            state_abbrev = state.lower() if state else 'fl'
            
            if self.cfg.layer in ['fdot_tc', 'sunbiz']:
                # State-level layers: "<layer>_<state>"
                expected = f"{layer_internal}_{state_abbrev}"
            elif self.cfg.layer == 'flood_zones':
                # National-level layers: "<layer>"
                expected = f"{layer_internal}"
            elif entity_type == "city":
                # City-level layers: "<layer>_<state>_<county>_<city>"
                expected = f"{layer_internal}_{state_abbrev}_{county_internal}_{city_internal}"
            elif entity_type in ["unincorporated", "unified", "incorporated"]:
                # County suffixes: "<layer>_<state>_<county>_<suffix>"
                expected = f"{layer_internal}_{state_abbrev}_{county_internal}_{entity_type}"
            elif self.cfg.layer in ['streets', 'address_points', 'subdivisions', 'buildings'] or entity_type == "county":
                # County-level layers: "<layer>_<state>_<county>"
                expected = f"{layer_internal}_{state_abbrev}_{county_internal}"
            else:
                # Fallback - determine based on available components
                if city_internal and city_internal not in ["unincorporated", "unified", "incorporated"]:
                    expected = f"{layer_internal}_{state_abbrev}_{county_internal}_{city_internal}"
                elif county_internal:
                    expected = f"{layer_internal}_{state_abbrev}_{county_internal}"
                else:
                    expected = f"{layer_internal}_{state_abbrev}"
            
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
        
        Returns layer_state_county_city format (e.g., zoning_fl_alachua_gainesville)
        If parsing fails: returns "ERROR"
        """
        # Check if this layer has a hardcoded entity in the config
        config = LAYER_CONFIGS.get(self.cfg.layer, {})
        hardcoded_entity = config.get('entity')
        if hardcoded_entity:
            return hardcoded_entity
        
        title = record.get('title', '')
        
        # Get state from record, or infer from county if missing
        state_db = record.get('state')
        if state_db and str(state_db).strip() and str(state_db).strip().upper() not in ('NULL', 'NONE'):
            state_internal = str(state_db).strip().lower()
        else:
            # Infer state from county
            county_db = record.get('county', '')
            if county_db:
                county_internal = format_name(county_db, 'county', external=False)
                if county_internal in FL_COUNTIES:
                    state_internal = 'fl'
                else:
                    # Could add other state county lists here in the future
                    state_internal = 'fl'  # Fallback for now
            else:
                state_internal = 'fl'  # Ultimate fallback
        
        # Try to parse the title first
        layer_parsed, county_parsed, city_parsed, entity_type = parse_title_to_entity(title)
        
        if layer_parsed and county_parsed and city_parsed:
            try:
                # Successfully parsed both county and city from title
                entity = entity_from_title_parse(layer_parsed, county_parsed, city_parsed, entity_type, state_internal)
                # Convert from state_county_city to layer_state_county_city
                entity = f"{layer_parsed}_{entity}"
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
                    entity = entity_from_title_parse(layer_parsed, county_internal, city_parsed, entity_type, state_internal)
                    # Convert from state_county_city to layer_state_county_city
                    entity = f"{layer_parsed}_{entity}"
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
                    entity = f"{self.cfg.layer}_{state_internal}_{county_internal}_{city_internal}"
                else:
                    entity = f"{self.cfg.layer}_{state_internal}_{county_internal}_{city_internal}"
            else:
                # No city or not flu/zoning layer
                if self.cfg.layer.lower() in ['flu', 'zoning']:
                    entity = f"{self.cfg.layer}_{state_internal}_{county_internal}_unincorporated"
                else:
                    # Most other layers are county-level (3-part format)
                    entity = f"{self.cfg.layer}_{state_internal}_{county_internal}"
            
            return entity
        
        # Complete failure
        return "ERROR"
    
    def _discover_entities_from_db(self) -> List[str]:
        """Discover entities for the layer by examining database titles."""
        sql = """
            SELECT title, state, county, city 
            FROM m_gis_data_catalog_main 
            WHERE status IS DISTINCT FROM 'DELETE'
        """
        rows = self.db.fetchall(sql) or []
        
        entities = set()
        
        for row in rows:
            title = row.get('title', '')
            
            # Get state from record, or infer from county if missing
            state_db = row.get('state')
            if state_db and str(state_db).strip() and str(state_db).strip().upper() not in ('NULL', 'NONE'):
                state_internal = str(state_db).strip().lower()
            else:
                # Infer state from county
                county_db = row.get('county', '')
                if county_db:
                    county_internal = format_name(county_db, 'county', external=False)
                    if county_internal in FL_COUNTIES:
                        state_internal = 'fl'
                    else:
                        # Could add other state county lists here in the future
                        state_internal = 'fl'  # Fallback for now
                else:
                    state_internal = 'fl'  # Ultimate fallback
            
            # Parse title to get layer/county/city
            layer_parsed, county_parsed, city_parsed, entity_type = parse_title_to_entity(title)
            
            if layer_parsed == self.cfg.layer:
                try:
                    entity = entity_from_title_parse(self.cfg.layer, county_parsed, city_parsed, entity_type, state_internal)
                    # Convert from state_county_city to layer_state_county_city
                    entity = f"{layer_parsed}_{entity}"
                    entities.add(entity)
                except (ValueError, TypeError):
                    # If title parsing fails, try fallback with DB fields
                    county_db = row.get('county', '')
                    city_db = row.get('city', '')
                    
                    if county_db and city_db:
                        county_internal = format_name(county_db, 'county', external=False)
                        city_internal = format_name(city_db, 'city', external=False)
                        if city_internal in {"unincorporated", "unified", "incorporated", "countywide"}:
                            entity = f"{self.cfg.layer}_{state_internal}_{county_internal}_{city_internal}"
                        else:
                            entity = f"{self.cfg.layer}_{state_internal}_{county_internal}_{city_internal}"
                        entities.add(entity)
        
        return sorted(entities)
    
    def _find_record_by_entity(self, entity: str) -> Optional[Dict[str, Any]]:
        """Find database record by matching entity to expected title."""
        try:
            state, county, city = split_entity(entity)
        except ValueError:
            return None
        
        # Generate expected title patterns for this entity
        entity_type = "city" if city not in {"unincorporated", "unified", "incorporated", "countywide"} else city
        expected = generate_expected_values(self.cfg.layer, state, county, city, entity_type)
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
# Entity Pattern Processing  
# ---------------------------------------------------------------------------

def extract_layers_from_patterns(include_patterns: list[str] = None, exclude_patterns: list[str] = None) -> list[str]:
    """Extract unique layer names from entity patterns.
    
    Args:
        include_patterns: List of include patterns (e.g., ['zoning_fl_*', 'flu_*'])
        exclude_patterns: List of exclude patterns
        
    Returns:
        List of unique layer names found in patterns
    """
    layers = set()
    
    # Extract layers from include patterns
    if include_patterns:
        for pattern in include_patterns:
            # Entity format is layer_state_county_city, so layer is first component
            if '_' in pattern:
                layer = pattern.split('_')[0]
                if layer in LAYER_CONFIGS:
                    layers.add(layer)
            elif pattern in LAYER_CONFIGS:
                # Direct layer name
                layers.add(pattern)
    
    # Note: exclude patterns don't add layers, they only filter
    
    return sorted(layers)

# ---------------------------------------------------------------------------
# CLI Interface
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare database for layers_scrape.py by detecting and fixing malformed records"
    )
    
    # Layer argument removed - layers are now extracted from entity patterns
    
    # Entity filtering options
    parser.add_argument("--include", nargs="*", metavar="ENTITY", 
                       help="Include only entities matching these patterns (required for CREATE mode)")
    parser.add_argument("--exclude", nargs="*", metavar="ENTITY",
                       help="Exclude entities matching these patterns")
    
    # Mode selection (detect and fill can be combined)
    parser.add_argument("--detect", action="store_true", 
                       help="Detect malformed records and missing fields (default if no other mode specified)")
    parser.add_argument("--fill", action="store_true", 
                       help="Fill missing fields from manual JSON file (can be combined with --detect)")
    
    # Exclusive modes (cannot be combined with detect/fill or each other)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--create", action="store_true", 
                           help="Create new records for specified entities")
    mode_group.add_argument("--update-states", action="store_true",
                           help="One-time update: Set all NULL state values to 'FL'")
    
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
    
    # Determine mode(s)
    if args.create:
        mode = "create"
    elif args.update_states:
        mode = "update_states"
    elif args.detect and args.fill:
        mode = "detect_and_fill"
    elif args.fill:
        mode = "fill"
    elif args.detect:
        mode = "detect"
    else:
        mode = "detect"  # default
    
    # Validate mode combinations
    if (args.create or args.update_states) and (args.detect or args.fill):
        print("[ERROR] CREATE and UPDATE-STATES modes cannot be combined with DETECT or FILL")
        sys.exit(1)
    
    # Validate CREATE mode requirements
    if mode == "create" and not args.include:
        print("[ERROR] CREATE mode requires --include entities to be specified")
        sys.exit(1)
    
    # Handle one-time state update mode
    if mode == "update_states":
        print("[INFO] Running one-time state update: Setting all NULL state values to 'FL'")
        
        if not args.apply:
            print("[ERROR] --update-states mode requires --apply flag to actually perform the update")
            sys.exit(1)
        
        # Connect to database
        db = DB()
        try:
            # Count records that need updating
            count_sql = "SELECT COUNT(*) as count FROM m_gis_data_catalog_main WHERE state IS NULL OR state = ''"
            count_result = db.fetchone(count_sql)
            count = count_result['count'] if count_result else 0
            
            if count == 0:
                print("[INFO] No records found with NULL state values. Update not needed.")
            else:
                print(f"[INFO] Found {count} records with NULL state values")
                
                # Perform the update
                update_sql = "UPDATE m_gis_data_catalog_main SET state = 'FL' WHERE state IS NULL OR state = ''"
                db.execute(update_sql)
                
                print(f"[SUCCESS] Updated {count} records to set state = 'FL'")
                
        except Exception as e:
            print(f"[ERROR] Failed to update state values: {e}")
            sys.exit(1)
        finally:
            db.close()
        
        return
    
    # Extract layers from entity patterns
    layers_to_process = extract_layers_from_patterns(args.include, args.exclude)
    
    if not layers_to_process:
        # No patterns specified or no valid layers found - process all layers
        layers_to_process = list(LAYER_CONFIGS.keys())
        print(f"[INFO] No entity patterns specified, processing all layers: {', '.join(layers_to_process)}")
    else:
        print(f"[INFO] Processing layers extracted from entity patterns: {', '.join(layers_to_process)}")
    
    # Process each layer separately
    for layer in layers_to_process:
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
    
    print(f"\n[INFO] ==================== Completed processing {len(layers_to_process)} layer(s) ====================")

if __name__ == "__main__":
    main()