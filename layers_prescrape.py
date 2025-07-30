#!/usr/bin/env python3
"""
Database preparation utility for layers_scrape.py

This script prepares the m_gis_data_catalog_main table to ensure it meets the requirements
for layers_scrape.py. It operates on existing database records and provides tools to:

1. DETECT mode: Find malformed records and missing fields, output CSV + JSON reports
2. FILL mode: Apply manual corrections and auto-derivable fields from JSON
3. CREATE mode: Create new records based on layer/county/city + manual info

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
MANIFEST_PATH = Path("layer_manifest.json")

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
    },
    'flu': {
        'category': '08_Land_Use_and_Zoning', 
        'layer_group': 'flu_zoning',
    }
}

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
        if entity_type in {"unincorporated", "unified", "countywide"}:
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
# Minimal Manifest Integration (for preprocessing commands only)
# ---------------------------------------------------------------------------

def extract_preprocessing_commands(layer: str, entity: str) -> str:
    """Extract preprocessing commands from manifest (between download and ogrinfo commands).
    
    Returns empty string if manifest is missing/invalid or no preprocessing found.
    """
    try:
        if not MANIFEST_PATH.exists():
            return ""
            
        with open(MANIFEST_PATH, 'r') as f:
            manifest_data = json.load(f)
            
        if layer not in manifest_data or 'entities' not in manifest_data[layer]:
            return ""
            
        if entity not in manifest_data[layer]['entities']:
            return ""
            
        commands = manifest_data[layer]['entities'][entity]
        if not isinstance(commands, list):
            return ""
            
        # Find commands between download and ogrinfo
        preprocessing = []
        in_preprocessing = False
        
        for cmd in commands:
            if isinstance(cmd, list) and len(cmd) > 0:
                cmd_str = ' '.join(str(x) for x in cmd)
                
                # Start preprocessing after download command
                if any(x in cmd_str for x in ['ags_extract_data2.py', 'download_data.py']):
                    in_preprocessing = True
                    continue
                    
                # Stop preprocessing at ogrinfo command
                if 'ogrinfo' in cmd_str:
                    in_preprocessing = False
                    break
                    
                # Collect preprocessing commands
                if in_preprocessing:
                    # Extract just the script name for processing_comments
                    if cmd[0] == 'python3' and len(cmd) > 1:
                        script_name = Path(cmd[1]).name
                        preprocessing.append(script_name)
                    else:
                        preprocessing.append(cmd_str)
        
        return "|".join(preprocessing)
        
    except Exception:
        return ""  # If any error occurs, return empty string

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
        title = f"{layer_external} - City of {city_external}"
    elif entity_type == "unincorporated":
        title = f"{layer_external} - {county_external} Unincorporated"
    elif entity_type == "unified":
        title = f"{layer_external} - {county_external} Unified"
    elif entity_type == "incorporated":
        title = f"{layer_external} - {county_external} Incorporated"
    else:
        title = f"{layer_external} - {city_external}"
    
    # Generate table name (internal format)
    layer_internal = format_name(layer, 'layer', external=False)
    city_internal = format_name(city_std, 'city', external=False)
    county_internal = format_name(county, 'county', external=False)
    
    if entity_type == "city":
        table_name = f"{layer_internal}_{city_internal}"
    else:
        table_name = f"{layer_internal}_{county_internal}_{entity_type}"
    
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
    entities: List[str] | None = None
    mode: str = "detect"  # detect | fill | create
    debug: bool = False
    generate_csv: bool = True
    apply_changes: bool = False  # For fill mode
    manual_file: str = "missing_fields.json"

# ---------------------------------------------------------------------------
# Main Processing Class
# ---------------------------------------------------------------------------

class LayersPrescrape:
    """Main engine for database preparation and validation."""
    
    def __init__(self, cfg: Config):
        self.cfg = cfg
        
        self.logger = logging.getLogger("LayersPrescrape")
        self.logger.setLevel(logging.DEBUG if cfg.debug else logging.INFO)
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
        if self.missing_fields and self.cfg.mode == "fill":
            self.logger.info(f"Writing missing field report → {self.cfg.manual_file}")
            with open(self.cfg.manual_file, "w", encoding="utf-8") as fh:
                json.dump(self.missing_fields, fh, indent=2)
        
        # Commit or rollback database changes
        if self.db:
            if self.cfg.mode in {"fill", "create"} and self.cfg.apply_changes:
                self.db.commit()
                self.logger.info("Database changes committed.")
            else:
                self.db.conn.rollback()
                if not self.cfg.apply_changes and self.cfg.mode in {"fill", "create"}:
                    self.logger.info("Apply flag not set - no database changes made.")
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
        
        self.logger.info(f"Found {len(records)} records for layer '{self.cfg.layer}'")
        
        # CSV headers - specific fields requested by user
        headers = [
            "entity", "title", "county", "city", "source_org", "src_url_file", 
            "format", "format_subtype", "download", "resource", "layer_group", 
            "layer_subgroup", "category", "sub_category", "sys_raw_folder", 
            "table_name", "fields_obj_transform"
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
        
        # Separate unique records from duplicates
        csv_rows = [headers]
        duplicate_groups = []
        unique_entities = 0
        duplicate_entities = 0
        
        for entity in sorted(entity_groups.keys()):
            records_for_entity = entity_groups[entity]
            
            if len(records_for_entity) == 1:
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
        
        # Add duplicate info summary
        csv_rows.append([])
        csv_rows.append([
            f"UNIQUE ENTITIES: {unique_entities}, DUPLICATE ENTITIES: {duplicate_entities}, TOTAL RECORDS: {total_records}"
        ] + [""] * (len(headers) - 1))
        
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
            csv_path = REPORTS_DIR / f"{self.cfg.layer}_prescrape_detect_{get_today_str()}.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(csv_rows)
            self.logger.info(f"Detection report written → {csv_path}")
        
        # Log summary
        if duplicate_entities > 0:
            self.logger.info(f"Detection complete: {total_records} records found, {unique_entities} unique entities, {duplicate_entities} duplicate entities")
        else:
            self.logger.info(f"Detection complete: {total_records} records found, all entities unique")
    
    def _run_fill_mode(self):
        """Apply manual corrections and auto-derivable fields from JSON."""
        self.logger.info("Running FILL mode - applying corrections from manual fields file.")
        
        # Load manual corrections
        if not Path(self.cfg.manual_file).exists():
            self.logger.error(f"Manual file not found: {self.cfg.manual_file}")
            return
        
        with open(self.cfg.manual_file, 'r') as f:
            manual_data = json.load(f)
        
        if not manual_data:
            self.logger.warning("No manual data found in file.")
            return
        
        # Process each entity in manual data
        changes_summary = []
        
        for entity, corrections in manual_data.items():
            self.logger.debug(f"Processing corrections for {entity}")
            
            # Find existing record
            record = self._find_record_by_entity(entity)
            
            if not record:
                self.logger.warning(f"No database record found for {entity} - skipping")
                continue
            
            # Apply corrections
            updates = {}
            change_log = []
            
            for field, new_value in corrections.items():
                if new_value and str(new_value) != "MANUAL_REQUIRED":
                    current_value = record.get(field)
                    if current_value != new_value:
                        updates[field] = new_value
                        change_log.append(f"{field}: '{current_value}' → '{new_value}'")
            
            if updates:
                changes_summary.append({
                    'entity': entity,
                    'changes': change_log,
                    'updates': updates,
                    'record_id': record.get('gid') or record.get('id')
                })
                
                # Apply updates if apply flag is set
                if self.cfg.apply_changes:
                    self._update_record(record, updates)
                    self.logger.info(f"Applied {len(updates)} updates to {entity}")
                else:
                    self.logger.info(f"Would apply {len(updates)} updates to {entity}")
        
        # Generate CSV report of changes
        if self.cfg.generate_csv and changes_summary:
            headers = ["entity", "field", "old_value", "new_value", "status"]
            csv_rows = [headers]
            
            for change in changes_summary:
                entity = change['entity']
                for change_desc in change['changes']:
                    field, change_text = change_desc.split(':', 1)
                    old_val, new_val = change_text.split(' → ', 1)
                    old_val = old_val.strip().strip("'")
                    new_val = new_val.strip().strip("'")
                    
                    status = "APPLIED" if self.cfg.apply_changes else "PENDING"
                    csv_rows.append([entity, field.strip(), old_val, new_val, status])
            
            csv_path = REPORTS_DIR / f"{self.cfg.layer}_prescrape_fill_{get_today_str()}.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(csv_rows)
            self.logger.info(f"Fill report written → {csv_path}")
        
        self.logger.info(f"Fill complete: {len(changes_summary)} entities processed")
    
    def _run_create_mode(self):
        """Create new records based on layer/county/city + manual info."""
        self.logger.info("Running CREATE mode - creating new database records.")
        
        if not self.cfg.entities:
            self.logger.error("CREATE mode requires entity specification")
            return
        
        # Load manual data if available
        manual_data = {}
        if Path(self.cfg.manual_file).exists():
            with open(self.cfg.manual_file, 'r') as f:
                manual_data = json.load(f)
        
        created_records = []
        
        for entity in self.cfg.entities:
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
            if self.cfg.apply_changes:
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
                status = "CREATED" if self.cfg.apply_changes else "PENDING"
                
                for field, value in record.items():
                    csv_rows.append([entity, field, str(value), status])
            
            csv_path = REPORTS_DIR / f"{self.cfg.layer}_prescrape_create_{get_today_str()}.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(csv_rows)
            self.logger.info(f"Creation report written → {csv_path}")
        
        self.logger.info(f"Create complete: {len(created_records)} records processed")
    
    # Helper methods
    
    def _generate_entity_from_record(self, record: Dict[str, Any]) -> str:
        """Generate entity name from database record title and fields.
        
        For flu/zoning: returns county_city format
        For other layers: returns just county name
        If parsing fails: returns "ERROR"
        """
        title = record.get('title', '')
        
        # Try to parse the title first
        layer_parsed, county_parsed, city_parsed, entity_type = parse_title_to_entity(title)
        
        if layer_parsed and county_parsed:
            try:
                # Successfully parsed title
                if city_parsed:
                    entity = entity_from_title_parse(layer_parsed, county_parsed, city_parsed, entity_type)
                else:
                    # County-only record, treat as unincorporated for flu/zoning
                    if self.cfg.layer.lower() in ['flu', 'zoning']:
                        county_internal = format_name(county_parsed, 'county', external=False)
                        entity = f"{county_internal}_unincorporated"
                    else:
                        entity = format_name(county_parsed, 'county', external=False)
                return entity
            except (ValueError, TypeError):
                pass
        
        # Title parsing failed, try fallback with county/city fields
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
    
    parser.add_argument("layer", help="Layer name (zoning, flu)")
    parser.add_argument("entities", nargs="*", 
                       help="Specific entities to process (for CREATE mode) or 'all' for all entities")
    
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
                       help="Apply changes to database (for FILL/CREATE modes)")
    parser.add_argument("--manual-file", default="missing_fields.json",
                       help="Path to manual fields JSON file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
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
    
    # Create config
    cfg = Config(
        layer=args.layer.lower(),
        entities=[e.lower() for e in args.entities] if args.entities else None,
        mode=mode,
        debug=args.debug,
        generate_csv=args.generate_csv,
        apply_changes=args.apply,
        manual_file=args.manual_file
    )
    
    # Run the processor
    processor = LayersPrescrape(cfg)
    processor.run()

if __name__ == "__main__":
    main()