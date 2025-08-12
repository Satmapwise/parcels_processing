#!/usr/bin/env python3
"""
Shared utilities and constants for layers_scrape.py and layers_prescrape.py

This module contains all common functionality used by both scripts:
- Database connection setup
- Entity parsing and validation logic
- Name formatting utilities
- Shared constants and configurations
"""

import os
import re
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List
import datetime


# ---------------------------------------------------------------------------
# Environment Setup and Database Configuration
# ---------------------------------------------------------------------------

def load_environment():
    """Load environment variables from .env file if available."""
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

# Load environment on import
load_environment()

# Database connection
PG_CONNECTION = os.getenv("PG_CONNECTION")

# Base directory configuration
DATA_ROOT = os.getenv("DATA_ROOT", "/srv/datascrub")
TOOLS_DIR = os.getenv("TOOLS_DIR", "/srv/tools/python/lib")


# ---------------------------------------------------------------------------
# Constants and Data Structures
# ---------------------------------------------------------------------------

# Valid state abbreviations for multi-state support
VALID_STATES = [
    'fl', 'ga', 'de', 'al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'dc', 'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wi', 'wv'
]

# Florida counties
FL_COUNTIES = {
    "alachua", "baker", "bay", "bradford", "brevard", "broward", "calhoun", "charlotte", "citrus", "clay",
    "collier", "columbia", "desoto", "dixie", "duval", "escambia", "flagler", "franklin", "gadsden", "gilchrist",
    "glades", "gulf", "hamilton", "hardee", "hendry", "hernando", "highlands", "hillsborough", "holmes",
    "indian_river", "jackson", "jefferson", "lafayette", "lake", "lee", "leon", "levy", "liberty", "madison",
    "manatee", "marion", "martin", "miami_dade", "monroe", "nassau", "okaloosa", "okeechobee", "orange", "osceola",
    "palm_beach", "pasco", "pinellas", "polk", "putnam", "santa_rosa", "sarasota", "seminole", "st_johns",
    "st_lucie", "sumter", "suwannee", "taylor", "union", "volusia", "wakulla", "walton", "washington",
}

# Georgia counties
GA_COUNTIES = {
    "appling", "atkinson", "bacon", "baker", "baldwin", "banks", "barrow", "bartow", "ben_hill", "berrien",
    "bibb", "bleckley", "brantley", "brooks", "bryan", "bulloch", "burke", "butts", "calhoun", "camden",
    "candler", "carroll", "catoosa", "charlton", "chatham", "chattahoochee", "chattooga", "cherokee", "clarke",
    "clay", "clayton", "clinch", "cobb", "coffee", "colquitt", "columbia", "cook", "coweta", "crawford",
    "crisp", "dade", "dawson", "decatur", "dekalb", "dodge", "dooly", "dougherty", "douglas", "early",
    "echols", "effingham", "elbert", "emanuel", "evans", "fannin", "fayette", "floyd", "forsyth", "franklin",
    "fulton", "gilmer", "glascock", "glynn", "gordon", "grady", "greene", "gwinnett", "habersham", "hall",
    "hancock", "haralson", "harris", "hart", "heard", "henry", "houston", "irwin", "jackson", "jasper",
    "jeff_davis", "jefferson", "jenkins", "johnson", "jones", "lamar", "lanier", "laurens", "lee", "liberty",
    "lincoln", "long", "lowndes", "lumpkin", "mcduffie", "mcintosh", "macon", "madison", "marion", "meriwether",
    "miller", "mitchell", "monroe", "montgomery", "morgan", "murray", "muscogee", "newton", "oconee", "oglethorpe",
    "paulding", "peach", "pickens", "pierce", "pike", "polk", "pulaski", "putnam", "quitman", "rabun",
    "randolph", "richmond", "rockdale", "schley", "screven", "seminole", "spalding", "stephens", "stewart", "sumter",
    "talbot", "taliaferro", "tattnall", "taylor", "telfair", "terrell", "thomas", "tift", "toombs", "towns",
    "treutlen", "troup", "turner", "twiggs", "union", "upson", "walker", "walton", "ware", "warren",
    "washington", "wayne", "webster", "wheeler", "white", "whitfield", "wilcox", "wilkes", "wilkinson", "worth"
}

# Delaware counties
DE_COUNTIES = {
    "new_castle", "kent", "sussex"
}

# Arizona counties
AZ_COUNTIES = {
    "apache", "cochise", "coconino", "gila", "graham", "greenlee", "la_paz", "maricopa", "mohave", "navajo",
    "pinal", "pima", "santa_cruz", "yavapai", "yuma"
}

# Alabama counties
AL_COUNTIES = {
    "autauga", "baldwin", "barbour", "bibb", "blount", "bullock", "butler", "calhoun", "chambers", "cherokee", 
    "chilton", "choctaw", "clarke", "clay", "cleburne", "coffee", "colbert", "conecuh", "coosa", "covington", 
    "crenshaw", "cullman", "dale", "dallas", "dekalb", "elmore", "escambia", "etowah", "fayette", "franklin", 
    "geneva", "greene", "hale", "henry", "houston", "jackson", "jefferson", "lamar", "lauderdale", "lawrence", 
    "lee", "limestone", "lowndes", "macon", "madison", "marengo", "marion", "marshall", "mobile", "monroe", 
    "montgomery", "morgan", "perry", "pickens", "pike", "randolph", "russell", "st_clair", "shelby", "sumter", 
    "talladega", "tallapoosa", "tuscaloosa", "walker", "washington", "wilcox", "winston"
    }

# State-county mapping for integrated states (with full county data)
STATE_COUNTIES = {
    'fl': FL_COUNTIES,
    'ga': GA_COUNTIES,
    'de': DE_COUNTIES,
    'az': AZ_COUNTIES,
    'al': AL_COUNTIES,
}

# States with full county data integration
INTEGRATED_STATES = set(STATE_COUNTIES.keys())

# Layer configurations with metadata
LAYER_CONFIGS = {
    'zoning': {
        'category': '08_Land_Use_and_Zoning',
        'layer_group': 'flu_zoning',
        'external_frmt': 'Zoning',
        'level': 'state_county_city',
        'processing_command': 'python3 {tools_dir}/update_zoning2.py {county} {city}',
    },
    'flu': {
        'category': '08_Land_Use_and_Zoning', 
        'layer_group': 'flu_zoning',
        'external_frmt': 'Future Land Use',
        'level': 'state_county_city',
        'processing_command': 'python3 {tools_dir}/update_flu.py {county} {city}',
    },
    'fema_flood': {
        'category': '12_Hazards',
        'layer_group': 'hazards',
        'external_frmt': 'FEMA Flood Zones',
        'level': 'national',
        'entity': 'fema_flood',
        'processing_command': None,  # No processing script needed
    },
    'parcel_geo': {
        'category': '05_Parcels',
        'layer_group': 'parcels',
        'external_frmt': 'Parcel Geometry',
        'level': 'state_county',
        'processing_command': 'python3 {tools_dir}/load_parcel_geometry.py {state} {county} current {data_date}',
    },
    'streets': {
        'category': '03_Transportation',
        'layer_group': 'base_map_overlay',
        'external_frmt': 'Streets',
        'level': 'state_county',
        'processing_command': 'python3 {tools_dir}/update_streets_county.py {county}',
    },
    'address_points': {
        'category': '05_Parcels',
        'layer_group': 'parcels',
        'external_frmt': 'Address Points',
        'level': 'state_county',
        'processing_command': 'python3 {tools_dir}/update_address_points.py {county}',
    },
    'subdivisions': {
        'category': '05_Parcels',
        'layer_group': 'parcels',
        'external_frmt': 'Subdivisions',
        'level': 'state_county',
        'processing_command': 'python3 {tools_dir}/update_subdivisions_county.py {county}',
    },
    'buildings': {
        'category': '05_Parcels',
        'layer_group': 'parcels',
        'external_frmt': 'Building Footprints',
        'level': 'state_county',
        'processing_command': 'python3 {tools_dir}/update_buildings.py {county}',
    },
    'traffic_counts': {
        'category': '03_Transportation',
        'layer_group': 'base_map_overlay',
        'external_frmt': 'Traffic Counts FDOT',
        'level': 'state',
        'entity': 'traffic_counts_fl',
        'processing_command': None,  # No processing script needed
    },
    'sunbiz': {
        'category': '21_Misc',
        'layer_group': 'misc',
        'external_frmt': 'Sunbiz',
        'level': 'state',
        'entity': 'sunbiz_fl',
        'processing_command': 'python3 {tools_dir}/sunbiz_corpdata_processing.py',
    }
}

# Entities to skip (blacklist) - for layers_scrape.py
SKIP_ENTITIES = {
    "flu_fl_collier_marco_island",
    "zoning_fl_pinellas_pinellas_park"
    }

# Format categories for pipeline control
FULL_PIPELINE_FORMATS = {
    'ags', 'arcgis', 'esri', 'ags_extract',
    'shp', 'zip', 'url',
    # File Geodatabase support
    'gdb', 'filegdb', 'file geodatabase', 'geodatabase', 'fgdb'
}
METADATA_ONLY_FORMATS = {'pdf'}


# ---------------------------------------------------------------------------
# Name Formatting Utilities
# ---------------------------------------------------------------------------

def _to_internal_format(name: str) -> str:
    """Convert name to internal format: lowercase, underscores, no special chars."""
    if not name or not name.strip():
        return ""
    
    # Normalize to lowercase and replace various separators with underscores
    result = re.sub(r'[^a-zA-Z0-9_\s-]', '', name.lower())
    result = re.sub(r'[\s-]+', '_', result)
    
    # Clean up multiple underscores and trim
    result = re.sub(r'_+', '_', result).strip('_')
    
    return result


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
    
    # Get layer mappings from LAYER_CONFIGS
    layer_mappings = {}
    layer_mappings_reverse = {}
    
    for layer_key, config in LAYER_CONFIGS.items():
        external_name = config.get('external_frmt', layer_key.title())
        layer_mappings[layer_key] = external_name
        layer_mappings_reverse[external_name.lower()] = layer_key
    
    # Add legacy mappings for backward compatibility (but don't override LAYER_CONFIGS)
    legacy_mappings = {
        'addr_pnts': 'Address Points',
        'bldg_ftpr': 'Building Footprints', 
        'subdiv': 'Subdivisions',
        'flood_zones': 'FEMA Flood Zones',
        'fdot_tc': 'Traffic Counts FDOT'
    }
    
    # Add legacy forward mappings (internal -> external) only if not already in LAYER_CONFIGS
    for legacy_internal, external_name in legacy_mappings.items():
        if legacy_internal not in layer_mappings:
            layer_mappings[legacy_internal] = external_name
        
        # For reverse mapping, only add legacy if the external name isn't already mapped to a current layer
        if external_name.lower() not in layer_mappings_reverse:
            layer_mappings_reverse[external_name.lower()] = legacy_internal
    
    # Special county mappings (internal -> external)
    county_special = {
        'miami_dade': 'Miami-Dade',
        'st_johns': 'St. Johns',
        'st_lucie': 'St. Lucie',
        'desoto': 'DeSoto',
        'palm_beach': 'Palm Beach',
        'santa_rosa': 'Santa Rosa',
        'indian_river': 'Indian River',
        'jeff_davis': 'Jeff Davis',
        'st_clair': 'St. Clair'
    }
    
    # Reverse mapping for counties (external -> internal)
    county_special_reverse = {v.lower(): k for k, v in county_special.items()}
    
    if name_type == 'layer':
        if external:
            return layer_mappings.get(name.lower(), name.title())
        else:
            return layer_mappings_reverse.get(name.lower(), _to_internal_format(name))
    
    elif name_type == 'county':
        if external:
            # Check special cases first
            return county_special.get(name.lower(), name.replace('_', ' ').title())
        else:
            # Check reverse special cases first
            base = county_special_reverse.get(name.lower(), _to_internal_format(name))
            # Normalize common concatenations to underscored forms
            concat_normalize = {
                'jeffdavis': 'jeff_davis',
                'stclair': 'st_clair',
            }
            return concat_normalize.get(base, base)
    
    elif name_type == 'city':
        if external:
            # Cities with special formatting (hyphens instead of spaces)
            city_special = {
                'howey_in_the_hills': 'Howey-in-the-Hills',
                'west_palm_beach': 'West Palm Beach',
                'coral_springs': 'Coral Springs',
                'boca_raton': 'Boca Raton',
                'fort_lauderdale': 'Fort Lauderdale',
                'fort_myers': 'Fort Myers',
                'fort_pierce': 'Fort Pierce',
                'cape_coral': 'Cape Coral',
                'saint_petersburg': 'St. Petersburg',
                'st_petersburg': 'St. Petersburg'
            }
            return city_special.get(name.lower(), name.replace('_', ' ').title())
        else:
            # Convert external city names to internal format
            city_special_reverse = {
                'howey-in-the-hills': 'howey_in_the_hills',
                'st. petersburg': 'st_petersburg',
                'saint petersburg': 'st_petersburg'
            }
            return city_special_reverse.get(name.lower(), _to_internal_format(name))
    
    elif name_type == 'state':
        if external:
            # Convert to uppercase for state abbreviations
            return name.upper()
        else:
            # Convert to lowercase for internal format
            return name.lower()
    
    # Default: just do basic conversion
    if external:
        return name.replace('_', ' ').title()
    else:
        return _to_internal_format(name)


# ---------------------------------------------------------------------------
# Entity Parsing and Validation
# ---------------------------------------------------------------------------

def parse_entity_pattern(pattern: str) -> tuple[str | None, str | None, str | None, str | None]:
    """Parse an entity pattern into (layer, state, county, city) components.
    
    Strategy:
    1. Check if pattern starts with any known layer name - extract and remove it
    2. Check if remaining starts with any valid state - extract and remove it  
    3. Check if remaining starts with any county from that state - extract and remove it
    4. Whatever remains is the city
    
    Returns tuple of (layer, state, county, city) where None means not found/parsed.
    """
    remaining = pattern.strip()
    if not remaining:
        return (None, None, None, None)
    
    # Step 1: Extract layer
    layer = None
    layer_names = list(LAYER_CONFIGS.keys())
    
    for layer_name in layer_names:
        if remaining.startswith(layer_name):
            layer = layer_name
            remaining = remaining[len(layer_name):].lstrip('_')
            break
    
    if not remaining:
        return (layer, None, None, None)
    
    # Step 2: Extract state
    state = None
    for state_abbrev in VALID_STATES:
        if remaining == state_abbrev or remaining.startswith(state_abbrev + '_'):
            state = state_abbrev
            remaining = remaining[len(state_abbrev):].lstrip('_')
            break
    
    if not remaining:
        return (layer, state, None, None)
    
    # Step 3: Extract county 
    county = None
    
    # If we have a state, check counties for that state
    if state and state in STATE_COUNTIES:
        for county_name in STATE_COUNTIES[state]:
            if remaining.startswith(county_name):
                county = county_name
                remaining = remaining[len(county_name):].lstrip('_')
                break
    
    # If no county found and no state identified, try to infer state from county
    if county is None and state is None:
        for state_abbrev, counties in STATE_COUNTIES.items():
            for county_name in counties:
                if remaining.startswith(county_name):
                    county = county_name
                    state = state_abbrev
                    remaining = remaining[len(county_name):].lstrip('_')
                    break
            if county is not None:
                break
    
    if not remaining:
        return (layer, state, county, None)
    
    # Step 4: Whatever remains is the city
    city = remaining if remaining else None
    return (layer, state, county, city)


def validate_state_abbreviation(state_value: str) -> Optional[str]:
    """Validate and normalize state abbreviation.
    
    Args:
        state_value: State value from database (can be None, empty, or abbreviation)
        
    Returns:
        External format state (e.g., 'FL') if valid, None if invalid/missing
    """
    if not state_value or str(state_value).strip().upper() in ('NULL', 'NONE', ''):
        return None
    
    state_internal = str(state_value).strip().lower()
    if state_internal in VALID_STATES:
        return format_name(state_internal, 'state', external=True)
    return None


# ---------------------------------------------------------------------------
# Database Utilities
# ---------------------------------------------------------------------------

def safe_catalog_val(val: Any) -> str:
    """Return value or **MISSING** if val is falsy/None."""
    if val in (None, "", "NULL", "null"):
        return "**MISSING**"
    return str(val)


# ---------------------------------------------------------------------------
# Directory Path Resolution
# ---------------------------------------------------------------------------

def resolve_layer_name(layer: str) -> str:
    """Resolve layer name, handling backwards compatibility aliases.
    
    Args:
        layer: Layer name (old or new format)
        
    Returns:
        Canonical layer name for use in configs
    """
    return LAYER_NAME_ALIASES.get(layer, layer)


def resolve_layer_directory(layer: str, state: str = None, county: str = None, city: str = None) -> str:
    """Resolve directory path for a layer using standardized format.
    
    Standard format: /srv/datascrub/<category>/<layer_subgroup>/<state>/<county>/<city>
    
    Args:
        layer: Layer name (e.g., 'zoning', 'flu')
        state: State abbreviation (e.g., 'fl') - converted to state_name
        county: County name in internal format (e.g., 'alachua')
        city: City name in internal format (e.g., 'gainesville') 
        
    Returns:
        Full directory path using standardized format
        
    Examples:
        resolve_layer_directory('zoning', 'fl', 'alachua', 'gainesville')
        -> '/srv/datascrub/08_Land_Use_and_Zoning/zoning/florida/alachua/gainesville'
        
        resolve_layer_directory('fema_flood')
        -> '/srv/datascrub/12_Hazards/fema_flood'
    """
    # Resolve layer name for backwards compatibility
    canonical_layer = resolve_layer_name(layer)
    config = LAYER_CONFIGS.get(canonical_layer, {})
    category = config.get('category', 'unknown')
    layer_subgroup = config.get('layer_subgroup', canonical_layer)
    
    # Convert state abbreviation to state name for directories
    state_name = None
    if state:
        state_mapping = {
            'fl': 'florida',
            'ga': 'georgia', 
            'de': 'delaware',
            'az': 'arizona'
        }
        state_name = state_mapping.get(state.lower(), state.lower())
    
    # Build standardized path: <DATA_ROOT>/<category>/<layer_subgroup>/<state>/<county>/<city>
    path_parts = [DATA_ROOT, category, layer_subgroup]
    
    # Add additional parts based on what's provided
    if state_name:
        path_parts.append(state_name)
        if county:
            path_parts.append(county)
            if city:
                path_parts.append(city)
    
    return '/'.join(path_parts)


# ---------------------------------------------------------------------------
# Backwards Compatibility Aliases
# ---------------------------------------------------------------------------

# For layers_scrape.py compatibility
counties = FL_COUNTIES  # Keep for backwards compatibility
layers = LAYER_CONFIGS.keys()

# Layer name mapping for backwards compatibility
LAYER_NAME_ALIASES = {
    # Old name -> New name
    'flood_zones': 'fema_flood',
    'addr_pnts': 'address_points', 
    'subdiv': 'subdivisions',
    'bldg_ftpr': 'buildings',
    'fdot_tc': 'traffic_counts',
}

# Reverse mapping for new name -> old name  
LAYER_NAME_REVERSE_ALIASES = {v: k for k, v in LAYER_NAME_ALIASES.items()}


# ---------------------------------------------------------------------------
# Date parsing and normalization utilities
# ---------------------------------------------------------------------------

_MONTHS_MAP = {
    'jan': 1, 'january': 1,
    'feb': 2, 'february': 2,
    'mar': 3, 'march': 3,
    'apr': 4, 'april': 4,
    'may': 5,
    'jun': 6, 'june': 6,
    'jul': 7, 'july': 7,
    'aug': 8, 'august': 8,
    'sep': 9, 'sept': 9, 'september': 9,
    'oct': 10, 'october': 10,
    'nov': 11, 'november': 11,
    'dec': 12, 'december': 12,
}

def _safe_date(year: int, month: int, day: int) -> Optional[datetime.date]:
    try:
        return datetime.date(year, month, day)
    except Exception:
        return None

def _strip_ordinal_suffix(s: str) -> str:
    return re.sub(r"\b(\d{1,2})(st|nd|rd|th)\b", r"\1", s, flags=re.IGNORECASE)

def parse_string_to_date(input_str: str) -> Optional[datetime.date]:
    """Parse a single date string in many common formats to a date object.

    Supported examples:
    - 2025-03-01, 2025-03-01T12:34:56Z
    - 03/01/2025, 3/1/2025, 03-01-2025
    - March 1, 2025, Mar 1, 2025 (with or without ordinal suffixes)
    - 20250301 (YYYYMMDD), 03012025 (MMDDYYYY)
    """
    if not input_str:
        return None

    s = _strip_ordinal_suffix(str(input_str)).strip()

    # ISO datetime or date (allow within underscores; avoid digit-adjacent)
    iso_dt_match = re.search(r"(?<!\d)\d{4}-\d{2}-\d{2}(?:[T\s]\S+)?(?!\d)", s)
    if iso_dt_match:
        iso_candidate = iso_dt_match.group(0)
        # Normalize common phrasing like "YYYY-MM-DD at HH:MM" to ISO
        iso_candidate = iso_candidate.replace(" at ", "T")
        # Trim trailing punctuation not part of ISO
        iso_candidate = iso_candidate.rstrip(".,);]")
        try:
            if 'T' in iso_candidate or ' ' in iso_candidate:
                # Normalize to date component
                return datetime.datetime.fromisoformat(iso_candidate.replace('Z', '+00:00')).date()
            return datetime.datetime.strptime(iso_candidate, '%Y-%m-%d').date()
        except Exception:
            pass

    # Month name, e.g., March 1, 2025 or Mar 1, 2025
    mn = re.search(r"\b([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})\b", s)
    if mn:
        mon_name, day_str, year_str = mn.groups()
        mon = _MONTHS_MAP.get(mon_name.strip().lower())
        if mon:
            dt = _safe_date(int(year_str), int(mon), int(day_str))
            if dt:
                return dt

    # US numeric with separators: MM/DD/YYYY or M/D/YYYY (also dashes)
    us = re.search(r"\b(\d{1,2})[\/-](\d{1,2})[\/-](\d{2,4})\b", s)
    if us:
        m_str, d_str, y_str = us.groups()
        year = int(y_str)
        if year < 100:
            year += 2000 if year < 70 else 1900
        dt = _safe_date(year, int(m_str), int(d_str))
        if dt:
            return dt

    # Compact YYYYMMDD
    ymd = re.search(r"\b(\d{4})(\d{2})(\d{2})\b", s)
    if ymd:
        y, m, d = ymd.groups()
        dt = _safe_date(int(y), int(m), int(d))
        if dt:
            return dt

    # Compact MMDDYYYY
    mdy = re.search(r"\b(\d{2})(\d{2})(\d{4})\b", s)
    if mdy:
        m, d, y = mdy.groups()
        dt = _safe_date(int(y), int(m), int(d))
        if dt:
            return dt

    return None

def extract_dates_from_text(text: str) -> List[datetime.date]:
    """Extract all recognizable dates from arbitrary text and return unique sorted dates."""
    if not text:
        return []
    # Find potential date substrings by regex windows and parse each
    candidates: List[str] = []
    # Gather various patterns
    candidates += re.findall(r"(?<!\d)\d{4}-\d{2}-\d{2}(?:[T\s]\S+)?(?!\d)", text)
    candidates += re.findall(r"\b\d{1,2}[\/-]\d{1,2}[\/-]\d{2,4}\b", text)
    candidates += re.findall(r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t|tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}\b", text, flags=re.IGNORECASE)
    candidates += re.findall(r"\b\d{8}\b", text)

    dates: List[datetime.date] = []
    seen = set()
    for cand in candidates:
        dt = parse_string_to_date(cand)
        if dt and dt not in seen:
            dates.append(dt)
            seen.add(dt)
    dates.sort()
    return dates

def normalize_data_date(text: str, prefer_recent: bool = True, max_years_back: int = 15) -> Optional[str]:
    """Normalize any date found in text to ISO 'YYYY-MM-DD'.

    - Scans the text for any recognizable date format.
    - Picks the most recent date by default (prefer_recent=True).
    - Ensures date is not in the future and not older than max_years_back.
    """
    today = datetime.date.today()
    earliest = today - datetime.timedelta(days=365 * max_years_back)
    dates = extract_dates_from_text(text)
    if not dates:
        # Try parsing the whole string directly if extract failed
        d = parse_string_to_date(text)
        dates = [d] if d else []
    if not dates:
        return None

    # Filter by reasonableness
    candidates = [d for d in dates if earliest <= d <= today]
    if not candidates:
        return None

    chosen = max(candidates) if prefer_recent else min(candidates)
    return chosen.strftime('%Y-%m-%d')
