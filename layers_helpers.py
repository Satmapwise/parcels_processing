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
from typing import Optional, Dict, Any, Tuple


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


# ---------------------------------------------------------------------------
# Constants and Data Structures
# ---------------------------------------------------------------------------

# Valid state abbreviations for multi-state support
VALID_STATES = {
    'fl': 'FL',  # Florida (primary)
    'ga': 'GA',  # Georgia (future)
    'de': 'DE',  # Delaware (future)
}

# Florida counties set
FL_COUNTIES = {
    "alachua", "baker", "bay", "bradford", "brevard", "broward", "calhoun", "charlotte", "citrus", "clay",
    "collier", "columbia", "desoto", "dixie", "duval", "escambia", "flagler", "franklin", "gadsden", "gilchrist",
    "glades", "gulf", "hamilton", "hardee", "hendry", "hernando", "highlands", "hillsborough", "holmes",
    "indian_river", "jackson", "jefferson", "lafayette", "lake", "lee", "leon", "levy", "liberty", "madison",
    "manatee", "marion", "martin", "miami_dade", "monroe", "nassau", "okaloosa", "okeechobee", "orange", "osceola",
    "palm_beach", "pasco", "pinellas", "polk", "putnam", "santa_rosa", "sarasota", "seminole", "st_johns",
    "st_lucie", "sumter", "suwannee", "taylor", "union", "volusia", "wakulla", "walton", "washington",
}

# Available layers
LAYERS = {
    "zoning", "flu", "fema_flood", "parcel_geo", "streets", "address_points", 
    "subdivisions", "buildings", "traffic_counts", "sunbiz"
}

# Layer configurations with metadata
LAYER_CONFIGS = {
    'zoning': {
        'category': '08_Land_Use_and_Zoning',
        'layer_group': 'flu_zoning',
        'layer_subgroup': 'zoning',
        'level': 'state_county_city',
    },
    'flu': {
        'category': '08_Land_Use_and_Zoning', 
        'layer_group': 'flu_zoning',
        'layer_subgroup': 'flu',
        'level': 'state_county_city',
    },
    'fema_flood': {
        'category': '12_Hazards',
        'layer_group': 'hazards',
        'layer_subgroup': 'fema_flood',
        'level': 'national',
        'entity': 'fema_flood',
    },
    'parcel_geo': {
        'category': '05_Parcels',
        'layer_group': 'parcels',
        'layer_subgroup': 'parcel_geo',
        'level': 'state_county',
    },
    'streets': {
        'category': '03_Transportation',
        'layer_group': 'base_map_overlay',
        'layer_subgroup': 'streets',
        'level': 'state_county',
    },
    'address_points': {
        'category': '05_Parcels',
        'layer_group': 'parcels',
        'layer_subgroup': 'address_points',
        'level': 'state_county',
    },
    'subdivisions': {
        'category': '05_Parcels',
        'layer_group': 'parcels',
        'layer_subgroup': 'subdivisions',
        'level': 'state_county',
    },
    'buildings': {
        'category': '05_Parcels',
        'layer_group': 'parcels',
        'layer_subgroup': 'buildings',
        'level': 'state_county',
    },
    'traffic_counts': {
        'category': '03_Transportation',
        'layer_group': 'base_map_overlay',
        'layer_subgroup': 'traffic_counts',
        'level': 'state',
        'entity': 'traffic_counts_fl',
    },
    'sunbiz': {
        'category': '21_Misc',
        'layer_group': 'misc',
        'layer_subgroup': 'sunbiz',
        'level': 'state',
        'entity': 'sunbiz_fl',
    }
}

# Entities to skip (blacklist) - for layers_scrape.py
SKIP_ENTITIES = {
    "hillsborough_temple_terrace",
    "charlotte_punta_gorda"
}

# Format categories for pipeline control
FULL_PIPELINE_FORMATS = {'ags', 'arcgis', 'esri', 'ags_extract', 'shp', 'zip', 'url'}
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
    
    # Special mappings for layers
    layer_mappings = {
        # internal -> external
        'flu': 'Future Land Use',
        'addr_pnts': 'Address Points',
        'address_points': 'Address Points',
        'bldg_ftpr': 'Building Footprints',
        'buildings': 'Buildings',
        'parcel_geo': 'Parcel Geometry',
        'flood_zones': 'Flood Zones',
        'fdot_tc': 'Traffic Counts FDOT',
        'subdiv': 'Subdivisions',
        'subdivisions': 'Subdivisions',
        'streets': 'Streets',
        'sunbiz': 'SunBiz',
        'zoning': 'Zoning'
    }
    
    # Reverse mapping for external -> internal
    layer_mappings_reverse = {v.lower(): k for k, v in layer_mappings.items()}
    
    # Special county mappings (internal -> external)
    county_special = {
        'miami_dade': 'Miami-Dade',
        'st_johns': 'St. Johns',
        'st_lucie': 'St. Lucie',
        'desoto': 'DeSoto',
        'palm_beach': 'Palm Beach',
        'santa_rosa': 'Santa Rosa',
        'indian_river': 'Indian River'
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
            return county_special_reverse.get(name.lower(), _to_internal_format(name))
    
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
    layer_names = list(LAYERS)
    
    for layer_name in layer_names:
        if remaining.startswith(layer_name):
            layer = layer_name
            remaining = remaining[len(layer_name):].lstrip('_')
            break
    
    if not remaining:
        return (layer, None, None, None)
    
    # Step 2: Extract state
    state = None
    for state_abbrev in VALID_STATES.keys():
        if remaining == state_abbrev or remaining.startswith(state_abbrev + '_'):
            state = state_abbrev
            remaining = remaining[len(state_abbrev):].lstrip('_')
            break
    
    if not remaining:
        return (layer, state, None, None)
    
    # Step 3: Extract county 
    county = None
    # Check FL counties regardless of state (we can infer state from county)
    for county_name in FL_COUNTIES:
        if remaining.startswith(county_name):
            county = county_name
            # If no state was identified yet, infer it from the county
            if state is None:
                state = 'fl'
            remaining = remaining[len(county_name):].lstrip('_')
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
    return VALID_STATES.get(state_internal)


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
        state_name = 'florida' if state.lower() == 'fl' else state.lower()
    
    # Build standardized path: /srv/datascrub/<category>/<layer_subgroup>/<state>/<county>/<city>
    path_parts = ['/srv/datascrub', category, layer_subgroup]
    
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
counties = FL_COUNTIES
layers = LAYERS

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