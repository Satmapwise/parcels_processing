#!/usr/bin/env python3
"""
Geospatial data processing pipeline with clean 4-stage architecture.

Usage: python layers_scrape.py <layer> [entities...] [options]

This script processes geospatial data layers through 4 main stages:
1. Download (layer_download)
2. Metadata Extraction (layer_metadata) 
3. Processing (layer_processing)
4. Upload (layer_upload)

Each stage reads configuration from the database and generates commands dynamically.
"""

import sys
import logging
import argparse
import subprocess
import shapefile  # pyshp
from datetime import datetime, timedelta
import os
import csv
import json
import re
import fnmatch
import psycopg2
import psycopg2.extras
from pathlib import Path

# ---------------------------------------------------------------------------
# Layer Configuration
# ---------------------------------------------------------------------------

# Note: Update scripts are now generated dynamically based on layer name

# Formats that should get full pipeline treatment
FULL_PIPELINE_FORMATS = {'ags', 'arcgis', 'esri', 'ags_extract', 'shp', 'zip', 'url'}

# Formats that should skip processing but still get metadata
METADATA_ONLY_FORMATS = {'pdf'}

# All other formats are excluded from the pipeline entirely

def should_process_entity(catalog_row: dict) -> tuple[bool, str]:
    """Determine if an entity should be processed and why.
    
    Returns:
        tuple[bool, str]: (should_process, reason)
    """
    fmt = (catalog_row.get('format') or '').lower()
    
    if fmt in FULL_PIPELINE_FORMATS:
        return True, f"Format '{fmt}' gets full pipeline treatment"
    elif fmt in METADATA_ONLY_FORMATS:
        return True, f"Format '{fmt}' gets metadata-only treatment"
    else:
        return False, f"Format '{fmt}' is excluded from pipeline"

# Note: should_run_processing logic is now integrated directly into layer_processing function

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
# Database connection
PG_CONNECTION = os.getenv("PG_CONNECTION")

# Entities to skip (blacklist)
SKIP_ENTITIES = {
    "hillsborough_temple_terrace",
    "charlotte_punta_gorda"
}

layers = {
    "zoning",
    "flu",
    "flood_zones",
    "parcel_geo",
    "streets",
    "addr_pnts",
    "subdiv",
    "bldg_ftpr"
    "fdot_tc",
    "sunbiz"
}

# Florida counties set
counties = {
    "miami_dade", "broward", "palm_beach", "hillsborough", "orange", "pinellas", 
    "duval", "lee", "polk", "brevard", "volusia", "pasco", "seminole", "sarasota",
    "manatee", "collier", "osceola", "marion", "lake", "st_lucie", "escambia",
    "leon", "alachua", "st_johns", "clay", "okaloosa", "hernando", "bay",
    "charlotte", "santa_rosa", "martin", "indian_river", "citrus", "sumter",
    "flagler", "highlands", "nassau", "monroe", "putnam", "walton", "columbia",
    "gadsden", "suwannee", "jackson", "hendry", "okeechobee", "levy", "desoto",
    "wakulla", "baker", "bradford", "hardee", "washington", "taylor", "gilchrist",
    "gulf", "union", "hamilton", "jefferson", "lafayette", "liberty", "madison",
    "glades", "calhoun", "dixie", "franklin"
}

# Work directory patterns
WORK_DIR_PATTERNS = {
    'zoning': os.path.join(
        '/srv/datascrub', '08_Land_Use_and_Zoning', 'zoning', 'florida', 
        'county', '{county}', 'current', 'source_data', '{city}'
    ),
    'flu': os.path.join(
        '/srv/datascrub', '08_Land_Use_and_Zoning', 'future_land_use', 'florida',
        'county', '{county}', 'current', 'source_data', '{city}'
    ),
}

# ---------------------------------------------------------------------------
# Configuration Class
# ---------------------------------------------------------------------------

class Config:
    def __init__(self, 
                 test_mode: bool = False,
                 debug: bool = False,
                 isolate_logs: bool = True,
                 run_download: bool = True,
                 run_metadata: bool = True,
                 run_processing: bool = True,
                 run_upload: bool = True,
                 generate_summary: bool = True,
                 process_anyway: bool = False):
        self.test_mode = test_mode
        self.debug = debug
        self.isolate_logs = isolate_logs
        self.start_time = datetime.now()
        
        # Phase toggles
        self.run_download = run_download
        self.run_metadata = run_metadata
        self.run_processing = run_processing
        self.run_upload = run_upload
        
        # Misc behavior flags
        self.generate_summary = generate_summary
        self.process_anyway = process_anyway

# Global config object
CONFIG = Config()

# ---------------------------------------------------------------------------
# Exception Classes
# ---------------------------------------------------------------------------

class LayerProcessingError(Exception):
    """Base exception for all processing errors."""
    def __init__(self, message, layer=None, entity=None):
        super().__init__(message)
        self.layer = layer
        self.entity = entity

    def __str__(self):
        return f"[{self.layer}/{self.entity}] {super().__str__()}"

class DownloadError(LayerProcessingError):
    """Exception for download failures."""
    pass

class ProcessingError(LayerProcessingError):
    """Exception for processing failures."""
    pass

class UploadError(LayerProcessingError):
    """Exception for upload failures."""
    pass

class SkipEntityError(LayerProcessingError):
    """Exception for when an entity should be skipped."""
    pass

# ---------------------------------------------------------------------------
# Utility Functions (Reused from original)
# ---------------------------------------------------------------------------

def initialize_logging(debug=False):
    """Initialize the logging system."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
    logging.info("Logging initialized.")

def setup_entity_logger(layer, entity, work_dir):
    """Set up a dedicated logger for an entity."""
    log_file_path = os.path.join(work_dir, f"{entity}.log")
    
    logger = logging.getLogger(f"{layer}.{entity}")
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    if logger.hasHandlers():
        logger.handlers.clear()

    if CONFIG.isolate_logs:
        os.makedirs(work_dir, exist_ok=True)
        file_handler = logging.FileHandler(log_file_path, mode='w')
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)

    if not CONFIG.isolate_logs:
        console_handler = logging.StreamHandler(sys.stdout)
        console_level = logging.DEBUG if CONFIG.debug else logging.INFO
        console_handler.setLevel(console_level)
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
    return logger

def split_entity(entity: str):
    """Return (county, city) parts for an entity identifier."""
    for county in sorted(counties, key=len, reverse=True):
        if entity == county:
            return county, ''
        prefix = f"{county}_"
        if entity.startswith(prefix):
            city = entity[len(prefix):]
            return county, city
    raise ValueError(f"Unable to parse county/city from entity '{entity}'.")

def resolve_work_dir(layer: str, entity: str):
    """Return (work_dir, county, city) for layer/entity."""
    # Handle special business logic cases
    if layer == 'zoning' and entity == 'duval_unified':
        # Duval Unified refers to Jacksonville city-county government
        county, city = 'duval', 'jacksonville'
        work_dir = '/srv/datascrub/08_Land_Use_and_Zoning/zoning/florida/county/duval/current/source_data/jacksonville'
        return work_dir, county, city

    # General case
    template = WORK_DIR_PATTERNS.get(layer, os.path.join('/srv/datascrub', '{layer}', '{county}', '{city}'))
    needs_city = '{city}' in template
    
    if needs_city:
        county, city = split_entity(entity)
    else:
        county, city = entity, ''

    work_dir = template.format(layer=layer, county=county, city=city)
    return work_dir, county, city

def _run_command(command, work_dir, logger):
    """Run a shell command in a specified directory."""
    if CONFIG.test_mode:
        logger.info(f"[TEST MODE] COMMAND SKIPPED IN {work_dir}: {' '.join(command)}")
        return
    
    logger.debug(f"Running command in {work_dir}: {' '.join(command)}")
    
    process = subprocess.run(command, cwd=work_dir, capture_output=True, text=True)

    # Handle download_data.py no-new-data conditions
    if len(command) > 1 and 'download_data.py' in command[1]:
        if process.returncode == 1:
            if CONFIG.process_anyway:
                logger.warning("download_data.py returned exit code 1 - no new data available, but continuing due to process_anyway=True")
                return process.stdout
            else:
                logger.info("download_data.py returned exit code 1 - no new data available - skipping entity")
                raise SkipEntityError("No new data available from server", layer=None, entity=None)
        elif process.returncode == 0:
            stdout_lower = process.stdout.lower()
            if any(phrase in stdout_lower for phrase in [
                '304 not modified', 'not modified on server', 'omitting download', 
                'no new data available from server'
            ]):
                if CONFIG.process_anyway:
                    logger.warning("download_data.py indicates no new data available, but continuing due to process_anyway=True")
                else:
                    logger.info("download_data.py indicates no new data available - skipping entity")
                    raise SkipEntityError("No new data available from server", layer=None, entity=None)

    if process.returncode != 0:
        # Check for deprecated URL/service errors before general failure
        error_output = f"{process.stdout}\n{process.stderr}".lower()
        
        # Detect deprecated/dead service URLs
        deprecated_patterns = [
            'service not started',
            'could not retrieve layer metadata', 
            'esridownloaderror',
            'authentication required',
            'login required',
            'service unavailable',
            'service disabled',
            'access denied',
            'unauthorized'
        ]
        
        if any(pattern in error_output for pattern in deprecated_patterns):
            logger.error(f"Deprecated/inaccessible URL detected for command: {' '.join(command)}")
            logger.error(f"Error indicators: {process.stderr}")
            raise DownloadError(f"URL appears to be deprecated or inaccessible: {process.stderr.strip()}", layer=None, entity=None)
        
        # General command failure
        logger.error(f"Error executing command: {' '.join(command)}")
        logger.error(f"STDOUT: {process.stdout}")
        logger.error(f"STDERR: {process.stderr}")
        raise ProcessingError(f"Command failed with exit code {process.returncode}: {' '.join(command)}")
    
    logger.debug(f"Command output: {process.stdout}")
    return process.stdout

def _run_source_comments(source_comments: str, work_dir: str, logger):
    """Run source_comments commands (pre-metadata processing from manifest).
    
    source_comments format: "[command1] [command2] [command3]"
    """
    if not source_comments or not source_comments.strip():
        return
    
    # Split commands by bracketed format: [cmd1] [cmd2] -> ['cmd1', 'cmd2']
    import re
    commands = re.findall(r'\[([^\]]+)\]', source_comments.strip())
    if not commands:
        # Fallback to old pipe format for backwards compatibility
        commands = [cmd.strip() for cmd in source_comments.split('|') if cmd.strip()]
    
    for i, cmd_str in enumerate(commands):
        logger.debug(f"Running source comment command {i+1}/{len(commands)}: {cmd_str}")
        
        if CONFIG.test_mode:
            logger.info(f"[TEST MODE] SOURCE COMMAND SKIPPED IN {work_dir}: {cmd_str}")
            continue
        
        # Handle different command types
        if cmd_str.endswith('.py'):
            # Python script - run with python3
            command = ['python3', cmd_str]
        else:
            # Shell command - run through shell
            command = ['bash', '-c', cmd_str]
        
        try:
            process = subprocess.run(command, cwd=work_dir, capture_output=True, text=True)
            
            if process.returncode != 0:
                logger.warning(f"Source comment command failed: {cmd_str}")
                logger.warning(f"STDERR: {process.stderr}")
                # Continue with other commands rather than failing completely
            else:
                logger.debug(f"Source comment command succeeded: {cmd_str}")
                if process.stdout:
                    logger.debug(f"Command output: {process.stdout}")
                    
        except Exception as e:
            logger.warning(f"Failed to execute source comment command '{cmd_str}': {e}")
            # Continue with other commands

# ---------------------------------------------------------------------------
# Database Functions
# ---------------------------------------------------------------------------

def _fetch_catalog_row(layer: str, county: str, city: str):
    """Return catalog row for the given layer/county/city or None if missing."""
    conn = psycopg2.connect(PG_CONNECTION)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        # Convert internal format names to external format for database query
        layer_external = format_name(layer, 'layer', external=True)
        county_external = format_name(county, 'county', external=True)
        city_external = format_name(city, 'city', external=True)
        
        sql = (
            "SELECT * FROM m_gis_data_catalog_main "
            "WHERE lower(layer_subgroup) = %s "
            "AND lower(county) = %s "
            "AND lower(city) = %s LIMIT 1"
        )
        params = (layer_external.lower(), county_external.lower(), city_external.lower())
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        cur.close()
        conn.close()

def _debug_main(message: str, logger):
    """Log main function debug messages to console when --debug is enabled, otherwise to entity logger."""
    if CONFIG.debug:
        logging.info(message)
    else:
        logger.debug(message)

def _get_existing_data_date(layer: str, entity: str) -> str:
    """Get the existing data_date for an entity from the CSV file."""
    summary_filename = f"{layer}_summary.csv"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    summary_filepath = os.path.join(script_dir, summary_filename)
    
    if not os.path.exists(summary_filepath):
        return None
    
    try:
        county, city = split_entity(entity)
        entity_key = f"{county}_{city}"
        
        with open(summary_filepath, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Skip summary rows
                if row.get('county', '').startswith('LAST UPDATED:'):
                    continue
                if f"{row.get('county', '')}_{row.get('city', '')}" == entity_key:
                    return row.get('data_date', '')
        return None
    except Exception:
        return None

def _fetch_entities_from_db(layer: str) -> list[str]:
    """Return list of entity strings for layer from database."""
    entities = []
    conn = psycopg2.connect(PG_CONNECTION)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        sql = (
            "SELECT county, city FROM m_gis_data_catalog_main "
            "WHERE status IS DISTINCT FROM 'DELETE' "
            "AND lower(layer_subgroup) = %s"
        )
        cur.execute(sql, (layer.lower(),))
        rows = cur.fetchall()
        for row in rows:
            entity = _entity_from_parts(row['county'], row['city'])
            entities.append(entity)
    except Exception as exc:
        logging.error(f"DB entity fetch failed: {exc}")
    finally:
        cur.close()
        conn.close()
    return list(dict.fromkeys(entities))  # de-dupe preserving order

def _entity_from_parts(county: str, city: str | None) -> str:
    """Return entity id from raw DB county/city values."""
    # Convert external DB values to internal format
    county_internal = format_name(county, 'county', external=False)
    city_internal = format_name(city, 'city', external=False) if city else ""
    
    if not city_internal:
        return county_internal
    return f"{county_internal}_{city_internal}"

def _parse_processing_comments(text):
    """Parse the processing_comments field into a list of command strings.
    
    Supports formats:
    1. Bracketed: "[command1] [command2] [command3]"
    2. JSON array: ["command1", "command2", "command3"]
    3. Legacy: newlines/semicolons
    """
    if not text:
        return []
    
    # Try bracketed format first: [cmd1] [cmd2] -> ['cmd1', 'cmd2']
    bracketed_commands = re.findall(r'\[([^\]]+)\]', text.strip())
    if bracketed_commands:
        return [cmd.strip() for cmd in bracketed_commands if cmd.strip()]
    
    # Try JSON array format
    try:
        data = json.loads(text)
        if isinstance(data, list):
            return [str(cmd).strip() for cmd in data if str(cmd).strip()]
    except Exception:
        pass
    
    # Fallback – split on newlines/semicolons
    commands = []
    for piece in re.split(r'[\n;]+', text):
        piece = piece.strip()
        if piece:
            commands.append(piece)
    return commands

# ---------------------------------------------------------------------------
# Main Pipeline Functions
# ---------------------------------------------------------------------------

def layer_download(layer: str, entity: str, county: str, city: str, catalog_row: dict, work_dir: str, logger):
    """Handle the download phase for an entity."""
    if not CONFIG.run_download:
        logger.debug(f"[DOWNLOAD] Skipping download for {layer}/{entity} (disabled in config)")
        return None

    fmt = (catalog_row.get('format') or '').lower()
    resource = catalog_row.get('resource') or catalog_row.get('src_url_file')
    table_name = catalog_row.get('table_name')

    # Simple AGS vs non-AGS distinction for tool selection
    if fmt == 'ags':
        # Use ags_extract_data2.py for ArcGIS Server services
        if not table_name:
            raise DownloadError('Missing table_name for AGS download', layer, entity)
        command = [
            'python3',
            os.path.join(os.path.dirname(__file__), 'download_tools', 'ags_extract_data2.py'),
            table_name,
            'delete',
            '15'
        ]
        _debug_main(f"[DOWNLOAD] Running AGS download for {layer}/{entity} (table: {table_name})", logger)
    else:
        # Use download_data.py for all other formats (SHP, CSV, PDF, etc.)
        # Most non-AGS downloads are ZIP files containing shapefiles
        if not resource:
            raise DownloadError('Missing resource/url for download_data.py', layer, entity)
        command = [
            'python3',
            os.path.join(os.path.dirname(__file__), 'download_tools', 'download_data.py'),
            resource
        ]
        _debug_main(f"[DOWNLOAD] Running file download for {layer}/{entity} (format: {fmt}, url: {resource})", logger)

    logger.debug(f"Running download for {layer}/{entity}")
    
    # Capture directory state before download for validation
    before_state = _get_directory_state(work_dir)
    
    try:
        _run_command(command, work_dir, logger)
    except SkipEntityError as e:
        # Handle "no new data" case - from download command
        _update_csv_status(layer, entity, 'download', 'NND', error_msg='Download command: no new data')
        raise  # Re-raise skip errors
    
    # Run source_comments commands (pre-metadata processing)
    source_comments = catalog_row.get('source_comments', '')
    if source_comments and source_comments.strip():
        _debug_main(f"[DOWNLOAD] Running source_comments for {layer}/{entity}: {source_comments}", logger)
        try:
            _run_source_comments(source_comments, work_dir, logger)
        except Exception as sc_err:
            logger.warning(f"Source comments failed for {layer}/{entity}: {sc_err}")
            # Don't fail the entire download for source comments errors
    
    # Validate download occurred (only in non-test mode)
    if not CONFIG.test_mode:
        try:
            _validate_download(work_dir, logger, before_state)
            
            # Additional validation for AGS downloads - check for empty/corrupt files
            if fmt in {'ags', 'arcgis', 'esri', 'ags_extract'}:
                _validate_ags_download(work_dir, table_name, logger)
            
            _debug_main(f"[DOWNLOAD] Download validation passed for {layer}/{entity}", logger)
            _update_csv_status(layer, entity, 'download', 'SUCCESS')
        except DownloadError as de:
            _update_csv_status(layer, entity, 'download', 'FAILED', str(de))
            raise DownloadError(str(de), layer, entity) from de
        
        # Find and return newest zip file if any
        try:
            zip_file = _find_latest_zip(work_dir, logger)
            if zip_file:
                _debug_main(f"[DOWNLOAD] Found zip file: {zip_file}", logger)
            return zip_file
        except Exception as z_err:
            logger.debug(f"Zip detection failed: {z_err}")
            return None
    else:
        logger.info(f"[TEST MODE] Skipping download validation for {layer}/{entity}")
        _update_csv_status(layer, entity, 'download', 'SUCCESS')  # Assume success in test mode
        return None

def extract_basic_file_metadata(work_dir: str, logger) -> dict:
    """Extract basic metadata from downloaded files (for non-shapefile formats like PDF)."""
    metadata = {}
    
    try:
        files = [f for f in os.listdir(work_dir) if os.path.isfile(os.path.join(work_dir, f))]
        if not files:
            return metadata
        
        # Find the largest file (likely the main data file)
        largest_file = None
        largest_size = 0
        
        for filename in files:
            file_path = os.path.join(work_dir, filename)
            file_size = os.path.getsize(file_path)
            if file_size > largest_size:
                largest_size = file_size
                largest_file = filename
        
        if largest_file:
            file_path = os.path.join(work_dir, largest_file)
            
            # Use conservative PDF metadata extraction for PDF files
            data_date = None
            if largest_file.lower().endswith('.pdf'):
                pdf_metadata = _extract_pdf_metadata_conservative(file_path, logger)
                if pdf_metadata:
                    data_date = pdf_metadata['data_date']
                    logger.debug(f"PDF metadata extracted using {pdf_metadata['method']}: {data_date}")
                else:
                    # No reasonable date found - don't fake freshness
                    logger.warning(f"No reasonable data date found for PDF: {largest_file}")
            else:
                # For non-PDF files, fall back to file modification time
                stat = os.stat(file_path)
                data_date = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d')
                logger.debug(f"Using file modification time for non-PDF: {data_date}")
            
            metadata = {
                'shp': largest_file,  # Store filename in shp field for consistency
                'epsg': '',  # Not available for non-geospatial files
                'field_names': '[]',  # Not available for non-structured files
                'file_size': largest_size
            }
            
            # Only add data_date if we found a reasonable one
            if data_date:
                metadata['data_date'] = data_date
                logger.debug(f"Basic metadata extracted from: {largest_file} (size: {largest_size} bytes, date: {data_date})")
            else:
                logger.debug(f"Basic metadata extracted from: {largest_file} (size: {largest_size} bytes, no data_date)")
            
    except Exception as e:
        logger.warning(f"Failed to extract basic file metadata: {e}")
    
    return metadata

def layer_metadata(layer: str, entity: str, county: str, city: str, catalog_row: dict, work_dir: str, logger):
    """Handle the metadata extraction phase for an entity."""
    if not CONFIG.run_metadata:
        logger.debug(f"[METADATA] Skipping metadata extraction for {layer}/{entity} (disabled in config)")
        return {}

    _debug_main(f"[METADATA] Extracting metadata for {layer}/{entity}", logger)
    
    fmt = (catalog_row.get('format') or '').lower()
    
    # Handle different formats differently
    if fmt in METADATA_ONLY_FORMATS:
        # For PDF and other non-geospatial formats, extract basic file metadata
        logger.debug(f"Extracting basic file metadata for format: {fmt}")
        metadata = extract_basic_file_metadata(work_dir, logger)
        
        if metadata:
            _debug_main(f"[METADATA] Basic metadata extracted: file:{metadata.get('shp', 'unknown')}, date:{metadata.get('data_date', 'unknown')}", logger)
        else:
            logger.warning("No basic metadata could be extracted")
            
        return metadata
        
    else:
        # For geospatial formats, try to find and process shapefiles
        shp_to_process = None
        try:
            shp_to_process = _find_shapefile(work_dir, logger)
        except DownloadError:
            logger.warning("No shapefile found for metadata extraction")
            return {}

        if shp_to_process:
            metadata = extract_shp_metadata(shp_to_process, logger)
            logger.debug(f"Metadata extracted from: {os.path.basename(shp_to_process)}")
            
            # Log key metadata extracted
            epsg = metadata.get('epsg', 'Unknown')
            data_date = metadata.get('data_date', 'Unknown')
            field_count = len(json.loads(metadata.get('field_names', '[]')))
            _debug_main(f"[METADATA] Extracted: EPSG:{epsg}, data_date:{data_date}, {field_count} fields", logger)
            
            # Normalize data_date to ensure it's not later than today
            if metadata.get('data_date'):
                today = datetime.now().strftime('%Y-%m-%d')
                if metadata['data_date'] > today:
                    logger.warning(f"Data date {metadata['data_date']} is later than today {today}, setting data_date to today")
                    metadata['data_date'] = today
            
            # Check if data_date matches existing CSV data (NND detection)
            try:
                existing_data_date = _get_existing_data_date(layer, entity)
                new_data_date = metadata.get('data_date')
                if existing_data_date and new_data_date == existing_data_date:
                    raise SkipEntityError("No new data available (data date unchanged)", layer=layer, entity=entity)
            except SkipEntityError:
                raise  # Re-raise SkipEntityError
            except Exception as e:
                logger.debug(f"Could not check existing data date: {e}")
            
            return metadata
        else:
            logger.warning("No shapefile found to process for metadata extraction")
            return {}

def layer_processing(layer: str, entity: str, county: str, city: str, catalog_row: dict, work_dir: str, logger):
    """Handle the processing phase for an entity."""
    if not CONFIG.run_processing:
        logger.debug(f"[PROCESSING] Skipping processing for {layer}/{entity} (disabled in config)")
        _update_csv_status(layer, entity, 'processing', 'SKIPPED', error_msg='Processing disabled in config')
        return

    fmt = (catalog_row.get('format') or '').lower()
    
    # Skip processing for metadata-only formats (like PDF)
    if fmt in METADATA_ONLY_FORMATS:
        reason = f"Format '{fmt}' skips processing stage"
        logger.info(f"[PROCESSING] Skipping processing for {layer}/{entity}: {reason}")
        _update_csv_status(layer, entity, 'processing', 'SKIPPED', error_msg=reason)
        return

    _debug_main(f"[PROCESSING] Starting processing for {layer}/{entity}", logger)
    logger.debug(f"Running processing for {layer}/{entity}")

    # 1. Run pre-processing commands from database
    processing_commands = _parse_processing_comments(catalog_row.get('processing_comments'))
    if processing_commands:
        logger.debug(f"[PROCESSING] Running {len(processing_commands)} pre-processing commands")
        for cmd_str in processing_commands:
            command = cmd_str.split() if isinstance(cmd_str, str) else cmd_str
            _run_command(command, work_dir, logger)

    # 2. Run layer-specific update script (dynamic generation)
    proc_dir = os.path.join(os.path.dirname(__file__), 'processing_tools')
    
    if layer == 'zoning' and os.path.exists(os.path.join(proc_dir, 'update_zoning2.py')):
        command = ['python3', os.path.join(proc_dir, 'update_zoning2.py'), county, city]
        script_name = 'update_zoning2.py'
    else:
        update_script = os.path.join(proc_dir, f'update_{layer}.py')
        if os.path.exists(update_script):
            command = ['python3', update_script, county, city]
            script_name = f'update_{layer}.py'
        else:
            reason = f"No update script found for layer '{layer}'"
            logger.info(f"[PROCESSING] Skipping processing for {layer}/{entity}: {reason}")
            _update_csv_status(layer, entity, 'processing', 'SKIPPED', error_msg=reason)
            return

    _debug_main(f"[PROCESSING] Running update script: {script_name}", logger)
    logger.debug(f"Running update script for {layer}/{entity}")
    try:
        _run_command(command, work_dir, logger)
        _update_csv_status(layer, entity, 'processing', 'SUCCESS')
        _debug_main(f"[PROCESSING] Processing completed for {layer}/{entity}", logger)
    except Exception as e:
        _update_csv_status(layer, entity, 'processing', 'FAILED', str(e))
        raise ProcessingError(f"Processing failed: {e}", layer, entity) from e

def layer_upload(layer: str, entity: str, county: str, city: str, catalog_row: dict, work_dir: str, logger, metadata: dict, raw_zip_name: str = None):
    """Handle the upload phase for an entity."""
    if not CONFIG.run_upload:
        logger.debug(f"[UPLOAD] Skipping upload for {layer}/{entity} (disabled in config)")
        return

    _debug_main(f"[UPLOAD] Updating catalog metadata for {layer}/{entity}", logger)

    fmt = (catalog_row.get('format') or '').lower()
    
    # Build SQL dynamically based on available metadata
    # Always update publish_date
    publish_date = datetime.now().strftime('%Y-%m-%d')
    
    # Start building SET clauses with fields we always want to update
    set_clauses = []
    placeholders = {
        'layer': layer,
        'county': county.lower().replace('_', ' '),
        'city': format_name(city, 'city', external=True),
        'publish_date': publish_date,
    }
    
    # Always update publish_date
    set_clauses.append("publish_date = '{publish_date}'")
    
    # Add metadata fields only if they have meaningful values
    if metadata.get('data_date'):
        set_clauses.append("data_date = '{data_date}'")
        placeholders['data_date'] = metadata['data_date']
    
    if metadata.get('epsg'):
        set_clauses.append("srs_epsg = '{epsg}'")
        placeholders['epsg'] = metadata['epsg']
    
    if metadata.get('shp'):
        set_clauses.append("sys_raw_file = '{shp}'")
        placeholders['shp'] = metadata['shp']
    
    if metadata.get('field_names'):
        set_clauses.append("field_names = '{field_names}'")
        placeholders['field_names'] = metadata['field_names']
    
    # Add raw_zip field for non-AGS formats if zip file exists
    if fmt not in {'ags', 'arcgis', 'esri', 'ags_extract'} and raw_zip_name:
        set_clauses.append("sys_raw_file_zip = '{raw_zip}'")
        placeholders['raw_zip'] = raw_zip_name
    
    # Build final SQL
    sql_update = (
        "UPDATE m_gis_data_catalog_main SET "
        + ", ".join(set_clauses) + " "
        "WHERE layer_subgroup = '{layer}' "
        "AND county = '{county}' "
        "AND city = '{city}';"
    )
    
    # Log what fields will be updated
    updating_fields = [clause.split(' = ')[0] for clause in sql_update.split('SET ')[1].split(' WHERE')[0].split(', ')]
    logger.debug(f"Updating fields: {', '.join(updating_fields)}")
    logger.debug(f"Upload placeholders - data_date: {metadata.get('data_date', 'not_set')}, publish_date: {publish_date}")

    # Substitute placeholders
    final_sql = sql_update.format(**placeholders)
    
    command = ['psql', '-d', 'gisdev', '-U', 'postgres', '-c', final_sql]
    
    logger.debug(f"Running upload command for {layer}/{entity}")
    try:
        _run_command(command, work_dir, logger)
        data_date = metadata.get('data_date', publish_date)
        _update_csv_status(layer, entity, 'upload', 'SUCCESS', data_date=data_date)
        _debug_main(f"[UPLOAD] Catalog metadata updated successfully for {layer}/{entity}", logger)
    except Exception as e:
        _update_csv_status(layer, entity, 'upload', 'FAILED', str(e))
        raise UploadError(f"Upload failed: {e}", layer, entity) from e

# ---------------------------------------------------------------------------
# Helper Functions (from original script)
# ---------------------------------------------------------------------------

def _get_directory_state(work_dir):
    """Get snapshot of directory state (filenames and modification times)."""
    state = {}
    try:
        for filename in os.listdir(work_dir):
            file_path = os.path.join(work_dir, filename)
            try:
                mtime = os.path.getmtime(file_path)
                state[filename] = mtime
            except OSError:
                continue
    except OSError:
        pass
    return state

def _extract_date_from_filename(pdf_path):
    """Extract date from PDF filename using common patterns."""
    filename = Path(pdf_path).stem.lower()
    
    patterns = [
        r'(\d{4}[-_]\d{2}[-_]\d{2})',  # YYYY-MM-DD or YYYY_MM_DD
        r'(\d{2}[-_]\d{2}[-_]\d{4})',  # MM-DD-YYYY or MM_DD_YYYY
        r'(\d{4})',  # Just year (will use Jan 1st)
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            date_str = match.group(1)
            try:
                # Try different date formats
                if len(date_str) == 4:  # Just year
                    return f"{date_str}-01-01"
                elif '-' in date_str or '_' in date_str:
                    normalized = date_str.replace('_', '-')
                    if len(normalized.split('-')[0]) == 4:  # YYYY-MM-DD
                        return normalized
                    else:  # MM-DD-YYYY, convert to YYYY-MM-DD
                        parts = normalized.split('-')
                        return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
            except (ValueError, IndexError):
                continue
    
    return None

def _is_reasonable_date(date_str):
    """Check if date is within reasonable range (last 10 years to last week)."""
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        now = datetime.now()
        ten_years_ago = now - timedelta(days=365*10)
        one_week_ago = now - timedelta(days=7)
        
        return ten_years_ago <= date_obj <= one_week_ago
    except (ValueError, TypeError):
        return False

def _extract_pdf_metadata_conservative(pdf_path, logger):
    """
    Extract metadata from PDF using conservative approach.
    
    Strategy:
    1. Try filename pattern extraction
    2. Try file modification time (but only if reasonable)
    3. Return None if no reasonable date found (avoids fake freshness)
    
    Returns dict with data_date and method, or None if no reasonable date found.
    """
    logger.debug(f"Extracting PDF metadata from: {pdf_path}")
    
    # 1. Try filename patterns first
    filename_date = _extract_date_from_filename(pdf_path)
    if filename_date and _is_reasonable_date(filename_date):
        logger.debug(f"Found reasonable date from filename: {filename_date}")
        return {'data_date': filename_date, 'method': 'filename_pattern'}
    
    # 2. Try file modification time as last resort
    try:
        file_mtime = os.path.getmtime(pdf_path)
        file_date = datetime.fromtimestamp(file_mtime).strftime('%Y-%m-%d')
        
        if _is_reasonable_date(file_date):
            logger.debug(f"Found reasonable date from file mtime: {file_date}")
            return {'data_date': file_date, 'method': 'file_modification'}
    except OSError:
        pass
    
    # 3. Conservative fallback: return None instead of current date
    # This prevents showing PDFs as "fresh" when they're actually stale
    logger.warning(f"No reasonable data date found for PDF: {pdf_path}")
    return None

def _validate_ags_download(work_dir, table_name, logger):
    """Validate AGS download by checking GeoJSON file content."""
    geojson_file = os.path.join(work_dir, f"{table_name}.geojson")
    
    if not os.path.exists(geojson_file):
        raise DownloadError(f"Expected GeoJSON file not found: {table_name}.geojson", layer=None, entity=None)
    
    try:
        # Check file size (empty or very small files are likely corrupted)
        file_size = os.path.getsize(geojson_file)
        if file_size < 100:  # Less than 100 bytes is likely just headers
            raise DownloadError(f"GeoJSON file appears empty or corrupted (size: {file_size} bytes)", layer=None, entity=None)
        
        # Check if it's valid JSON and has features
        with open(geojson_file, 'r') as f:
            try:
                data = json.load(f)
                
                # Check basic GeoJSON structure
                if not isinstance(data, dict) or data.get('type') != 'FeatureCollection':
                    raise DownloadError("GeoJSON file has invalid structure (not a FeatureCollection)", layer=None, entity=None)
                
                # Check if features array exists and has content
                features = data.get('features', [])
                if not isinstance(features, list):
                    raise DownloadError("GeoJSON file has invalid features array", layer=None, entity=None)
                
                if len(features) == 0:
                    raise DownloadError("GeoJSON file contains no features (likely deprecated/inaccessible URL)", layer=None, entity=None)
                
                logger.debug(f"AGS validation passed: {len(features)} features found in {table_name}.geojson")
                
            except json.JSONDecodeError as e:
                raise DownloadError(f"GeoJSON file is corrupted (JSON decode error): {e}", layer=None, entity=None)
                
    except (OSError, IOError) as e:
        raise DownloadError(f"Could not read GeoJSON file: {e}", layer=None, entity=None)

def _validate_download(work_dir, logger, before_state=None):
    """Validate that a download occurred by comparing directory state."""
    if before_state is not None:
        current_state = _get_directory_state(work_dir)
        changed_files = []
        
        for filename, current_mtime in current_state.items():
            if filename not in before_state:
                changed_files.append(f"{filename} (new)")
            elif current_mtime != before_state[filename]:
                changed_files.append(f"{filename} (modified)")
        
        if not changed_files:
            if CONFIG.process_anyway:
                logger.warning("No files changed during download, but continuing due to process_anyway=True")
                return True
            raise DownloadError("No files changed during download", layer=None, entity=None)
        
        changed_files_str = ", ".join(changed_files[:3])
        if len(changed_files) > 3:
            changed_files_str += f" and {len(changed_files) - 3} more"
        
        logger.debug(f"Download validation passed - found changed files: {changed_files_str}")
        return True
    else:
        # Fallback to 24-hour check
        now = datetime.now()
        day_ago = now - timedelta(days=1)

        recent_downloads = []
        for filename in os.listdir(work_dir):
            file_path = os.path.join(work_dir, filename)
            try:
                mtime = os.path.getmtime(file_path)
                mod_time = datetime.fromtimestamp(mtime)
                if mod_time >= day_ago:
                    recent_downloads.append((mod_time, filename))
            except OSError:
                continue

        if not recent_downloads:
            if CONFIG.process_anyway:
                logger.warning("No files found modified within the last 24 hours, but continuing due to process_anyway=True")
                return True
            raise DownloadError("No files found modified within the last 24 hours", layer=None, entity=None)

        if recent_downloads:
            recent_files_str = ", ".join([f[1] for f in sorted(recent_downloads, key=lambda x: x[0], reverse=True)[:3]])
            logger.debug(f"Download validation passed – found recent files: {recent_files_str}")
        
        return True

def _find_shapefile(work_dir, logger):
    """Find the most recent shapefile in work_dir."""
    candidate_files = []
    for filename in os.listdir(work_dir):
        if filename.lower().endswith(".shp"):
            logger.debug(f"Found shapefile: {filename}")
            file_path = os.path.join(work_dir, filename)
            try:
                mtime = os.path.getmtime(file_path)
                mod_time = datetime.fromtimestamp(mtime)
                candidate_files.append((mod_time, file_path))
            except OSError:
                continue

    if not candidate_files:
        raise DownloadError("No shapefile found in directory.", layer=None, entity=None)

    candidate_files.sort(key=lambda x: x[0], reverse=True)
    newest_shp_path = candidate_files[0][1]

    logger.debug(f"Using shapefile: {os.path.basename(newest_shp_path)} from {candidate_files[0][0].strftime('%Y-%m-%d %H:%M')}")
    return newest_shp_path

def _find_latest_zip(work_dir, logger):
    """Return the basename of the newest *.zip file in work_dir or None if none exist."""
    zips = []
    for fname in os.listdir(work_dir):
        if fname.lower().endswith('.zip'):
            path = os.path.join(work_dir, fname)
            try:
                mtime = os.path.getmtime(path)
                zips.append((mtime, fname))
            except OSError:
                continue
    if not zips:
        return None
    zips.sort(key=lambda t: t[0], reverse=True)
    newest = zips[0][1]
    logger.debug(f"Detected newest zip file: {newest}")
    return newest

def extract_shp_metadata(shp_path, logger):
    """Return metadata for a shapefile including EPSG code, data date, and field names."""
    metadata = {}

    # Resolve the actual shapefile path
    resolved_path = None
    if os.path.isdir(shp_path):
        candidates = [f for f in os.listdir(shp_path) if f.lower().endswith(".shp")]
        if candidates:
            resolved_path = os.path.join(shp_path, candidates[0])
    elif os.path.isfile(shp_path):
        resolved_path = shp_path
    else:
        parent = os.path.dirname(shp_path) or "."
        if os.path.isdir(parent):
            candidates = [f for f in os.listdir(parent) if f.lower().endswith(".shp")]
            if candidates:
                resolved_path = os.path.join(parent, candidates[0])

    if resolved_path is None or not os.path.exists(resolved_path):
        logger.warning(f"Shapefile not found for metadata extraction: {shp_path}")
        return metadata

    metadata["shp"] = os.path.basename(resolved_path)

    try:
        result = subprocess.run(
            ["ogrinfo", "-ro", "-al", "-so", resolved_path],
            capture_output=True,
            text=True,
            check=True,
        )
        logger.debug(f"OGRINFO output: {result.stdout}")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.warning(f"ogrinfo failed while reading {resolved_path}: {e}")
        return metadata

    # Extract EPSG code from WKT
    projcs_match = re.search(
        r'^(?:\s*)(PROJCS|GEOGCS|PROJCRS|GEOGCRS)\["([^\"]+)"',
        result.stdout,
        re.MULTILINE,
    )
    if projcs_match:
        srs_type, srs_name = projcs_match.groups()
        canonical_name = re.sub(r'[^a-z0-9]+', '_', srs_name.lower()).strip('_')

        name_to_epsg = {
            "gcs_wgs_1984": "4326",
            "wgs_84": "4326",
            "wgs_84_pseudo_mercator": "3857",
            "nad_1983_stateplane_florida_east_fips_0901_feet": "2236",
            "nad_1983_stateplane_florida_west_fips_0902_feet": "2237",
            "nad_1983_stateplane_florida_north_fips_0903_feet": "2238",
            "nad83_harn_florida_east_ftus": "2881",
            "nad83_harn_florida_west_ftus": "2882",
            "nad_1983_2011_stateplane_florida_west_fips_0902_ft_us": "6443",
            "nad83_florida_east_ftus": "2236",
            "nad83_florida_west_ftus": "2237",
            "nad83_florida_north_ftus": "2238",
        }

        if canonical_name in name_to_epsg:
            metadata["epsg"] = name_to_epsg[canonical_name]
            logger.debug(f"Mapped {srs_type} name '{srs_name}' to EPSG:{metadata['epsg']}")
        else:
            logger.debug(f"SRS name '{srs_name}' not in lookup table; unable to map to EPSG.")

    # Extract data date (simplified version - could expand with full logic from original)
    today = datetime.now().date()
    data_date = today  # Default fallback
    
    # Try DBF_DATE_LAST_UPDATE
    m = re.search(r"DBF_DATE_LAST_UPDATE=([0-9]{4}-[0-9]{2}-[0-9]{2})", result.stdout)
    if m:
        try:
            candidate_date = datetime.strptime(m.group(1), "%Y-%m-%d").date()
            if candidate_date <= today:
                data_date = candidate_date
        except ValueError:
            pass

    metadata["data_date"] = data_date.strftime("%Y-%m-%d")
    metadata["update_date"] = datetime.now().date().strftime("%Y-%m-%d")

    # Extract field names
    field_names = []
    try:
        sf = shapefile.Reader(resolved_path)
        field_names = [f[0] for f in sf.fields[1:]]  # skip deletion flag
        logger.debug(f"Extracted {len(field_names)} field names via pyshp: {field_names}")
    except Exception as e:
        logger.debug(f"pyshp field extraction failed ({e})")

    metadata["field_names"] = json.dumps(field_names) if field_names else "[]"

    return metadata

# ---------------------------------------------------------------------------
# Queue Management Functions
# ---------------------------------------------------------------------------

def set_queue(layer, entities):
    """Validate layer and entity list and return a queue of entities to process."""
    logging.info(f"Setting queue for layer '{layer}' and entities '{entities or 'all'}'")

    # Get entities from database
    layer_entities = set(_fetch_entities_from_db(layer))
    
    if not layer_entities and not entities:
        raise ValueError("No entities found in database and no entities supplied – cannot determine processing queue.")

    # No entities supplied → process ALL from database
    if not entities:
        logging.info(f"No entities specified, queuing all {len(layer_entities)} entities for layer '{layer}'")
        queue = sorted(layer_entities)
    else:
        # Process specific entities
        if isinstance(entities, str):
            entities = [entities]

        invalid = [e for e in entities if '*' not in e and '?' not in e and e not in layer_entities and e not in counties]
        if invalid:
            raise ValueError(f"Invalid entity/ies specified: {invalid}")

        # Expand wildcard patterns
        expanded = []
        for pattern in entities:
            if '*' in pattern or '?' in pattern:
                matches = fnmatch.filter(sorted(layer_entities), pattern)
                if not matches:
                    logging.warning(f"Pattern '{pattern}' matched no entities; skipping.")
                else:
                    logging.info(f"Pattern '{pattern}' expanded to {len(matches)} entities: {matches}")
                    expanded.extend(matches)
            else:
                expanded.append(pattern)

        # Deduplicate while preserving order
        seen = set()
        queue = []
        for e in expanded:
            if e not in seen:
                queue.append(e)
                seen.add(e)

    # Filter out blacklisted entities
    if SKIP_ENTITIES:
        original_count = len(queue)
        skipped_entities = [e for e in queue if e in SKIP_ENTITIES]
        queue = [e for e in queue if e not in SKIP_ENTITIES]
        skipped_count = original_count - len(queue)
        if skipped_count > 0:
            logging.info(f"Skipped {skipped_count} blacklisted entities: {sorted(skipped_entities)}")

    return queue

# ---------------------------------------------------------------------------
# Main Processing Function
# ---------------------------------------------------------------------------

def process_layer(layer, queue):
    """Process a layer for entities in the queue using the 4-stage pipeline."""
    if CONFIG.run_download:
        logging.info(f"Starting processing for layer '{layer}' with {len(queue)} entities")
    else:
        logging.info(f"Starting processing for layer '{layer}' (download disabled)")
    
    # Initialize CSV status tracking for entities in queue
    _initialize_csv_status(layer, queue)
    
    results = []
    for entity in queue:
        entity_start_time = datetime.now()
        try:
            # Setup
            work_dir, county, city = resolve_work_dir(layer, entity)
            entity_logger = setup_entity_logger(layer, entity, work_dir)
            
            logging.info(f"--- Processing entity: {entity} ---")
            
            # Get catalog row
            catalog_row = _fetch_catalog_row(layer, county, city)
            if catalog_row is None:
                raise RuntimeError(f"Catalog row not found for {layer}/{entity}")

            # Check if entity should be processed based on format
            should_process, process_reason = should_process_entity(catalog_row)
            if not should_process:
                logging.info(f"Skipping entity {entity}: {process_reason}")
                results.append({
                    'layer': layer, 'entity': entity, 'status': 'skipped',
                    'warning': f"Format excluded: {process_reason}", 'data_date': None, 'runtime_seconds': 0
                })
                continue

            logging.info(f"Processing entity {entity}: {process_reason}")

            # Initialize variables
            raw_zip_name = None
            metadata = {}

            # Stage 1: Download
            try:
                raw_zip_name = layer_download(layer, entity, county, city, catalog_row, work_dir, entity_logger)
            except SkipEntityError as e:
                raise  # Re-raise to skip entire entity

            # Stage 2: Metadata
            try:
                metadata = layer_metadata(layer, entity, county, city, catalog_row, work_dir, entity_logger)
            except SkipEntityError as e:
                # Handle metadata-based NND (data date unchanged)
                if "data date unchanged" in str(e):
                    _update_csv_status(layer, entity, 'download', 'NND', error_msg='Metadata check: data date unchanged')
                raise  # Re-raise to skip entire entity

            # Stage 3: Processing
            layer_processing(layer, entity, county, city, catalog_row, work_dir, entity_logger)

            # Stage 4: Upload
            layer_upload(layer, entity, county, city, catalog_row, work_dir, entity_logger, metadata, raw_zip_name)

            # Record success
            entity_end_time = datetime.now()
            entity_runtime = round((entity_end_time - entity_start_time).total_seconds())
            result_entry = {
                'layer': layer,
                'entity': entity,
                'status': 'success',
                'data_date': metadata.get('data_date') or datetime.now().date(),
                'runtime_seconds': f'{entity_runtime}s',
            }
            if metadata.get('epsg'):
                result_entry['epsg'] = metadata['epsg']
            if metadata.get('shp'):
                result_entry['shp_name'] = metadata['shp']
            if metadata.get('_defaulted_today'):
                warning_msg = 'data_date defaulted to current day'
                entity_logger.warning(warning_msg)
                result_entry['warning'] = warning_msg

            results.append(result_entry)
            logging.info(f"--- Successfully processed entity: {entity} ---")

        except SkipEntityError as e:
            # Handle NND cases with publish date update
            logging.info(f"Skipping entity {entity} for layer {layer}: {e}")
            
            # If it's a "no new data" case, update publish_date to show we checked
            if "No new data available" in str(e) or "data date unchanged" in str(e):
                try:
                    layer_upload(layer, entity, county, city, catalog_row, work_dir, entity_logger, {})
                    logging.info(f"Updated publish_date for {entity} (NND case)")
                except Exception as publish_error:
                    logging.warning(f"Failed to update publish_date for {entity}: {publish_error}")
            
            entity_end_time = datetime.now()
            entity_runtime = (entity_end_time - entity_start_time).total_seconds()
            results.append({
                'layer': layer, 'entity': entity, 'status': 'skipped', 
                'warning': str(e), 'data_date': None, 'runtime_seconds': entity_runtime
            })
        except LayerProcessingError as e:
            logging.error(f"Failed to process entity {entity} for layer {layer}: {e}")
            entity_end_time = datetime.now()
            entity_runtime = (entity_end_time - entity_start_time).total_seconds()
            results.append({
                'layer': layer, 'entity': entity, 'status': 'failure', 
                'error': str(e), 'data_date': None, 'runtime_seconds': entity_runtime
            })

    # Calculate stats
    total_entities = len(results)
    successful_entities = len([r for r in results if r.get('status') == 'success'])
    logging.info(f"{successful_entities}/{total_entities} entities processed successfully")
    return results

# ---------------------------------------------------------------------------
# Enhanced CSV Summary Generation
# ---------------------------------------------------------------------------

def generate_summary(results):
    """Generate/update a living CSV summary document organized by county."""
    if not results or not CONFIG.generate_summary:
        return

    layer = results[0]['layer']
    summary_filename = f"{layer}_summary.csv"  # No date in filename - living document
    script_dir = os.path.dirname(os.path.abspath(__file__))
    summary_filepath = os.path.join(script_dir, summary_filename)
    
    headers = ['county', 'city', 'data_date', 'download_status', 'processing_status', 
               'upload_status', 'error_message', 'timestamp']
    
    try:
        # Read existing CSV data if it exists
        existing_data = {}
        if os.path.exists(summary_filepath):
            with open(summary_filepath, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Skip summary rows
                    if row.get('layer', '').startswith('LAST UPDATED:'):
                        continue
                    entity_key = f"{row.get('county', '')}_{row.get('city', '')}"
                    existing_data[entity_key] = row
        
        # Process results and update data
        for result in results:
            entity = result['entity']
            county, city = split_entity(entity)
            entity_key = f"{county}_{city}"
            
            # Get existing row or create new one
            if entity_key in existing_data:
                row = existing_data[entity_key]
            else:
                row = {h: '' for h in headers}
                row['county'] = county
                row['city'] = city
            
            # Update based on result status
            status = result.get('status', 'failure')
            error_msg = result.get('error', '') or result.get('warning', '')
            
            if status == 'skipped' and ('No new data available' in str(error_msg) or 'data date unchanged' in str(error_msg)):
                # No new data case
                row['download_status'] = 'NND'
                row['processing_status'] = ''
                row['upload_status'] = ''
                # Don't clear error_message - preserve the source information set by _update_csv_status
            elif status == 'skipped' and 'Format excluded' in str(error_msg):
                # Format not supported by pipeline
                row['download_status'] = 'SKIPPED'
                row['processing_status'] = 'SKIPPED'
                row['upload_status'] = 'SKIPPED'
                row['error_message'] = str(error_msg)
            elif status == 'success':
                # Full success
                row['download_status'] = 'SUCCESS'
                row['processing_status'] = 'SUCCESS'
                row['upload_status'] = 'SUCCESS'
                row['error_message'] = ''
                row['data_date'] = result.get('data_date', '')
            else:
                # Failure - need to determine which stage failed
                download_status, processing_status, upload_status = _determine_failure_stage(result)
                row['download_status'] = download_status
                row['processing_status'] = processing_status
                row['upload_status'] = upload_status
                row['error_message'] = str(error_msg)
            
            # Set timestamp
            row['timestamp'] = datetime.now().strftime('%m/%d/%y %I:%M %p')
            existing_data[entity_key] = row
        
        # Sort data by county, then city
        sorted_data = sorted(existing_data.values(), key=lambda x: (x['county'], x['city']))
        
        # Calculate summary statistics
        total_entities = len(sorted_data)
        download_success = len([r for r in sorted_data if r['download_status'] == 'SUCCESS'])
        download_total = len([r for r in sorted_data if r['download_status'] in ['SUCCESS', 'FAILED']])
        processing_success = len([r for r in sorted_data if r['processing_status'] == 'SUCCESS'])
        processing_total = len([r for r in sorted_data if r['processing_status'] in ['SUCCESS', 'FAILED']])
        upload_success = len([r for r in sorted_data if r['upload_status'] == 'SUCCESS'])
        upload_total = len([r for r in sorted_data if r['upload_status'] in ['SUCCESS', 'FAILED']])
        
        # Format runtime
        end_time = datetime.now()
        total_runtime = (end_time - CONFIG.start_time).total_seconds()
        runtime_str = _format_runtime_detailed(total_runtime)
        
        # Create summary row with timestamp spread across columns
        now = datetime.now()
        if layer.lower() in ['flu', 'zoning']:
            # Spread across 3 columns: county, city, data_date
            summary_row = {
                'county': 'LAST UPDATED:',
                'city': now.strftime('%m/%d/%y'),
                'data_date': now.strftime('%I:%M %p'),
                'download_status': f"{download_success}/{download_total}" if download_total > 0 else "0/0",
                'processing_status': f"{processing_success}/{processing_total}" if processing_total > 0 else "0/0",
                'upload_status': f"{upload_success}/{upload_total}" if upload_total > 0 else "0/0",
                'error_message': '',
                'timestamp': runtime_str
            }
        else:
            # Use 2 columns: county, data_date
            summary_row = {
                'county': 'LAST UPDATED:',
                'city': '',
                'data_date': now.strftime('%m/%d/%y %I:%M %p'),
                'download_status': f"{download_success}/{download_total}" if download_total > 0 else "0/0",
                'processing_status': f"{processing_success}/{processing_total}" if processing_total > 0 else "0/0",
                'upload_status': f"{upload_success}/{upload_total}" if upload_total > 0 else "0/0",
                'error_message': '',
                'timestamp': runtime_str
            }
        
        # Write the CSV file
        with open(summary_filepath, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            # Write data rows sorted by county/city
            for row in sorted_data:
                writer.writerow(row)
            
            # Write summary row
            writer.writerow(summary_row)
        
        logging.info(f"Summary file updated: {summary_filepath} ({len(sorted_data)} entities)")
        
    except IOError as e:
        logging.error(f"Could not write summary file: {e}")

def _determine_failure_stage(result):
    """Determine which stage failed based on the error message."""
    error_msg = str(result.get('error', '')).lower()
    
    # Check for download-related errors
    if any(term in error_msg for term in ['download', 'ags_extract', 'download_data', 'connection', 'url', 'http']):
        return 'FAILED', '', ''
    
    # Check for processing-related errors  
    if any(term in error_msg for term in ['processing', 'update_', 'ogr2ogr', 'shapefile', 'geometry']):
        return 'SUCCESS', 'FAILED', ''
    
    # Check for upload-related errors
    if any(term in error_msg for term in ['upload', 'psql', 'database', 'catalog']):
        return 'SUCCESS', 'SUCCESS', 'FAILED'
    
    # Default: assume download failed if we can't determine
    return 'FAILED', '', ''

def _format_runtime_detailed(seconds):
    """Format runtime as 'Xhr Ymin Zsec'."""
    if seconds < 60:
        return f"{int(seconds)}sec"
    
    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)
    
    if minutes < 60:
        return f"{minutes}min {remaining_seconds}sec"
    
    hours = int(minutes // 60)
    remaining_minutes = int(minutes % 60)
    
    return f"{hours}hr {remaining_minutes}min {remaining_seconds}sec"

def _initialize_csv_status(layer, queue):
    """Initialize CSV status columns to null for entities in the processing queue."""
    if not CONFIG.generate_summary:
        return
        
    summary_filename = f"{layer}_summary.csv"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    summary_filepath = os.path.join(script_dir, summary_filename)
    
    headers = ['county', 'city', 'data_date', 'download_status', 'processing_status', 
               'upload_status', 'error_message', 'timestamp']
    
    try:
        # Read existing CSV data if it exists
        existing_data = {}
        if os.path.exists(summary_filepath):
            with open(summary_filepath, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Skip summary rows
                    if row.get('county', '').startswith('LAST UPDATED:'):
                        continue
                    entity_key = f"{row.get('county', '')}_{row.get('city', '')}"
                    existing_data[entity_key] = row
        
        # Initialize status columns for entities in queue
        for entity in queue:
            county, city = split_entity(entity)
            entity_key = f"{county}_{city}"
            
            if entity_key in existing_data:
                row = existing_data[entity_key]
            else:
                row = {h: '' for h in headers}
                row['county'] = county
                row['city'] = city
            
            # Clear status columns for this run
            row['download_status'] = ''
            row['processing_status'] = ''
            row['upload_status'] = ''
            row['error_message'] = ''
            
            existing_data[entity_key] = row
        
        # Write back the initialized CSV
        _write_csv_file(summary_filepath, headers, existing_data)
        
    except IOError as e:
        logging.error(f"Could not initialize CSV status: {e}")

def _update_csv_status(layer, entity, stage, status, error_msg='', data_date=''):
    """Update CSV status for a specific entity and stage."""
    if not CONFIG.generate_summary:
        return
        
    summary_filename = f"{layer}_summary.csv"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    summary_filepath = os.path.join(script_dir, summary_filename)
    
    headers = ['county', 'city', 'data_date', 'download_status', 'processing_status', 
               'upload_status', 'error_message', 'timestamp']
    
    try:
        # Read existing CSV data
        existing_data = {}
        if os.path.exists(summary_filepath):
            with open(summary_filepath, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Skip summary rows
                    if row.get('county', '').startswith('LAST UPDATED:'):
                        continue
                    entity_key = f"{row.get('county', '')}_{row.get('city', '')}"
                    existing_data[entity_key] = row
        
        # Update the specific entity
        county, city = split_entity(entity)
        entity_key = f"{county}_{city}"
        
        if entity_key in existing_data:
            row = existing_data[entity_key]
            
            # Update the specific stage status
            if stage == 'download':
                row['download_status'] = status
                if status == 'NND':  # No new data
                    row['processing_status'] = ''
                    row['upload_status'] = ''
                    # Don't clear error_message here - will be set below based on error_msg parameter
            elif stage == 'processing':
                row['processing_status'] = status
            elif stage == 'upload':
                row['upload_status'] = status
                if status == 'SUCCESS' and data_date:
                    row['data_date'] = data_date
            
            # Set error message based on status
            if status == 'FAILED' and error_msg:
                row['error_message'] = str(error_msg)
            elif status == 'NND' and error_msg:
                # Keep error message for NND to show source of detection
                row['error_message'] = str(error_msg)
            elif status == 'SKIPPED' and error_msg:
                # Keep error message for SKIPPED to show why stage was skipped
                row['error_message'] = str(error_msg)
            elif status == 'SUCCESS':
                row['error_message'] = ''
            
            # Update timestamp
            row['timestamp'] = datetime.now().strftime('%m/%d/%y %I:%M %p')
            
            existing_data[entity_key] = row
            
            # Write back the updated CSV
            _write_csv_file(summary_filepath, headers, existing_data)
        
    except IOError as e:
        logging.error(f"Could not update CSV status: {e}")

def _write_csv_file(filepath, headers, data_dict):
    """Write CSV file with sorted data."""
    # Sort data by county, then city
    sorted_data = sorted(data_dict.values(), key=lambda x: (x['county'], x['city']))
    
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        
        # Write data rows (filter to only include valid header fields)
        for row in sorted_data:
            filtered_row = {k: v for k, v in row.items() if k in headers}
            writer.writerow(filtered_row)

# ---------------------------------------------------------------------------
# Main Function
# ---------------------------------------------------------------------------

def main():
    """Main script execution."""
    parser = argparse.ArgumentParser(description="Clean 4-stage geospatial data processing pipeline.")
    parser.add_argument("layer", help="The layer to process.")
    parser.add_argument("entities", nargs='*', help="Optional entity IDs. If omitted, all entities for the layer will be processed.")
    parser.add_argument("--test-mode", action="store_true", help="Run in test mode, skipping actual execution.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    parser.add_argument("--no-log-isolation", dest='isolate_logs', action='store_false', help="Show all logs in console.")
    parser.add_argument("--no-download", action="store_true", help="Skip the download phase.")
    parser.add_argument("--no-metadata", action="store_true", help="Skip the metadata extraction phase.")
    parser.add_argument("--no-processing", action="store_true", help="Skip the processing phase.")
    parser.add_argument("--no-upload", action="store_true", help="Skip the upload phase.")
    parser.add_argument("--no-summary", action="store_true", help="Skip the summary generation.")
    parser.add_argument("--process-anyway", action="store_true", help="Continue processing even when download returns 'no new data'.")
    
    args = parser.parse_args()

    # Initialize config
    global CONFIG
    CONFIG = Config(
        test_mode=args.test_mode,
        debug=args.debug,
        isolate_logs=args.isolate_logs,
        run_download=not args.no_download,
        run_metadata=not args.no_metadata,
        run_processing=not args.no_processing,
        run_upload=not args.no_upload,
        generate_summary=not args.no_summary,
        process_anyway=args.process_anyway
    )
    
    initialize_logging(CONFIG.debug)

    logging.info(f"Script started at {CONFIG.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    if CONFIG.test_mode:
        logging.warning("--- RUNNING IN TEST MODE ---")

    results = []
    try:
        # Set the queue of entities to process
        queue = set_queue(args.layer, args.entities)

        if not queue:
            logging.info("No entities to process.")
            return

        # Process the layer using 4-stage pipeline
        results = process_layer(args.layer, queue)

    except (ValueError, NotImplementedError) as e:
        logging.critical(f"A critical error occurred: {e}")
        sys.exit(1)
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Generate summary
        generate_summary(results)
        end_time = datetime.now()
        logging.info(f"Script finished at {end_time.strftime('%Y-%m-%d %H:%M:%S')}. Total runtime: {end_time - CONFIG.start_time}")

if __name__ == "__main__":
    main() 