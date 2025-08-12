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
from typing import Optional
import fnmatch
import psycopg2
import psycopg2.extras
from pathlib import Path
import shutil
import shlex

# Import modular Selenium OpenData downloader (optional)
try:
    from download_opendata import (
        init_selenium as _selenium_init,
        shutdown_selenium as _selenium_shutdown,
        download_opendata as _selenium_download_opendata,
    )
except Exception:
    _selenium_init = None
    _selenium_shutdown = None
    _selenium_download_opendata = None

# Shared Selenium state across layers
_SELENIUM_DRIVER = None
_SELENIUM_REMAINING = set()

# ---------------------------------------------------------------------------
# Layer Configuration
# ---------------------------------------------------------------------------

# Note: Update scripts are now generated dynamically based on layer name

# Format constants now imported from layers_helpers.py

def should_process_entity(catalog_row: dict, entity: str, SKIP_ENTITIES: set) -> tuple[bool, str]:
    """Determine if an entity should be processed and why.
    
    Returns:
        tuple[bool, str]: (should_process, reason)
    """
    if entity in SKIP_ENTITIES:
        return False, f"Entity '{entity}' is in the SKIP_ENTITIES set"
    
    fmt = (catalog_row.get('format') or '').lower()
    
    if fmt in FULL_PIPELINE_FORMATS:
        return True, f"Format '{fmt}' gets full pipeline treatment"
    elif fmt in METADATA_ONLY_FORMATS:
        return True, f"Format '{fmt}' gets metadata-only treatment"
    else:
        return False, f"Format '{fmt}' is excluded from pipeline"

def should_run_processing(catalog_row: dict) -> tuple[bool, str]:
    """Determine if an entity should run the processing stage and why.
    
    Returns:
        tuple[bool, str]: (should_run, reason)
    
    Processing is skipped for metadata-only formats like PDF or when
    fields_obj_transform is missing.
    """
    fmt = (catalog_row.get('format') or '').lower()
    if fmt in METADATA_ONLY_FORMATS:
        return False, f"Format '{fmt}' skips processing stage"
    fields_obj_transform = catalog_row.get('fields_obj_transform')
    if not fields_obj_transform or str(fields_obj_transform).strip() == '':
        return False, 'fields_obj_transform is null or empty'
    return True, 'Processing enabled'

# ---------------------------------------------------------------------------
# Name Formatting Utilities - now imported from layers_helpers.py
# ---------------------------------------------------------------------------
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

# Import shared utilities and constants
from layers_helpers import (
    PG_CONNECTION, VALID_STATES, FL_COUNTIES, LAYER_CONFIGS,
    SKIP_ENTITIES, FULL_PIPELINE_FORMATS, METADATA_ONLY_FORMATS,
    format_name, safe_catalog_val, 
    resolve_layer_name, resolve_layer_directory,
    DATA_ROOT, TOOLS_DIR,
    # Date helpers
    normalize_data_date, parse_string_to_date,
    # Backwards compatibility aliases
    counties, layers
)

# All constants now imported from layers_helpers.py



# Work directory patterns now configured in layers_helpers.py LAYER_CONFIGS

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
                 process_anyway: bool = False,
                 slow_threshold_seconds: int | None = None):
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
        self.slow_threshold_seconds = slow_threshold_seconds

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



# Old entity parsing functions removed - now using database components exclusively

def resolve_work_dir(layer: str, entity: str, entity_components: dict = None):
    """Return work_dir for layer/entity."""
    # Get entity components from database if not provided
    if entity_components is None or entity not in entity_components:
        # This should not happen in normal operation - entity_components should always be provided
        raise ValueError(f"Entity '{entity}' components not provided. This indicates a programming error.")
    
    components = entity_components[entity]
    state = components['state']
    county = components['county'] 
    city = components['city']
    
    # Handle special business logic cases
    if layer == 'zoning' and state == 'fl' and county == 'duval' and city == 'unified':
        # Duval Unified refers to Jacksonville city-county government
        county, city = 'duval', 'jacksonville'
        work_dir = resolve_layer_directory('zoning', 'fl', 'duval', 'jacksonville')
        return work_dir

    # General case - use new directory resolution from LAYER_CONFIGS
    work_dir = resolve_layer_directory(layer, state, county, city)
    # Ensure directory exists here so creation responsibility lives in layers_scrape
    try:
        os.makedirs(work_dir, exist_ok=True)
    except Exception as _e:
        # Logging will be configured later; silently ignore here
        pass
    return work_dir

def _run_command_live(command, work_dir, logger):
    """Run a shell command with live output streaming."""
    if CONFIG.test_mode:
        logger.info(f"[TEST MODE] COMMAND SKIPPED IN {work_dir}: {' '.join(command)}")
        return ""
    
    logger.info(f"Running command in {work_dir}: {' '.join(command)}")
    
    # Use Popen for live output streaming
    process = subprocess.Popen(
        command, 
        cwd=work_dir, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT, 
        text=True, 
        bufsize=1, 
        universal_newlines=True
    )
    
    stdout_lines = []
    
    # Stream output in real-time
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            output = output.rstrip()
            print(output)  # Live output to console
            stdout_lines.append(output)
    
    # Wait for process to complete and get return code
    return_code = process.wait()
    
    # Handle download_data.py no-new-data vs error conditions
    if len(command) > 1 and 'download_data.py' in command[1]:
        stdout_combined = '\n'.join(stdout_lines)
        stdout_lower = stdout_combined.lower()
        nnd_phrases = [
            '304 not modified',
            'not modified on server',
            'omitting download',
            'no new data available from server',
            'no new data available',
        ]
        if return_code == 1:
            # Only treat rc=1 as NND if output contains a known NND phrase; otherwise fall through to error handling
            if any(phrase in stdout_lower for phrase in nnd_phrases):
                if CONFIG.process_anyway:
                    logger.warning("download_data.py returned exit code 1 - no new data available, but continuing due to process_anyway=True")
                    return stdout_combined
                else:
                    logger.info("download_data.py returned exit code 1 - no new data available - skipping entity")
                    raise SkipEntityError("No new data available from server", layer=None, entity=None)
        elif return_code == 0:
            if any(phrase in stdout_lower for phrase in nnd_phrases):
                if CONFIG.process_anyway:
                    logger.warning("download_data.py indicates no new data available, but continuing due to process_anyway=True")
                else:
                    logger.info("download_data.py indicates no new data available - skipping entity")
                    raise SkipEntityError("No new data available from server", layer=None, entity=None)

    if return_code != 0:
        # Check for deprecated URL/service errors before general failure
        error_output = '\n'.join(stdout_lines).lower()
        
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
            logger.error(f"Error indicators: {error_output}")
            raise DownloadError(f"URL appears to be deprecated or inaccessible: {error_output}", layer=None, entity=None)
        
        # General command failure
        logger.error(f"Error executing command: {' '.join(command)}")
        logger.error(f"STDOUT: {chr(10).join(stdout_lines)}")
        raise ProcessingError(f"Command failed with exit code {return_code}: {' '.join(command)}")
    
    return '\n'.join(stdout_lines)

def _run_command(command, work_dir, logger):
    """Run a shell command in a specified directory."""
    # Use live output when debug is enabled and log isolation is disabled
    if CONFIG.debug and not CONFIG.isolate_logs:
        return _run_command_live(command, work_dir, logger)
    else:
        return _run_command_regular(command, work_dir, logger)

def _run_command_regular(command, work_dir, logger):
    """Run a shell command in a specified directory with captured output."""
    if CONFIG.test_mode:
        logger.info(f"[TEST MODE] COMMAND SKIPPED IN {work_dir}: {' '.join(command)}")
        return
    
    logger.debug(f"Running command in {work_dir}: {' '.join(command)}")
    
    process = subprocess.run(command, cwd=work_dir, capture_output=True, text=True)

    # Handle download_data.py no-new-data vs error conditions
    if len(command) > 1 and 'download_data.py' in command[1]:
        combined_lower = f"{process.stdout}\n{process.stderr}".lower()
        nnd_phrases = [
            '304 not modified',
            'not modified on server',
            'omitting download',
            'no new data available from server',
            'no new data available',
        ]
        if process.returncode == 1:
            # Only treat rc=1 as NND if output contains a known NND phrase; otherwise let general error handling proceed
            if any(phrase in combined_lower for phrase in nnd_phrases):
                if CONFIG.process_anyway:
                    logger.warning("download_data.py returned exit code 1 - no new data available, but continuing due to process_anyway=True")
                    return process.stdout
                else:
                    logger.info("download_data.py returned exit code 1 - no new data available - skipping entity")
                    raise SkipEntityError("No new data available from server", layer=None, entity=None)
        elif process.returncode == 0:
            if any(phrase in combined_lower for phrase in nnd_phrases):
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
    
    Commands that start with "WARNING:" will only generate warnings on failure.
    All other commands will fail the entity if they fail.
    """
    if not source_comments or not source_comments.strip():
        return
    
    # Split commands by bracketed format: [cmd1] [cmd2] -> ['cmd1', 'cmd2']
    import re
    commands = re.findall(r'\[([^\]]+)\]', source_comments.strip())
    if not commands:
        # Fallback to old pipe format for backwards compatibility
        commands = [cmd.strip() for cmd in source_comments.split('|') if cmd.strip()]
    
    for i, raw_cmd in enumerate(commands):
        logger.debug(f"Evaluating source comment command {i+1}/{len(commands)}: {raw_cmd}")

        if CONFIG.test_mode:
            logger.info(f"[TEST MODE] SOURCE COMMAND SKIPPED IN {work_dir}: {raw_cmd}")
            continue

        # Check if this is a warning-only command
        is_warning_only = str(raw_cmd).startswith('WARNING:')
        cmd_str = raw_cmd[8:].strip() if is_warning_only else str(raw_cmd).strip()
        if is_warning_only:
            logger.debug(f"Command marked as warning-only: {cmd_str}")

        # Validate whether it looks like a command; skip notes
        if not _looks_like_command(cmd_str):
            logger.info(f"Skipping non-command note in source_comments: {raw_cmd}")
            continue

        # Handle different command types
        if cmd_str.endswith('.py') or cmd_str.lower().startswith(('python ', 'python3 ')):
            # Python script - run with python3 if not explicitly specified
            if cmd_str.lower().startswith(('python ', 'python3 ')):
                command = ['bash', '-c', cmd_str]
            else:
                command = ['python3', cmd_str]
        else:
            # Shell command - run through shell
            command = ['bash', '-c', cmd_str]
        
        try:
            process = subprocess.run(command, cwd=work_dir, capture_output=True, text=True)
            
            if process.returncode != 0:
                if is_warning_only:
                    logger.warning(f"Source comment command failed (warning-only): {cmd_str}")
                    logger.warning(f"STDERR: {process.stderr}")
                    # Continue with other commands for warning-only commands
                else:
                    logger.error(f"Source comment command failed: {cmd_str}")
                    logger.error(f"STDERR: {process.stderr}")
                    raise ProcessingError(f"Source comment command failed: {cmd_str}", layer=None, entity=None)
            else:
                logger.debug(f"Source comment command succeeded: {cmd_str}")
                if process.stdout:
                    logger.debug(f"Command output: {process.stdout}")
                    
        except Exception as e:
            if is_warning_only:
                logger.warning(f"Failed to execute source comment command '{cmd_str}' (warning-only): {e}")
                # Continue with other commands for warning-only commands
            else:
                logger.error(f"Failed to execute source comment command '{cmd_str}': {e}")
                raise ProcessingError(f"Failed to execute source comment command: {e}", layer=None, entity=None)

# ---------------------------------------------------------------------------
# Database Functions
# ---------------------------------------------------------------------------

def _fetch_catalog_row(layer: str, state: str, county: str, city: str):
    """Return catalog row for the given layer/state/county/city or None if missing."""
    conn = psycopg2.connect(PG_CONNECTION)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        # Convert internal format names to external format for database query
        # Note: layer_subgroup is stored in internal format in database, so don't convert layer
        state_external = format_name(state, 'state', external=True) if state else None
        county_external = format_name(county, 'county', external=True) if county else None
        city_external = format_name(city, 'city', external=True) if city else None
        
        # Build SQL query based on which fields are provided
        sql_parts = ["SELECT * FROM m_gis_data_catalog_main WHERE lower(layer_subgroup) = %s"]
        params = [layer.lower()]
        
        # Add state condition
        if state_external:
            sql_parts.append("AND (state = %s OR state IS NULL)")
            params.append(state_external)
        
        # Add county condition
        if county_external:
            sql_parts.append("AND lower(county) = %s")
            params.append(county_external.lower())
        else:
            sql_parts.append("AND county IS NULL")
        
        # Add city condition  
        if city_external:
            # Check for both the specific city and NULL (for default city cases)
            sql_parts.append("AND (lower(city) = %s OR city IS NULL)")
            params.append(city_external.lower())
        else:
            sql_parts.append("AND city IS NULL")
            
        sql = " ".join(sql_parts) + " LIMIT 1"
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

def _get_existing_data_date(layer: str, entity: str, entity_components: dict = None) -> str:
    """Get the existing data_date for an entity from the CSV file."""
    summary_filename = f"{layer}_summary.csv"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    summaries_dir = os.path.join(script_dir, "summaries")
    os.makedirs(summaries_dir, exist_ok=True)
    summary_filepath = os.path.join(summaries_dir, summary_filename)
    
    if not os.path.exists(summary_filepath):
        return None
    
    try:
        # Get entity components from provided dict or database
        if entity_components is None or entity not in entity_components:
            # Fallback to database query (should be rare)
            all_entities_dict = get_all_entities_from_db()
            if entity not in all_entities_dict:
                raise ValueError(f"Entity '{entity}' not found in database. Cannot determine components.")
            components = all_entities_dict[entity]
        else:
            components = entity_components[entity]
        
        state = components['state']
        county = components['county']
        city = components['city']
        
        # For special entities with NULL county/city, use the entity itself as key
        if county is None and city is None:
            entity_key = entity
        else:
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
            "SELECT state, county, city FROM m_gis_data_catalog_main "
            "WHERE status IS DISTINCT FROM 'DELETE' "
            "AND lower(layer_subgroup) = %s"
        )
        cur.execute(sql, (layer.lower(),))
        rows = cur.fetchall()
        for row in rows:
            entity = _entity_from_parts(layer, row['state'], row['county'], row['city'])
            entities.append(entity)
    except Exception as exc:
        logging.error(f"DB entity fetch failed: {exc}")
    finally:
        cur.close()
        conn.close()
    return list(dict.fromkeys(entities))  # de-dupe preserving order

# Old pattern-based entity fetching removed - now using get_filtered_entities with fnmatch

def _entity_from_parts(layer: str, state: str | None, county: str, city: str | None) -> str:
    """Return entity id from raw DB values in appropriate format based on layer level."""
    # Convert external DB values to internal format
    county_internal = format_name(county, 'county', external=False)
    city_internal = format_name(city, 'city', external=False) if city else ""
    
    # Handle state - infer from county if missing
    if state and str(state).strip() and str(state).strip().upper() not in ('NULL', 'NONE'):
        state_internal = str(state).strip().lower()
    else:
        # Infer state from county for backwards compatibility
        if county_internal in FL_COUNTIES:
            state_internal = 'fl'
        else:
            state_internal = 'fl'  # Default fallback
    
    # Check layer level to determine entity format
    from layers_helpers import LAYER_CONFIGS
    layer_config = LAYER_CONFIGS.get(layer, {})
    layer_level = layer_config.get('level', 'state_county_city')  # Default to 4-part
    
    if layer_level == 'state_county':
        # County-level layer: use 3-part format (layer_state_county)
        return f"{layer}_{state_internal}_{county_internal}"
    else:
        # City-level or other layers: use 4-part format (layer_state_county_city)
        if not city_internal:
            city_internal = 'countywide'  # Default for county-only records
        return f"{layer}_{state_internal}_{county_internal}_{city_internal}"

def _looks_like_command(command_text: str) -> bool:
    """Heuristically determine whether a string is an executable command vs a human note.

    Rules (order matters):
    - Empty/whitespace -> False
    - Strip WARNING: prefix before calling this
    - Accept if first token resolves on PATH (shutil.which) or is in builtin whitelist
    - Accept if command starts with python/python3 or ends with .py
    - Accept if it starts with ./, ../, or /
    - Accept if it contains common shell operators (|, &&, ||, >, <)
    - Otherwise reject (likely a free-form note)
    """
    if not command_text or not str(command_text).strip():
        return False

    s = str(command_text).strip()

    # Skip likely prose: sentence ending with a period and no shell-ish characters
    if s.endswith('.') and ' -' not in s and '/' not in s and '\\' not in s and '"' not in s and "'" not in s:
        return False

    # Shell pipelines or logical operators are likely commands
    if any(tok in s for tok in ['|', '&&', '||', '>', '<']):
        return True

    # Explicit script indications
    if s.lower().startswith(('python ', 'python3 ')) or s.lower().endswith('.py'):
        return True

    # Path-like invocation
    if s.startswith(('./', '../', '/')):
        return True

    # Tokenize conservatively
    try:
        tokens = shlex.split(s)
    except Exception:
        tokens = s.split()
    if not tokens:
        return False

    # Ignore leading sudo
    first = tokens[0]
    if first == 'sudo' and len(tokens) > 1:
        first = tokens[1]

    # Common builtins that won't resolve via which but are valid under bash -c
    shell_builtins = {'cd', 'echo', 'true', 'false', 'test', 'exit'}
    if first in shell_builtins:
        return True

    # Common CLI tools used in this pipeline
    common_tools = {
        'rm', 'mv', 'cp', 'mkdir', 'rmdir', 'unzip', 'zip', 'ogr2ogr', 'ogrinfo', 'gdal_translate', 'gdalwarp',
        'sed', 'awk', 'grep', 'find', 'psql', 'bash', 'sh', 'chmod', 'ls', 'cat', 'head', 'tail'
    }
    if first in common_tools:
        return True

    # PATH resolution
    if shutil.which(first):
        return True

    return False

def _parse_processing_comments(text):
    """Parse the processing_comments field into a list of command strings.
    
    Supports formats:
    1. Bracketed: "[command1] [command2] [command3]"
    2. JSON array: ["command1", "command2", "command3"]
    3. Legacy: newlines/semicolons
    """
    if not text:
        return []
    
    # Helper to strip WARNING: prefix for validation
    def _strip_warning_prefix(s: str) -> str:
        return s[8:].strip() if isinstance(s, str) and s.startswith('WARNING:') else s

    # Try bracketed format first: [cmd1] [cmd2] -> ['cmd1', 'cmd2']
    bracketed_commands = re.findall(r'\[([^\]]+)\]', text.strip())
    if bracketed_commands:
        cmds = [cmd.strip() for cmd in bracketed_commands if cmd.strip()]
        # Validate – only keep things that look like real commands
        cmds = [c for c in cmds if _looks_like_command(_strip_warning_prefix(c))]
        return cmds
    
    # Try JSON array format
    try:
        data = json.loads(text)
        if isinstance(data, list):
            cmds = [str(cmd).strip() for cmd in data if str(cmd).strip()]
            cmds = [c for c in cmds if _looks_like_command(_strip_warning_prefix(c))]
            return cmds
    except Exception:
        pass
    
    # Fallback – split on newlines/semicolons, but filter out non-commands
    commands = []
    for piece in re.split(r'[\n;]+', text):
        piece = piece.strip()
        if piece and _looks_like_command(_strip_warning_prefix(piece)):
            commands.append(piece)
    return commands

# ---------------------------------------------------------------------------
# Main Pipeline Functions
# ---------------------------------------------------------------------------

def layer_download(layer: str, entity: str, state: str, county: str, city: str, catalog_row: dict, work_dir: str, logger, entity_components: dict = None):
    """Handle the download phase for an entity.
    
    Refactored to:
    - Select download method primarily via `download` field with fallback to `format`
    - Perform download validation (detect changed files)
    - If any changed file is a .zip, process it
    - Perform data validation on all changed files (including any extracted from zip)
    """
    if not CONFIG.run_download:
        logger.debug(f"[DOWNLOAD] Skipping download for {layer}/{entity} (disabled in config)")
        return None, None

    fmt = (catalog_row.get('format') or '').lower()
    # Use download_method exclusively (legacy 'download' is AUTO and ignored)
    method_field = (catalog_row.get('download_method') or '').strip().upper()
    resource = catalog_row.get('resource') or catalog_row.get('src_url_file')
    src_url_file_lower = ((catalog_row.get('src_url_file') or '').strip().lower())
    table_name = catalog_row.get('table_name')

    # Helper: detect ArcGIS Hub/OpenData pages
    def _is_arcgis_hub_opendata(u: str | None) -> bool:
        if not u:
            return False
        u_low = u.lower()
        # Common Hub/OpenData domains and path hints
        if 'opendata.arcgis.com' in u_low or 'hub.arcgis.com' in u_low:
            # Exclude direct API download endpoints and direct ZIPs → handle via WGET
            if '/api/download/' in u_low or u_low.endswith('.zip'):
                return False
            return True
        if 'arcgis.com' in u_low and '/datasets/' in u_low:
            return True
        return False

    # Determine download method (prefer download_method; else fallback by format/URL)
    if method_field in {"AGS", "WGET", "SELENIUM"}:
        # Safety override: if explicitly marked SELENIUM but the URL is a direct ZIP or Hub API download, use WGET
        if method_field == 'SELENIUM':
            is_direct_zip = src_url_file_lower.endswith('.zip') or ('.zip?' in src_url_file_lower) or ('.zip&' in src_url_file_lower)
            is_hub_api = '/api/download/' in src_url_file_lower or src_url_file_lower.startswith('https://hub.arcgis.com/api/download') or src_url_file_lower.startswith('http://hub.arcgis.com/api/download')
            if is_direct_zip or is_hub_api:
                selected_method = 'WGET'
                _debug_main(f"[DOWNLOAD] Overriding SELENIUM → WGET for direct download URL: {resource}", logger)
            else:
                selected_method = method_field
        else:
            selected_method = method_field
    else:
        if fmt == 'ags':
            selected_method = 'AGS'
        else:
            # Auto-detect using ONLY src_url_file (download URL)
            hub_candidate = (catalog_row.get('src_url_file') or '').strip()
            if fmt == 'selenium' or _is_arcgis_hub_opendata(hub_candidate):
                selected_method = 'SELENIUM'
                _debug_main(f"[DOWNLOAD] Auto-detected ArcGIS Hub/OpenData URL; using SELENIUM for {layer}/{entity}", logger)
            else:
                selected_method = 'WGET'

    # Build command based on selected method
    if selected_method == 'AGS':
        if not table_name:
            raise DownloadError('Missing table_name for AGS download', layer, entity)
        command = [
            'python3',
            os.path.join(TOOLS_DIR, 'ags_extract_data2.py'),
            table_name,
            'delete',
            '15'
        ]
        _debug_main(f"[DOWNLOAD] Running AGS download for {layer}/{entity} (table: {table_name})", logger)
    elif selected_method == 'WGET':
        if not resource:
            raise DownloadError('Missing resource/url for download_data.py', layer, entity)
        # Pre-validate manual vs auto-generated DB fields to classify skip vs error clearly
        src_url_file_val = (catalog_row.get('src_url_file') or '').strip()
        sys_raw_folder_val = (catalog_row.get('sys_raw_folder') or '').strip() if catalog_row.get('sys_raw_folder') else ''
        # Missing manual field: treat as skip (not an error)
        if not src_url_file_val:
            raise SkipEntityError('Missing src_url_file (manual field)', layer=layer, entity=entity)
        # Missing auto-generated field: treat as error
        if not sys_raw_folder_val:
            raise DownloadError('Missing sys_raw_folder (auto-generated path) for this record', layer, entity)
        command = [
            'python3',
            os.path.join(TOOLS_DIR, 'download_data.py'),
            resource
        ]
        _debug_main(f"[DOWNLOAD] Running WGET-style download for {layer}/{entity} (format: {fmt}, url: {resource})", logger)
    elif selected_method == 'SELENIUM':
        # Use modular Selenium downloader when available
        if _selenium_download_opendata is None or _selenium_init is None:
            raise DownloadError('Selenium downloader not available in environment', layer, entity)
        # Determine URL and target directory (use src_url_file ONLY)
        sel_url = (catalog_row.get('src_url_file') or '').strip()
        if not sel_url:
            raise DownloadError('Missing src_url_file for Selenium download', layer, entity)
        target_dir = work_dir  # use resolved work_dir as target

        # Initialize shared driver lazily (per process)
        global _SELENIUM_DRIVER
        if _SELENIUM_DRIVER is None:
            try:
                headless = True  # always headless in pipeline
                _SELENIUM_DRIVER = _selenium_init(download_dir="/srv/datascrub/_batch_downloads", headless=headless, chromium=True, debug=CONFIG.debug)
                logger.debug("Initialized shared Selenium driver for batch")
            except Exception as e:
                raise DownloadError(f"Failed to initialize Selenium driver: {e}", layer, entity)

        # Perform Selenium download (module handles waiting + basic validation + transfer)
        try:
            # Provide existing catalog data_date (if available) to enable NND shortcut
            existing_date = None
            try:
                existing_date = str(catalog_row.get('data_date') or '').strip() or None
            except Exception:
                existing_date = None
            result = _selenium_download_opendata(
                _SELENIUM_DRIVER,
                entity=entity,
                url=sel_url,
                target_dir=target_dir,
                catalog_data_date=existing_date,
                transfer=True,
                debug=CONFIG.debug,
            )
        except Exception as e:
            raise DownloadError(f"Selenium error: {e}", layer, entity)

        status = (result or {}).get('status', 'FAILED')
        if status == 'SKIPPED_NND':
            _update_csv_status(layer, entity, 'download', 'NND', error_msg='Selenium: no new data', entity_components=entity_components)
            # Propagate as SkipEntityError to skip processing
            raise SkipEntityError("No new data available (selenium)", layer=layer, entity=entity)
        if status != 'SUCCESS':
            msg = (result or {}).get('message', 'Unknown Selenium failure')
            raise DownloadError(f"Selenium download failed: {msg}", layer, entity)

        # For ZIP-aware follow-ups, capture zip file name if present in changes (post-transfer)
        transferred = result.get('transferred_files', []) or []
        changed_files = [os.path.join(target_dir, f) for f in transferred]

        # Validate via existing validation step to keep behavior consistent
        before_state = _get_directory_state(work_dir)
        after_state = {**before_state}
        for f in changed_files:
            try:
                after_state[os.path.basename(f)] = os.path.getmtime(f)
            except Exception:
                pass

        # Run data validation against the new files
        fmt_lower = fmt
        zip_file = None
        data_date = None
        try:
            # If a zip file among transferred files, run zip_processing
            changed_zip_candidates = [f for f in changed_files if f.lower().endswith('.zip')]
            if changed_zip_candidates:
                newest_zip = max(changed_zip_candidates, key=lambda f: os.path.getmtime(f))
                zip_file = newest_zip
                _debug_main(f"[DOWNLOAD] Found zip file among Selenium changes: {zip_file}", logger)
                data_date = zip_processing(zip_file, work_dir, logger)

            # Data validation on changed files (including extracted/modified from zip if any)
            if zip_file:
                post_unzip_state = _get_directory_state(work_dir)
                extracted_or_modified = []
                for name, mtime in post_unzip_state.items():
                    if name not in before_state or before_state.get(name) != mtime:
                        extracted_or_modified.append(name)
                changed_files = list({*changed_files, *[os.path.join(work_dir, f) for f in extracted_or_modified]})
            try:
                _validate_data_files([os.path.basename(p) for p in changed_files], fmt_lower, work_dir, logger)
            except DownloadError:
                # Fallback: scan directory for files matching expected format
                fallback_candidates = _collect_files_matching_format(work_dir, fmt_lower)
                if fallback_candidates:
                    logger.debug(f"[DOWNLOAD] Fallback format scan found: {fallback_candidates}")
                    _validate_data_files(fallback_candidates, fmt_lower, work_dir, logger)
                else:
                    raise
            # Prefer the newer of: zip-derived data_date vs page-reported date
            try:
                page_date_raw = (result.get('data_date') or '').strip()
                # Normalize both dates using centralized parser
                def _to_date(d: str):
                    if not d:
                        return None
                    parsed = parse_string_to_date(d)
                    return parsed
                zip_d = _to_date(data_date) if data_date else None
                page_d = _to_date(page_date_raw) if page_date_raw else None
                if page_d and (not zip_d or page_d > zip_d):
                    # Override with newer page date in YYYY-MM-DD for consistency
                    data_date = page_d.strftime("%Y-%m-%d")
            except Exception:
                pass

            _debug_main(f"[DOWNLOAD] Selenium download and validation passed for {layer}/{entity}", logger)
            _update_csv_status(layer, entity, 'download', 'SUCCESS', entity_components=entity_components)
        except DownloadError as de:
            _update_csv_status(layer, entity, 'download', 'FAILED', str(de), entity_components=entity_components)
            raise DownloadError(str(de), layer, entity) from de

        return zip_file, data_date
    else:
        raise DownloadError(f"Unknown download method: {selected_method}", layer, entity)

    logger.debug(f"Running download for {layer}/{entity}")

    # Capture directory state before download for validation
    before_state = _get_directory_state(work_dir)

    try:
        _run_command(command, work_dir, logger)
    except SkipEntityError as e:
        # Handle "no new data" case - from download command
        _update_csv_status(layer, entity, 'download', 'NND', error_msg='Download command: no new data', entity_components=entity_components)
        raise  # Re-raise skip errors

    # Validate download and collect changed files
    changed_files = []
    zip_file = None
    data_date = None

    if not CONFIG.test_mode:
        try:
            changed_files, after_state = _validate_download_and_collect_changes(work_dir, logger, before_state)

            # Additional validation for AGS downloads - check for empty/corrupt files
            if fmt in {'ags', 'arcgis', 'esri', 'ags_extract'} or selected_method == 'AGS':
                _validate_ags_download(work_dir, table_name, logger)

            # If a zip file was among the changed files, process the newest one and include extracted files in changed set
            changed_zip_candidates = [f for f in changed_files if f.lower().endswith('.zip')]
            if changed_zip_candidates:
                # pick the newest by mtime
                newest_zip = max(
                    changed_zip_candidates,
                    key=lambda f: after_state.get(f, 0.0)
                )
                zip_file = newest_zip

                # snapshot before unzip (after download)
                pre_unzip_state = dict(after_state)
                _debug_main(f"[DOWNLOAD] Found zip file among changes: {zip_file}", logger)
                data_date = zip_processing(zip_file, work_dir, logger)
                # capture files created by unzip
                post_unzip_state = _get_directory_state(work_dir)
                # include files that are new OR modified by the unzip operation
                extracted_or_modified = []
                for name, mtime in post_unzip_state.items():
                    if name not in pre_unzip_state or pre_unzip_state.get(name) != mtime:
                        extracted_or_modified.append(name)
                # include extracted/modified files in changed files for data validation
                changed_files = list({*changed_files, *extracted_or_modified})

            # Run data validation on the changed files
            try:
                _validate_data_files(changed_files, fmt, work_dir, logger)
            except DownloadError:
                # Fallback: mtime-based change detection may miss files if unzip preserves timestamps
                fallback_candidates = _collect_files_matching_format(work_dir, fmt)
                if fallback_candidates:
                    logger.debug(f"[DOWNLOAD] Fallback format scan found: {fallback_candidates}")
                    _validate_data_files(fallback_candidates, fmt, work_dir, logger)
                else:
                    raise

            _debug_main(f"[DOWNLOAD] Download and data validation passed for {layer}/{entity}", logger)
            _update_csv_status(layer, entity, 'download', 'SUCCESS', entity_components=entity_components)
        except DownloadError as de:
            _update_csv_status(layer, entity, 'download', 'FAILED', str(de), entity_components=entity_components)
            raise DownloadError(str(de), layer, entity) from de
    else:
        logger.info(f"[TEST MODE] Skipping download validation for {layer}/{entity}")
        _update_csv_status(layer, entity, 'download', 'SUCCESS', entity_components=entity_components)

    # Note: source_comments moved to run within layer_metadata before extraction

    return zip_file, data_date

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

def _catalog_fallback_metadata_from_row(catalog_row: dict, logger) -> dict:
    """Build fallback metadata from an existing catalog row.

    Returns keys compatible with pipeline metadata: 'data_date', 'epsg', 'shp', and 'raw_zip'.
    """
    fallback: dict = {}
    if not catalog_row:
        return fallback

    # Data date: normalize to YYYY-MM-DD when possible
    try:
        raw_date = (catalog_row.get('data_date') or '').strip()
        if raw_date:
            parsed = parse_string_to_date(raw_date)
            if parsed:
                fallback['data_date'] = parsed.strftime('%Y-%m-%d')
            else:
                if re.match(r'^\d{4}-\d{2}-\d{2}$', raw_date):
                    fallback['data_date'] = raw_date
    except Exception:
        pass

    # EPSG
    try:
        epsg_val = catalog_row.get('srs_epsg') or catalog_row.get('epsg')
        if epsg_val is not None and str(epsg_val).strip():
            fallback['epsg'] = str(epsg_val).strip()
    except Exception:
        pass

    # Filenames
    try:
        shp_name = (catalog_row.get('sys_raw_file') or '').strip()
        if shp_name:
            fallback['shp'] = shp_name
    except Exception:
        pass

    try:
        raw_zip = (catalog_row.get('sys_raw_file_zip') or '').strip()
        if raw_zip:
            fallback['raw_zip'] = raw_zip
    except Exception:
        pass

    if fallback:
        logger.debug(f"[FALLBACK] Built catalog fallback: keys={list(fallback.keys())}")
    return fallback

def layer_metadata(layer: str, entity: str, state: str, county: str, city: str, catalog_row: dict, work_dir: str, logger, zip_date: str = None):
    """Handle the metadata extraction phase for an entity."""
    if not CONFIG.run_metadata:
        logger.debug(f"[METADATA] Skipping metadata extraction for {layer}/{entity} (disabled in config)")
        # Return empty metadata; downstream will attempt catalog fallback
        return {}

    _debug_main(f"[METADATA] Extracting metadata for {layer}/{entity}", logger)
    
    fmt = (catalog_row.get('format') or '').lower()
    
    # Run source_comments commands (pre-metadata processing) BEFORE extraction
    source_comments = catalog_row.get('source_comments', '')
    if source_comments and str(source_comments).strip():
        _debug_main(f"[METADATA] Running source_comments for {layer}/{entity}: {source_comments}", logger)
        _run_source_comments(source_comments, work_dir, logger)
    
    # Handle different formats differently
    if fmt in METADATA_ONLY_FORMATS:
        # For PDF and other non-geospatial formats, extract basic file metadata
        logger.debug(f"Extracting basic file metadata for format: {fmt}")
        metadata = extract_basic_file_metadata(work_dir, logger)
        if zip_date: # Override data_date with zip_date if provided
            metadata['data_date'] = zip_date
        
        if metadata:
            _debug_main(f"[METADATA] Basic metadata extracted: file:{metadata.get('shp', 'unknown')}, date:{metadata.get('data_date', 'unknown')}", logger)
        else:
            logger.warning("No basic metadata could be extracted")
        
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
            if zip_date: # Override data_date with zip_date if provided
                data_date = zip_date
                metadata['data_date'] = data_date
                logger.debug(f"Overriding data_date with zip_date: {data_date}")
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
            
        else:
            logger.warning("No shapefile found to process for metadata extraction")
            # Fall through to catalog fallback below
            metadata = {}
    
    # Catalog fallback: if required fields are missing, attempt to use existing catalog values
    try:
        fallback = _catalog_fallback_metadata_from_row(catalog_row, logger)
        # Only fill fields that are missing
        for key in ("data_date", "epsg", "shp"):
            if key not in metadata or not metadata.get(key):
                val = fallback.get(key)
                if val:
                    metadata[key] = val
        if metadata:
            _debug_main(f"[METADATA] Applied catalog fallback where needed for {layer}/{entity}", logger)
    except Exception as _e:
        # Non-fatal; proceed with whatever metadata we have
        pass

    return metadata

def layer_processing(layer: str, entity: str, state: str, county: str, city: str, catalog_row: dict, work_dir: str, logger, metadata: dict, entity_components: dict = None):
    """Handle the processing phase for an entity."""
    if not CONFIG.run_processing:
        logger.debug(f"[PROCESSING] Skipping processing for {layer}/{entity} (disabled in config)")
        _update_csv_status(layer, entity, 'processing', 'SKIPPED', error_msg='Processing disabled in config', entity_components=entity_components)
        return

    fmt = (catalog_row.get('format') or '').lower()
    
    # Skip processing for metadata-only formats (like PDF)
    if fmt in METADATA_ONLY_FORMATS:
        reason = f"Format '{fmt}' skips processing stage"
        logger.info(f"[PROCESSING] Skipping processing for {layer}/{entity}: {reason}")
        _update_csv_status(layer, entity, 'processing', 'SKIPPED', error_msg=reason, entity_components=entity_components)
        return

    _debug_main(f"[PROCESSING] Starting processing for {layer}/{entity}", logger)
    logger.debug(f"Running processing for {layer}/{entity}")

    # If metadata is missing critical values due to skipped earlier stages, merge from catalog
    try:
        if metadata is None:
            metadata = {}
        fallback = _catalog_fallback_metadata_from_row(catalog_row, logger)
        filled_any = False
        for key in ("data_date", "epsg", "shp"):
            if not metadata.get(key) and fallback.get(key):
                metadata[key] = fallback[key]
                filled_any = True
        if filled_any:
            _debug_main(f"[PROCESSING] Filled missing metadata from catalog for {layer}/{entity}", logger)
    except Exception:
        pass

    # 1. Run pre-processing commands from database
    processing_commands = _parse_processing_comments(catalog_row.get('processing_comments'))
    if processing_commands:
        logger.debug(f"[PROCESSING] Running {len(processing_commands)} pre-processing commands")
        for raw_cmd in processing_commands:
            # Check if this is a warning-only command
            is_warning_only = str(raw_cmd).startswith('WARNING:')
            cmd_str = raw_cmd[8:].strip() if is_warning_only else str(raw_cmd).strip()
            if is_warning_only:
                logger.debug(f"Processing command marked as warning-only: {cmd_str}")

            # Validate whether it looks like a command; skip notes
            if not _looks_like_command(cmd_str):
                logger.info(f"Skipping non-command note in processing_comments: {raw_cmd}")
                continue

            # Tokenize for direct execution
            try:
                command = shlex.split(cmd_str)
            except Exception:
                command = cmd_str.split()
            
            try:
                _run_command(command, work_dir, logger)
            except Exception as e:
                if is_warning_only:
                    logger.warning(f"Processing command failed (warning-only): {cmd_str}")
                    logger.warning(f"Error: {e}")
                else:
                    logger.error(f"Processing command failed: {cmd_str}")
                    logger.error(f"Error: {e}")
                    raise ProcessingError(f"Processing command failed: {cmd_str}", layer, entity) from e

    # 2. Run layer-specific processing command (from config)
    layer_config = LAYER_CONFIGS.get(layer, {})
    processing_command = layer_config.get('processing_command')
    
    if processing_command is None:
        reason = f"No processing command configured for layer '{layer}'"
        logger.info(f"[PROCESSING] Skipping processing for {layer}/{entity}: {reason}")
        _update_csv_status(layer, entity, 'processing', 'SKIPPED', error_msg=reason, entity_components=entity_components)
        return
    
    # Prepare placeholders for command substitution
    placeholders = {
        'tools_dir': TOOLS_DIR,
        'state': state,
        'county': county,
        'city': city or '',
        'data_date': metadata.get("data_date", ""),
        'layer': layer,
        'entity': entity,
        'work_dir': work_dir,
    }
    
    # Substitute placeholders in the command
    try:
        command_str = processing_command.format(**placeholders)
        command = command_str.split()
    except KeyError as e:
        reason = f"Missing placeholder '{e}' in processing command for layer '{layer}'"
        logger.info(f"[PROCESSING] Skipping processing for {layer}/{entity}: {reason}")
        _update_csv_status(layer, entity, 'processing', 'SKIPPED', error_msg=reason, entity_components=entity_components)
        return
    
    _debug_main(f"[PROCESSING] Running processing command: {command_str}", logger)
    logger.debug(f"Running processing command for {layer}/{entity}")
    try:
        _run_command(command, work_dir, logger)
        _update_csv_status(layer, entity, 'processing', 'SUCCESS', entity_components=entity_components)
        _debug_main(f"[PROCESSING] Processing completed for {layer}/{entity}", logger)
    except Exception as e:
        _update_csv_status(layer, entity, 'processing', 'FAILED', str(e), entity_components=entity_components)
        raise ProcessingError(f"Processing failed: {e}", layer, entity) from e

def layer_upload(layer: str, entity: str, state: str, county: str, city: str, catalog_row: dict, work_dir: str, logger, metadata: dict, raw_zip_name: str = None, entity_components: dict = None):
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
        'county': format_name(county, 'county', external=True),
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
        output = _run_command(command, work_dir, logger)
        
        # Check if the UPDATE actually affected any rows
        if output and 'UPDATE 0' in output:
            # It's possible that 0 rows were affected because the record already matches
            # the intended values. Verify by selecting the target row and comparing fields.
            try:
                fields_to_check = [clause.split(' = ')[0] for clause in set_clauses]

                # Map column name -> expected value from placeholders used in UPDATE
                column_to_expected_value = {}
                for column_name in fields_to_check:
                    if column_name == 'publish_date':
                        column_to_expected_value[column_name] = placeholders.get('publish_date', '')
                    elif column_name == 'data_date':
                        column_to_expected_value[column_name] = placeholders.get('data_date', '')
                    elif column_name == 'srs_epsg':
                        column_to_expected_value[column_name] = placeholders.get('epsg', '')
                    elif column_name == 'sys_raw_file':
                        column_to_expected_value[column_name] = placeholders.get('shp', '')
                    elif column_name == 'field_names':
                        column_to_expected_value[column_name] = placeholders.get('field_names', '')
                    elif column_name == 'sys_raw_file_zip':
                        column_to_expected_value[column_name] = placeholders.get('raw_zip', '')
                    else:
                        # Fallback: do not check unknown columns
                        column_to_expected_value[column_name] = None

                select_sql = (
                    "SELECT " + ", ".join(fields_to_check) + " FROM m_gis_data_catalog_main "
                    "WHERE layer_subgroup = '{layer}' AND county = '{county}' AND city = '{city}';"
                ).format(**placeholders)

                # Use unaligned, tuples-only output with a safe delimiter for parsing
                verify_cmd = ['psql', '-d', 'gisdev', '-U', 'postgres', '-t', '-A', '-F', '|', '-c', select_sql]
                verify_output = _run_command(verify_cmd, work_dir, logger) or ''
                verify_output = verify_output.strip()

                if not verify_output:
                    # No row matched the WHERE clause; this is a genuine failure
                    error_msg = (
                        f"No matching record found in database for layer='{layer}', county='{county}', city='{city}'"
                    )
                    logger.error(error_msg)
                    _update_csv_status(layer, entity, 'upload', 'FAILED', error_msg, entity_components=entity_components)
                    raise UploadError(error_msg, layer, entity)

                # If multiple rows come back (unexpected), just take the first line
                first_line = verify_output.splitlines()[0]
                db_values = first_line.split('|') if first_line else []

                # Compare values (string comparison); skip columns where expected value is None
                all_match = True
                for idx, column_name in enumerate(fields_to_check):
                    expected_value = column_to_expected_value.get(column_name)
                    if expected_value is None:
                        continue
                    db_value = db_values[idx] if idx < len(db_values) else ''
                    if str(db_value) != str(expected_value):
                        all_match = False
                        logger.debug(
                            f"[UPLOAD] Mismatch for column '{column_name}': db='{db_value}' vs expected='{expected_value}'"
                        )
                        break

                if all_match:
                    # Treat as success: record already up to date
                    info_msg = (
                        f"No changes applied for {layer}/{entity}: targeted record already up to date"
                    )
                    logger.info(info_msg)
                    data_date = metadata.get('data_date', publish_date)
                    _update_csv_status(layer, entity, 'upload', 'SUCCESS', data_date=data_date, entity_components=entity_components)
                    _debug_main(f"[UPLOAD] Catalog metadata already current for {layer}/{entity}", logger)
                    return
                else:
                    # Row exists but values differ — treat as failure
                    error_msg = (
                        f"UPDATE 0 for layer='{layer}', county='{county}', city='{city}', and values differ from intended update"
                    )
                    logger.error(error_msg)
                    _update_csv_status(layer, entity, 'upload', 'FAILED', error_msg, entity_components=entity_components)
                    raise UploadError(error_msg, layer, entity)

            except Exception as inner_e:
                # Any verification error should be treated as upload failure
                error_msg = f"Upload verification failed: {inner_e}"
                logger.error(error_msg)
                _update_csv_status(layer, entity, 'upload', 'FAILED', error_msg, entity_components=entity_components)
                raise UploadError(error_msg, layer, entity) from inner_e
        
        data_date = metadata.get('data_date', publish_date)
        _update_csv_status(layer, entity, 'upload', 'SUCCESS', data_date=data_date, entity_components=entity_components)
        _debug_main(f"[UPLOAD] Catalog metadata updated successfully for {layer}/{entity}", logger)
    except Exception as e:
        _update_csv_status(layer, entity, 'upload', 'FAILED', str(e), entity_components=entity_components)
        raise UploadError(f"Upload failed: {e}", layer, entity) from e

# ---------------------------------------------------------------------------
# Helper Functions (from original script)
# ---------------------------------------------------------------------------

def zip_processing(zip_file: str, work_dir: str, logger) -> str:
    """Process a downloaded zip file: unzip it and extract data date.
    
    Args:
        zip_file: Name of the zip file to process
        work_dir: Working directory where the zip file is located
        logger: Logger instance for logging
        
    Returns:
        Data date in YYYY-MM-DD format extracted from zip_rename_date.sh
        
    Raises:
        ProcessingError: If zip processing fails
    """
    zip_path = os.path.join(work_dir, zip_file)
    
    if not os.path.exists(zip_path):
        raise ProcessingError(f"Zip file not found: {zip_path}", layer=None, entity=None)
    
    logger.debug(f"Processing zip file: {zip_file}")
    
    # Step 1: Unzip the file
    try:
        # Preserve directory hierarchy so *.gdb directories remain intact
        unzip_command = ['unzip', '-o', zip_file]
        logger.debug(f"Running unzip command: {' '.join(unzip_command)}")
        
        if CONFIG.test_mode:
            logger.info(f"[TEST MODE] UNZIP COMMAND SKIPPED IN {work_dir}: {' '.join(unzip_command)}")
        else:
            process = subprocess.run(unzip_command, cwd=work_dir, capture_output=True, text=True)
            
            if process.returncode != 0:
                logger.error(f"Unzip command failed: {' '.join(unzip_command)}")
                logger.error(f"STDERR: {process.stderr}")
                raise ProcessingError(f"Unzip failed with exit code {process.returncode}: {process.stderr.strip()}", layer=None, entity=None)
            
            logger.debug(f"Unzip completed successfully")
            if process.stdout:
                logger.debug(f"Unzip output: {process.stdout}")
                
    except Exception as e:
        raise ProcessingError(f"Failed to unzip {zip_file}: {e}", layer=None, entity=None)
    
    # Step 2: Run zip_rename_date.sh to extract data date
    try:
        rename_command = ['zip_rename_date.sh', zip_file]
        logger.debug(f"Running zip_rename_date.sh: {' '.join(rename_command)}")
        
        if CONFIG.test_mode:
            logger.info(f"[TEST MODE] ZIP_RENAME_DATE COMMAND SKIPPED IN {work_dir}: {' '.join(rename_command)}")
            # Return a test date for test mode
            return datetime.now().strftime('%Y-%m-%d')
        else:
            process = subprocess.run(rename_command, cwd=work_dir, capture_output=True, text=True)
            
            if process.returncode != 0:
                logger.error(f"zip_rename_date.sh command failed: {' '.join(rename_command)}")
                logger.error(f"STDERR: {process.stderr}")
                raise ProcessingError(f"zip_rename_date.sh failed with exit code {process.returncode}: {process.stderr.strip()}", layer=None, entity=None)
            
            # Extract data date from stdout (should be in YYYY-MM-DD format)
            data_date = process.stdout.strip()
            
            # Validate the date format
            try:
                datetime.strptime(data_date, '%Y-%m-%d')
                logger.debug(f"Extracted data date from zip: {data_date}")
                return data_date
            except ValueError:
                logger.error(f"Invalid date format from zip_rename_date.sh: '{data_date}'")
                raise ProcessingError(f"zip_rename_date.sh returned invalid date format: '{data_date}'", layer=None, entity=None)
                
    except Exception as e:
        raise ProcessingError(f"Failed to run zip_rename_date.sh on {zip_file}: {e}", layer=None, entity=None)

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
    """Extract date from filename using broad patterns and normalize to ISO."""
    filename = Path(pdf_path).stem
    # Try centralized parsing on several candidate tokens
    # 1) Whole name
    from layers_helpers import normalize_data_date
    iso = normalize_data_date(filename)
    if iso:
        return iso
    # 2) Replace separators to spaces and try again
    cleaned = re.sub(r'[._-]', ' ', filename)
    iso = normalize_data_date(cleaned)
    if iso:
        return iso
    return None

def _is_reasonable_date(date_str):
    """Check if date is within reasonable range (last 10-15 years up to today)."""
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        today = datetime.now().date()
        ten_years_ago = today - timedelta(days=365*15)
        return ten_years_ago <= date_obj <= today
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
        
        # Validate that downloaded files are not HTML content (PDF viewer pages)
        for filename in changed_files:
            if filename.endswith(" (new)") or filename.endswith(" (modified)"):
                actual_filename = filename.split(" (")[0]
                file_path = os.path.join(work_dir, actual_filename)
                if os.path.exists(file_path):
                    _validate_file_content(file_path, actual_filename, logger)
        
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

def _collect_files_matching_format(work_dir: str, fmt: str) -> list[str]:
    """Return filenames in work_dir that match the expected format.

    Used as a fallback when mtime-based change detection misses files (e.g., unzip preserves timestamps).
    """
    fmt_lower = (fmt or '').lower()
    try:
        names = os.listdir(work_dir)
    except OSError:
        return []

    def matches(name: str) -> bool:
        n = name.lower()
        if fmt_lower in {'shp', 'shapefile'}:
            return n.endswith('.shp')
        if fmt_lower in {'gdb', 'filegdb', 'file geodatabase', 'geodatabase', 'fgdb'}:
            return n.endswith('.gdb') and os.path.isdir(os.path.join(work_dir, name))
        if fmt_lower in {'geojson', 'json'}:
            return n.endswith('.geojson') or n.endswith('.json')
        if fmt_lower == 'pdf':
            return n.endswith('.pdf')
        if fmt_lower in {'kml', 'kmz'}:
            return n.endswith('.kml') or n.endswith('.kmz')
        if fmt_lower in {'csv'}:
            return n.endswith('.csv')
        if fmt_lower in {'xlsx', 'xls'}:
            return n.endswith('.xlsx') or n.endswith('.xls')
        if fmt_lower in {'tif', 'tiff', 'geotiff'}:
            return n.endswith('.tif') or n.endswith('.tiff')
        if fmt_lower in {'mdb', 'accdb'}:
            return n.endswith('.mdb') or n.endswith('.accdb')
        if fmt_lower in {'txt'}:
            return n.endswith('.txt')
        return False

    return [name for name in names if matches(name)]


def _validate_download_and_collect_changes(work_dir, logger, before_state=None):
    """Validate that a download occurred and return the changed file list and new directory state.

    Returns tuple: (changed_files, current_state)
    - changed_files: list of filenames (no annotations)
    - current_state: mapping filename -> mtime
    """
    current_state = _get_directory_state(work_dir)

    if before_state is not None:
        changed_files = []
        for filename, current_mtime in current_state.items():
            if filename not in before_state or current_mtime != before_state[filename]:
                changed_files.append(filename)

        if not changed_files:
            if CONFIG.process_anyway:
                logger.warning("No files changed during download, but continuing due to process_anyway=True")
                return [], current_state
            raise DownloadError("No files changed during download", layer=None, entity=None)

        # Validate not HTML masquerading as binary for each changed file
        for filename in changed_files:
            file_path = os.path.join(work_dir, filename)
            if os.path.exists(file_path) and os.path.isfile(file_path):
                _validate_file_content(file_path, filename, logger)

        # Log a short summary
        preview = ", ".join(changed_files[:3]) + (f" and {len(changed_files)-3} more" if len(changed_files) > 3 else "")
        logger.debug(f"Download validation passed - changed files: {preview}")
        return changed_files, current_state

    # Fallback: last 24 hours
    now = datetime.now()
    day_ago = now - timedelta(days=1)
    recent_files = []
    for filename in os.listdir(work_dir):
        file_path = os.path.join(work_dir, filename)
        try:
            mtime = os.path.getmtime(file_path)
            mod_time = datetime.fromtimestamp(mtime)
            if mod_time >= day_ago:
                recent_files.append(filename)
        except OSError:
            continue

    if not recent_files:
        if CONFIG.process_anyway:
            logger.warning("No files found modified within the last 24 hours, but continuing due to process_anyway=True")
            return [], current_state
        raise DownloadError("No files found modified within the last 24 hours", layer=None, entity=None)

    preview = ", ".join(sorted(recent_files)[:3]) + (f" and {len(recent_files)-3} more" if len(recent_files) > 3 else "")
    logger.debug(f"Download validation passed – recent files: {preview}")
    return recent_files, current_state

def _validate_data_files(changed_files, fmt: str, work_dir: str, logger):
    """Validate that the changed files include at least one file matching the record's format.

    For AGS/service formats, this check is skipped because a separate AGS validation is used.
    """
    if not changed_files:
        if CONFIG.process_anyway:
            logger.warning("No changed files found for data validation, continuing due to process_anyway=True")
            return True
        raise DownloadError("No changed files found for data validation", layer=None, entity=None)

    fmt_lower = (fmt or '').lower()

    # Skip for AGS/service-style formats
    if fmt_lower in {'ags', 'arcgis', 'esri', 'ags_extract', 'wms', 'wmts'}:
        return True

    # Build predicates by expected format
    def is_match(filename: str) -> bool:
        name_lower = filename.lower()
        if fmt_lower in {'shp', 'shapefile'}:
            return name_lower.endswith('.shp')
        if fmt_lower in {'gdb', 'filegdb', 'file geodatabase', 'geodatabase', 'fgdb'}:
            # allow directories with .gdb as well as files
            path = os.path.join(work_dir, filename)
            return name_lower.endswith('.gdb') and os.path.isdir(path)
        if fmt_lower in {'geojson', 'json'}:
            return name_lower.endswith('.geojson') or name_lower.endswith('.json')
        if fmt_lower == 'pdf':
            return name_lower.endswith('.pdf')
        if fmt_lower in {'kml', 'kmz'}:
            return name_lower.endswith('.kml') or name_lower.endswith('.kmz')
        if fmt_lower in {'csv'}:
            return name_lower.endswith('.csv')
        if fmt_lower in {'xlsx', 'xls'}:
            return name_lower.endswith('.xlsx') or name_lower.endswith('.xls')
        if fmt_lower in {'tif', 'tiff', 'geotiff'}:
            return name_lower.endswith('.tif') or name_lower.endswith('.tiff')
        if fmt_lower in {'mdb', 'accdb'}:
            return name_lower.endswith('.mdb') or name_lower.endswith('.accdb')
        if fmt_lower in {'txt'}:
            return name_lower.endswith('.txt')
        # Fallback: accept common geodata types
        return any(name_lower.endswith(ext) for ext in ('.shp', '.geojson', '.gdb', '.kml', '.kmz', '.csv', '.xlsx', '.xls', '.tif', '.tiff', '.pdf'))

    matching = [f for f in changed_files if is_match(f)]
    if matching:
        logger.debug(f"Data validation: found files matching format '{fmt_lower}': {', '.join(matching[:3])}{'...' if len(matching) > 3 else ''}")
        return True

    message = (
        f"No files matching expected format '{fmt_lower}' were found among changed files. "
        f"Changed: {', '.join(changed_files[:5])}{'...' if len(changed_files) > 5 else ''}"
    )
    if CONFIG.process_anyway:
        logger.warning(message + " (continuing due to process_anyway=True)")
        return True
    raise DownloadError(message, layer=None, entity=None)

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

def _validate_file_content(file_path: str, filename: str, logger):
    """Validate that a downloaded file is not HTML content masquerading as another file type.
    
    This detects cases where PDF viewer URLs return HTML instead of the actual PDF.
    """
    try:
        # Read first 1KB of the file to check for HTML content
        with open(file_path, 'rb') as f:
            content_start = f.read(1024)
        
        # Check if content starts with HTML indicators
        content_start_str = content_start.decode('utf-8', errors='ignore').lower()
        
        html_indicators = [
            '<!doctype html',
            '<html',
            '<head>',
            '<body>',
            'content-type: text/html',
            'pdf viewer',
            'document viewer',
            'viewer.html',
            'viewer.aspx'
        ]
        
        is_html = any(indicator in content_start_str for indicator in html_indicators)
        
        if is_html:
                # Check if the filename suggests it should be a different file type
                expected_types = {
                    '.pdf': 'PDF',
                    '.shp': 'Shapefile',
                    '.zip': 'ZIP archive',
                    '.xlsx': 'Excel file',
                    '.xls': 'Excel file',
                    '.csv': 'CSV file'
                }
                
                file_ext = os.path.splitext(filename)[1].lower()
                expected_type = expected_types.get(file_ext, 'binary file')
                
                error_msg = f"Downloaded file '{filename}' contains HTML content instead of {expected_type}. This likely indicates a PDF viewer URL that returned the viewer page instead of the actual file."
                
                if not CONFIG.process_anyway:
                    logger.error(error_msg)
                    raise DownloadError(error_msg, layer=None, entity=None)
                else:
                    logger.warning(f"HTML content detected but continuing due to process_anyway=True: {error_msg}")
        
        logger.debug(f"File content validation passed for {filename}")
        
    except Exception as e:
        logger.warning(f"Could not validate file content for {filename}: {e}")
        # Don't fail the download if we can't read the file


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

# Legacy set_queue function removed - replaced with new entity filtering system

# ---------------------------------------------------------------------------
# Main Processing Function
# ---------------------------------------------------------------------------

def process_layer(layer, queue, entity_components):
    """Process a layer for entities in the queue using the 4-stage pipeline.

    Queue adjustment: move SELENIUM entities to the front so they execute first
    within the layer, enabling us to initialize/close the Selenium driver only once
    across the entire run while still grouping by layer.
    """
    if CONFIG.run_download:
        logging.info(f"Starting processing for layer '{layer}' with {len(queue)} entities")
    else:
        logging.info(f"Starting processing for layer '{layer}' (download disabled)")
    
    # Initialize CSV status tracking for entities in queue
    _initialize_csv_status(layer, queue, entity_components)
    
    # Reorder queue: SELENIUM-first within this layer
    try:
        def _is_selenium_entity(ent: str) -> bool:
            comps = entity_components.get(ent, {})
            cached = comps.get('db_fields') if isinstance(comps, dict) else None
            dl = (cached.get('download') if cached else None) or ''
            return str(dl).strip().upper() == 'SELENIUM'
        selenium_entities = [e for e in queue if _is_selenium_entity(e)]
        non_selenium_entities = [e for e in queue if not _is_selenium_entity(e)]
        queue = selenium_entities + non_selenium_entities
    except Exception:
        # If anything goes wrong, fall back to original order
        pass

    results = []
    # Track SELENIUM entities from this layer; add to global remaining set
    try:
        for e in queue:
            comps = entity_components.get(e, {})
            cached = comps.get('db_fields') if isinstance(comps, dict) else None
            dl = (cached.get('download') if cached else None) or ''
            if str(dl).strip().upper() == 'SELENIUM':
                _SELENIUM_REMAINING.add(e)
    except Exception:
        pass

    for entity in queue:
        entity_start_time = datetime.now()
        try:
            # Get entity components from dictionary
            if entity not in entity_components:
                raise RuntimeError(f"Entity '{entity}' not found in entity components")
            
            components = entity_components[entity]
            state = components['state']
            county = components['county']
            city = components['city']
            
            # Setup
            work_dir = resolve_work_dir(layer, entity, entity_components)
            entity_logger = setup_entity_logger(layer, entity, work_dir)
            
            logging.info(f"--- Processing entity: {entity} ---")
            
            # Get catalog row (prefer cached dependent fields when available)
            cached_row = components.get('db_fields') if isinstance(components, dict) else None
            catalog_row = dict(cached_row) if cached_row else _fetch_catalog_row(layer, state, county, city)
            if catalog_row is None:
                raise RuntimeError(f"Catalog row not found for {layer}/{entity}")

            # Check if entity should be processed based on format
            should_process, process_reason = should_process_entity(catalog_row, entity, SKIP_ENTITIES)
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
                raw_zip_name, data_date = layer_download(layer, entity, state, county, city, catalog_row, work_dir, entity_logger, entity_components)
            except SkipEntityError as e:
                raise  # Re-raise to skip entire entity

            # Stage 2: Metadata
            try:
                metadata = layer_metadata(layer, entity, state, county, city, catalog_row, work_dir, entity_logger, data_date)
            except SkipEntityError as e:
                # Handle metadata-based NND (data date unchanged)
                if "data date unchanged" in str(e):
                    _update_csv_status(layer, entity, 'download', 'NND', error_msg='Metadata check: data date unchanged', entity_components=entity_components)
                raise  # Re-raise to skip entire entity

            # Stage 3: Processing
            processing_skipped = False
            should_proc, proc_reason = should_run_processing(catalog_row)
            if should_proc:
                layer_processing(layer, entity, state, county, city, catalog_row, work_dir, entity_logger, metadata, entity_components)
            else:
                entity_logger.info(f"[PROCESSING] Skipping processing for {layer}/{entity}: {proc_reason}")
                _update_csv_status(layer, entity, 'processing', 'SKIPPED', error_msg=proc_reason, entity_components=entity_components)
                processing_skipped = True

            # If no raw_zip_name from download (e.g., download skipped), use catalog fallback when available
            try:
                if not raw_zip_name:
                    cz = (catalog_row.get('sys_raw_file_zip') or '').strip()
                    if cz:
                        raw_zip_name = cz
                        entity_logger.debug(f"[UPLOAD PREP] Using catalog sys_raw_file_zip as fallback: {raw_zip_name}")
            except Exception:
                pass

            # Stage 4: Upload
            layer_upload(layer, entity, state, county, city, catalog_row, work_dir, entity_logger, metadata, raw_zip_name, entity_components)

            # Record success
            entity_end_time = datetime.now()
            entity_runtime = round((entity_end_time - entity_start_time).total_seconds())
            result_entry = {
                'layer': layer,
                'entity': entity,
                'status': 'success',
                'data_date': metadata.get('data_date') or datetime.now().date(),
                'runtime_seconds': f'{entity_runtime}s',
                'start_time_iso': entity_start_time.isoformat(),
                'end_time_iso': entity_end_time.isoformat(),
                'start_time_display': entity_start_time.strftime('%m/%d/%y %I:%M %p'),
            }
            # Flag slow entities if threshold configured
            try:
                if CONFIG.slow_threshold_seconds is not None and entity_runtime >= int(CONFIG.slow_threshold_seconds):
                    result_entry['slow'] = True
            except Exception:
                pass
            if processing_skipped:
                result_entry['processing_skipped'] = True
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

            # If this entity was a SELENIUM entity, mark it as processed
            try:
                comps = entity_components.get(entity, {})
                cached = comps.get('db_fields') if isinstance(comps, dict) else None
                dl = (cached.get('download') if cached else None) or ''
                if str(dl).strip().upper() == 'SELENIUM':
                    if entity in _SELENIUM_REMAINING:
                        _SELENIUM_REMAINING.discard(entity)
                        logging.debug(f"Marked SELENIUM entity complete: {entity}; remaining: {len(_SELENIUM_REMAINING)}")
                    # If none remain anywhere, shutdown the driver now
                    if _selenium_shutdown and _SELENIUM_DRIVER and len(_SELENIUM_REMAINING) == 0:
                        try:
                            _selenium_shutdown(_SELENIUM_DRIVER)
                        except Exception:
                            pass
                        finally:
                            globals()['_SELENIUM_DRIVER'] = None
                            logging.info("Closed shared Selenium driver (all SELENIUM entities completed)")
            except Exception:
                pass

        except SkipEntityError as e:
            # Handle NND cases with publish date update
            logging.info(f"Skipping entity {entity} for layer {layer}: {e}")
            
            # If it's a "no new data" case, update publish_date to show we checked
            if "No new data available" in str(e) or "data date unchanged" in str(e):
                try:
                    layer_upload(layer, entity, state, county, city, catalog_row, work_dir, entity_logger, {})
                    logging.info(f"Updated publish_date for {entity} (NND case)")
                except Exception as publish_error:
                    logging.warning(f"Failed to update publish_date for {entity}: {publish_error}")
            
            entity_end_time = datetime.now()
            entity_runtime = (entity_end_time - entity_start_time).total_seconds()
            results.append({
                'layer': layer, 'entity': entity, 'status': 'skipped', 
                'warning': str(e), 'data_date': None, 'runtime_seconds': entity_runtime
            })
            # On skip, also mark selenium completion if applicable
            try:
                comps = entity_components.get(entity, {})
                cached = comps.get('db_fields') if isinstance(comps, dict) else None
                dl = (cached.get('download') if cached else None) or ''
                if str(dl).strip().upper() == 'SELENIUM':
                    if entity in _SELENIUM_REMAINING:
                        _SELENIUM_REMAINING.discard(entity)
                    if _selenium_shutdown and _SELENIUM_DRIVER and len(_SELENIUM_REMAINING) == 0:
                        try:
                            _selenium_shutdown(_SELENIUM_DRIVER)
                        except Exception:
                            pass
                        finally:
                            globals()['_SELENIUM_DRIVER'] = None
                            logging.info("Closed shared Selenium driver (all SELENIUM entities completed)")
            except Exception:
                pass
        except LayerProcessingError as e:
            logging.error(f"Failed to process entity {entity} for layer {layer}: {e}")
            entity_end_time = datetime.now()
            entity_runtime = (entity_end_time - entity_start_time).total_seconds()
            results.append({
                'layer': layer, 'entity': entity, 'status': 'failure', 
                'error': str(e), 'data_date': None, 'runtime_seconds': entity_runtime
            })
            # On failure, also mark selenium completion if applicable
            try:
                comps = entity_components.get(entity, {})
                cached = comps.get('db_fields') if isinstance(comps, dict) else None
                dl = (cached.get('download') if cached else None) or ''
                if str(dl).strip().upper() == 'SELENIUM':
                    if entity in _SELENIUM_REMAINING:
                        _SELENIUM_REMAINING.discard(entity)
                    if _selenium_shutdown and _SELENIUM_DRIVER and len(_SELENIUM_REMAINING) == 0:
                        try:
                            _selenium_shutdown(_SELENIUM_DRIVER)
                        except Exception:
                            pass
                        finally:
                            globals()['_SELENIUM_DRIVER'] = None
                            logging.info("Closed shared Selenium driver (all SELENIUM entities completed)")
            except Exception:
                pass

    # Calculate stats
    total_entities = len(results)
    successful_entities = len([r for r in results if r.get('status') == 'success'])
    logging.info(f"{successful_entities}/{total_entities} entities processed successfully")
    return results

# ---------------------------------------------------------------------------
# Enhanced CSV Summary Generation
# ---------------------------------------------------------------------------

def generate_summary(results, entity_components: dict = None):
    """Generate/update a living CSV summary document organized by county."""
    if not results or not CONFIG.generate_summary:
        return

    layer = results[0]['layer']
    summary_filename = f"{layer}_summary.csv"  # No date in filename - living document
    script_dir = os.path.dirname(os.path.abspath(__file__))
    summaries_dir = os.path.join(script_dir, "summaries")
    os.makedirs(summaries_dir, exist_ok=True)
    summary_filepath = os.path.join(summaries_dir, summary_filename)
    
    # Use new format headers
    headers = ['entity', 'data_date', 'download_status', 'processing_status', 
               'upload_status', 'error_message', 'timestamp']
    
    try:
        # Read existing CSV data if it exists
        existing_data = {}
        if os.path.exists(summary_filepath):
            with open(summary_filepath, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                # Check if this is old format (has county/city columns)
                old_format = 'county' in reader.fieldnames
                
                for row in reader:
                    # Skip summary rows
                    if old_format:
                        if row.get('county', '').startswith('LAST UPDATED:'):
                            continue
                    else:
                        if row.get('entity', '').startswith('LAST UPDATED:'):
                            continue
                    
                    if old_format:
                        # Convert old format to new format
                        county = row.get('county', '')
                        city = row.get('city', '')
                        entity_key = f"{county}_{city}"
                        # Create new format row
                        new_row = {
                            'entity': entity_key,
                            'data_date': row.get('data_date', ''),
                            'download_status': row.get('download_status', ''),
                            'processing_status': row.get('processing_status', ''),
                            'upload_status': row.get('upload_status', ''),
                            'error_message': row.get('error_message', ''),
                            'timestamp': row.get('timestamp', '')
                        }
                        existing_data[entity_key] = new_row
                    else:
                        # Already new format
                        entity_key = row.get('entity', '')
                        existing_data[entity_key] = row
        
        # Process results and update data
        for result in results:
            entity = result['entity']
            # Get entity components from provided dict or database
            if entity_components is None or entity not in entity_components:
                # Fallback to database query (should be rare)
                all_entities_dict = get_all_entities_from_db()
                if entity not in all_entities_dict:
                    raise ValueError(f"Entity '{entity}' not found in database. Cannot determine components.")
                components = all_entities_dict[entity]
            else:
                components = entity_components[entity]
            
            # Use the original entity name as the key
            entity_key = entity
            
            # Get existing row or create new one
            if entity_key in existing_data:
                row = existing_data[entity_key]
            else:
                row = {h: '' for h in headers}
                row['entity'] = entity
            
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
            elif status == 'skipped' and 'fields_obj_transform is null or empty' in str(error_msg):
                # fields_obj_transform is null/empty → only processing is skipped now
                # Preserve download status from prior updates if present; default to SUCCESS
                if not row.get('download_status'):
                    row['download_status'] = existing_data.get(entity, {}).get('download_status') or 'SUCCESS'
                row['processing_status'] = 'SKIPPED'
                # Allow upload to proceed; will be set by later logic if needed
                row['upload_status'] = row.get('upload_status', '')
                row['error_message'] = str(error_msg)
            elif status == 'success':
                # Full success; preserve prior stage statuses when present
                # - Keep 'NND' or 'SUCCESS' for download if already set
                # - Keep 'SKIPPED' for processing if it was intentionally skipped
                # - Keep upload status if already recorded
                if result.get('processing_skipped'):
                    row['processing_status'] = 'SKIPPED'
                else:
                    row['processing_status'] = row.get('processing_status') or 'SUCCESS'
                row['download_status'] = row.get('download_status') or 'SUCCESS'
                row['upload_status'] = row.get('upload_status') or 'SUCCESS'
                row['error_message'] = ''
                row['data_date'] = result.get('data_date', '')
            else:
                # Failure - need to determine which stage failed
                download_status, processing_status, upload_status = _determine_failure_stage(result)
                row['download_status'] = download_status
                row['processing_status'] = processing_status
                row['upload_status'] = upload_status
                row['error_message'] = str(error_msg)
            
            # Preserve per-entity timestamp: set from result if provided; do not overwrite otherwise
            if not row.get('timestamp'):
                # Prefer the per-entity start time from results
                ts = result.get('start_time_display')
                if ts:
                    row['timestamp'] = ts
                else:
                    # Fallback only for legacy rows that lack a timestamp
                    row['timestamp'] = datetime.now().strftime('%m/%d/%y %I:%M %p')
            existing_data[entity_key] = row
        
        # Sort data by entity
        sorted_data = sorted(existing_data.values(), key=lambda x: x['entity'])
        
        # Calculate summary statistics
        total_entities = len(sorted_data)
        download_success = len([r for r in sorted_data if r['download_status'] == 'SUCCESS'])
        download_total = len([r for r in sorted_data if r['download_status'] in ['SUCCESS', 'FAILED']])
        processing_success = len([r for r in sorted_data if r['processing_status'] == 'SUCCESS'])
        processing_total = len([r for r in sorted_data if r['processing_status'] in ['SUCCESS', 'FAILED']])
        upload_success = len([r for r in sorted_data if r['upload_status'] == 'SUCCESS'])
        upload_total = len([r for r in sorted_data if r['upload_status'] in ['SUCCESS', 'FAILED']])
        
        # Per-layer runtime: compute from earliest start_time among this layer's results
        end_time = datetime.now()
        try:
            start_times = []
            for r in results:
                st_iso = r.get('start_time_iso')
                if st_iso:
                    try:
                        # Handle possible timezone suffix Z by replacing with +00:00
                        st = datetime.fromisoformat(st_iso.replace('Z', '+00:00'))
                        start_times.append(st)
                    except Exception:
                        pass
            if start_times:
                earliest = min(start_times)
                total_runtime = (end_time - earliest).total_seconds()
            else:
                # Fallback to script runtime if per-entity starts are missing
                total_runtime = (end_time - CONFIG.start_time).total_seconds()
        except Exception:
            total_runtime = (end_time - CONFIG.start_time).total_seconds()
        runtime_str = _format_runtime_detailed(total_runtime)
        
        # Create summary row
        now = datetime.now()
        summary_row = {
            'entity': 'LAST UPDATED:',
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
            
            # Write data rows sorted by entity
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

def _initialize_csv_status(layer, queue, entity_components: dict = None):
    """Initialize CSV status columns to null for entities in the processing queue."""
    if not CONFIG.generate_summary:
        return
        
    summary_filename = f"{layer}_summary.csv"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    summaries_dir = os.path.join(script_dir, "summaries")
    os.makedirs(summaries_dir, exist_ok=True)
    summary_filepath = os.path.join(summaries_dir, summary_filename)
    
    # Use new format headers
    headers = ['entity', 'data_date', 'download_status', 'processing_status', 
               'upload_status', 'error_message', 'timestamp']
    
    try:
        # Read existing CSV data if it exists
        existing_data = {}
        if os.path.exists(summary_filepath):
            with open(summary_filepath, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                # Check if this is old format (has county/city columns)
                old_format = 'county' in reader.fieldnames
                
                for row in reader:
                    # Skip summary rows
                    if old_format:
                        if row.get('county', '').startswith('LAST UPDATED:'):
                            continue
                    else:
                        if row.get('entity', '').startswith('LAST UPDATED:'):
                            continue
                    
                    if old_format:
                        # Convert old format to new format
                        county = row.get('county', '')
                        city = row.get('city', '')
                        entity_key = f"{county}_{city}"
                        # Create new format row
                        new_row = {
                            'entity': entity_key,
                            'data_date': row.get('data_date', ''),
                            'download_status': row.get('download_status', ''),
                            'processing_status': row.get('processing_status', ''),
                            'upload_status': row.get('upload_status', ''),
                            'error_message': row.get('error_message', ''),
                            'timestamp': row.get('timestamp', '')
                        }
                        existing_data[entity_key] = new_row
                    else:
                        # Already new format
                        entity_key = row.get('entity', '')
                        existing_data[entity_key] = row
        
        # Initialize status columns for entities in queue
        for entity_name in queue:
            # Use the original entity name as the key
            entity_key = entity_name
            
            if entity_key in existing_data:
                row = existing_data[entity_key]
            else:
                row = {h: '' for h in headers}
                row['entity'] = entity_name
            
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

def _update_csv_status(layer, entity, stage, status, error_msg='', data_date='', entity_components: dict = None):
    """Update CSV status for a specific entity and stage."""
    if not CONFIG.generate_summary:
        return
        
    summary_filename = f"{layer}_summary.csv"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    summaries_dir = os.path.join(script_dir, "summaries")
    os.makedirs(summaries_dir, exist_ok=True)
    summary_filepath = os.path.join(summaries_dir, summary_filename)
    
    # Use new format headers
    headers = ['entity', 'data_date', 'download_status', 'processing_status', 
               'upload_status', 'error_message', 'timestamp']
    
    try:
        # Read existing CSV data
        existing_data = {}
        if os.path.exists(summary_filepath):
            with open(summary_filepath, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                # Check if this is old format (has county/city columns)
                old_format = 'county' in reader.fieldnames
                
                for row in reader:
                    # Skip summary rows
                    if old_format:
                        if row.get('county', '').startswith('LAST UPDATED:'):
                            continue
                    else:
                        if row.get('entity', '').startswith('LAST UPDATED:'):
                            continue
                    
                    if old_format:
                        # Convert old format to new format
                        county = row.get('county', '')
                        city = row.get('city', '')
                        entity_key = f"{county}_{city}"
                        # Create new format row
                        new_row = {
                            'entity': entity_key,
                            'data_date': row.get('data_date', ''),
                            'download_status': row.get('download_status', ''),
                            'processing_status': row.get('processing_status', ''),
                            'upload_status': row.get('upload_status', ''),
                            'error_message': row.get('error_message', ''),
                            'timestamp': row.get('timestamp', '')
                        }
                        existing_data[entity_key] = new_row
                    else:
                        # Already new format
                        entity_key = row.get('entity', '')
                        existing_data[entity_key] = row
        
        # Use the original entity name as the key
        entity_key = entity
        
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
            
            existing_data[entity_key] = row
            
            # Write back the updated CSV
            _write_csv_file(summary_filepath, headers, existing_data)
        
    except IOError as e:
        logging.error(f"Could not update CSV status: {e}")

def _write_csv_file(filepath, headers, data_dict):
    """Write CSV file with sorted data."""
    # Handle both old format (county/city) and new format (entity)
    if 'entity' in headers:
        # New format: sort by entity
        sorted_data = sorted(data_dict.values(), key=lambda x: x.get('entity', ''))
    else:
        # Old format: sort by county, then city
        sorted_data = sorted(data_dict.values(), key=lambda x: (x.get('county', ''), x.get('city', '') or ''))
    
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        
        # Write data rows (filter to only include valid header fields)
        for row in sorted_data:
            filtered_row = {k: v for k, v in row.items() if k in headers}
            writer.writerow(filtered_row)

# ---------------------------------------------------------------------------
# Entity Discovery and Filtering Functions  
# ---------------------------------------------------------------------------

def get_all_entities_from_db() -> dict[str, dict]:
    """Get all entities from database with their component parts and dependent fields.

    Returns:
        Dictionary mapping entity strings to their component parts plus cached DB fields.
    """
    entity_dict = {}
    conn = psycopg2.connect(PG_CONNECTION)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        # Get all valid records with populated layer_subgroup; fetch full rows for dependent field cache
        sql = (
            "SELECT * FROM m_gis_data_catalog_main "
            "WHERE status IS DISTINCT FROM 'DELETE' "
            "AND layer_subgroup IS NOT NULL"
        )
        cur.execute(sql)
        rows = cur.fetchall()
        
        logging.debug(f"Retrieved {len(rows)} entities from database")
        
        for row in rows:
            layer = row['layer_subgroup']
            state = row['state']
            county = row['county'] 
            city = row['city']
            
            # Convert to internal format for entity construction
            state_internal = format_name(state, 'state', external=False) if state else None
            county_internal = format_name(county, 'county', external=False) if county else None
            city_internal = format_name(city, 'city', external=False) if city else None
            
            # Build entity string
            entity = _entity_from_parts(layer, state_internal, county_internal, city_internal)
            
            # Store with component parts and full DB row for dependent fields
            entity_dict[entity] = {
                'layer': layer,
                'state': state_internal,
                'county': county_internal, 
                'city': city_internal,
                'db_fields': dict(row),
            }
            
    except Exception as exc:
        logging.error(f"DB entity fetch failed: {exc}")
    finally:
        cur.close()
        conn.close()
    return entity_dict

def apply_entity_filters(entities: list[str], include_patterns: list[str] = None, exclude_patterns: list[str] = None) -> list[str]:
    """Apply include/exclude filters to entity list."""
    if not entities:
        return []
    
    # Apply include filters
    if include_patterns:
        included = set()
        for pattern in include_patterns:
            matches = fnmatch.filter(entities, pattern)
            included.update(matches)
            if matches:
                logging.info(f"Include pattern '{pattern}' matched {len(matches)} entities")
            else:
                logging.warning(f"Include pattern '{pattern}' matched no entities")
        entities = list(included)
    
    # Apply exclude filters
    if exclude_patterns:
        for pattern in exclude_patterns:
            before_count = len(entities)
            entities = [e for e in entities if not fnmatch.fnmatch(e, pattern)]
            excluded_count = before_count - len(entities)
            if excluded_count > 0:
                logging.info(f"Exclude pattern '{pattern}' excluded {excluded_count} entities")
    
    return sorted(entities)

def get_filtered_entities(include_patterns: list[str] = None, exclude_patterns: list[str] = None) -> tuple[list[str], dict[str, dict]]:
    """Get entities from database with include/exclude filters applied using simple pattern matching.
    
    Returns:
        Tuple of (filtered_entities_list, entity_components_dict)
    """
    # Get all entities with their component parts
    all_entities_dict = get_all_entities_from_db()
    all_entities = list(all_entities_dict.keys())
    
    logging.info(f"Found {len(all_entities)} total entities in database")
    
    # Start with all entities
    filtered_entities = set(all_entities)
    
    # Apply include filters
    if include_patterns:
        included = set()
        for pattern in include_patterns:
            matches = fnmatch.filter(all_entities, pattern)
            included.update(matches)
            if matches:
                logging.info(f"Include pattern '{pattern}' matched {len(matches)} entities")
            else:
                logging.warning(f"Include pattern '{pattern}' matched no entities")
        filtered_entities = included
    
    # Apply exclude filters
    if exclude_patterns:
        for pattern in exclude_patterns:
            before_count = len(filtered_entities)
            filtered_entities = {e for e in filtered_entities if not fnmatch.fnmatch(e, pattern)}
            excluded_count = before_count - len(filtered_entities)
            if excluded_count > 0:
                logging.info(f"Exclude pattern '{pattern}' excluded {excluded_count} entities")
    
    filtered_entities_list = sorted(list(filtered_entities))
    logging.info(f"After applying filters: {len(filtered_entities_list)} entities selected")
    
    # Return filtered entities and their components
    filtered_components = {entity: all_entities_dict[entity] for entity in filtered_entities_list}
    
    return filtered_entities_list, filtered_components

def group_entities_by_layer(entities: list[str], entity_components: dict[str, dict]) -> dict[str, list[str]]:
    """Group entities by their layer component using the entity components dictionary."""
    groups = {}
    
    for entity in entities:
        if entity in entity_components:
            layer = entity_components[entity]['layer']
            if layer not in groups:
                groups[layer] = []
            groups[layer].append(entity)
    
    return groups

# ---------------------------------------------------------------------------
# Main Function
# ---------------------------------------------------------------------------

def main():
    """Main script execution."""
    parser = argparse.ArgumentParser(description="Clean 4-stage geospatial data processing pipeline.")
    
    # Entity filtering (replaces old layer + entities args)
    parser.add_argument("--include", nargs='*', help="Entity patterns to include (e.g., 'zoning_fl_*', 'flu_fl_miami_dade_*')")
    parser.add_argument("--exclude", nargs='*', help="Entity patterns to exclude")
    
    # Processing options
    parser.add_argument("--test-mode", action="store_true", help="Run in test mode, skipping actual execution.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    parser.add_argument("--no-log-isolation", dest='isolate_logs', action='store_false', help="Show all logs in console.")
    parser.add_argument("--no-download", action="store_true", help="Skip the download phase.")
    parser.add_argument("--no-metadata", action="store_true", help="Skip the metadata extraction phase.")
    parser.add_argument("--no-processing", action="store_true", help="Skip the processing phase.")
    parser.add_argument("--no-upload", action="store_true", help="Skip the upload phase.")
    parser.add_argument("--no-summary", action="store_true", help="Skip the summary generation.")
    parser.add_argument("--process-anyway", action="store_true", help="Continue processing even when download returns 'no new data'.")
    parser.add_argument("--slow-threshold", type=int, default=None, help="Flag entities taking >= this many seconds as slow in the summary.")
    
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
        process_anyway=args.process_anyway,
        slow_threshold_seconds=args.slow_threshold
    )
    
    initialize_logging(CONFIG.debug)

    # Prevent running with download, metadata, and processing all disabled
    if (not CONFIG.run_download) and (not CONFIG.run_metadata) and (not CONFIG.run_processing):
        logging.critical("Invalid options: cannot disable download, metadata, and processing all at once. Nothing to do.")
        sys.exit(2)

    logging.info(f"Script started at {CONFIG.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    if CONFIG.test_mode:
        logging.warning("--- RUNNING IN TEST MODE ---")

    results = []
    try:
        # Get all entities from database and apply filters
        all_entities, entity_components = get_filtered_entities(args.include, args.exclude)
        
        if not all_entities:
            logging.info("No entities to process after applying filters.")
            return

        # Group entities by layer for processing. Also seed the global selenium remaining set.
        entities_by_layer = group_entities_by_layer(all_entities, entity_components)
        try:
            global _SELENIUM_REMAINING
            _SELENIUM_REMAINING.clear()
            for layer_name, ents in entities_by_layer.items():
                for e in ents:
                    comps = entity_components.get(e, {})
                    cached = comps.get('db_fields') if isinstance(comps, dict) else None
                    dl = (cached.get('download') if cached else None) or ''
                    if str(dl).strip().upper() == 'SELENIUM':
                        _SELENIUM_REMAINING.add(e)
        except Exception:
            pass
        
        # Process each layer separately
        for layer, entities in entities_by_layer.items():
            logging.info(f"Processing layer '{layer}' with {len(entities)} entities")
            layer_results = process_layer(layer, entities, entity_components)
            
            # Generate summary for this layer's results
            if layer_results:
                generate_summary(layer_results, entity_components)
            
            results.extend(layer_results)

    except (ValueError, NotImplementedError) as e:
        logging.critical(f"A critical error occurred: {e}")
        sys.exit(1)
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)
    finally:
        end_time = datetime.now()
        # Ensure Selenium driver is closed if still open
        try:
            if _selenium_shutdown and _SELENIUM_DRIVER:
                _selenium_shutdown(_SELENIUM_DRIVER)
        except Exception:
            pass
        logging.info(f"Script finished at {end_time.strftime('%Y-%m-%d %H:%M:%S')}. Total runtime: {end_time - CONFIG.start_time}")

if __name__ == "__main__":
    main() 