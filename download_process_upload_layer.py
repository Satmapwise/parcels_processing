# Script to get any layer from any entity
# get_all_layers.py <layer> <entity>
# Layer is required, entity is optional
#   <entity> is a specific county or county_city (depending on layer) to download, process, and upload
#   if <entity> is not provided, all entities for the layer will be downloaded, processed, and uploaded

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
import paramiko
import os
import re
import fnmatch  # at top with other imports

# ---------------------------------------------------------------------------
# Load the layer/entity manifest so the script is entirely data-driven.
# ---------------------------------------------------------------------------

MANIFEST_FILE = os.path.join(os.path.dirname(__file__), 'layer_manifest.json')
try:
    with open(MANIFEST_FILE, 'r') as _mf:
        LAYER_CFG = json.load(_mf)
except FileNotFoundError as _e:
    raise RuntimeError(f"Manifest file not found: {MANIFEST_FILE}. "
                       "Please create the JSON manifest before running.") from _e

# Initial supported layers come straight from the manifest.  We may re-assign
# later in the file after legacy blocks have executed to ensure the manifest
# always wins.
layers = set(LAYER_CFG.keys())

# Override legacy hard-coded layer list with the values loaded from the manifest
layers = set(LAYER_CFG.keys())

# Entities to skip (blacklist) - add problematic entities here
SKIP_ENTITIES = {
    "hillsborough_temple_terrace",  # Example blacklisted entity
    # Add more problematic entities here as needed
}

counties = {
    "miami-dade",
    "broward",
    "palm_beach",
    "hillsborough",
    "orange",
    "pinellas",
    "duval",
    "lee",
    "polk",
    "brevard",
    "volusia",
    "pasco",
    "seminole",
    "sarasota",
    "manatee",
    "collier",
    "osceola",
    "marion",
    "lake",
    "st_lucie",
    "escambia",
    "leon",
    "alachua",
    "st_johns",
    "clay",
    "okaloosa",
    "hernando",
    "bay",
    "charlotte",
    "santa_rosa",
    "martin",
    "indian_river",
    "citrus",
    "sumter",
    "flagler",
    "highlands",
    "nassau",
    "monroe",
    "putnam",
    "walton",
    "columbia",
    "gadsden",
    "suwannee",
    "jackson",
    "hendry",
    "okeechobee",
    "levy",
    "desoto",
    "wakulla",
    "baker",
    "bradford",
    "hardee",
    "washington",
    "taylor",
    "gilchrist",
    "gulf",
    "union",
    "hamilton",
    "jefferson",
    "lafayette",
    "liberty",
    "madison",
    "glades",
    "calhoun",
    "dixie",
    "franklin"
    }

entities = {
    "miami-dade_incorporated", "miami-dade_unincorporated", 
    "broward_unified", "broward_unincorporated", 
    "palm_beach_unified", 
    "hillsborough_plant_city", "hillsborough_tampa", "hillsborough_temple_terrace", "hillsborough_unincorporated", 
    "orange_apopka", "orange_bay_lake", "orange_belle_isle", "orange_eatonville", "orange_edgewood", "orange_lake_buena_vista", "orange_maitland", "orange_oakland", "orange_ocoee", "orange_orlando", "orange_unincorporated", "orange_windermere", "orange_winter_garden", "orange_winter_park", 
    "pinellas_belleair", "pinellas_belleair_beach", "pinellas_belleair_bluffs", "pinellas_belleair_shore", "pinellas_clearwater", "pinellas_dunedin", "pinellas_gulfport", "pinellas_indian_rocks_beach", "pinellas_indian_shores", "pinellas_kenneth_city", "pinellas_largo", "pinellas_madeira_beach", "pinellas_north_redington_beach", "pinellas_oldsmar", "pinellas_pinellas_park", "pinellas_redington_beach", "pinellas_redington_shores", "pinellas_safety_harbor", "pinellas_seminole", "pinellas_south_pasadena", "pinellas_st_pete_beach", "pinellas_st_petersburg", "pinellas_tarpon_springs", "pinellas_treasure_island", "pinellas_unincorporated", 
    "duval_unified", "lee_unincorporated", "lee_bonita_springs", "lee_cape_coral", "lee_fort_myers", "lee_fort_myers_beach", "lee_sanibel", 
    "polk_unincorporated", "polk_auburndale", "polk_bartow", "polk_davenport", "polk_dundee", "polk_eagle_lake", "polk_fort_meade", "polk_frostproof", "polk_haines_city", "polk_highland_park", "polk_hillcrest_heights", "polk_lake_alfred", "polk_lake_hamilton", "polk_lake_wales", "polk_lakeland", "polk_mulberry", "polk_polk_city", "polk_winter_haven", 
    "brevard_cape_canaveral", "brevard_cocoa", "brevard_cocoa_beach", "brevard_indian_harbour_beach", "brevard_indiatlantic", "brevard_malabar", "brevard_melbourne", "brevard_melbourne_beach", "brevard_melbourne_village", "brevard_palm_bay", "brevard_palm_shores", "brevard_rockledge", "brevard_satellite_beach", "brevard_titusville", "brevard_unincorporated", "brevard_west_melbourne", 
    "volusia_daytona_beach", "volusia_daytona_beach_shores", "volusia_de_bary", "volusia_de_land", "volusia_deltona", "volusia_edgewater", "volusia_flagler_beach", "volusia_holly_hill", "volusia_lake_helen", "volusia_new_smyrna_beach", "volusia_oak_hill", "volusia_orange_city", "volusia_ormond_beach", "volusia_pierson", "volusia_ponce_inlet", "volusia_port_orange", "volusia_south_daytona", "volusia_countywide", "volusia_unincorporated", 
    "pasco_dade_city", "pasco_new_port_richey", "pasco_port_richey", "pasco_san_antonio", "pasco_st_leo", "pasco_unincorporated", "pasco_zephyrhills", 
    "seminole_altamonte_springs", "seminole_casselberry", "seminole_lake_mary", "seminole_longwood", "seminole_oviedo", "seminole_sanford", "seminole_unincorporated", "seminole_winter_springs", 
    "sarasota_unincorporated", "sarasota_longboat_key", "sarasota_north_port", "sarasota_sarasota", "sarasota_venice", 
    "manatee_unincorporated", "manatee_anna_maria", "manatee_bradenton", "manatee_bradenton_beach", "manatee_holmes_beach", "manatee_longboat_key", "manatee_palmetto", 
    "collier_unincorporated", "collier_everglades", "collier_marco_island", "collier_naples", 
    "osceola_st_cloud", "osceola_unincorporated", "osecola_kissimmee", 
    "marion_belleview", "marion_dunnellon", "marion_mcintosh", "marion_ocala", "marion_reddick", "marion_unincorporated", 
    "lake_clermont", "lake_eustis", "lake_fruitland_park", "lake_groveland", "lake_lady_lake", "lake_leesburg", "lake_minneola", "lake_mount_dora", "lake_tavares", "lake_umatilla", "lake_astatula", "lake_howey-in-the-hills", "lake_mascotte", "lake_montverde", "lake_unincorporated", 
    "st_lucie_ft_pierce", "st_lucie_port_st_lucie", "st_lucie_unincorporated", 
    "escambia_century", "escambia_pensacola", "escambia_unincorporated", 
    "leon_unified", "alachua_alachua", "alachua_archer", "alachua_gainesville", "alachua_hawthorne", "alachua_high_springs", "alachua_lacrosse", "alachua_micanopy", "alachua_newberry", "alachua_waldo", "alachua_unincorporated", 
    "st_johns_hastings", "st_johns_marineland", "st_johns_st_augustine", "st_johns_st_augustine_beach", "st_johns_unincorporated", 
    "clay_green_cove_springs", "clay_keystone_heights", "clay_orange_park", "clay_penney_farms", "clay_unincorporated", 
    "okaloosa_cinco_bayou", "okaloosa_crestview", "okaloosa_destin", "okaloosa_fort_walton_beach", "okaloosa_laurel_hill", "okaloosa_mary_esther", "okaloosa_niceville", "okaloosa_shalimar", "okaloosa_unincorporated", "okaloosa_valparaiso", 
    "hernando_brooksville", "hernando_unincorporated", "hernando_weeki_wachee", 
    "bay_callaway", "bay_cedar_grove", "bay_lynn_haven", "bay_mexico_beach", "bay_panama_city", "bay_panama_city_beach", "bay_parker", "bay_springfield", "bay_unincorporated", 
    "charlotte_unincorporated", "charlotte_punta_gorda", 
    "santa_rosa_gulf_breeze", "santa_rosa_jay", "santa_rosa_milton", "santa_rosa_unincorporated", 
    "martin_jupiter_island", "martin_ocean_breeze_park", "martin_sewalls_point", "martin_stuart", "martin_unincorporated", 
    "indian_river_fellsmere", "indian_river_indian_river_shores", "indian_river_orchid", "indian_river_sebastian", "indian_river_unincorporated", "indian_river_vero_beach", 
    "citrus_crystal_river", "citrus_inverness", "citrus_unincorporated", 
    "sumter_bushnell", "sumter_center_hill", "sumter_coleman", "sumter_unincorporated", "sumter_webster", "sumter_wildwood", 
    "flagler_beverly_beach", "flagler_bunnell", "flagler_flagler_beach", "flagler_marineland", "flagler_palm_coast", "flagler_unincorporated", 
    "highlands_unincorporated", "highlands_avon_park", "highlands_lake_placid", "highlands_sebring", 
    "nassau_callahan", "nassau_fernandina_beach", "nassau_hilliard", "nassau_unincorporated", 
    "monroe_islamorada_village_of_islands", "monroe_key_colony_beach", "monroe_key_west", "monroe_layton", "monroe_marathon", "monroe_unincorporated", 
    "putnam_crescent_city", "putnam_interlachen", "putnam_palatka", "putnam_pomona_park", "putnam_unincorporated", "putnam_welaka", 
    "walton_de_funiak_springs", "walton_freeport", "walton_paxton", "walton_unincorporated", 
    "columbia_fort_white", "columbia_lake_city", "columbia_unincorporated", 
    "gadsden_chattahoochee", "gadsden_greensboro", "gadsden_gretna", "gadsden_havana", "gadsden_midway", "gadsden_quincy", "gadsden_unincorporated", 
    "suwannee_branford", "suwannee_live_oak", "suwannee_unincorporated", 
    "jackson_alford", "jackson_bascom", "jackson_campbellton", "jackson_cottondale", "jackson_graceville", "jackson_grand_ridge", "jackson_greenwood", "jackson_jacob_city", "jackson_malone", "jackson_marianna", "jackson_sneads", "jackson_unincorporated", 
    "hendry_unincorporated", "hendry_clewiston", "hendry_labelle", 
    "okeechobee_unincorporated", "okeechobee_okeechobee", 
    "levy_bronson", "levy_cedar_key", "levy_chiefland", "levy_fanning_springs", "levy_inglis", "levy_otter_creek", "levy_unincorporated", "levy_williston", "levy_yankeetown", 
    "desoto_unincorporated", "desoto_arcadia", 
    "wakulla_sopchoppy", "wakulla_st_marks", "wakulla_unincorporated", 
    "baker_glen_st_mary", "baker_macclenny", "baker_unincorporated", 
    "bradford_brooker", "bradford_hampton", "bradford_keystone_heights", "bradford_lawtey", "bradford_starke", "bradford_unincorporated", 
    "hardee_unincorporated", "hardee_wauchula", "hardee_zolfo_springs", "hardee_bowling_green", 
    "washington_caryville", "washington_chipley", "washington_ebro", "washington_unincorporated", "washington_vernon", "washington_wausau", 
    "taylor_perry", "taylor_unincorporated", 
    "gilchrist_bell", "gilchrist_fanning_springs", "gilchrist_trenton", "gilchrist_unincorporated",
    "gulf_port_st_joe", "gulf_unincorporated", "gulf_wewahitchka", 
    "union_lake_butler", "union_raiford", "union_unincorporated", "union_worthington_springs", 
    "hamilton_jasper", "hamilton_jennings", "hamilton_unincorporated", "hamilton_white_springs", 
    "jefferson_monticello", "jefferson_unincorporated", 
    "lafayette_mayo", "lafayette_unincorporated", 
    "liberty_bristol", "liberty_unincorporated", 
    "madison_greenville", "madison_lee", "madison_madison", "madison_unincorporated", 
    "glades_unincorporated", "glades_moore_haven", 
    "calhoun_altha", "calhoun_blountstown", "calhoun_unincorporated", 
    "dixie_cross_city", "dixie_horseshoe_beach", "dixie_unincorporated", 
    "franklin_apalachicola", "franklin_carrabelle", "franklin_unincorporated"
    }

class Config:
    def __init__(self, 
                 test_mode=False, debug=True, isolate_logs=True,
                 run_download=True, run_metadata=True, run_processing=True, 
                 generate_json=True, run_upload=False, remote_enabled=False, remote_execute=False,
                 generate_summary=True, process_anyway=False
                 ):
        """
        Configuration class to hold script settings.
        """
        self.test_mode = test_mode # Dry fire external commands
        self.debug = debug # Set logging level to DEBUG
        self.isolate_logs = isolate_logs # Isolate logs to files
        self.start_time = datetime.now()
        self.run_download = run_download # Run the download phase
        self.run_metadata = run_metadata # Run the metadata extraction phase
        self.run_processing = run_processing # Run the processing phase
        self.generate_summary = generate_summary # Generate a summary file 
        self.generate_json = generate_json # Generate a json file (nulls upload)
        self.process_anyway = process_anyway # Continue processing even when download returns "no new data"
        if self.generate_json == True:
            self.run_upload = False
            self.remote_enabled = False
            self.remote_execute = False
        else: 
            self.run_upload = run_upload # Run the upload phase
            self.remote_enabled = remote_enabled # Connect to remote hosts
            if self.test_mode == True:
                self.remote_execute = False
            else:
                self.remote_execute = remote_execute # Run transfer and execute remote commands
        
        # More configuration can be added here
        # e.g. database credentials, server details
        # For now, keeping it simple
        

        # -----------------------------
        # Upload-phase configuration (all env-driven)
        # -----------------------------
        # Path where legacy update scripts deposit .backup / .bat files
        self.local_backup_dir = '/var/www/apps/mapwise/htdocs/x342/'

        # Remote incoming directory on map servers
        self.remote_incoming_dir = '/home/bmay/incoming/'

        # Remote hosts (comma-separated list)
        self.ssh_hosts = [h.strip() for h in os.environ.get(
            'REMOTE_SSH_HOSTS',
            'mapserver-m2,mapserver-prod'
        ).split(',') if h.strip()]

        # SSH credentials – user plus either key or password
        self.ssh_user = os.environ.get('REMOTE_SSH_USER', os.getenv('USER', ''))
        self.ssh_keyfile = os.environ.get('REMOTE_SSH_KEYFILE')  # optional
        self.ssh_password = os.environ.get('REMOTE_SSH_PASSWORD')  # optional (discouraged)

        # Feature toggles
        #   remote_enabled – attempt to connect to remote hosts
        #   remote_execute – actually transfer and run commands
        # REMOTE_EXECUTE defaults to true, but we force it off when test_mode=True
        

# Global config object
CONFIG = Config()
TEST_DATA = False

class LayerProcessingError(Exception):
    """Base exception for all processing errors in this script."""
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
    """Exception for when an entity should be skipped (e.g., no new data available)."""
    pass


# Function to parse the input and set the queue
def set_queue(layer, entities):
    """
    Validates layer and entity list and returns a queue of entities to process.

    Parameters
    ----------
    layer : str
        Layer name (must exist in manifest).
    entities : list[str]
        Zero or more entity identifiers provided on CLI.
        • []  → process ALL entities for the layer.
        • ["orange_unincorporated", "orange_orlando"] → process only those.
    """
    logging.info(f"Setting queue for layer '{layer}' and entities '{entities or 'all'}'")

    if layer not in layers:
        raise ValueError(f"Invalid layer specified: '{layer}'. Must be one of {layers}")

    try:
        layer_entities = set(LAYER_CFG[layer]['entities'].keys())
    except KeyError:
        raise ValueError(f"Layer '{layer}' not found in manifest.")

    # -----------------------------------
    # No entities supplied → process ALL
    # -----------------------------------
    if not entities:
        logging.info(f"No entities specified, queuing all {len(layer_entities)} entities for layer '{layer}'")
        queue = sorted(layer_entities)
    else:
        # Allow entities to be supplied as list (from argparse); also support old
        # behaviour where a *single* entity string could be passed.
        if isinstance(entities, str):
            entities = [entities]

        invalid = [e for e in entities if '*' not in e and '?' not in e and e not in layer_entities and e not in counties]
        if invalid:
            raise ValueError(f"Invalid entity/ies specified: {invalid}")

        # -------------------------------------------------
        # Expand wildcard patterns (alachua_* etc.)
        # -------------------------------------------------
        expanded = []
        for pattern in entities:
            if '*' in pattern or '?' in pattern:
                matches = fnmatch.filter(layer_entities, pattern)
                if not matches:
                    logging.warning(f"Pattern '{pattern}' matched no entities in manifest; skipping.")
                else:
                    logging.info(f"Pattern '{pattern}' expanded to {len(matches)} entities.")
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

    # Filter out blacklisted entities (applies to all cases)
    if SKIP_ENTITIES:
        original_count = len(queue)
        skipped_entities = [e for e in queue if e in SKIP_ENTITIES]
        queue = [e for e in queue if e not in SKIP_ENTITIES]
        skipped_count = original_count - len(queue)
        if skipped_count > 0:
            logging.info(f"Skipped {skipped_count} blacklisted entities: {sorted(skipped_entities)}")

    return queue


def _expand_glob_patterns(command, work_dir, logger):
    """
    Expand glob patterns in command arguments using Python's glob module.
    Returns a new command list with glob patterns expanded.
    """
    import glob
    import os
    expanded_command = []
    
    # Change to the working directory for glob expansion
    original_cwd = os.getcwd()
    try:
        os.chdir(work_dir)
        
        for arg in command:
            # Check if this argument contains glob patterns
            if isinstance(arg, str) and any(char in arg for char in '*?[]'):
                # This might be a glob pattern, try to expand it
                try:
                    # Use glob to find matching files
                    matches = glob.glob(arg)
                    if matches:
                        # Add all matches to the command
                        expanded_command.extend(matches)
                        logger.debug(f"Expanded glob pattern '{arg}' to: {matches}")
                    else:
                        # No matches found, keep the original pattern
                        expanded_command.append(arg)
                        logger.warning(f"Glob pattern '{arg}' matched no files")
                except Exception as e:
                    # If glob expansion fails, keep the original argument
                    expanded_command.append(arg)
                    logger.debug(f"Failed to expand glob pattern '{arg}': {e}")
            else:
                # No glob pattern, keep as-is
                expanded_command.append(arg)
    finally:
        # Always restore the original working directory
        os.chdir(original_cwd)
    
    return expanded_command


def _run_command(command, work_dir, logger):
    """
    Runs a shell command in a specified directory and handles execution.
    """
    if CONFIG.test_mode:
        logger.info(f"[TEST MODE] COMMAND SKIPPED IN {work_dir}: \n\n{' '.join(command)}\n")
        return
    else:
        logger.debug(f"Running command in {work_dir}: \n\n{' '.join(command)}\n")
    
    # Expand glob patterns in the command
    expanded_command = _expand_glob_patterns(command, work_dir, logger)
    
    # Using shell=False and passing command as a list is more secure
    process = subprocess.run(expanded_command, cwd=work_dir, capture_output=True, text=True)

    # For download_data.py commands, check for no new data conditions
    if (len(command) > 1 and 'download_data.py' in command[1]):
        # Check for exit code 1 from download_data.py (indicates no new data)
        if process.returncode == 1:
            if CONFIG.process_anyway:
                logger.warning("download_data.py returned exit code 1 - no new data available, but continuing with processing due to process_anyway=True")
            else:
                logger.info("download_data.py returned exit code 1 - no new data available - skipping entity")
                raise SkipEntityError("No new data available from server", layer=None, entity=None)
        # Check for "not modified" messages in output when exit code is 0
        elif process.returncode == 0:
            stdout_lower = process.stdout.lower()
            if (
                '304 not modified' in stdout_lower
                or 'not modified on server' in stdout_lower
                or 'omitting download' in stdout_lower
                or 'no new data available from server' in stdout_lower
            ):
                if CONFIG.process_anyway:
                    logger.warning("download_data.py indicates no new data available, but continuing with processing due to process_anyway=True")
                else:
                    logger.info("download_data.py indicates no new data available - skipping entity")
                    raise SkipEntityError("No new data available from server", layer=None, entity=None)

    # Only after skip logic, check for generic errors
    if process.returncode != 0:
        logger.error(f"Error executing command: {' '.join(expanded_command)}")
        logger.error(f"STDOUT: {process.stdout}")
        logger.error(f"STDERR: {process.stderr}")
        raise ProcessingError(f"Command failed with exit code {process.returncode}: {' '.join(expanded_command)}")
    
    logger.debug(f"Command output: {process.stdout}")
    return process.stdout


def setup_entity_logger(layer, entity, work_dir):
    """Sets up a dedicated logger for an entity to a file."""
    log_file_path = os.path.join(work_dir, f"{entity}.log")
    
    # Create a unique logger for each entity
    logger = logging.getLogger(f"{layer}.{entity}")
    logger.propagate = False # Prevent logs from bubbling up to the root logger
    logger.setLevel(logging.DEBUG) # Always capture debug level to the file

    # Clear existing handlers to avoid duplication if function is called multiple times
    if logger.hasHandlers():
        logger.handlers.clear()

    if CONFIG.isolate_logs:
        # Ensure directory exists so the log file can be created
        os.makedirs(work_dir, exist_ok=True)

        file_handler = logging.FileHandler(log_file_path, mode='w')  # Overwrite log each run
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)

    # Console handler – always present when logs are NOT isolated
    if not CONFIG.isolate_logs:
        console_handler = logging.StreamHandler(sys.stdout)
        console_level = logging.DEBUG if CONFIG.debug else logging.INFO
        console_handler.setLevel(console_level)
        console_formatter = logging.Formatter('%(message)s')  # Keep console output clean
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
    return logger


# Function to initialize the logging system
def initialize_logging(debug=False):
    """Initializes the logging system."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
    logging.info("Logging initialized.")


def parse_upload_block(text):
    """Return an upload plan dict parsed from *text* (stdout).

    Expected markers are printed by legacy update scripts.  If no block is
    found, returns ``None``.
    """
    lines = text.splitlines()
    in_block = False
    remote_backup = None
    commands = []
    for line in lines:
        if '----- SCRIPT to update on server' in line:
            in_block = True
            continue
        if '----- END SCRIPT to update on server' in line:
            break
        if not in_block:
            continue
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith('pg_restore') and '.backup' in stripped:
            m = re.search(r'"([^"]+\.backup)"', stripped)
            if m:
                remote_backup = m.group(1)
            commands.append(stripped)
        elif stripped.startswith('psql'):
            commands.append(stripped)

    if not remote_backup:
        return None

    basename = os.path.basename(remote_backup).rsplit('.', 1)[0]
    remote_bat = remote_backup.rsplit('.', 1)[0] + '.bat'

    return {
        'basename': basename,
        'remote_backup': remote_backup,
        'remote_bat': remote_bat,
        'commands': commands,
    }


def _ssh_connect(host):
    """Return an open Paramiko SSHClient using CONFIG credentials."""
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    kwargs = {"hostname": host, "username": CONFIG.ssh_user}
    if CONFIG.ssh_keyfile:
        kwargs["key_filename"] = CONFIG.ssh_keyfile
    elif CONFIG.ssh_password:
        kwargs["password"] = CONFIG.ssh_password
    client.connect(**kwargs)
    return client


def _looks_like_download(cmd_list):
    """Return True if *cmd_list* is one of our known download scripts."""
    if not cmd_list:
        return False
    first = os.path.basename(cmd_list[1])
    return first in {"ags_extract_data2.py", "download_data.py"}


def _looks_like_update(cmd_list):
    """Return True if *cmd_list* is an update script that might generate upload plans."""
    if not cmd_list:
        return False
    first = os.path.basename(cmd_list[1])
    return "update" in first and first.endswith(".py")


def _should_skip_when_no_download(cmd_list):
    """Return True if command should be skipped when run_download=False."""
    if not cmd_list:
        return False
    
    # Commands that depend on files created by download steps
    skip_commands = {
        'unzip',      # Depends on zip files from download
        'mv',         # Moving files that may not exist
        'zip',        # Zipping files that may not exist
        'zip_rename_date.sh',  # Depends on zip files
        'ogr2ogr',    # May depend on files from download
    }
    
    # Check if the command is in our skip list
    if len(cmd_list) > 0:
        cmd_name = os.path.basename(cmd_list[0])
        return cmd_name in skip_commands
    
    return False


def _get_directory_state(work_dir):
    """
    Get a snapshot of the current directory state (filenames and modification times).
    Returns a dict mapping filename to modification timestamp.
    """
    state = {}
    try:
        for filename in os.listdir(work_dir):
            file_path = os.path.join(work_dir, filename)
            try:
                mtime = os.path.getmtime(file_path)
                state[filename] = mtime
            except OSError:
                # Skip files we can't access
                continue
    except OSError:
        # Directory doesn't exist or can't be accessed
        pass
    return state


def _validate_download(work_dir, logger, before_state=None):
    """
    Validate that a download occurred by comparing directory state.
    
    If before_state is provided, compare against it to detect any changes.
    Otherwise, fall back to checking for files modified in the last 24 hours.
    
    This validates that a download actually occurred, but doesn't identify specific shapefiles.
    Raises DownloadError if no download is detected.
    """
    if before_state is not None:
        # Compare against pre-download state
        current_state = _get_directory_state(work_dir)
        changed_files = []
        
        # Check for new or modified files
        for filename, current_mtime in current_state.items():
            if filename not in before_state:
                changed_files.append(f"{filename} (new)")
            elif current_mtime != before_state[filename]:
                changed_files.append(f"{filename} (modified)")
        
        if not changed_files:
            raise DownloadError("No files changed during download", layer=None, entity=None)
        
        changed_files_str = ", ".join(changed_files[:3])  # Show first 3 changes
        if len(changed_files) > 3:
            changed_files_str += f" and {len(changed_files) - 3} more"
        
        logger.debug(f"Download validation passed - found changed files: {changed_files_str}")
        return True
    else:
        # Fallback to 24-hour check for backward compatibility
        now = datetime.now()
        day_ago = now - timedelta(days=1)

        # Check if any files were downloaded recently (any file type)
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

        if CONFIG.run_download == True and not recent_downloads:
            raise DownloadError("No files found modified within the last 24 hours to indicate recent download.", layer=None, entity=None)

        if CONFIG.run_download == True and recent_downloads:
            recent_files_str = ", ".join([f[1] for f in sorted(recent_downloads, key=lambda x: x[0], reverse=True)[:3]])
            logger.debug(f"Download validation passed – found recent files: {recent_files_str}")
        
        return True


def _find_shapefile(work_dir, logger):
    """
    Find the most recent shapefile in *work_dir*.
    Returns the path to the newest shapefile, or raises DownloadError if none found.
    """
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
                # File might have been removed during processing; skip it.
                continue

    if not candidate_files:
        raise DownloadError("No shapefile found in directory.", layer=None, entity=None)

    # Sort by modification time (most recent first) and get the path
    candidate_files.sort(key=lambda x: x[0], reverse=True)
    newest_shp_path = candidate_files[0][1]

    logger.debug(f"Using shapefile: {os.path.basename(newest_shp_path)} from {candidate_files[0][0].strftime('%Y-%m-%d %H:%M')}")
    return newest_shp_path


# Function to download and process a layer
def download_process_layer(layer, queue):
    """
    Dispatcher function to download and process a layer for entities in the queue.
    """
    if CONFIG.run_download == True and CONFIG.run_metadata == False and CONFIG.run_processing == False:
        logging.info(f"Starting download for layer '{layer}'")
    elif CONFIG.run_download == True and CONFIG.run_metadata == True and CONFIG.run_processing == False:
        logging.info(f"Starting download and metadata extraction for layer '{layer}'")
    elif CONFIG.run_download == True and CONFIG.run_metadata == True and CONFIG.run_processing == True:
        logging.info(f"Starting download, metadata extraction, and processing for layer '{layer}'")
    elif CONFIG.run_download == False and CONFIG.run_metadata == False and CONFIG.run_processing == True:
        logging.info(f"Starting processing for layer '{layer}'")
    elif CONFIG.run_download == False and CONFIG.run_metadata == True and CONFIG.run_processing == False:
        logging.info(f"Starting metadata extraction for layer '{layer}'")
    # else:
    #     logging.info(f"Skipping layer '{layer}': no processes active.")
    #     return [] # No need to run anything
    
    results = []
    for entity in queue:
        try:
            # Setup working directory first as logger needs it
            try:
                work_dir, county, city = resolve_work_dir(layer, entity)
            except ValueError as e:
                logging.error(str(e))
                results.append({'layer': layer, 'entity': entity, 'status': 'failure', 'error': str(e), 'data_date': None})
                continue
            logging.debug(f"Working directory: {work_dir}")
            
            # Setup logger for this specific entity
            entity_logger = setup_entity_logger(layer, entity, work_dir)

            logging.info(f"--- Processing entity: {entity} ---")

            # ------------------------------------------------------------------
            # Manifest-driven command execution (generic for all layers).
            # ------------------------------------------------------------------
            
            manifest_entry = LAYER_CFG[layer]['entities'].get(entity)
            if manifest_entry is None:
                raise ProcessingError(
                    f"Manifest is missing entry for {layer}/{entity}", layer, entity
                )

            # -------------------------
            # 1. Unified command loop (download / pre-proc / proc)
            # -------------------------

            metadata = {}
            processing_started = False
            shp_path = None
            update_script_output = None  # Track output from update scripts
            download_step_found = False  # Track if a download step was encountered
            ogrinfo_encountered = False  # Track if ogrinfo step was encountered
            for cmd in manifest_entry:

                # Placeholder 'ogrinfo' → run metadata extraction helper
                if isinstance(cmd, str) and cmd.strip().lower() == "ogrinfo":
                    ogrinfo_encountered = True
                    
                    # Check if run_download is True but no download step was found
                    if CONFIG.run_download == True and not download_step_found:
                        raise DownloadError(
                            f"Download is enabled but no download step found before ogrinfo step for {layer}/{entity}. "
                            f"Expected a download command (ags_extract_data2.py or download_data.py) before ogrinfo.",
                            layer, entity
                        )
                    
                    if CONFIG.run_metadata == True:
                        shp_to_process = None
                        if shp_path:
                            shp_to_process = shp_path
                        else:
                            # If no shp_path was set by a download step, scan the directory
                            # for a pre-existing file. This supports process-only runs.
                            try:
                                shp_to_process = _find_shapefile(work_dir, entity_logger)
                            except DownloadError:
                                pass # It's ok if no shapefile is found here.

                        if shp_to_process:
                            metadata = extract_shp_metadata(shp_to_process, entity_logger)
                            entity_logger.debug(f"Metadata extracted from: {os.path.basename(shp_to_process)}")
                        else:
                            entity_logger.warning("ogrinfo placeholder encountered but no shapefile found to process.")
                    else:
                        entity_logger.info(f"Skipping metadata extraction for {layer}/{entity} (disabled in config)")
                    continue  # skip _run_command

                cmd_list = cmd.split() if isinstance(cmd, str) else cmd

                # Substitute placeholders like {shp}, {epsg} from metadata
                if metadata:
                    formatted_cmd_list = []
                    for item in cmd_list:
                        if isinstance(item, str):
                            for key, value in list(metadata.items()):
                                placeholder = f"{{{key}}}"
                                if placeholder in item:
                                    item = item.replace(placeholder, str(value))
                        formatted_cmd_list.append(item)
                    cmd_list = formatted_cmd_list

                # Run the command
                if _looks_like_download(cmd_list): # Detects download commands
                    if CONFIG.run_download == True:
                        logging.debug(f"Running download command: {cmd_list}")
                    else:
                        logging.debug(f"Skipping download command: {cmd_list} (disabled in config)")
                    download_step_found = True  # Mark that we found a download step
                    if CONFIG.run_download == True:
                        entity_logger.debug(f"Running download for {layer}/{entity}")
                        # Capture directory state BEFORE download
                        before_state = _get_directory_state(work_dir)
                        try:
                            _run_command(cmd_list, work_dir, entity_logger)
                        except SkipEntityError as e:
                            # Skip validation and further processing for this entity
                            raise
                        if CONFIG.test_mode == False: # Only validate in non-test mode
                            try:
                                _validate_download(work_dir, entity_logger, before_state)
                            except DownloadError as de:
                                raise DownloadError(str(de), layer, entity) from de
                        else:
                            entity_logger.info(f"[TEST MODE] Skipping download validation for {layer}/{entity}")
                    else:
                        entity_logger.info(f"Skipping download for {layer}/{entity} (disabled in config)")
                else:
                    if CONFIG.run_processing == True:
                        if processing_started == False:
                            logging.debug(f"Running processing for {layer}/{entity}")
                            processing_started = True
                        
                        # Check if command should be skipped when download is disabled
                        if CONFIG.run_download == False and _should_skip_when_no_download(cmd_list):
                            entity_logger.info(f"Skipping command {cmd_list[0]} (depends on download files)")
                            continue
                        
                        # Check if this is an update script that might generate upload plans
                        if _looks_like_update(cmd_list):
                            logging.debug(f"Running update script for {layer}/{entity}")
                            update_script_output = _run_command(cmd_list, work_dir, entity_logger)
                        else:
                            stdout = _run_command(cmd_list, work_dir, entity_logger) # Runs all other commands
                    else:
                        if processing_started == False:
                            logging.info(f"Skipping processing for {layer}/{entity} (disabled in config)")
                            processing_started = True

            # Check if run_download is True but no download step was found in the entire manifest
            if CONFIG.run_download == True and not download_step_found:
                raise DownloadError(
                    f"Download is enabled but no download step found in manifest for {layer}/{entity}. "
                    f"Expected at least one download command (ags_extract_data2.py or download_data.py) in the manifest.",
                    layer, entity
                )
            
            # Record successful result with EPSG if we found it
            result_entry = {
                'layer': layer,
                'entity': entity,
                'status': 'success',
                'data_date': metadata.get('data_date') or datetime.now().date(),
            }
            if metadata.get('epsg'):
                result_entry['epsg'] = metadata['epsg']
            if metadata.get('_defaulted_today'):
                warning_msg = 'data_date defaulted to current day'
                entity_logger.warning(warning_msg)
                result_entry['warning'] = warning_msg

            # (processing of commands now handled in unified loop above)
            upload_plan = None  # will be filled when parse_upload_block triggers

            # After executing all commands, try to parse any upload plan printed
            # from the update script output (if available) or the last command output
            try:
                # Prefer update script output for upload plan parsing
                output_to_parse = update_script_output if update_script_output else (stdout if 'stdout' in locals() else None)
                if output_to_parse:
                    parsed = parse_upload_block(output_to_parse)
                    if parsed:
                        upload_plan = parsed
                        entity_logger.debug(f"Upload plan parsed from {'update script' if update_script_output else 'last command'} output")
            except Exception as e:
                entity_logger.debug(f"Failed to parse upload block: {e}")

            result_entry['upload_plan'] = upload_plan

            results.append(result_entry)
            logging.info(f"--- Successfully processed entity: {entity} ---")

        except SkipEntityError as e:
            logging.info(f"Skipping entity {entity} for layer {layer}: {e}")
            results.append({'layer': layer, 'entity': entity, 'status': 'skipped', 'warning': str(e), 'data_date': None})
        except LayerProcessingError as e:
            logging.error(f"Failed to process entity {entity} for layer {layer}: {e}")
            results.append({'layer': layer, 'entity': entity, 'status': 'failure', 'error': str(e), 'data_date': None})

    return results


# Function to upload the data (push to prod)
def upload_layer(results):
    """Transfer files and execute SQL on remote map servers."""

    logging.info("Starting upload process …")

    # Filter results that contain an upload plan
    items = [r for r in results if r.get('status') == 'success' and r.get('upload_plan')]

    if not items:
        logging.info("No upload plans found in results; nothing to do.")
        return
    logging.debug(f"Upload plans found: {items}")
    
    if not CONFIG.remote_enabled:
        logging.info("Remote upload disabled via configuration; skipping.")
        return

    for host in CONFIG.ssh_hosts:
        logging.info(f"Connecting to remote host {host} …")
        try:
            ssh = _ssh_connect(host)
        except Exception as e:
            logging.error(f"Failed to connect to {host}: {e}")
            raise UploadError(f"SSH connection failed to {host}: {e}") from e

        # If we only want to test connection, close immediately when remote_execute is False
        if not CONFIG.remote_execute:
            logging.info(f"Connection to {host} established (remote_execute disabled).")
            ssh.close()
            continue

        sftp = ssh.open_sftp()

        for row in items:
            plan = row['upload_plan']

            local_backup = os.path.join(CONFIG.local_backup_dir, f"{plan['basename']}.backup")
            local_bat = os.path.join(CONFIG.local_backup_dir, f"{plan['basename']}.bat")

            # Transfer files – overwrite existing ones
            try:
                logging.info(f"[{host}] uploading {os.path.basename(local_backup)} …")
                sftp.put(local_backup, plan['remote_backup'])
                sftp.put(local_bat, plan['remote_bat'])
            except FileNotFoundError as fe:
                logging.error(f"Local file missing: {fe}")
                raise UploadError(f"Local file missing: {fe}") from fe
            except Exception as te:
                logging.error(f"SFTP error: {te}")
                raise UploadError(f"SFTP failure to {host}: {te}") from te

            # Execute remote commands
            for cmd in plan['commands']:
                logging.info(f"[{host}] executing: {cmd}")
                stdin, stdout, stderr = ssh.exec_command(cmd)
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    err = stderr.read().decode()
                    logging.error(f"Command failed (status {exit_status}): {err}")
                    raise UploadError(f"Remote command failed on {host}: {cmd}\n{err}")

        sftp.close()
        ssh.close()
        logging.info(f"Upload to {host} completed successfully.")

    logging.info("All uploads completed.")

# Function to generate the upload plan json
def generate_json(results):
    """Generate a JSON file containing upload plans for all entities that have them.
    If the file exists, merge new plans in, overwriting by entity, and sort alphabetically."""
    
    if not results:
        logging.warning("No results to generate JSON for.")
        return
    
    # Filter results that contain an upload plan
    items_with_plans = [r for r in results if r.get('status') == 'success' and r.get('upload_plan')]
    
    if not items_with_plans:
        logging.info("No upload plans found in results; nothing to generate.")
        return
    
    # Get the layer name from the first result
    layer = items_with_plans[0]['layer']
    json_filename = f"{layer}_upload_plans_{CONFIG.start_time.strftime('%Y-%m-%d')}.json"
    
    logging.info(f"Generating upload plans JSON file: {json_filename}")
    
    # Structure the data for JSON output (new plans)
    new_upload_plans = {}
    for item in items_with_plans:
        plan = item['upload_plan']
        new_upload_plans[item['entity']] = {
            'basename': plan['basename'],
            'remote_backup': plan['remote_backup'],
            'remote_bat': plan['remote_bat'],
            'commands': plan['commands']
        }
    
    # If file exists, load and merge
    if os.path.exists(json_filename):
        try:
            with open(json_filename, 'r') as jsonfile:
                existing_upload_plans = json.load(jsonfile)
        except Exception as e:
            logging.error(f"Could not read existing JSON file: {e}")
            existing_upload_plans = {}
        # Merge: overwrite existing with new
        existing_upload_plans.update(new_upload_plans)
        merged_upload_plans = existing_upload_plans
    else:
        merged_upload_plans = new_upload_plans
    
    # Sort by entity name
    sorted_upload_plans = {k: merged_upload_plans[k] for k in sorted(merged_upload_plans)}
    
    # Write the JSON file
    try:
        with open(json_filename, 'w') as jsonfile:
            json.dump(sorted_upload_plans, jsonfile, indent=2)
        logging.info(f"Upload plans JSON file generated successfully: {json_filename}")
        logging.info(f"Generated plans for {len(sorted_upload_plans)} entities (merged mode)")
    except IOError as e:
        logging.error(f"Could not write JSON file: {e}")
        raise

# Function to generate a summary
def generate_summary(results):
    """Generates a CSV summary of the processing run."""
    if not results:
        logging.warning("No results to generate a summary for.")
        return
    
    if CONFIG.generate_summary == False:
        logging.info("Skipping summary generation.")
        return

    # Get the layer name from the first result
    layer = results[0]['layer']
    summary_filename = f"{layer}_summary_{CONFIG.start_time.strftime('%Y-%m-%d')}.csv"
    
    headers = ['layer', 'entity', 'status', 'data_date', 'warning', 'error']
    
    # Check if file already exists
    file_exists = os.path.exists(summary_filename)
    
    if file_exists:
        logging.info(f"Updating existing summary file: {summary_filename}")
    else:
        logging.info(f"Creating new summary file: {summary_filename}")
    
    try:
        if file_exists:
            # Read existing data and update/replace rows
            existing_rows = {}
            with open(summary_filename, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Use entity as the key to identify duplicates
                    existing_rows[row['entity']] = row
            
            # Update existing rows with new results
            updated_count = 0
            new_count = 0
            for result in results:
                entity = result['entity']
                row = {h: result.get(h, '') for h in headers}
                
                if entity in existing_rows:
                    existing_rows[entity] = row
                    updated_count += 1
                else:
                    existing_rows[entity] = row
                    new_count += 1
            
            # Write back all rows (existing + updated + new) in alphabetical order
            with open(summary_filename, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                # Sort rows by entity name for consistent ordering
                sorted_rows = sorted(existing_rows.values(), key=lambda row: row['entity'])
                for row in sorted_rows:
                    writer.writerow(row)
            
            logging.info(f"Summary file updated successfully. Updated: {updated_count}, Added: {new_count}, Total: {len(existing_rows)}")
        else:
            # Create new file
            with open(summary_filename, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                # Sort results by entity name for consistent ordering
                sorted_results = sorted(results, key=lambda result: result['entity'])
                for result in sorted_results:
                    row = {h: result.get(h, '') for h in headers}
                    writer.writerow(row)
            
            logging.info(f"Summary file generated successfully with {len(results)} entries.")
            
    except IOError as e:
        logging.error(f"Could not write summary file: {e}")


# ---------------------------------------------------------------------------
# Utility: extract basic metadata from the first shapefile in the working
# directory (or a specified path).  Currently returns EPSG code only.
# ---------------------------------------------------------------------------
def extract_shp_metadata(shp_path, logger):
    """Return simple metadata (currently just EPSG code) for a shapefile.

    Parameters
    ----------
    shp_path : str
        Absolute or relative path to the .shp file.
    logger : logging.Logger
        Logger for diagnostic output.

    Returns
    -------
    dict
        e.g. {"epsg": "3857"} or {} if not found / errored.
    """

    metadata = {}

    # ------------------------------------------------------------------
    # Resolve the actual shapefile path.  Caller may provide either:
    #   • direct path to a .shp
    #   • a directory containing exactly one .shp
    #   • a ***missing*** path because unzip step hasn't been written – we
    #     try to locate any *.shp in the same directory.
    # ------------------------------------------------------------------

    resolved_path = None
    if os.path.isdir(shp_path):
        # Caller passed a directory → search inside
        candidates = [f for f in os.listdir(shp_path) if f.lower().endswith(".shp")]
        if candidates:
            resolved_path = os.path.join(shp_path, candidates[0])
    elif os.path.isfile(shp_path):
        resolved_path = shp_path
    else:
        # Path not found; maybe the caller passed <dir>/<name>.shp that no
        # longer exists – fall back to directory scan.
        parent = os.path.dirname(shp_path) or "."
        if os.path.isdir(parent):
            candidates = [f for f in os.listdir(parent) if f.lower().endswith(".shp")]
            if candidates:
                resolved_path = os.path.join(parent, candidates[0])

    if resolved_path is None or not os.path.exists(resolved_path):
        logger.warning(f"Shapefile not found for metadata extraction: {shp_path}")
        return metadata

    # Record shapefile base name for downstream placeholder substitution
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

    # ------------------------------------------------------------------
    # Get the EPSG code from the WKT.
    # ------------------------------------------------------------------

    # Accept both WKT1 (PROJCS/GEOGCS) and WKT2 (PROJCRS/GEOGCRS) keywords
    projcs_match = re.search(
        r'^(?:\s*)(PROJCS|GEOGCS|PROJCRS|GEOGCRS)\["([^\"]+)"',
        result.stdout,
        re.MULTILINE,
    )
    if projcs_match:
        srs_type, srs_name = projcs_match.groups()
        # Canonicalize: lower-case and replace non-alphanum with underscore
        canonical_name = re.sub(r'[^a-z0-9]+', '_', srs_name.lower()).strip('_')

        # Lookup table – extend as needed
        name_to_epsg = {
            # Geographic WGS84
            "gcs_wgs_1984": "4326",
            "wgs_84": "4326",  # Alias used in WKT2 GEOGCRS headers

            # Web Mercator / Pseudo-Mercator
            "wgs_84_pseudo_mercator": "3857",

            # Florida State Plane (NAD83 1983)
            "nad_1983_stateplane_florida_east_fips_0901_feet": "2236",
            "nad_1983_stateplane_florida_west_fips_0902_feet": "2237",
            "nad_1983_stateplane_florida_north_fips_0903_feet": "2238",

            # Florida State Plane (NAD83 HARN)
            "nad83_harn_florida_east_ftus": "2881",
            "nad83_harn_florida_west_ftus": "2882",

            # Florida State Plane (NAD83 2011)
            "nad_1983_2011_stateplane_florida_west_fips_0902_ft_us": "6443",

            # Florida State Plane (NAD83, WKT2 naming – no 'stateplane' / 'fips')
            "nad83_florida_east_ftus": "2236",
            "nad83_florida_west_ftus": "2237",
            "nad83_florida_north_ftus": "2238",
        }

        if canonical_name in name_to_epsg:
            metadata["epsg"] = name_to_epsg[canonical_name]
            logger.debug(
                f"Mapped {srs_type} name '{srs_name}' to EPSG:{metadata['epsg']}"
            )
        else:
            logger.debug(
                f"SRS name '{srs_name}' not in lookup table; unable to map to EPSG."
            )
    else:
        logger.debug("No PROJCS/GEOGCS definition found in ogrinfo output.")
        logger.debug(f"OGRINFO output: {result.stdout}")
    
    # ------------------------------------------------------------------
    # Attempt to derive the *data* date of the shapefile.  Fallback ladder:
    #   1. Sidecar metadata XML (FGDC/ISO) next to the shapefile
    #   2. Attribute-table fields (UPDATE_DT, LAST_EDIT, etc.)
    #   3. DBF_DATE_LAST_UPDATE line in ogrinfo output
    #   4. .dbf header date (YY MM DD)
    #   5. Date encoded in a sibling .zip filename (YYYYMMDD)
    #   6. Shapefile modification time
    # ------------------------------------------------------------------

    data_date = None
    accepted_candidates = []  # list of (date, source, trust)

    today = datetime.now().date()
    MIN_DATE = datetime(2015, 1, 1).date()

    def _accept(candidate, trust):
        """Return True if *candidate* date should be accepted based on trust.

        High-trust sources are always accepted.  Medium/low trust dates that
        equal *today* (likely auto-generated) are rejected so lower rungs can
        attempt a better value.
        """
        if candidate is None:
            return False
        if trust in ("medium", "low") and candidate == today:
            return False
        if candidate < MIN_DATE:
            return False
        return True

    def _parse_datestr(s):
        """Return a date from 'YYYY-MM-DD' or 'YYYYMMDD' string."""
        try:
            s = s.strip()
            if re.fullmatch(r"\d{8}", s):
                return datetime.strptime(s, "%Y%m%d").date()
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    def _note(source, dd, trust, accepted):
        """Debug log every candidate date considered."""
        logger.debug(
            f"[data_date] source={source} candidate={dd} trust={trust} accepted={accepted}"
        )
        if accepted and dd is not None:
            accepted_candidates.append((dd, source, trust))

    # (1) Sidecar XML / metadata file (high trust)
    for cand in [
        resolved_path + ".xml",
        os.path.splitext(resolved_path)[0] + ".xml",
        os.path.splitext(resolved_path)[0] + "_metadata.xml",
    ]:
        if not os.path.exists(cand):
            continue
        try:
            with open(cand, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read()
            m = re.search(r"(\d{4}-\d{2}-\d{2})", text) or re.search(r"(\d{8})", text)
            if m:
                dd = _parse_datestr(m.group(1))
                accepted = _accept(dd, "high")
                _note("sidecar_xml", dd, "high", accepted)
                # Keep note only; final selection happens after all sources evaluated
                # if accepted:
                #     logger.debug(f"Accepted sidecar date {dd} from {cand}")
        except Exception as e:
            logger.debug(f"Failed reading sidecar metadata {cand}: {e}")

    # (2) Attribute-table date fields (high trust)
    try:
        sf = shapefile.Reader(resolved_path)
        field_names = [f[0].lower() for f in sf.fields[1:]]
        candidate_cols = [
            "update_dt","updated","last_edit","lastupdate",
            "edit_date","date_upd","datadate","effective",
            "rev_date","eff_date","effdate","date_eff"
        ]
        for col in candidate_cols:
            if col in field_names:
                idx = field_names.index(col)
                values = [rec[idx] for rec in sf.records() if rec[idx]]
                parsed = [_parse_datestr(str(v)) for v in values]
                parsed = [p for p in parsed if p]
                if parsed:
                    dd = max(parsed)
                    accepted = _accept(dd, "high")
                    _note(f"attribute_col:{col}", dd, "high", accepted)
                    # Keep note only; final selection happens after all sources evaluated
                    # if accepted:
                    #     logger.debug(f"Accepted attribute date {dd} from {f'attribute_col:{col}'}")
                    break
    except ImportError:
        logger.debug("pyshp not available; skipping attribute-date check.")
    except Exception as e:
        logger.debug(f"Error scanning attribute table for date: {e}")

    # (3) DBF_DATE_LAST_UPDATE in ogrinfo output (medium trust)
    m = re.search(r"DBF_DATE_LAST_UPDATE=([0-9]{4}-[0-9]{2}-[0-9]{2})", result.stdout)
    if m:
        dd = _parse_datestr(m.group(1))
        accepted = _accept(dd, "medium")
        _note("dbf_last_update", dd, "medium", accepted)
        # Keep note only; final selection happens after all sources evaluated
        # if accepted:
        #     logger.debug(f"Accepted DBF_DATE_LAST_UPDATE date {dd}")

    # (4) DBF header date bytes (medium trust)
    dbf_path = os.path.splitext(resolved_path)[0] + ".dbf"
    if os.path.exists(dbf_path):
        try:
            with open(dbf_path, "rb") as fh:
                fh.seek(1)
                y, mth, d = fh.read(3)
                year = 1900 + y
                if year < 1990:
                    year += 100
                dd = datetime(year, mth, d).date()
                accepted = _accept(dd, "medium")
                _note("dbf_header", dd, "medium", accepted)
                # Keep note only; final selection happens after all sources evaluated
                # if accepted:
                #     logger.debug(f"Accepted DBF header date {dd}")
        except Exception as e:
            logger.debug(f"Failed reading DBF header date: {e}")

    # (5) Zip filename pattern (YYYYMMDD) (low trust)
    parent = os.path.dirname(resolved_path) or "."
    try:
        for fname in os.listdir(parent):
            if fname.lower().endswith(".zip"):
                m = re.search(r"(\d{8})", fname)
                if m:
                    dd = _parse_datestr(m.group(1))
                    accepted = _accept(dd, "low")
                    _note("zip_filename", dd, "low", accepted)
                    # Keep note only; final selection happens after all sources evaluated
                    if accepted:
                        # logger.debug(f"Accepted zip filename date {dd} from {fname}")
                        break
    except Exception as e:
        logger.debug(f"Zip filename date check failed: {e}")

    # (6) Shapefile modification time (low trust)
    dd = datetime.fromtimestamp(os.path.getmtime(resolved_path)).date()
    accepted = _accept(dd, "low")
    _note("file_mtime", dd, "low", accepted)
    # Keep note only; final selection happens after all sources evaluated
    # if accepted:
    #     logger.debug(f"Accepted file mtime date {dd}")

    # Choose the most recent accepted date across all sources.
    if accepted_candidates:
        # Sort by date asc, take last (latest).
        data_date = sorted(accepted_candidates, key=lambda t: t[0])[-1][0]
        logger.debug(f"Chosen data_date (latest accepted): {data_date}")
    else:
        # Final fallback – if everything was rejected, use today.
        data_date = today
        metadata["_defaulted_today"] = True

    metadata["data_date"] = data_date.strftime("%Y-%m-%d") if data_date else ""

    # Always stamp update_date as today for provenance
    metadata["update_date"] = datetime.now().date().strftime("%Y-%m-%d")

    return metadata


# ---------------------------------------------------------------------------
# Helper: derive county and city components from an entity string.
# Handles multi-word counties such as "st_lucie" or "santa_rosa" by matching
# the longest county prefix present in the predefined `counties` set.
# ---------------------------------------------------------------------------

def split_entity(entity: str):
    """Return (county, city) parts for an *entity* identifier.

    The *entity* parameter is expected to be the manifest key such as
    ``hillsborough_plant_city`` or ``st_lucie_unincorporated``.

    The logic attempts to find the *longest* county name from the global
    ``counties`` set that is a prefix of *entity*.  This allows us to handle
    both single-word and multi-word county names.

    Examples
    --------
    >>> split_entity('hillsborough_plant_city')
    ('hillsborough', 'plant_city')
    >>> split_entity('st_lucie_unincorporated')
    ('st_lucie', 'unincorporated')
    >>> split_entity('santa_rosa_unincorporated')
    ('santa_rosa', 'unincorporated')
    """
    # Sort counties by length descending so we match the longest prefix first
    for county in sorted(counties, key=len, reverse=True):
        if entity == county:
            return county, ''  # County-only entity (rare)
        prefix = f"{county}_"
        if entity.startswith(prefix):
            city = entity[len(prefix):]
            return county, city
    # If we get here, no county matched – raise for calling code to handle.
    raise ValueError(f"Unable to parse county/city from entity '{entity}'.")


# ---------------------------------------------------------------------------
# Work-directory patterns per layer.  Keys are layer names; values are
# ``str.format`` templates that can reference {layer}, {county} and {city}.
# If a layer is not listed, a generic 3-part template is used.
# ---------------------------------------------------------------------------

# Base directory on the scraper host – adjust if your environment differs.
DATA_ROOT = '/srv/datascrub'

WORK_DIR_PATTERNS = {
    # ------------------------------------------------------------------
    # Layer-specific layouts – use str.format with {{layer}}, {{county}}, {{city}}
    # ------------------------------------------------------------------

    # Zoning –  …/08_Land_Use_and_Zoning/zoning/florida/county/<county>/current/source_data/<city>
    'zoning': os.path.join(
        DATA_ROOT,
        '08_Land_Use_and_Zoning',
        'zoning',
        'florida',
        'county',
        '{county}',
        'current',
        'source_data',
        '{city}'
    ),

    # FLU – stored under *future_land_use* folder
    # …/08_Land_Use_and_Zoning/future_land_use/florida/county/<county>/current/source_data/<city>
    'flu': os.path.join(
        DATA_ROOT,
        '08_Land_Use_and_Zoning',
        'future_land_use',
        'florida',
        'county',
        '{county}',
        'current',
        'source_data',
        '{city}'
    ),
}


def resolve_work_dir(layer: str, entity: str):
    """Return (work_dir, county, city) for *layer* / *entity*.

    The logic consults ``WORK_DIR_PATTERNS`` to decide whether the template
    requires a *city* component.  If so we call :func:`split_entity`.  If not,
    we treat the entire entity string as the county id.
    """
    if TEST_DATA:
        county, city = split_entity(entity)
        if city == 'unincorporated' and county == 'hillsborough':
            work_dir = os.path.join('data', layer, county, 'unincorporated_hillsborough')
        elif city == 'unincorporated' and county == 'orange':
            work_dir = os.path.join('data', layer, county, 'unincorporated_orange')
        else:
            work_dir = os.path.join('data', layer, county, city)
        return work_dir, county, city
    else:
        if layer == 'zoning' and entity == 'hillsborough_plant_city':
            county = 'hillsborough'
            city = 'plant_city'
            work_dir = '/mnt/sdb/datascrub/08_Land_Use_and_Zoning/zoning/florida/county/hillsborough/current/source_data/plant_city'
            return work_dir, county, city
        elif layer == 'zoning' and entity == 'duval_unified':
            county = 'duval'
            city = 'jacksonville'
            work_dir = '/srv/datascrub/08_Land_Use_and_Zoning/zoning/florida/county/duval/current/source_data/jacksonville'
            return work_dir, county, city
        template = WORK_DIR_PATTERNS.get(layer, os.path.join('data', '{layer}', '{county}', '{city}'))
        needs_city = '{city}' in template
        if needs_city:
            county, city = split_entity(entity)
        else:
            county, city = entity, ''

    work_dir = template.format(layer=layer, county=county, city=city)
    return work_dir, county, city


# Main function
def main():
    """Main script execution."""
    parser = argparse.ArgumentParser(description="Download, process, and upload geospatial data layers.")
    parser.add_argument("layer", help="The layer to process.")
    parser.add_argument("entities", nargs='*', help=(
        "Optional space-separated list of entity IDs (e.g. 'hillsborough_tampa orange_orlando'). "
        "If omitted, all entities defined for the layer will be processed.") )
    parser.add_argument("--test-mode", action="store_true", help="Run in test mode, skipping actual execution of external tools and uploads.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging on the console.")
    parser.add_argument("--no-log-isolation", dest='isolate_logs', action='store_false', help="Show all logs in the console instead of isolating them to files.")
    parser.add_argument("--no-download", action="store_true", help="Skip the download phase.")
    parser.add_argument("--no-metadata", action="store_true", help="Skip the metadata extraction phase.")
    parser.add_argument("--no-processing", action="store_true", help="Skip the processing phase.")
    parser.add_argument("--no-upload", action="store_true", help="Skip the upload phase.")
    parser.add_argument("--no-summary", action="store_true", help="Skip the summary generation.")
    parser.add_argument("--process-anyway", action="store_true", help="Continue processing even when download returns 'no new data' (except for commands that depend on download files).")
    
    args = parser.parse_args()

    # Initialize config and logging
    global CONFIG
    global TEST_DATA
    CONFIG = Config()
    if args.test_mode:
        CONFIG.test_mode = True
    if args.debug:
        CONFIG.debug = True
    if args.isolate_logs is False:
        CONFIG.isolate_logs = False
    if args.no_download:
        CONFIG.run_download = False
    if args.no_metadata:
        CONFIG.run_metadata = False
    if args.no_processing:
        CONFIG.run_processing = False
    if args.no_upload:
        CONFIG.run_upload = False
    if args.no_summary:
        CONFIG.generate_summary = False
    if args.process_anyway:
        CONFIG.process_anyway = True
    initialize_logging(CONFIG.debug)

    logging.info(f"Script started at {CONFIG.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    if CONFIG.test_mode:
        logging.warning("--- RUNNING IN TEST MODE ---")

    results = []
    try:
        # 1. Set the queue of entities to process
        queue = set_queue(args.layer, args.entities)

        # Check if queue is empty after blacklist filtering
        if not queue:
            logging.info("No entities to process.")
            return

        # 2. Download and process the layer for each entity
        results = download_process_layer(args.layer, queue)

        # 3. Upload the processed data
        if CONFIG.generate_json == True:
            generate_json(results)
        elif CONFIG.run_upload == True:
            upload_layer(results)
        else:
            logging.info(f"Skipping upload for layer '{args.layer}' (disabled in config)")

    except (ValueError, NotImplementedError) as e:
        logging.critical(f"A critical error occurred: {e}")
        sys.exit(1)
    except Exception as e:
        logging.critical(f"An unexpected error occurred in the main workflow: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # 4. Generate a summary of the run
        generate_summary(results)
        end_time = datetime.now()
        logging.info(f"Script finished at {end_time.strftime('%Y-%m-%d %H:%M:%S')}. Total runtime: {end_time - CONFIG.start_time}")


if __name__ == "__main__":
    main()