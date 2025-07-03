# Script to get any layer from any entity
# get_all_layers.py <layer> <entity>
# Layer is required, entity is optional
#   <entity> is a specific county or county_city (depending on layer) to download, process, and upload
#   if <entity> is not provided, all entities for the layer will be downloaded, processed, and uploaded

import sys
import logging
import argparse
import subprocess
from datetime import datetime
import os
import csv
import json
import re

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
                 test_mode=True, debug=True, isolate_logs=False,
                 run_download=False, run_metadata=True, run_processing=False, run_upload=False,
                 generate_summary=False
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
        self.run_upload = run_upload # Run the upload phase
        self.generate_summary = generate_summary # Generate a summary file
        # More configuration can be added here
        # e.g. database credentials, server details
        # For now, keeping it simple

# Global config object
CONFIG = Config()

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


# Function to parse the input and set the queue
def set_queue(layer, entity):
    """
    Validates layer and entity and returns a queue of entities to process.
    """
    logging.info(f"Setting queue for layer '{layer}' and entity '{entity or 'all'}'")
    if layer not in layers:
        raise ValueError(f"Invalid layer specified: '{layer}'. Must be one of {layers}")

    try:
        layer_entities = set(LAYER_CFG[layer]['entities'].keys())
    except KeyError:
        raise ValueError(f"Layer '{layer}' not found in manifest.")

    queue = []
    if entity:
        # Allow direct entity name or county-wide alias if listed separately.
        if entity not in layer_entities and entity not in counties:
            raise ValueError(f"Invalid entity specified: '{entity}'.")
        queue.append(entity)
    else:
        logging.info(f"No entity specified, queuing all {len(layer_entities)} entities for layer '{layer}'")
        queue.extend(sorted(layer_entities))

    logging.info(f"Queue set with {len(queue)} items.")
    return queue


def _run_command(command, work_dir, logger):
    """
    Runs a shell command in a specified directory and handles execution.
    """
    logger.debug(f"Running command: {' '.join(command)} in {work_dir}")
    if CONFIG.test_mode:
        logger.info(f"[TEST MODE] Would run: {' '.join(command)}")
        return
    
    # Using shell=False and passing command as a list is more secure
    process = subprocess.run(command, cwd=work_dir, capture_output=True, text=True)

    if process.returncode != 0:
        logger.error(f"Error executing command: {' '.join(command)}")
        logger.error(f"STDOUT: {process.stdout}")
        logger.error(f"STDERR: {process.stderr}")
        raise ProcessingError(f"Command failed with exit code {process.returncode}")
    
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

    # File handler - always logs DEBUG and above to the entity's log file
    file_handler = logging.FileHandler(log_file_path, mode='w') # Overwrite log each run
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    # Console handler - only adds if log isolation is off
    if not CONFIG.isolate_logs:
        console_handler = logging.StreamHandler(sys.stdout)
        console_level = logging.DEBUG if CONFIG.debug else logging.INFO
        console_handler.setLevel(console_level)
        console_formatter = logging.Formatter('%(message)s') # Keep console output clean
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
    return logger


# Function to initialize the logging system
def initialize_logging(debug=False):
    """Initializes the logging system."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
    logging.info("Logging initialized.")

# Function to download and process a layer
def download_process_layer(layer, queue):
    """
    Dispatcher function to download and process a layer for entities in the queue.
    """
    logging.info(f"Starting download and process for layer '{layer}'")
    
    results = []
    for entity in queue:
        try:
            # Setup working directory first as logger needs it
            parts = entity.split('_')
            county = parts[0]
            city = '_'.join(parts[1:])
            work_dir = os.path.join('data', layer, county, city)
            os.makedirs(work_dir, exist_ok=True)
            logging.debug(f"Working directory: {work_dir}")
            
            # Setup logger for this specific entity
            entity_logger = setup_entity_logger(layer, entity, work_dir)

            entity_logger.info(f"--- Processing entity: {entity} ---")

            # ------------------------------------------------------------------
            # Manifest-driven command execution (generic for all layers).
            # ------------------------------------------------------------------
            
            manifest_entry = LAYER_CFG[layer]['entities'].get(entity)
            if manifest_entry is None:
                raise ProcessingError(
                    f"Manifest is missing entry for {layer}/{entity}", layer, entity
                )

            # -------------------------
            # 1. Download phase
            # -------------------------
            if CONFIG.run_download == True:
                entity_logger.info(f"Running download phase for {entity}")
                download_cmds = manifest_entry.get("download") or manifest_entry.get("commands", [])
                for cmd in download_cmds:
                    cmd_list = cmd.split() if isinstance(cmd, str) else cmd
                    _run_command(cmd_list, work_dir, entity_logger)

            # -------------------------
            # 2. Metadata extraction (first .shp found)
            # -------------------------
            metadata = {}
            if CONFIG.run_metadata == True:
                entity_logger.info(f"Running metadata extraction phase for {entity}")
                shp_files = [f for f in os.listdir(work_dir) if f.lower().endswith('.shp')]
                if shp_files:
                    shp_path = os.path.join(work_dir, shp_files[0])
                    metadata = extract_shp_metadata(shp_path, entity_logger)
                    entity_logger.debug(f"Metadata: {metadata}")
                else:
                    entity_logger.warning("No shapefile found in work_dir after download phase; skipping metadata extraction.")

            # -------------------------
            # 3. Processing phase (allow simple {epsg} placeholder substitution)
            # -------------------------
            if CONFIG.run_processing == True:
                entity_logger.info(f"Running processing phase for {entity}")
                processing_cmds = manifest_entry.get("processing", [])
                for cmd in processing_cmds:
                    if isinstance(cmd, str):
                        try:
                            cmd_formatted = cmd.format(**metadata)
                        except KeyError as ke:
                            entity_logger.error(f"Missing metadata key {ke} for command '{cmd}'. Skipping this command.")
                            continue
                        cmd_list = cmd_formatted.split()
                    else:
                        # If it's a list we attempt placeholder replacement on each token
                        cmd_list = [token.format(**metadata) if isinstance(token, str) else token for token in cmd]
                    _run_command(cmd_list, work_dir, entity_logger)

            # Record successful result with EPSG if we found it
            result_entry = {
                'layer': layer,
                'entity': entity,
                'status': 'success',
                'data_date': datetime.now().date(),
            }
            if metadata.get('epsg'):
                result_entry['epsg'] = metadata['epsg']

            results.append(result_entry)
            entity_logger.info(f"--- Successfully processed entity: {entity} ---")

        except LayerProcessingError as e:
            logging.error(f"Failed to process entity {entity} for layer {layer}: {e}")
            results.append({'layer': layer, 'entity': entity, 'status': 'failure', 'error': str(e), 'data_date': None})

    return results


# ---------------------------------------------------------------------------
# Legacy: zoning download and process workflow
# ---------------------------------------------------------------------------
def _download_process_zoning(layer, entity, county, city, work_dir, logger):
    """Implementation of the zoning download and process workflow."""
    logger.info(f"Running ZONING workflow for {entity}")
    
    shp_name_raw = f"zoning_{entity}.shp"
    
    # --- Download Step ---
    try:
        logger.info("Step 1: Downloading data...")
        # e.g., ags_extract_data2.py zoning_alachua_city delete 15
        download_script = os.path.join('..', '..', '..', '..', 'download_tools', 'ags_extract_data2.py')
        download_arg = f"zoning_{entity}"
        _run_command([sys.executable, download_script, download_arg, "delete", "15"], work_dir, logger)
        # Simple check if the shapefile was created
        if not os.path.exists(os.path.join(work_dir, shp_name_raw)) and not CONFIG.test_mode:
            raise DownloadError("Shapefile not found after download script execution.", layer, entity)
    except Exception as e:
        raise DownloadError(f"Data download failed: {e}", layer, entity) from e

    # --- Processing Step ---
    try:
        logger.info("Step 2: Processing data...")
        # Note: The psql command from docs is layer-specific and complex.
        # For now, we'll just call the python processing script.
        # A full implementation would require a template for the psql command.
        logger.info("Skipping psql update for support.zoning_transform (requires credentials).")

        processing_script = os.path.join('..', '..', '..', '..', 'processing_tools', 'update_zoning_v3.py')
        _run_command([sys.executable, processing_script, county, city], work_dir, logger)

    except Exception as e:
        raise ProcessingError(f"Data processing failed: {e}", layer, entity) from e
    
    logger.info("Zoning workflow completed successfully.")
    # In a real run, we'd extract the data_date from a file or the download process
    return {'layer': layer, 'entity': entity, 'status': 'success', 'data_date': datetime.now().date()}


# ---------------------------------------------------------------------------
# Legacy: flu download and process workflow
# ---------------------------------------------------------------------------
def _download_process_flu(layer, entity, county, city, work_dir, logger):
    """Implementation of the flu download and process workflow."""
    logger.info(f"Running FLU workflow for {entity}")
    
    shp_name_raw = f"flu_{entity}.shp"
    shp_name_single = f"flu_{county}_{city}_single.shp"

    # --- Download Step ---
    try:
        logger.info("Step 1: Downloading data...")
        # e.g., ags_extract_data2.py flu_alachua_city delete 15
        download_script = os.path.join('..', '..', '..', '..', 'download_tools', 'ags_extract_data2.py')
        download_arg = f"flu_{entity}"
        _run_command([sys.executable, download_script, download_arg, "delete", "15"], work_dir, logger)
        if not os.path.exists(os.path.join(work_dir, shp_name_raw)) and not CONFIG.test_mode:
            raise DownloadError("Shapefile not found after download script execution.", layer, entity)
    except Exception as e:
        raise DownloadError(f"Data download failed: {e}", layer, entity) from e

    # --- Processing Step ---
    try:
        logger.info("Step 2: Processing data (ogr2ogr)...")
        # e.g., ogr2ogr -explodecollections -select "FLU, FLU_NAME" flu_alachua_alachua_single.shp flu_alachua_city.shp
        _run_command([
            "ogr2ogr", "-explodecollections",
            "-select", "FLU,FLU_NAME",
            shp_name_single,
            shp_name_raw
        ], work_dir, logger)

        logger.info("Step 3: Processing data (update script)...")
        # Note: Skipping psql update for support.flu_transform (requires credentials).
        # We assume `update_zoning_v3.py` is the generic update script for now.
        # The docs mentioned `update_flu.py` which is not present.
        processing_script = os.path.join('..', '..', '..', '..', 'processing_tools', 'update_zoning_v3.py')
        _run_command([sys.executable, processing_script, county, city], work_dir, logger)

    except Exception as e:
        raise ProcessingError(f"Data processing failed: {e}", layer, entity) from e

    logger.info("FLU workflow completed successfully.")
    return {'layer': layer, 'entity': entity, 'status': 'success', 'data_date': datetime.now().date()}

# Function to upload the data (push to prod)
def upload_layer(results):
    """
    Uploads processed data. Connects to servers, transfers files, runs psql.
    """
    logging.info("Starting upload process...")
    successful_items = [r for r in results if 'success' in r['status']]
    
    if not successful_items:
        logging.info("No successful items to upload.")
        return

    if CONFIG.test_mode:
        logging.info(f"[TEST MODE] Would upload data for {len(successful_items)} items.")
        return

    # This is where SSH/SCP logic would go (e.g., using paramiko or subprocess)
    raise NotImplementedError("Upload logic is not yet implemented.")

# Function to generate a summary
def generate_summary(results):
    """Generates a CSV summary of the processing run."""
    if not results:
        logging.warning("No results to generate a summary for.")
        return
    
    if CONFIG.generate_summary == False:
        logging.info("Skipping summary generation.")
        return

    summary_filename = f"summary_{CONFIG.start_time.strftime('%Y%m%d_%H%M%S')}.csv"
    logging.info(f"Generating summary file: {summary_filename}")

    headers = ['layer', 'entity', 'status', 'data_date', 'error']
    
    try:
        with open(summary_filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for result in results:
                # Ensure all keys are present
                row = {h: result.get(h, '') for h in headers}
                writer.writerow(row)
        logging.info("Summary file generated successfully.")
    except IOError as e:
        logging.error(f"Could not write summary file: {e}")


# ---------------------------------------------------------------------------
# Legacy: generic manifest-driven workflow (applies to every layer/entity without a
# bespoke Python implementation).
# ---------------------------------------------------------------------------
def _download_process_generic(layer, entity, county, city, work_dir, logger):
    """Generic download & process workflow sourced entirely from the JSON manifest."""
    logger.info(f"Running GENERIC workflow for {layer}/{entity}")

    try:
        commands = LAYER_CFG[layer]['entities'][entity]['commands']
    except KeyError as _e:
        raise ProcessingError(f"Manifest is missing commands for {layer}/{entity}: {_e}", layer, entity)

    for cmd in commands:
        # Accept either list or single string commands
        if isinstance(cmd, str):
            cmd_list = cmd.split()
        else:
            cmd_list = cmd
        _run_command(cmd_list, work_dir, logger)

    logger.info("Generic workflow completed successfully.")
    return {'layer': layer, 'entity': entity, 'status': 'success', 'data_date': datetime.now().date()}


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

    if not os.path.exists(shp_path):
        logger.warning(f"Shapefile not found for metadata extraction: {shp_path}")
        return metadata

    try:
        result = subprocess.run(
            ["ogrinfo", "-ro", "-al", "-so", shp_path],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.warning(f"ogrinfo failed while reading {shp_path}: {e}")
        return metadata

    # ------------------------------------------------------------------
    # Parse the first PROJCS/GEOGCS line from the WKT and map it to an EPSG
    # code via a lookup table (to be expanded).
    # ------------------------------------------------------------------

    projcs_match = re.search(r'^(?:\s*)(PROJCS|GEOGCS)\["([^\"]+)"', result.stdout, re.MULTILINE)
    if projcs_match:
        srs_type, srs_name = projcs_match.groups()
        # Canonicalize: lower-case and replace non-alphanum with underscore
        canonical_name = re.sub(r'[^a-z0-9]+', '_', srs_name.lower()).strip('_')

        # Lookup table – extend as needed
        name_to_epsg = {
            # Geographic WGS84
            "gcs_wgs_1984": "4326",

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

    return metadata


# Main function
def main():
    """Main script execution."""
    parser = argparse.ArgumentParser(description="Download, process, and upload geospatial data layers.")
    parser.add_argument("layer", help="The layer to process.")
    parser.add_argument("entity", nargs='?', help="The specific entity (e.g., miami-dade_unincorporated) to process. If omitted, all entities for the layer are processed.")
    parser.add_argument("--test-mode", action="store_true", help="Run in test mode, skipping actual execution of external tools and uploads.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging on the console.")
    parser.add_argument("--no-log-isolation", dest='isolate_logs', action='store_false', help="Show all logs in the console instead of isolating them to files.")
    parser.add_argument("--no-download", action="store_true", help="Skip the download phase.")
    parser.add_argument("--no-metadata", action="store_true", help="Skip the metadata extraction phase.")
    parser.add_argument("--no-processing", action="store_true", help="Skip the processing phase.")
    parser.add_argument("--no-upload", action="store_true", help="Skip the upload phase.")
    parser.add_argument("--no-summary", action="store_true", help="Skip the summary generation.")
    
    args = parser.parse_args()

    # Initialize config and logging
    global CONFIG
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
    initialize_logging(CONFIG.debug)

    logging.info(f"Script started at {CONFIG.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    if CONFIG.test_mode:
        logging.warning("--- RUNNING IN TEST MODE ---")

    results = []
    try:
        # 1. Set the queue of entities to process
        queue = set_queue(args.layer, args.entity)

        # 2. Download and process the layer for each entity
        results = download_process_layer(args.layer, queue)

        # 3. Upload the processed data
        if CONFIG.run_upload == True:
            upload_layer(results)

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