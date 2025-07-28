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

# ---------------------------------------------------------------------------
# Configuration and Constants
# ---------------------------------------------------------------------------

# Database connection - should match layers_scrape.py
PG_CONNECTION = "host=gisdb.manatee.org port=5433 dbname=gis user=smay sslmode=require"

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

def title_case(s: str) -> str:
    """Return a human-friendly title-case string.
    
    • Replaces underscores with spaces so words are separated correctly.
    • Capitalises each word except short stop-words ('of', 'and', etc.) unless the word
      is the first in the string.
    """
    cleaned = " ".join(part for part in s.replace("_", " ").split())
    words = cleaned.split()

    def cap_token(tok: str, is_first: bool) -> str:
        """Capitalize a token, preserving stop-words and hyphenated sub-parts."""
        # Handle hyphenated names like "miami-dade" or "howey-in-the-hills"
        parts = tok.split("-")
        new_parts: list[str] = []
        stop_words = {"of", "and", "in", "the"}
        abbrev_map = {"st": "St", "ft": "Ft", "mt": "Mt"}
        for j, p in enumerate(parts):
            first_in_phrase = is_first and j == 0
            plow = p.lower()
            if plow in abbrev_map:
                new_parts.append(abbrev_map[plow])
            elif first_in_phrase or (plow not in stop_words and len(p) > 2):
                new_parts.append(p.capitalize())
            else:
                new_parts.append(p.lower())
        return "-".join(new_parts)

    return " ".join(cap_token(w, i == 0) for i, w in enumerate(words))

def norm_city(city: Optional[str]) -> str:
    """Normalise a city string to lowercase+underscores (non-alnum → _ , collapse). Accepts None."""
    if not city:
        return ""
    cleaned = re.sub(r"[^a-z0-9]+", "_", city.lower())
    return cleaned.strip("_")

def norm_county(county: Optional[str]) -> str:
    """Normalise county name by converting non-alnum to underscores and removing the word 'county'."""
    if not county:
        return ""
    county_lc = county.lower().replace("county", "")
    cleaned = re.sub(r"[^a-z0-9]+", "_", county_lc)
    return cleaned.strip("_")

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
        self.conn = psycopg2.connect(conn_str)
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

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

    # Normalise layer name
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
    county_suffix_re = re.compile(r"^([A-Za-z\s\-]+?)\s+(unincorporated|incorporated|unified|countywide)$", re.I)
    county_only_re = re.compile(r"^([A-Za-z\s\-]+?)\s+county$", re.I)

    m_city = city_re.match(rest_main)
    if m_city:
        city = m_city.group(1).strip().lower()
        return (layer_norm, None, city, "city")

    m_cnty = county_suffix_re.match(rest_main)
    if m_cnty:
        county = m_cnty.group(1).strip().lower()
        suffix = m_cnty.group(2).strip().lower()
        return (layer_norm, county, suffix, suffix)

    m_cnty_only = county_only_re.match(rest_main)
    if m_cnty_only:
        county = m_cnty_only.group(1).strip().lower()
        return (layer_norm, county, None, None)

    # Fallback: cannot parse
    return (None, None, None, None)

def entity_from_title_parse(layer: str, county_from_title: str, city_from_title: str, entity_type: str) -> str:
    """Convert parsed title components to entity name format (county_city)."""
    if county_from_title and city_from_title:
        # Normalize county and city names to match entity format
        county_norm = norm_county(county_from_title)
        if entity_type in {"unincorporated", "unified", "countywide"}:
            entity = f"{county_norm}_{entity_type}"
        else:
            city_norm = norm_city(city_from_title)
            entity = f"{county_norm}_{city_norm}"
    elif county_from_title and not city_from_title:
        # County-only title (e.g., "Zoning - Walton County") -> treat as unincorporated
        county_norm = norm_county(county_from_title)
        entity = f"{county_norm}_unincorporated"
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

def validate_record_for_layers_scrape(record: dict) -> List[str]:
    """Validate that a database record meets layers_scrape.py requirements.
    
    Returns list of validation issues (empty list means valid).
    """
    issues = []
    
    # Required identification fields
    if not record.get('layer_subgroup'):
        issues.append("Missing layer_subgroup")
    if not record.get('county'):
        issues.append("Missing county")
    if not record.get('city'):
        issues.append("Missing city")
    
    # Format-specific requirements
    fmt = (record.get('format') or '').lower()
    if not fmt:
        issues.append("Missing format")
    elif fmt in ['ags', 'arcgis', 'esri', 'ags_extract']:
        # AGS downloads need table_name
        if not record.get('table_name'):
            issues.append("AGS format missing table_name")
    else:
        # Direct downloads need resource or src_url_file
        if not (record.get('resource') or record.get('src_url_file')):
            issues.append("Direct download missing resource/src_url_file")
    
    # Check for common field formatting issues
    if record.get('county'):
        county_val = str(record['county']).lower()
        if not county_val.replace(' ', '').replace('_', '').isalnum():
            issues.append("County contains invalid characters")
            
    if record.get('city'):
        city_val = str(record['city'])
        if not city_val.replace(' ', '').replace('_', '').replace('-', '').isalnum():
            issues.append("City contains invalid characters")
    
    return issues

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
    
    # Generate title
    layer_title = layer.capitalize() if layer.islower() else layer
    county_tc = title_case(county)
    city_tc = title_case(city_std)
    
    if entity_type == "city":
        title = f"{layer_title} - City of {city_tc}"
    elif entity_type == "unincorporated":
        title = f"{layer_title} - {county_tc} Unincorporated"
    elif entity_type == "unified":
        title = f"{layer_title} - {county_tc} Unified"
    elif entity_type == "incorporated":
        title = f"{layer_title} - {county_tc} Incorporated"
    else:
        title = f"{layer_title} - {city_tc}"
    
    # Generate table name
    if entity_type == "city":
        table_name = f"{layer}_{city_std}"
    else:
        table_name = f"{layer}_{county}_{entity_type}"
    
    # Generate sys_raw_folder
    category = config.get('category', '08_Land_Use_and_Zoning')
    sys_raw_folder = f"/srv/datascrub/{category}/{layer}/florida/county/{county.lower()}/current/source_data/{city_std.lower()}"
    
    return {
        'title': title,
        'county': county.lower().replace('_', ' '),  # layers_scrape expects lowercase with spaces
        'city': title_case(city_std),  # layers_scrape expects title case
        'layer_subgroup': layer,
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
    detect_malformed_only: bool = False  # For detect mode

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
        
        # Database connection
        self.db = DB(PG_CONNECTION)
        
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
        
        # Write missing fields JSON if any issues found
        if self.missing_fields and self.cfg.mode in {"detect", "fill"}:
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
        """Detect malformed records and output CSV + missing fields JSON."""
        mode_desc = "malformed records only" if self.cfg.detect_malformed_only else "all records"
        self.logger.info(f"Running DETECT mode - finding {mode_desc} and missing fields.")
        
        # Discover entities from database records
        entities = self._discover_entities_from_db()
        
        if not entities:
            self.logger.warning(f"No entities found for layer '{self.cfg.layer}'")
            return
        
        self.logger.info(f"Found {len(entities)} entities for layer '{self.cfg.layer}'")
        
        # CSV headers
        headers = [
            "entity", "title", "county", "city", "layer_subgroup", "format",
            "resource", "src_url_file", "table_name", "processing_comments",
            "validation_issues", "missing_fields", "suggested_fixes"
        ]
        
        csv_rows = [headers]
        total_entities = 0
        valid_entities = 0
        entities_with_issues = 0
        
        for entity in sorted(entities):
            total_entities += 1
            county, city = split_entity(entity)
            
            # Find database record by title matching
            record = self._find_record_by_entity(entity)
            
            if not record:
                # No record found
                csv_row = [
                    entity, "**NO RECORD FOUND**", county, city, "", "", "", "", "", "",
                    "No database record found", "All fields missing", "Create new record"
                ]
                self.missing_fields[entity] = {
                    "layer_subgroup": self.cfg.layer,
                    "county": county.lower().replace('_', ' '),
                    "city": title_case(city),
                    "format": "MANUAL_REQUIRED",
                    "resource_or_table_name": "MANUAL_REQUIRED"
                }
                entities_with_issues += 1
                has_issues = True
            else:
                # Validate existing record
                issues = validate_record_for_layers_scrape(record)
                
                # Check for preprocessing commands
                current_processing = record.get('processing_comments', '') or ''
                expected_processing = extract_preprocessing_commands(self.cfg.layer, entity)
                
                missing_fields = []
                suggested_fixes = []
                
                # Check required fields
                if not record.get('layer_subgroup'):
                    missing_fields.append("layer_subgroup")
                    self.missing_fields[entity]["layer_subgroup"] = self.cfg.layer
                
                if not record.get('format'):
                    missing_fields.append("format")
                    self.missing_fields[entity]["format"] = "MANUAL_REQUIRED"
                
                fmt = (record.get('format') or '').lower()
                if fmt in ['ags', 'arcgis', 'esri', 'ags_extract']:
                    if not record.get('table_name'):
                        missing_fields.append("table_name")
                        self.missing_fields[entity]["table_name"] = "MANUAL_REQUIRED"
                else:
                    if not (record.get('resource') or record.get('src_url_file')):
                        missing_fields.append("resource")
                        self.missing_fields[entity]["resource"] = "MANUAL_REQUIRED"
                
                # Check preprocessing commands
                if expected_processing and current_processing != expected_processing:
                    if not current_processing:
                        missing_fields.append("processing_comments")
                    self.missing_fields[entity]["processing_comments"] = expected_processing
                    suggested_fixes.append(f"Set processing_comments to: {expected_processing}")
                
                # Generate expected values for comparison
                try:
                    layer_parsed, county_parsed, city_parsed, entity_type = parse_title_to_entity(record.get('title', ''))
                    if layer_parsed == self.cfg.layer:
                        expected = generate_expected_values(self.cfg.layer, county, city, entity_type)
                        
                        # Check for auto-correctable fields
                        for field, expected_val in expected.items():
                            if field in record and record[field] != expected_val:
                                suggested_fixes.append(f"Update {field}: '{record[field]}' → '{expected_val}'")
                                self.missing_fields[entity][field] = expected_val
                except Exception:
                    suggested_fixes.append("Title parsing failed - manual review needed")
                
                csv_row = [
                    entity,
                    safe_catalog_val(record.get('title')),
                    safe_catalog_val(record.get('county')),
                    safe_catalog_val(record.get('city')),
                    safe_catalog_val(record.get('layer_subgroup')),
                    safe_catalog_val(record.get('format')),
                    safe_catalog_val(record.get('resource')),
                    safe_catalog_val(record.get('src_url_file')),
                    safe_catalog_val(record.get('table_name')),
                    safe_catalog_val(record.get('processing_comments')),
                    "; ".join(issues) if issues else "None",
                    "; ".join(missing_fields) if missing_fields else "None",
                    "; ".join(suggested_fixes) if suggested_fixes else "None"
                ]
                
                has_issues = bool(issues or missing_fields)
                if has_issues:
                    entities_with_issues += 1
                else:
                    valid_entities += 1
            
            # Add row to CSV (filter based on detect_malformed_only flag)
            if self.cfg.detect_malformed_only:
                # Only include records with issues
                if has_issues:
                    csv_rows.append(csv_row)
            else:
                # Include all records
                csv_rows.append(csv_row)
        
        # Add summary row
        csv_rows.append([])
        if self.cfg.detect_malformed_only:
            csv_rows.append([
                "SUMMARY", f"{entities_with_issues} malformed records found", 
                f"out of {total_entities} total", "", "", "", "", "", "", "", "", "", ""
            ])
        else:
            csv_rows.append([
                "SUMMARY", f"{valid_entities}/{total_entities} valid", 
                f"{entities_with_issues} with issues", "", "", "", "", "", "", "", "", "", ""
            ])
        
        # Write CSV report
        if self.cfg.generate_csv:
            suffix = "malformed" if self.cfg.detect_malformed_only else "detect"
            csv_path = REPORTS_DIR / f"{self.cfg.layer}_prescrape_{suffix}_{get_today_str()}.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(csv_rows)
            self.logger.info(f"Detection report written → {csv_path}")
        
        # Log summary
        if self.cfg.detect_malformed_only:
            self.logger.info(f"Detection complete: {entities_with_issues} malformed records found out of {total_entities} total")
        else:
            self.logger.info(f"Detection complete: {valid_entities}/{total_entities} valid, {entities_with_issues} with issues")
    
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
                        county_norm = norm_county(county_db)
                        city_norm = norm_city(city_db)
                        if city_norm in {"unincorporated", "unified", "incorporated", "countywide"}:
                            entity = f"{county_norm}_{city_norm}"
                        else:
                            entity = f"{county_norm}_{city_norm}"
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
    parser.add_argument("--detect-malformed", action="store_true",
                       help="In DETECT mode, show only malformed records")
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
        manual_file=args.manual_file,
        detect_malformed_only=args.detect_malformed
    )
    
    # Run the processor
    processor = LayersPrescrape(cfg)
    processor.run()

if __name__ == "__main__":
    main()