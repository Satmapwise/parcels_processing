#!/usr/bin/env python3

"""
Opendata-to-AGS URL Converter

A specialized spin-off of layers_prescrape.py focused on detecting opendata portal URLs
and converting them to ArcGIS REST service URLs. Uses the same argument parsing and
record finding logic as layers_prescrape but focuses solely on URL conversion.

Usage:
    python3 opendata_to_ags.py --include "zoning_fl_*"
    python3 opendata_to_ags.py --include "buildings" --debug
    python3 opendata_to_ags.py  # Process all layers
"""

import sys
import os
import csv
import logging
import argparse
import fnmatch
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict

# Import core logic from layers_prescrape
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import argparse
import csv
import fnmatch
import logging
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

# Import core logic from layers_prescrape
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from layers_prescrape import Config, DB
from layers_helpers import (
    format_name, LAYER_CONFIGS, PG_CONNECTION, FL_COUNTIES, GA_COUNTIES, AL_COUNTIES, DE_COUNTIES, AZ_COUNTIES
)
# Import our clean Selenium-based extraction
from selenium_opendata import extract_arcgis_url_from_opendata

# Enhanced opendata portal detection
def is_opendata_portal(url):
    """Enhanced check if URL is an opendata portal (not a direct ArcGIS service)."""
    if not url:
        return False
    
    url_lower = url.lower()
    
    # First, exclude direct ArcGIS service URLs
    if any(pattern in url_lower for pattern in ['rest/services', 'mapserver', 'featureserver']):
        return False
    
    # Enhanced opendata portal indicators
    opendata_indicators = [
        # ArcGIS Hub patterns
        'hub.arcgis.com',
        'opendata.arcgis.com',
        
        # County/city specific ArcGIS opendata patterns
        'geodata-',
        'data-',
        'public-',
        'data1-',
        'data2-',
        'data3-',
        
        # General opendata patterns
        'opendata',
        'data.',
        'geoportal',
        'datahub',
        'open-data',
        'gis-open',
        
        # Portal patterns (but exclude ArcGIS Portal URLs)
        'portal',  # Note: This may catch some ArcGIS Portal URLs, but they're often opendata
        
        # Florida-specific patterns
        'floridagio.gov',
        'data.florida.gov',
        'gis.doh.state.fl.us',
        
        # County/city specific patterns
        'miamidade.gov/gis',
        'broward.org/gis',
        'pinellascounty.org/gis',
        'data.cityof',
        'gis.county',
        'opendata.county'
    ]
    
    # Check for opendata indicators
    is_opendata = any(indicator in url_lower for indicator in opendata_indicators)
    
    # Exclude ArcGIS Portal URLs (they're not opendata portals)
    if 'portal' in url_lower and ('arcgis.com/portal' in url_lower or 'maps.' in url_lower and 'portal' in url_lower):
        return False
    
    # Special case: Allow opendata URLs that contain .zip if they're from opendata portals
    if is_opendata and ('.zip' in url_lower or '.zip?' in url_lower):
        # Only exclude if it's a direct download from a non-opendata source
        # Most opendata portals that serve zip files are still valid opendata URLs
        return True
    
    return is_opendata

# URL validation function
def is_url_accessible(url, timeout=10):
    """Check if URL is accessible before attempting conversion."""
    try:
        import urllib.request
        import ssl
        
        # Create SSL context that doesn't verify certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Create request with headers
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0 (compatible; OpenDataExtractor/1.0)')
        
        # Try to open the URL
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_context) as response:
            return response.status == 200
    except Exception as e:
        logging.debug(f"URL validation failed for {url}: {e}")
        return False

# Simple ArcGIS URL validation
def validate_arcgis_url(url):
    """Simple validation for ArcGIS service URLs."""
    if not url:
        return False, "Empty URL"
    
    # Check for ArcGIS service patterns
    arcgis_patterns = ['rest/services', 'MapServer', 'FeatureServer']
    if any(pattern in url for pattern in arcgis_patterns):
        return True, "Valid ArcGIS service URL"
    
    return False, "Not an ArcGIS service URL"

# Output directory
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

@dataclass
class OpendataConfig:
    """Configuration for opendata-to-AGS conversion."""
    layer: str
    include_entities: List[str] | None = None
    exclude_entities: List[str] | None = None
    debug: bool = False
    generate_csv: bool = True
    max_candidates: int = 3  # Max ArcGIS URLs to extract per opendata portal
    apply: bool = False  # Whether to actually update the database

class OpendataToAGS:
    """Main engine for opendata-to-AGS URL conversion."""
    
    def __init__(self, cfg: OpendataConfig):
        self.cfg = cfg
        
        self.logger = logging.getLogger("OpendataToAGS")
        self.logger.setLevel(logging.DEBUG if cfg.debug else logging.INFO)
        
        # Only add handler if logger doesn't already have one
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            self.logger.addHandler(handler)
        
        print(f"[DEBUG] Initializing OpendataToAGS for layer '{cfg.layer}'")
        
        # Database connection
        try:
            self.db = DB(PG_CONNECTION)
        except Exception as e:
            print(f"[ERROR] Failed to initialize database connection: {e}")
            sys.exit(1)
        
        # Load existing CSV data for skipping already-processed URLs
        self.processed_urls = self._load_existing_csv_data()
    
    def _load_existing_csv_data(self):
        """Load existing CSV data to skip already-processed URLs."""
        processed_urls = {}
        csv_path = REPORTS_DIR / f"{self.cfg.layer}_opendata_to_ags.csv"
        
        if csv_path.exists():
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        old_url = row.get('old_url', '').strip()
                        new_ags_url = row.get('new_ags_url', '').strip()
                        ags_valid = row.get('ags_valid', '').strip()
                        
                        if old_url:
                            # Cache ALL results, including failed extractions
                            processed_urls[old_url] = {
                                'new_ags_url': new_ags_url,
                                'ags_valid': ags_valid,
                                'validation_reason': row.get('validation_reason', ''),
                                'relevance_score': row.get('relevance_score', '0.00')
                            }
                
                self.logger.info(f"Loaded {len(processed_urls)} already-processed URLs from existing CSV")
            except Exception as e:
                self.logger.warning(f"Could not load existing CSV data: {e}")
                processed_urls = {}
        
        return processed_urls
    
    def _apply_from_csv(self):
        """Apply successful conversions directly from existing CSV data."""
        csv_path = REPORTS_DIR / f"{self.cfg.layer}_opendata_to_ags.csv"
        
        if not csv_path.exists():
            self.logger.error(f"No CSV file found at {csv_path}. Run without --apply first to generate conversions.")
            return
        
        # Build entity dictionary from database (similar to layers_scrape.py)
        entity_dict = self._get_entities_from_db()
        if not entity_dict:
            self.logger.warning(f"No entities found for layer '{self.cfg.layer}'")
            return
        
        # Apply entity filtering
        if self.cfg.include_entities or self.cfg.exclude_entities:
            filtered_entities = self._apply_entity_filters(list(entity_dict.keys()))
            entity_dict = {entity: entity_dict[entity] for entity in filtered_entities}
        
        if not entity_dict:
            filter_desc = f"include: {self.cfg.include_entities}" if self.cfg.include_entities else f"exclude: {self.cfg.exclude_entities}"
            self.logger.warning(f"No records found matching entity filters ({filter_desc})")
            return
        
        # Load CSV data and apply successful conversions
        update_count = 0
        skipped_count = 0
        error_count = 0
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    entity = row.get('entity', '').strip()
                    old_url = row.get('old_url', '').strip()
                    new_ags_url = row.get('new_ags_url', '').strip()
                    ags_valid = row.get('ags_valid', '').strip()
                    
                    # Skip if not a successful conversion
                    if not old_url or new_ags_url in ['NO_AGS_FOUND', 'NOT_OPENDATA', 'URL_NOT_ACCESSIBLE'] or ags_valid != 'YES':
                        continue
                    
                    # Check if this entity exists in our database entities
                    if entity not in entity_dict:
                        self.logger.debug(f"Entity not found in database: {entity}")
                        continue
                    
                    # Get the database record components
                    components = entity_dict[entity]
                    
                    # Find and update the database record
                    try:
                        # Build dynamic WHERE clause to handle null values
                        where_conditions = ["layer_subgroup = %s", "status IS DISTINCT FROM 'DELETE'"]
                        params = [components['layer']]
                        
                        # Add state condition
                        if components['state_external']:
                            where_conditions.append("state = %s")
                            params.append(components['state_external'])
                        else:
                            where_conditions.append("state IS NULL")
                        
                        # Add county condition
                        if components['county_external']:
                            where_conditions.append("county = %s")
                            params.append(components['county_external'])
                        else:
                            where_conditions.append("county IS NULL")
                        
                        # Add city condition
                        if components['city_external']:
                            where_conditions.append("city = %s")
                            params.append(components['city_external'])
                        else:
                            where_conditions.append("city IS NULL")
                        
                        update_sql = f"""
                            UPDATE m_gis_data_catalog_main 
                            SET src_url_file = %s 
                            WHERE {' AND '.join(where_conditions)}
                        """
                        
                        # Add the new URL as the first parameter
                        params.insert(0, new_ags_url)
                        
                        self.db.execute(update_sql, params)
                        rows_affected = self.db.cur.rowcount
                        
                        if rows_affected > 0:
                            update_count += 1
                            self.logger.info(f"✅ Applied {entity}: {old_url} → {new_ags_url}")
                        else:
                            self.logger.warning(f"No record found for {entity}")
                            error_count += 1
                            
                    except Exception as e:
                        self.logger.error(f"❌ Failed to update {entity}: {e}")
                        error_count += 1
        
        except Exception as e:
            self.logger.error(f"Error reading CSV file: {e}")
            return
        
        # Commit all changes
        self.db.commit()
        
        # Summary
        self.logger.info(f"Apply complete: {update_count} database updates applied, {skipped_count} skipped (filters), {error_count} errors")
    
    def _get_entities_from_db(self) -> dict[str, dict]:
        """Get all entities from database for the current layer with their component parts."""
        entity_dict = {}
        
        sql = """
            SELECT layer_subgroup, state, county, city 
            FROM m_gis_data_catalog_main 
            WHERE status IS DISTINCT FROM 'DELETE' 
            AND layer_subgroup = %s
            AND src_url_file IS NOT NULL 
            AND src_url_file != ''
        """
        
        records = self.db.fetchall(sql, (self.cfg.layer,))
        
        for record in records:
            layer = record.get('layer_subgroup')
            state_external = record.get('state')
            county_external = record.get('county')
            city_external = record.get('city')
            
            # Convert to internal format for entity generation
            state_internal = format_name(state_external, 'state', external=False) if state_external else None
            county_internal = format_name(county_external, 'county', external=False) if county_external else None
            city_internal = format_name(city_external, 'city', external=False) if city_external else None
            
            # Build entity string using the same logic as layers_scrape.py
            entity = self._entity_from_parts(layer, state_internal, county_internal, city_internal)
            
            if entity == "ERROR":
                continue  # Skip records that can't generate valid entities
            
            # Store with both internal (for entity generation) and external (for DB queries) component parts
            entity_dict[entity] = {
                'layer': layer,
                'state_internal': state_internal,
                'county_internal': county_internal,
                'city_internal': city_internal,
                'state_external': state_external,
                'county_external': county_external,
                'city_external': city_external
            }
        
        return entity_dict
    
    def _entity_from_parts(self, layer: str, state: str | None, county: str | None, city: str | None) -> str:
        """Return entity id from raw DB values in appropriate format (copied from layers_scrape.py)."""
        # Convert external DB values to internal format
        county_internal = format_name(county, 'county', external=False) if county else None
        city_internal = format_name(city, 'city', external=False) if city else None
        
        # Handle state - infer from county if missing
        if state and str(state).strip() and str(state).strip().upper() not in ('NULL', 'NONE'):
            state_internal = str(state).strip().lower()
        else:
            # Infer state from county for backwards compatibility
            if county_internal and county_internal in FL_COUNTIES:
                state_internal = 'fl'
            elif county_internal and county_internal in GA_COUNTIES:
                state_internal = 'ga'
            elif county_internal and county_internal in AL_COUNTIES:
                state_internal = 'al'
            elif county_internal and county_internal in DE_COUNTIES:
                state_internal = 'de'
            elif county_internal and county_internal in AZ_COUNTIES:
                state_internal = 'az'
        
        # Check layer level to determine entity format
        layer_config = LAYER_CONFIGS.get(layer, {})
        layer_level = layer_config.get('level', 'state_county_city')  # Default to 4-part
        
        if layer_level == 'state_county':
            # County-level layer: use 3-part format (layer_state_county)
            if not county_internal:
                return "ERROR"  # County is required for county-level layers
            return f"{layer}_{state_internal}_{county_internal}"
        elif layer_level == 'state':
            # State-level layer: use 2-part format (layer_state)
            return f"{layer}_{state_internal}"
        elif layer_level == 'national':
            # National-level layer: use 1-part format (layer)
            return layer
        else:
            # City-level or other layers: use 4-part format (layer_state_county_city)
            if not county_internal:
                return "ERROR"  # County is required for city-level layers
            if not city_internal:
                city_internal = 'countywide'  # Default for county-only records
            return f"{layer}_{state_internal}_{county_internal}_{city_internal}"
    
    def _apply_entity_filters(self, entities: list[str]) -> list[str]:
        """Apply include/exclude filters to entity list."""
        if not entities:
            return []
        
        # Apply include filters
        if self.cfg.include_entities:
            included = set()
            for pattern in self.cfg.include_entities:
                matches = fnmatch.filter(entities, pattern)
                included.update(matches)
                if matches:
                    self.logger.info(f"Include pattern '{pattern}' matched {len(matches)} entities")
                else:
                    self.logger.warning(f"Include pattern '{pattern}' matched no entities")
            entities = list(included)
        
        # Apply exclude filters
        if self.cfg.exclude_entities:
            for pattern in self.cfg.exclude_entities:
                before_count = len(entities)
                entities = [e for e in entities if not fnmatch.fnmatch(e, pattern)]
                excluded_count = before_count - len(entities)
                if excluded_count > 0:
                    self.logger.info(f"Exclude pattern '{pattern}' excluded {excluded_count} entities")
        
        return sorted(entities)
    
    def run(self):
        """Main execution method."""
        if self.cfg.apply:
            self.logger.info(f"Applying successful conversions from CSV for layer '{self.cfg.layer}'")
            self._apply_from_csv()
        else:
            self.logger.info(f"Running opendata-to-AGS conversion for layer '{self.cfg.layer}'")
            self._run_conversion()
    
    def _find_records_with_urls(self) -> List[Dict[str, Any]]:
        """Find all records for the layer that have URLs."""
        sql = """
            SELECT * FROM m_gis_data_catalog_main 
            WHERE status IS DISTINCT FROM 'DELETE' 
            AND layer_subgroup = %s
            AND src_url_file IS NOT NULL 
            AND src_url_file != ''
            ORDER BY title
        """
        
        records = self.db.fetchall(sql, (self.cfg.layer,))
        
        if not records:
            self.logger.warning(f"No records with URLs found for layer '{self.cfg.layer}'")
            return []
        
        self.logger.info(f"Found {len(records)} records with URLs for layer '{self.cfg.layer}'")
        return records
    
    def _generate_entity_from_record(self, record: Dict[str, Any]) -> str:
        """Generate entity name from database record using actual database fields."""
        if not record:
            return "ERROR"
            
        # Get layer from layer_subgroup field
        layer_subgroup = record.get('layer_subgroup')
        if not layer_subgroup:
            return "ERROR"
        layer_subgroup = str(layer_subgroup).strip().lower()
        if not layer_subgroup:
            return "ERROR"
        
        # Get state, county, city from database fields
        state_db = record.get('state')
        county_db = record.get('county')
        city_db = record.get('city')
        
        # Normalize state (default to 'fl' if missing)
        if state_db and str(state_db).strip() and str(state_db).strip().upper() not in ('NULL', 'NONE', ''):
            state_internal = str(state_db).strip().lower()
        else:
            state_internal = 'fl'  # Default for Florida
        
        # Normalize county
        if county_db and str(county_db).strip() and str(county_db).strip().upper() not in ('NULL', 'NONE', ''):
            county_internal = format_name(str(county_db).strip(), 'county', external=False)
        else:
            return "ERROR"
        
        # Normalize city
        if city_db and str(city_db).strip() and str(city_db).strip().upper() not in ('NULL', 'NONE', ''):
            city_internal = format_name(str(city_db).strip(), 'city', external=False)
            # Handle special city types
            if city_internal in {"unincorporated", "unified", "incorporated", "countywide"}:
                entity = f"{layer_subgroup}_{state_internal}_{county_internal}_{city_internal}"
            else:
                entity = f"{layer_subgroup}_{state_internal}_{county_internal}_{city_internal}"
        else:
            # County-level only
            entity = f"{layer_subgroup}_{state_internal}_{county_internal}"
        
        return entity
    
    def _filter_records_by_entity_patterns(self, records: List[Dict[str, Any]]) -> List[Tuple[str, Dict[str, Any]]]:
        """Filter records by entity include/exclude patterns."""
        entity_records = []
        
        for record in records:
            entity = self._generate_entity_from_record(record)
            if entity == "ERROR":
                continue
            
            # Apply include/exclude filters
            if self.cfg.include_entities:
                if not any(self._entity_matches_pattern(entity, pattern) for pattern in self.cfg.include_entities):
                    continue
            
            if self.cfg.exclude_entities:
                if any(self._entity_matches_pattern(entity, pattern) for pattern in self.cfg.exclude_entities):
                    continue
            
            entity_records.append((entity, record))
        
        return entity_records
    
    def _entity_matches_pattern(self, entity: str, pattern: str) -> bool:
        """Check if entity matches a pattern (supports wildcards)."""
        import fnmatch
        return fnmatch.fnmatch(entity.lower(), pattern.lower())
    
    def _convert_opendata_to_ags(self, url: str, layer_keywords: List[str] = None) -> List[Tuple[str, float, bool, str]]:
        """
        Convert opendata portal URL to ArcGIS URLs using Selenium.
        
        Returns:
            List of (ags_url, relevance_score, is_valid, validation_reason) tuples
        """
        try:
            self.logger.debug(f"Converting opendata URL: {url}")
            
            # Use Selenium to extract the ArcGIS URL
            ags_url = extract_arcgis_url_from_opendata(url, headless=True)
            
            if not ags_url:
                self.logger.warning(f"No ArcGIS URL extracted from {url}")
                return []
            
            # Validate the extracted URL
            is_valid, reason = validate_arcgis_url(ags_url)
            
            # Simple relevance scoring based on URL patterns
            score = 1.0  # Default score
            if layer_keywords:
                for keyword in layer_keywords:
                    if keyword.lower() in ags_url.lower():
                        score += 0.5
            
            if self.cfg.debug:
                self.logger.debug(f"  → {ags_url} (score: {score:.2f}, valid: {is_valid})")
            
            return [(ags_url, score, is_valid, reason)]
            
        except Exception as e:
            self.logger.error(f"Error converting {url}: {e}")
            return []
    
    def _run_conversion(self):
        """Main conversion logic."""
        # Find all records with URLs
        all_records = self._find_records_with_urls()
        if not all_records:
            return
        
        # Filter by entity patterns
        entity_records = self._filter_records_by_entity_patterns(all_records)
        
        if self.cfg.include_entities or self.cfg.exclude_entities:
            filter_desc = []
            if self.cfg.include_entities:
                filter_desc.append(f"include: {', '.join(self.cfg.include_entities)}")
            if self.cfg.exclude_entities:
                filter_desc.append(f"exclude: {', '.join(self.cfg.exclude_entities)}")
            filter_desc = f"({'; '.join(filter_desc)})"
            
            self.logger.info(f"Filtered to {len(entity_records)} records matching entity filters {filter_desc}")
            
            if not entity_records:
                self.logger.warning(f"No records found matching entity filters {filter_desc}")
                return
        
        # Process each record for opendata-to-AGS conversion
        csv_rows = []
        headers = ["entity", "og_title", "old_url", "new_ags_url", "relevance_score", "ags_valid", "validation_reason"]
        csv_rows.append(headers)
        
        opendata_count = 0
        conversion_count = 0
        update_count = 0
        
        for entity, record in entity_records:
            old_url = record.get('src_url_file', '').strip()
            og_title = record.get('title', '')
            record_id = record.get('id')
            
            if not old_url:
                continue
            
            # Check if URL is an opendata portal
            if is_opendata_portal(old_url):
                opendata_count += 1
                
                # Check if URL was already processed (successfully or failed)
                if old_url in self.processed_urls:
                    processed_data = self.processed_urls[old_url]
                    new_ags_url = processed_data['new_ags_url']
                    
                    # Skip if it was a failed extraction (don't retry)
                    if new_ags_url in ['NO_AGS_FOUND', 'URL_NOT_ACCESSIBLE', 'NOT_OPENDATA']:
                        self.logger.debug(f"⏭️  Skipping previously failed URL for {entity}: {old_url} ({new_ags_url})")
                    else:
                        self.logger.debug(f"⏭️  Skipping already-processed URL for {entity}: {old_url}")
                    
                    csv_rows.append([
                        entity, og_title, old_url, processed_data['new_ags_url'],
                        processed_data['relevance_score'], processed_data['ags_valid'], 
                        processed_data['validation_reason']
                    ])
                    continue
                
                self.logger.debug(f"🔍 Processing opendata URL for {entity}: {old_url}")
                
                # Validate URL accessibility before attempting conversion
                if not is_url_accessible(old_url):
                    self.logger.warning(f"⚠️  URL not accessible for {entity}: {old_url}")
                    csv_rows.append([
                        entity, og_title, old_url, "URL_NOT_ACCESSIBLE", "0.00", "NO", "URL_VALIDATION_FAILED"
                    ])
                    continue
                
                # Extract layer keywords from the layer name
                layer_keywords = [self.cfg.layer]
                config = LAYER_CONFIGS.get(self.cfg.layer, {})
                if 'external_frmt' in config:
                    layer_keywords.append(config['external_frmt'])
                
                # Convert to ArcGIS URLs
                ags_candidates = self._convert_opendata_to_ags(old_url, layer_keywords)
                
                if ags_candidates:
                    conversion_count += 1
                    # Use the first (best) candidate for database update
                    best_ags_url, score, is_valid, reason = ags_candidates[0]
                    
                    # Update database if apply=True and URL is valid
                    if self.cfg.apply and is_valid and record_id:
                        try:
                            update_sql = "UPDATE m_gis_data_catalog_main SET src_url_file = %s WHERE id = %s"
                            self.db.execute(update_sql, (best_ags_url, record_id))
                            update_count += 1
                            self.logger.info(f"✅ Updated {entity}: {old_url} → {best_ags_url}")
                        except Exception as e:
                            self.logger.error(f"❌ Failed to update {entity}: {e}")
                            reason = f"UPDATE_FAILED: {e}"
                    
                    # Add each candidate as a separate row
                    for ags_url, score, is_valid, reason in ags_candidates:
                        csv_rows.append([
                            entity, og_title, old_url, ags_url, 
                            f"{score:.2f}", "YES" if is_valid else "NO", reason
                        ])
                else:
                    # No ArcGIS URLs found
                    csv_rows.append([
                        entity, og_title, old_url, "NO_AGS_FOUND", "0.00", "NO", "NO_EXTRACTION"
                    ])
            else:
                # Skip non-opendata URLs entirely - don't add them to CSV
                if self.cfg.debug:
                    if old_url.lower().endswith('.zip') or '.zip?' in old_url.lower():
                        reason = "DIRECT_ZIP_DOWNLOAD"
                    elif any(pattern in old_url.lower() for pattern in ['rest/services', 'mapserver', 'featureserver']):
                        reason = "DIRECT_ARCGIS_SERVICE"
                    else:
                        reason = "REGULAR_URL"
                    
                    self.logger.debug(f"⏭️  Skipping non-opendata URL for {entity}: {old_url} ({reason})")
        
        # Write CSV report
        if self.cfg.generate_csv:
            csv_path = REPORTS_DIR / f"{self.cfg.layer}_opendata_to_ags.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(csv_rows)
            self.logger.info(f"Conversion report written → {csv_path}")
        
        # Summary
        if self.cfg.apply:
            self.logger.info(f"Conversion complete: {opendata_count} opendata URLs found, {conversion_count} successful conversions, {update_count} database updates applied")
        else:
            self.logger.info(f"Conversion complete: {opendata_count} opendata URLs found, {conversion_count} successful conversions (preview mode - use --apply to update database)")

def build_opendata_arg_parser() -> argparse.ArgumentParser:
    """Build argument parser specific to opendata-to-AGS conversion."""
    parser = argparse.ArgumentParser(
        description="Convert opendata portal URLs to ArcGIS REST service URLs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 opendata_to_ags.py --include "flu_fl_*"                    # Process only FLU layers
  python3 opendata_to_ags.py --include "zoning_fl_*" --debug         # Process zoning with debug
  python3 opendata_to_ags.py --exclude "*_unincorporated"            # Skip unincorporated areas
  python3 opendata_to_ags.py --apply                                 # Apply changes to database
        """
    )
    
    # Entity filtering options
    parser.add_argument("--include", nargs="*", metavar="ENTITY", 
                       help="Include only entities matching these patterns (supports wildcards: *, ?, [abc], [!abc])")
    parser.add_argument("--exclude", nargs="*", metavar="ENTITY",
                       help="Exclude entities matching these patterns (supports wildcards: *, ?, [abc], [!abc])")
    
    # Mode options
    parser.add_argument("--apply", action="store_true", 
                       help="Apply successful conversions to database (default: preview mode)")
    parser.add_argument("--debug", action="store_true", 
                       help="Enable debug logging")
    parser.add_argument("--no-csv", dest="generate_csv", action="store_false", default=True,
                       help="Skip CSV report generation")
    
    return parser

def main():
    """Main entry point."""
    parser = build_opendata_arg_parser()
    args = parser.parse_args()
    
    # Process all layers (no layer-specific filtering in this tool)
    layers_to_process = list(LAYER_CONFIGS.keys())
    
    for layer in layers_to_process:
        print(f"\n[INFO] ==================== Processing layer: {layer.upper()} ====================")
        
        cfg = OpendataConfig(
            layer=layer,
            include_entities=[e.lower() for e in args.include] if args.include else None,
            exclude_entities=[e.lower() for e in args.exclude] if args.exclude else None,
            debug=args.debug,
            generate_csv=args.generate_csv,
            max_candidates=3,
            apply=args.apply
        )
        
        converter = OpendataToAGS(cfg)
        converter.run()
    
    print(f"\n[INFO] ==================== Completed processing {len(layers_to_process)} layer(s) ====================")

if __name__ == "__main__":
    main()
