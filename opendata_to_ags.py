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
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict

# Import core logic from layers_prescrape
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from layers_prescrape import (
    Config, DB, build_arg_parser, 
    format_name, LAYER_CONFIGS, PG_CONNECTION
)
# Import our clean Selenium-based extraction
from selenium_opendata import extract_arcgis_url_from_opendata

# Simple opendata portal detection
def is_opendata_portal(url):
    """Simple check if URL is an opendata portal (not a direct ArcGIS service or direct download)."""
    # First, exclude direct ArcGIS service URLs
    if any(pattern in url.lower() for pattern in ['rest/services', 'mapserver', 'featureserver']):
        return False
    
    # Skip direct downloads (.zip files)
    if url.lower().endswith('.zip') or '.zip?' in url.lower():
        return False
    
    # Then check for opendata portal indicators
    opendata_indicators = [
        'hub.arcgis.com', 'opendata', 'data.', 'geoportal', 'portal', 'datahub', 'open-data'
    ]
    return any(indicator in url.lower() for indicator in opendata_indicators)

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
                        
                        if old_url and new_ags_url != 'NO_AGS_FOUND':
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
        
        # Get all records for this layer to match against CSV data
        records = self._find_records_with_urls()
        if not records:
            self.logger.warning(f"No records with URLs found for layer '{self.cfg.layer}'")
            return
        
        # Create a lookup by URL for fast matching
        url_to_record = {record.get('src_url_file', '').strip(): record for record in records}
        
        # Apply entity filtering
        entity_records = []
        for record in records:
            entity = self._generate_entity_from_record(record)
            entity_records.append((entity, record))
        
        if self.cfg.include_entities or self.cfg.exclude_entities:
            entity_records = self._filter_records_by_entity_patterns(entity_records)
        
        if not entity_records:
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
                    old_url = row.get('old_url', '').strip()
                    new_ags_url = row.get('new_ags_url', '').strip()
                    ags_valid = row.get('ags_valid', '').strip()
                    entity = row.get('entity', '').strip()
                    
                    # Skip if not a successful conversion
                    if not old_url or new_ags_url in ['NO_AGS_FOUND', 'NOT_OPENDATA', 'URL_NOT_ACCESSIBLE'] or ags_valid != 'YES':
                        continue
                    
                    # Find the corresponding database record
                    if old_url not in url_to_record:
                        self.logger.debug(f"URL not found in current records: {old_url}")
                        continue
                    
                    record = url_to_record[old_url]
                    record_id = record.get('id')
                    
                    if not record_id:
                        self.logger.warning(f"No record ID found for {entity}: {old_url}")
                        error_count += 1
                        continue
                    
                    # Check if this entity matches our filters
                    entity_matches = any(entity == filtered_entity for filtered_entity, _ in entity_records)
                    if not entity_matches:
                        skipped_count += 1
                        continue
                    
                    # Apply the update to the database
                    try:
                        update_sql = "UPDATE m_gis_data_catalog_main SET src_url_file = %s WHERE id = %s"
                        self.db.execute(update_sql, (new_ags_url, record_id))
                        update_count += 1
                        self.logger.info(f"✅ Applied {entity}: {old_url} → {new_ags_url}")
                    except Exception as e:
                        self.logger.error(f"❌ Failed to update {entity}: {e}")
                        error_count += 1
        
        except Exception as e:
            self.logger.error(f"Error reading CSV file: {e}")
            return
        
        # Summary
        self.logger.info(f"Apply complete: {update_count} database updates applied, {skipped_count} skipped (filters), {error_count} errors")
    
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
        layer_internal = format_name(self.cfg.layer, 'layer', external=False)
        layer_external = format_name(self.cfg.layer, 'layer', external=True)
        
        sql = """
            SELECT * FROM m_gis_data_catalog_main 
            WHERE status IS DISTINCT FROM 'DELETE' 
            AND (lower(title) LIKE %s OR lower(title) LIKE %s)
            AND src_url_file IS NOT NULL 
            AND src_url_file != ''
            ORDER BY title
        """
        
        records = self.db.fetchall(sql, (f'%{layer_internal}%', f'%{layer_external.lower()}%'))
        
        if not records:
            self.logger.warning(f"No records with URLs found for layer '{self.cfg.layer}'")
            return []
        
        self.logger.info(f"Found {len(records)} records with URLs for layer '{self.cfg.layer}'")
        return records
    
    def _generate_entity_from_record(self, record: Dict[str, Any]) -> str:
        """Generate entity name from database record (imported logic from layers_prescrape)."""
        # Check if this layer has a hardcoded entity in the config
        config = LAYER_CONFIGS.get(self.cfg.layer, {})
        hardcoded_entity = config.get('entity')
        if hardcoded_entity:
            return hardcoded_entity
        
        title = record.get('title', '')
        
        # Get state from record, or infer from county if missing
        state_db = record.get('state')
        if state_db and str(state_db).strip() and str(state_db).strip().upper() not in ('NULL', 'NONE'):
            state = str(state_db).strip().lower()
        else:
            # Try to infer state from county
            county_db = record.get('county')
            if county_db:
                county_internal = format_name(str(county_db).strip(), 'county', external=False)
                # For now, assume FL - could be enhanced to support other states
                state = 'fl'
            else:
                state = 'fl'  # Default fallback
        
        # Generate entity using title parsing (simplified version)
        from layers_prescrape import parse_title_to_entity
        layer_parsed, county_parsed, city_parsed, entity_type = parse_title_to_entity(title)
        
        if layer_parsed and county_parsed:
            state_internal = state.lower()
            county_internal = format_name(county_parsed, 'county', external=False)
            
            if city_parsed and city_parsed not in {"unincorporated", "unified", "incorporated"}:
                city_internal = format_name(city_parsed, 'city', external=False)
                entity = f"{self.cfg.layer}_{state_internal}_{county_internal}_{city_internal}"
            elif city_parsed in {"unincorporated", "unified", "incorporated"}:
                entity = f"{self.cfg.layer}_{state_internal}_{county_internal}_{city_parsed}"
            else:
                # County-level layer
                entity = f"{self.cfg.layer}_{state_internal}_{county_internal}"
            
            return entity
        
        # Fallback: generate from database fields if title parsing fails
        county_db = record.get('county')
        city_db = record.get('city')
        
        if county_db:
            county_internal = format_name(str(county_db).strip(), 'county', external=False)
            state_internal = state.lower()
            
            if city_db and str(city_db).strip():
                city_internal = format_name(str(city_db).strip(), 'city', external=False)
                if city_internal in {"unincorporated", "unified", "incorporated"}:
                    entity = f"{self.cfg.layer}_{state_internal}_{county_internal}_{city_internal}"
                else:
                    entity = f"{self.cfg.layer}_{state_internal}_{county_internal}_{city_internal}"
            else:
                entity = f"{self.cfg.layer}_{state_internal}_{county_internal}"
            
            return entity
        
        # Complete failure
        return "ERROR"
    
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
                
                # Check if URL was already processed successfully
                if old_url in self.processed_urls:
                    self.logger.debug(f"⏭️  Skipping already-processed URL for {entity}: {old_url}")
                    processed_data = self.processed_urls[old_url]
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
                # Not an opendata portal - categorize the type
                if self.cfg.debug:
                    if old_url.lower().endswith('.zip') or '.zip?' in old_url.lower():
                        reason = "DIRECT_ZIP_DOWNLOAD"
                    elif any(pattern in old_url.lower() for pattern in ['rest/services', 'mapserver', 'featureserver']):
                        reason = "DIRECT_ARCGIS_SERVICE"
                    else:
                        reason = "REGULAR_URL"
                    
                    csv_rows.append([
                        entity, og_title, old_url, "NOT_OPENDATA", "0.00", "N/A", reason
                    ])
        
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

def main():
    """Main entry point."""
    parser = build_arg_parser()
    # Remove some arguments that don't apply to this tool
    parser.description = "Convert opendata portal URLs to ArcGIS REST service URLs"
    
    args = parser.parse_args()
    
    # Determine layers to process
    if hasattr(args, 'layers') and args.layers:
        layers_to_process = args.layers
    else:
        # Process all layers if none specified
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
            apply=args.apply if hasattr(args, 'apply') else False
        )
        
        converter = OpendataToAGS(cfg)
        converter.run()
    
    print(f"\n[INFO] ==================== Completed processing {len(layers_to_process)} layer(s) ====================")

if __name__ == "__main__":
    main()
