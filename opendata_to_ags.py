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
    """Simple check if URL is an opendata portal (not a direct ArcGIS service)."""
    # First, exclude direct ArcGIS service URLs
    if any(pattern in url.lower() for pattern in ['rest/services', 'mapserver', 'featureserver']):
        return False
    
    # Then check for opendata portal indicators
    opendata_indicators = [
        'hub.arcgis.com', 'opendata', 'data.', 'geoportal', 'portal', 'datahub', 'open-data'
    ]
    return any(indicator in url.lower() for indicator in opendata_indicators)

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
    
    def run(self):
        """Main execution method."""
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
        
        for entity, record in entity_records:
            old_url = record.get('src_url_file', '').strip()
            og_title = record.get('title', '')
            
            if not old_url:
                continue
            
            # Check if URL is an opendata portal
            if is_opendata_portal(old_url):
                opendata_count += 1
                self.logger.debug(f"Processing opendata URL for {entity}: {old_url}")
                
                # Extract layer keywords from the layer name
                layer_keywords = [self.cfg.layer]
                config = LAYER_CONFIGS.get(self.cfg.layer, {})
                if 'external_frmt' in config:
                    layer_keywords.append(config['external_frmt'])
                
                # Convert to ArcGIS URLs
                ags_candidates = self._convert_opendata_to_ags(old_url, layer_keywords)
                
                if ags_candidates:
                    conversion_count += 1
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
                # Not an opendata portal - skip or note as regular URL
                if self.cfg.debug:
                    csv_rows.append([
                        entity, og_title, old_url, "NOT_OPENDATA", "0.00", "N/A", "REGULAR_URL"
                    ])
        
        # Write CSV report
        if self.cfg.generate_csv:
            csv_path = REPORTS_DIR / f"{self.cfg.layer}_opendata_to_ags.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(csv_rows)
            self.logger.info(f"Conversion report written → {csv_path}")
        
        # Summary
        self.logger.info(f"Conversion complete: {opendata_count} opendata URLs found, {conversion_count} successful conversions")

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
            max_candidates=3
        )
        
        converter = OpendataToAGS(cfg)
        converter.run()
    
    print(f"\n[INFO] ==================== Completed processing {len(layers_to_process)} layer(s) ====================")

if __name__ == "__main__":
    main()
