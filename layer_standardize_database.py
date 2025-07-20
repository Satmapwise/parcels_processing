#!/usr/bin/env python3
"""Layer database/manifest standardization utility

This tool synchronises information between:
1. layer_manifest.json
2. m_gis_data_catalog_main
3. <layer>_transform tables (currently zoning_transform and flu_transform)

It supports the following modes:
• default (update) – read manifest, compare DB rows and apply fixes
• --check – read only, output CSV report but make no DB changes
• --manual-fill – read JSON of missing fields and apply only those edits
• --create – create a brand-new record in the DB (requires manual data if optional fields are missing)
• --check-orphans – find DB records lacking manifest entries

Config flags (global):
optional_conditions – toggle extra checks
generate_CSV        – toggle CSV creation
debug               – DEBUG log level
test_mode           – run without touching the DB (still produces reports)

The CLI is documented at the bottom of the file.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# --------------------------------------------------
# Environment / configuration
# --------------------------------------------------

# The .env file MUST contain a line like:
# PG_CONNECTION=host=localhost port=5432 dbname=gisdev user=postgres password=secret
load_dotenv()
PG_CONNECTION: str | None = os.getenv("PG_CONNECTION")

# Global defaults – can be overridden by CLI flags
optional_conditions_default = False
generate_CSV_default = True
debug_default = True
test_mode_default = True

REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

MISSING_FIELDS_JSON = Path("missing_fields.json")

MANIFEST_PATH = Path("layer_manifest.json")

# --------------------------------------------------
# Helper / formatting utilities
# --------------------------------------------------

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
        for j, p in enumerate(parts):
            first_in_phrase = is_first and j == 0
            if first_in_phrase or (p.lower() not in stop_words and len(p) > 2):
                new_parts.append(p.capitalize())
            else:
                new_parts.append(p.lower())
        return "-".join(new_parts)

    return " ".join(cap_token(w, i == 0) for i, w in enumerate(words))


def get_today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

# --------------------------------------------------
# Normaliser helpers
# --------------------------------------------------

import re

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

# --------------------------------------------------
# Manifest processing
# --------------------------------------------------

class ManifestError(RuntimeError):
    pass


class ManifestManager:
    """Utility for working with layer_manifest.json"""

    def __init__(self, manifest_path: Path | str = MANIFEST_PATH):
        self.path = Path(manifest_path)
        if not self.path.exists():
            raise ManifestError(f"Manifest file not found: {self.path}")
        try:
            self._data: Dict[str, Any] = json.loads(self.path.read_text())
        except json.JSONDecodeError as exc:
            raise ManifestError(f"Invalid JSON in manifest: {exc}") from exc

    # --------------------------------------------------
    # Basic queries
    # --------------------------------------------------

    def get_layers(self) -> List[str]:
        return list(self._data.keys())

    def get_entities(self, layer: str) -> List[str]:
        try:
            return list(self._data[layer]["entities"].keys())
        except KeyError:
            raise ManifestError(f"Layer '{layer}' not found in manifest")

    def get_entity_commands(self, layer: str, entity: str) -> List[Any]:
        try:
            return self._data[layer]["entities"][entity]
        except KeyError as exc:
            raise ManifestError(f"Entity '{entity}' not found under layer '{layer}'") from exc

    # --------------------------------------------------
    # Derived helpers
    # --------------------------------------------------

    @staticmethod
    def is_ags_download(command_block: List[Any]) -> bool:
        """Determine if entity is downloaded via ArcGIS REST (AGS) – heuristic."""
        # First command is usually ["python3", "/srv/tools/python/lib/ags_extract_data2.py", ...]
        if not command_block:
            return False
        first = command_block[0]
        if isinstance(first, list) and any("ags_extract" in part for part in first):
            return True
        return False

    @staticmethod
    def _find_update_command(cmds: List[Any]) -> Optional[List[str]]:
        for cmd in cmds:
            if isinstance(cmd, list) and any("update_zoning" in part for part in cmd) or any("update_flu" in part for part in cmd):
                return cmd  # type: ignore [return-value]
        return None

    def get_target_city(self, cmds: List[Any], entity: str) -> str:
        """Return target city used in DB tables (may differ from manifest entity)."""
        update_cmd = self._find_update_command(cmds)
        if update_cmd and len(update_cmd) >= 3:
            # update script, county, city
            return update_cmd[-1].lower()
        # Fallback to manifest entity's city part
        parts = entity.split("_", 1)
        return parts[1] if len(parts) == 2 else entity

    # --------------------------------------------------
    # Resolution helpers
    # --------------------------------------------------

    def find_county_for_city(self, layer: str, city: str) -> Optional[str]:
        """Given a city name (lower-case), return the unique county hosting that city in the manifest.
        Returns None if not found or ambiguous (multiple counties share that city)."""
        city_lc = city.lower()
        matches = []
        for entity in self.get_entities(layer):
            parts = entity.split("_", 1)
            if len(parts) != 2:
                continue
            county_part, city_part = parts
            if city_part == city_lc:
                matches.append(county_part)

        if not matches:
            return None

        # If multiple matches, prefer the county that exactly matches the city name (e.g. alachua_alachua)
        for c in matches:
            if c == city_lc:
                return c

        # If still ambiguous, return None to signal caller to handle
        return matches[0] if len(matches) == 1 else None

# --------------------------------------------------
# Database utilities
# --------------------------------------------------

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

# --------------------------------------------------
# Formatting helpers (layer-specific conventions)
# --------------------------------------------------

# Florida county list (lowercase, underscores). Used for entity parsing in LayerStandardizer._split_entity
FL_COUNTIES: set[str] = {
    "alachua","baker","bay","bradford","brevard","broward","calhoun","charlotte","citrus","clay",
    "collier","columbia","desoto","dixie","duval","escambia","flagler","franklin","gadsden","gilchrist",
    "glades","gulf","hamilton","hardee","hendry","hernando","highlands","hillsborough","holmes",
    "indian_river","jackson","jefferson","lafayette","lake","lee","leon","levy","liberty","madison",
    "manatee","marion","martin","miami_dade","monroe","nassau","okaloosa","okeechobee","orange","osceola",
    "palm_beach","pasco","pinellas","polk","putnam","santa_rosa","sarasota","seminole","st_johns",
    "st_lucie","sumter","suwannee","taylor","union","volusia","wakulla","walton","washington",
}

class Formatter:
    LAYER_GROUP = {"zoning": "flu_zoning", "flu": "flu_zoning"}
    CATEGORY = {"zoning": "08_Land_Use_and_Zoning", "flu": "08_Land_Use_and_Zoning"}

    @staticmethod
    def format_entity_to_title(layer: str, county: str, city: str, entity_type: str) -> str:
        """Renamed for clarity, but keep old name as alias below.
        """
        layer_title = layer.capitalize() if layer.islower() else layer
        county_tc, city_tc = map(title_case, (county, city))
        if entity_type == "city":
            return f"{layer_title} - City of {city_tc}"
        elif entity_type == "unincorporated":
            return f"{layer_title} - {county_tc} Unincorporated"
        elif entity_type == "unified":
            return f"{layer_title} - {county_tc} Unified"
        elif entity_type == "incorporated":
            return f"{layer_title} - {county_tc} Incorporated"
        elif entity_type == "countywide":  # Alias → Unified
            return f"{layer_title} - {county_tc} Unified"
        else:
            return f"{layer_title} - {city_tc}"

    # Back-compat alias for earlier tests
    format_title = format_entity_to_title

    @staticmethod
    def format_title_to_entity(title: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Inverse of format_entity_to_title.
        Attempts to reconstruct layer, county, city, and entity_type from a title.
        Examples:
          Zoning - City of Gainesville        -> ("zoning", "alachua", "gainesville", "city")
          Zoning - Alachua Unincorporated      -> ("zoning", "alachua", "unincorporated", "unincorporated")
          Future Land Use - Duval Unified      -> ("flu",    "duval",   "unified",       "unified")
        Returns lowercase values; if parsing fails returns (None, None, None, None).
        """
        # Split the string into layer part and the remainder.
        try:
            layer_part, rest = title.split(" - ", 1)
        except ValueError:
            return (None, None, None, None)

        # Normalise layer name
        layer_norm = layer_part.strip().lower()
        layer_norm = layer_norm.replace("future land use", "flu")  # Preferred short name

        # The remainder may contain multiple " - " separated pieces, e.g.
        #   "Broward County - County Unified - AGS"
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

    @staticmethod
    def format_table_name(layer: str, county: str, city: str, entity_type: str) -> str:
        # For cities → <layer>_<city>
        # For unincorporated/unified → <layer>_<county>_<suffix>
        layer_lc, county_lc, city_lc = map(str.lower, (layer, county, city))
        if entity_type == "city":
            return f"{layer_lc}_{city_lc}"
        else:
            return f"{layer_lc}_{county_lc}_{entity_type}"

    @staticmethod
    def format_temp_table_name(layer: str, county: str, city: str) -> str:
        layer_prefix = "raw_zon" if layer == "zoning" else "raw_flu"
        return f"{layer_prefix}_{county.lower()}_{city.lower()}"

    @staticmethod
    def get_sys_raw_folder(category: str, layer: str, county: str, city: str) -> Path:
        """Return the system RAW folder path.

        We now key the top-level folder by *category* rather than the legacy
        layer_group.  Example:
          /srv/datascrub/08_Land_Use_and_Zoning/zoning/florida/county/duval/current/source_data/jacksonville
        """
        return Path(
            f"/srv/datascrub/{category}/{layer}/florida/county/{county.lower()}/current/source_data/{city.lower()}"
        )

# --------------------------------------------------
# Layer standardizer core
# --------------------------------------------------

@dataclass
class Config:
    layer: str
    entities: List[str] | None = None
    optional_conditions: bool = optional_conditions_default
    generate_CSV: bool = generate_CSV_default
    debug: bool = debug_default
    test_mode: bool = test_mode_default
    mode: str = "update"  # update | check | manual-fill | create


class LayerStandardizer:
    """Core engine performing checks / updates."""

    def __init__(self, cfg: Config, manifest: ManifestManager):
        self.cfg = cfg
        self.manifest = manifest

        self.logger = logging.getLogger("LayerStandardizer")
        self.logger.setLevel(logging.DEBUG if cfg.debug else logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        self.logger.addHandler(handler)

        # DB connection – always connect; test_mode only prevents writebacks.
        if not PG_CONNECTION:
            self.logger.error("PG_CONNECTION not found in environment. Aborting.")
            sys.exit(1)

        self.db: DB = DB(PG_CONNECTION)

        # Place to accumulate missing manual fields
        self.missing_fields: Dict[str, Dict[str, str]] = defaultdict(dict)

    # --------------------------------------------------
    # Public orchestration
    # --------------------------------------------------

    def run(self):
        if self.cfg.mode == "check":
            self._run_check_mode()
        elif self.cfg.mode == "manual-fill":
            self._run_manual_fill_mode()
        elif self.cfg.mode == "create":
            self._run_create_mode()
        elif self.cfg.mode == "check-orphans":
            self._run_check_orphans_mode()
        else:
            self._run_update_mode()

        # After operation, write missing-fields JSON if needed
        if self.missing_fields and self.cfg.mode in {"update", "check"}:
            self.logger.info(f"Writing missing field report → {MISSING_FIELDS_JSON}")
            with MISSING_FIELDS_JSON.open("w", encoding="utf-8") as fh:
                json.dump(self.missing_fields, fh, indent=2)

        # Close DB connection; commit only when not in test_mode and in write modes
        if self.db:
            if not self.cfg.test_mode and self.cfg.mode in {"update", "manual-fill", "create"}:
                self.db.commit()
            else:
                self.db.conn.rollback()
            self.db.close()

    # --------------------------------------------------
    # Mode implementations (placeholders for now)
    # --------------------------------------------------

    def _run_check_mode(self):
        self.logger.info("Running in CHECK mode – no DB modifications will be made.")

        header_catalog = [
            "layer",
            "county",
            "city",
            "target_city",
            "title",
            "catalog_city",
            "src_url_file",
            "format",
            "download",
            "resource",
            "layer_group",
            "layer_subgroup",
            "category",
            "sys_raw_folder",
            "table_name",
            "fields_obj_transform",
        ]

        header_transform = [
            "transform_record_exists",
            "transform_city_name",
            "transform_temp_table_name",
        ] if self.cfg.layer in {"zoning", "flu"} else []

        csv_rows: List[List[str]] = [header_catalog + header_transform]

        # track duplicates across all entities in this run
        self.duplicates_list: List[List[str]] = []
        duplicate_entities: set[str] = set()
        missing_entities: set[str] = set()
        missing_transform_entities: set[str] = set()
        column_missing_counts = [0]*(len(header_catalog)+len(header_transform))

        present_entities_found = set()

        for entity in sorted(self._select_entities()):
            self.logger.debug(f"Checking entity {entity}")
            county, city = self._split_entity(entity)

            expected = self._expected_values(entity, county, city)

            target_city_raw = self.manifest.get_target_city(self.manifest.get_entity_commands(self.cfg.layer, entity), entity)
            target_city_fmt = norm_city(target_city_raw)
            target_city_disp = title_case(target_city_raw.replace('_',' ')) if target_city_raw else ""
            self.logger.debug(f"Parsed for entity: county={county}, city={city}, target_city={target_city_disp}")

            matches = self._fetch_catalog_rows(county, target_city_fmt)
            dup_status = False
            if not matches:
                cat_row = None
                self.logger.debug("  --> FAILURE")
            elif len(matches) == 1:
                cat_row = matches[0]
                self.logger.debug(f"Matched DB title: {cat_row.get('title')}")
                present_entities_found.add(entity)
                self.logger.debug("  --> SUCCESS")
            else:
                cat_row = matches[0]  # pick first but mark duplicate
                dup_status = True
                present_entities_found.add(entity)
                self.logger.debug(f"Duplicate rows ({len(matches)}) – using first title: {cat_row.get('title')}")
                self.logger.debug("  --> DUPLICATE")
                # record all duplicate rows including the extras beyond first
                for extra in matches:
                    # Build full row data for each duplicate
                    dup_cat_values = [
                        self.cfg.layer,
                        county,
                        city,
                        target_city_disp,
                        safe_catalog_val(extra.get("title")),
                        safe_catalog_val(extra.get("city")),  # catalog city
                        safe_catalog_val(extra.get("src_url_file")),
                        safe_catalog_val(extra.get("format")),
                        safe_catalog_val(extra.get("download")),
                        safe_catalog_val(extra.get("resource")),
                        safe_catalog_val(extra.get("layer_group")),
                        safe_catalog_val(extra.get("layer_subgroup")),
                        safe_catalog_val(extra.get("category")),
                        safe_catalog_val(extra.get("sys_raw_folder")),
                        safe_catalog_val(extra.get("table_name")),
                        safe_catalog_val(extra.get("fields_obj_transform")),
                    ]
                    # Add transform values for duplicates
                    dup_transform_values: List[str] = []
                    if header_transform:
                        tr_row = self._fetch_transform_row(county, city)
                        if tr_row is None:
                            dup_transform_values = ["NO", "**MISSING**", "**MISSING**"]
                        else:
                            dup_transform_values = [
                                "YES",
                                safe_catalog_val(tr_row.get("city_name")),
                                safe_catalog_val(tr_row.get("temp_table_name")),
                            ]
                    dup_row_values = dup_cat_values + dup_transform_values
                    self.duplicates_list.append(dup_row_values)
                    duplicate_entities.add(entity)

            if cat_row is None:
                cat_values = [self.cfg.layer, county, city, target_city_disp, "RECORD MISSING", ""] + ["" for _ in range(len(header_catalog)-6)]
            elif dup_status:
                cat_values = [
                    self.cfg.layer,
                    county,
                    city,
                    target_city_disp,
                    safe_catalog_val(cat_row.get("title")),
                    safe_catalog_val(cat_row.get("city")),  # catalog city
                    safe_catalog_val(cat_row.get("src_url_file")),
                    safe_catalog_val(cat_row.get("format")),
                    safe_catalog_val(cat_row.get("download")),
                    safe_catalog_val(cat_row.get("resource")),
                    safe_catalog_val(cat_row.get("layer_group")),
                    safe_catalog_val(cat_row.get("layer_subgroup")),
                    safe_catalog_val(cat_row.get("category")),
                    safe_catalog_val(cat_row.get("sys_raw_folder")),
                    safe_catalog_val(cat_row.get("table_name")),
                    safe_catalog_val(cat_row.get("fields_obj_transform")),
                ]
            else:
                cat_values = [
                    self.cfg.layer,
                    county,
                    city,
                    target_city_disp,
                    safe_catalog_val(cat_row.get("title")),
                    safe_catalog_val(cat_row.get("city")),  # catalog city
                    safe_catalog_val(cat_row.get("src_url_file")),
                    safe_catalog_val(cat_row.get("format")),
                    safe_catalog_val(cat_row.get("download")),
                    safe_catalog_val(cat_row.get("resource")),
                    safe_catalog_val(cat_row.get("layer_group")),
                    safe_catalog_val(cat_row.get("layer_subgroup")),
                    safe_catalog_val(cat_row.get("category")),
                    safe_catalog_val(cat_row.get("sys_raw_folder")),
                    safe_catalog_val(cat_row.get("table_name")),
                    safe_catalog_val(cat_row.get("fields_obj_transform")),
                ]

            transform_values: List[str] = []
            if header_transform:
                tr_row = self._fetch_transform_row(county, city)
                if tr_row is None:
                    transform_values = ["NO", "**MISSING**", "**MISSING**"]
                    missing_transform_entities.add(entity)
                else:
                    transform_values = [
                        "YES",
                        safe_catalog_val(tr_row.get("city_name")),
                        safe_catalog_val(tr_row.get("temp_table_name")),
                    ]

            row_values = cat_values + transform_values
            # check for missing markers
            if any(isinstance(v,str) and "**MISSING**" in v for v in row_values):
                missing_entities.add(entity)
            # update per-column missing counts (skip transform_record_exists column at index 15)
            for idx,val in enumerate(row_values):
                if isinstance(val,str) and "**MISSING**" in val:
                    column_missing_counts[idx]+=1
                # Special handling: don't count "NO" as missing for transform_record_exists column
                elif isinstance(val,str) and val=="NO" and idx != len(header_catalog):
                    column_missing_counts[idx]+=1

            csv_rows.append(row_values)

        # Sort rows after header: layer, county
        csv_rows_body = csv_rows[1:]
        csv_rows_body.sort(key=lambda r: (
            str(r[0]) if len(r) > 0 and r[0] is not None else "",
            str(r[1]) if len(r) > 1 and r[1] is not None else "",
            str(r[2]) if len(r) > 2 and r[2] is not None else "",
        ))
        csv_rows = [csv_rows[0]] + csv_rows_body

        # Summary row (before duplicates section)
        total_entities = len(set(self._select_entities()))
        success_entities = len(present_entities_found)
        
        # Build summary row to align with data columns
        total_columns = len(header_catalog) + len(header_transform)
        summary_row = [""] * total_columns
        
        # Fixed summary info in first columns
        summary_row[0] = "SUMMARY"
        summary_row[1] = f"{success_entities}/{total_entities}"
        summary_row[2] = f"Missing field: {len(missing_entities)}"
        summary_row[3] = f"Duplicates: {len(duplicate_entities)}"
        
        # Column totals aligned with their respective data columns
        status_idx = len(header_catalog)  # first column in transform section when present
        fot_idx = header_catalog.index("fields_obj_transform")

        for idx in range(4, len(header_catalog)):
            if idx == status_idx:
                summary_row[idx] = f"missing transform: {len(missing_transform_entities)}"
            elif idx == fot_idx:
                summary_row[idx] = f"fields_obj_transform missing: {column_missing_counts[idx]}"
            else:
                summary_row[idx] = f"{header_catalog[idx]}: {column_missing_counts[idx]}"

        csv_rows.append([])
        csv_rows.append(summary_row)

        # Append duplicates section if any duplicates recorded
        if self.duplicates_list:
            csv_rows.append([])  # blank line
            # Use the same header as the main data, but prefix with "DUPLICATES"
            dup_header = ["DUPLICATES"] + (header_catalog + header_transform)[1:]
            csv_rows.append(dup_header)
            csv_rows.extend(self.duplicates_list)

        self._write_csv_report(csv_rows)

        processed_entities = set(self._select_entities())
        missing_records = [e for e in processed_entities if e not in present_entities_found]

        if not self.cfg.debug:
            self.logger.info("--- Check Summary ---")
            self.logger.info(f"Total entities processed: {len(processed_entities)}")
            self.logger.info(f"Entities missing DB records: {len(missing_records)}")
            self.logger.info(f"Duplicate rows: {len(self.duplicates_list)}")
        else:
            self.logger.debug("--- Detailed Check Summary ---")
            self.logger.debug(f"Processed entities ({len(processed_entities)}): {sorted(processed_entities)}")
            if missing_records:
                self.logger.debug(f"Entities missing a DB record ({len(missing_records)}): {missing_records}")
            if self.duplicates_list:
                dup_entities = [f"{row[1]}_{row[2]}" for row in self.duplicates_list]  # county_city format
                self.logger.debug(f"Duplicate rows ({len(self.duplicates_list)}): {dup_entities}")

    def _run_update_mode(self):
        """Update existing catalog / transform rows to match expected values derived from the manifest.

        Rules:
        • Do *not* create new catalog rows – if none found we just note it in the CSV summary.
        • Only overwrite fields that are blank / NULL **or** clearly differ from the expected value.
        • Keep track of what we actually changed so the CSV only shows cells the script generated/modified.
        • A companion CSV called <layer>_database_update_<date>.csv is written to the reports folder.
        """
        self.logger.info("Running in UPDATE mode – updating existing DB rows only (no record creation).")

        # ------------------------------------------------------------------
        # CSV setup   (mostly mirrors the check-mode headers)
        # ------------------------------------------------------------------
        header_catalog = [
            "layer",
            "county",
            "city",
            "target_city",
            "target_title",
            "title",
            "catalog_city",
            "src_url_file",
            "format",
            "download",
            "resource",
            "layer_group",
            "layer_subgroup",
            "category",
            "sys_raw_folder",
            "table_name",
            "fields_obj_transform",
        ]

        header_transform = [
            "transform_record_exists",
            "transform_city_name",
            "transform_temp_table_name",
        ] if self.cfg.layer in {"zoning", "flu"} else []

        csv_rows: List[List[str]] = [header_catalog + header_transform]

        # Counters for the summary row
        total_entities = 0
        found_entities = 0  # records that had a catalog match
        updated_entities = 0  # entities where at least one field was changed in catalog or transform
        missing_entities: List[str] = []
        duplicate_entities: List[str] = []

        # Track per-column change counts (indexes relative to combined header)
        total_columns = len(header_catalog) + len(header_transform)
        change_counts = [0] * total_columns

        # Counters for special summary values
        missing_transform_entities_count = 0  # status == MISSING
        missing_fot_count = 0  # fields_obj_transform null/none across existing rows

        for entity in sorted(self._select_entities()):
            total_entities += 1
            county, city = self._split_entity(entity)

            # Compute expected values (title, layer_group, etc.)
            expected = self._expected_values(entity, county, city)

            # Determine target_city (pretty display name) via manifest helper
            cmds = self.manifest.get_entity_commands(self.cfg.layer, entity)
            target_city_raw = self.manifest.get_target_city(cmds, entity)
            target_city_disp = title_case(target_city_raw.replace("_", " ")) if target_city_raw else ""

            # Fetch matching catalog rows
            matches = self._fetch_catalog_rows(county, norm_city(target_city_raw))

            if not matches:
                # No record – log & add minimal row.
                self.logger.warning(f"No catalog record found for {entity}; skipping.")
                missing_entities.append(entity)
                blank_row = [
                    self.cfg.layer,
                    county,
                    city,
                    target_city_disp,
                    "",                    # target_title missing
                    expected["title"],      # expected new title
                ] + ["" for _ in range(len(header_catalog) - 6 + len(header_transform))]
                csv_rows.append(blank_row)
                continue
            elif len(matches) > 1:
                duplicate_entities.append(entity)
                self.logger.warning(f"Duplicate catalog records ({len(matches)}) for {entity}; using the first match for updates.")

            cat_row = matches[0]
            found_entities += 1

            # --------------------------------------------------
            # Re-compute expected values so we can tweak prefix by referring to
            # the existing DB title (to decide City/Town/Village)
            # --------------------------------------------------
            original_title_lower = str(cat_row.get("title", "")).lower()

            if "town of" in original_title_lower:
                expected_prefix = "Town of"
            elif "village of" in original_title_lower:
                expected_prefix = "Village of"
            else:
                expected_prefix = "City of"

            # Replace the prefix only for normal municipality entities
            if expected_prefix != "City of":
                # Replace the default "City of" with the detected municipal prefix
                expected["title"] = expected["title"].replace("City of", expected_prefix, 1)

            # Preserve hyphens found in original county/city names
            county_tc = title_case(county.replace("_", " "))
            city_tc = title_case(city.replace("_", " "))

            county_dash = county_tc.replace(" ", "-")
            city_dash = city_tc.replace(" ", "-")

            if county_dash in cat_row.get("title", "") and county_dash not in expected["title"]:
                expected["title"] = expected["title"].replace(county_tc, county_dash, 1)

            if city_dash in cat_row.get("title", "") and city_dash not in expected["title"]:
                expected["title"] = expected["title"].replace(city_tc, city_dash, 1)

            # ------------------------------------------------------------------
            # Determine which fields need updating
            # ------------------------------------------------------------------
            update_fields: Dict[str, Any] = {}
            csv_row = [
                self.cfg.layer,
                county,
                city,
                target_city_disp,
                safe_catalog_val(cat_row.get("title")) if cat_row else "",  # pre-change title
                expected["title"],  # expected / desired title
            ]
            # catalog_city, src_url_file, format, download, resource stay blank (script doesn't touch)
            csv_row += ["", "", "", "", ""]  # 5 columns

            # Build (field, column_index) mapping dynamically to remain robust if header order changes
            updatable_catalog_fields = [
                (fld, header_catalog.index(fld))
                for fld in (
                    "title",
                    "layer_group",
                    "layer_subgroup",
                    "category",
                    "sys_raw_folder",
                    "table_name",
                    "fields_obj_transform",
                )
            ]

            for field_name, csv_idx in updatable_catalog_fields:
                expected_val = expected.get(field_name)
                current_val = cat_row.get(field_name)
                if current_val in (None, "", "NULL", "null") or (current_val != expected_val):
                    update_fields[field_name] = expected_val
                    # Extend csv_row list to proper length if not already
                    while len(csv_row) <= csv_idx:
                        csv_row.append("")
                    csv_row[csv_idx] = str(expected_val)
                    change_counts[csv_idx] += 1

            # Ensure csv_row has full catalog length
            while len(csv_row) < len(header_catalog):
                csv_row.append("")

            # ------------------------------------------------------------------
            # Apply catalog updates (if any)
            # ------------------------------------------------------------------
            if update_fields:
                self._update_catalog_row(cat_row, update_fields)
                updated_entities += 1

            # ------------------------------------------------------------------
            # Transform table handling (zoning / flu only)
            # ------------------------------------------------------------------
            if header_transform:
                tr_updates: Dict[str, Any] = {}
                tr_row = self._fetch_transform_row(county, city)
                expected_temp_name = expected["temp_table_name"]

                if tr_row:
                    # Check temp_table_name
                    if tr_row.get("temp_table_name") != expected_temp_name:
                        tr_updates["temp_table_name"] = expected_temp_name
                # No creation of transform rows here – just log missing

                # Prepare transform columns for CSV – default blanks
                transform_csv_vals = ["", "", ""]

                if tr_updates and tr_row:
                    self._update_transform_row(county, city, tr_updates)
                    transform_csv_vals[0] = "UPDATED"
                    transform_csv_vals[2] = expected_temp_name
                    # count change for temp_table_name column
                    change_counts[len(header_catalog) + 2] += 1
                elif tr_row:
                    # Exists but nothing changed
                    transform_csv_vals[0] = "NO_CHANGE"
                else:
                    transform_csv_vals[0] = "MISSING"
                    missing_transform_entities_count += 1

                csv_row.extend(transform_csv_vals)

            # Track missing fields_obj_transform for summary
            if cat_row:
                cur_fot = cat_row.get("fields_obj_transform")
                if cur_fot in (None, "", "NULL", "null"):
                    missing_fot_count += 1

            csv_rows.append(csv_row)

        # ------------------------------------------------------------------
        # Summary row (records found, changed records, per-column change counts)
        # ------------------------------------------------------------------
        header_all = header_catalog + header_transform
        summary_row = ["" for _ in range(total_columns)]
        summary_row[0] = "SUMMARY"
        summary_row[1] = f"{found_entities}/{total_entities}"
        summary_row[2] = f"changed: {updated_entities}"
        summary_row[3] = f"dbmissing: {len(missing_entities)}"  # target_city column

        status_idx = len(header_catalog)  # first column in transform section when present
        fot_idx = header_catalog.index("fields_obj_transform")

        for idx in range(4, total_columns):
            if idx == status_idx:
                summary_row[idx] = f"tmissing: {missing_transform_entities_count}"
            elif idx == fot_idx:
                summary_row[idx] = f"fotmissing: {missing_fot_count}"
            else:
                summary_row[idx] = f"{header_all[idx]}: {change_counts[idx]}"

        csv_rows.append([])
        csv_rows.append(summary_row)

        # ------------------------------------------------------------------
        # Write CSV
        # ------------------------------------------------------------------
        csv_path = REPORTS_DIR / f"{self.cfg.layer}_database_update_{get_today_str()}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            for row in csv_rows:
                writer.writerow(row)
        self.logger.info(f"Update CSV written → {csv_path}")

    # ------------------------------------------------------------------
    # SQL helper methods
    # ------------------------------------------------------------------
    def _update_catalog_row(self, cat_row: Dict[str, Any], updates: Dict[str, Any]):
        """Apply UPDATE to m_gis_data_catalog_main.

        Prefers primary-key columns `gid` or `id` if present; otherwise falls back to a
        composite WHERE clause on (county, city, title) which are unique in practice.
        """
        if not updates:
            return

        set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
        params: Tuple[Any, ...] = tuple(updates.values())

        gid = cat_row.get("gid")
        cid = cat_row.get("id")

        if gid is not None:
            sql = f"UPDATE m_gis_data_catalog_main SET {set_clause} WHERE gid = %s"
            params += (gid,)
        elif cid is not None:
            sql = f"UPDATE m_gis_data_catalog_main SET {set_clause} WHERE id = %s"
            params += (cid,)
        else:
            # Fallback to county+city+title (all comparisons case-insensitive)
            sql = (
                f"UPDATE m_gis_data_catalog_main SET {set_clause} "
                f"WHERE lower(county) = %s AND lower(city) = %s AND lower(title) = %s"
            )
            params += (
                str(cat_row.get("county", "")).lower(),
                str(cat_row.get("city", "")).lower(),
                str(cat_row.get("title", "")).lower(),
            )

        if self.cfg.test_mode:
            self.logger.debug(f"TEST-MODE: Would execute SQL: {sql} params={params}")
        else:
            self.db.execute(sql, params)
            self.logger.debug(
                "Updated catalog row (%s): %s",
                cat_row.get("title", "unknown title"),
                list(updates.keys()),
            )

    def _update_transform_row(self, county: str, city: str, updates: Dict[str, Any]):
        if not updates:
            return
        table = f"{self.cfg.layer}_transform"
        set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
        sql = f"UPDATE {table} SET {set_clause} WHERE county = %s AND city_name = %s"
        params = tuple(updates.values()) + (county.upper(), city.upper())
        if self.cfg.test_mode:
            self.logger.debug(f"TEST-MODE: Would execute SQL: {sql} params={params}")
        else:
            self.db.execute(sql, params)
            self.logger.debug(f"Updated transform row ({county}, {city}): {list(updates.keys())}")

    def _run_manual_fill_mode(self):
        self.logger.info("Running MANUAL-FILL mode – applying user-provided field values only.")
        if not MISSING_FIELDS_JSON.exists():
            self.logger.error("Missing-fields JSON not found. Aborting manual-fill mode.")
            return
        data = json.loads(MISSING_FIELDS_JSON.read_text())
        # TODO: iterate and update DB rows accordingly.
        self.logger.debug(f"Loaded {len(data)} manual fill records (placeholder)")

    def _run_create_mode(self):
        self.logger.info("Running CREATE mode – inserting new DB records.")
        # TODO: implement record creation
        self.logger.warning("CREATE mode not yet implemented in this scaffold.")

    def _run_check_orphans_mode(self):
        """Identify DB catalog rows that lack manifest entries and output like check mode."""
        self.logger.info("Running CHECK-ORPHANS mode – searching for DB records without manifest counterparts.")

        # Get all manifest entities for this layer to compare against
        all_manifest_entities = set(self.manifest.get_entities(self.cfg.layer))

        # Define headers (same as normal check mode)
        header_catalog = [
            "layer",
            "county", 
            "city",
            "target_city",
            "title",
            "catalog_city",
            "src_url_file",
            "format",
            "download",
            "resource",
            "layer_group",
            "layer_subgroup",
            "category",
            "sys_raw_folder",
            "table_name",
            "fields_obj_transform",
        ]

        header_transform = [
            "transform_record_exists",
            "transform_city_name", 
            "transform_temp_table_name",
        ] if self.cfg.layer in {"zoning", "flu"} else []

        csv_rows: List[List[str]] = []

        # Find catalog orphans
        catalog_orphans = self._find_catalog_orphans(all_manifest_entities)
        
        # Find transform orphans 
        transform_orphans = self._find_transform_orphans(all_manifest_entities) if self.cfg.layer in {"zoning", "flu"} else []

        # Build catalog orphans section
        if catalog_orphans:
            csv_rows.append(["CATALOG ORPHANS"] + [""] * (len(header_catalog) + len(header_transform) - 1))
            csv_rows.append(header_catalog + header_transform)
            
            for orphan_data in catalog_orphans:
                csv_rows.append(orphan_data)

        # Build transform orphans section  
        if transform_orphans:
            if catalog_orphans:
                csv_rows.append([])  # blank line separator
            csv_rows.append(["TRANSFORM ORPHANS"] + [""] * (len(header_catalog) + len(header_transform) - 1))
            csv_rows.append(header_catalog + header_transform)
            
            for orphan_data in transform_orphans:
                csv_rows.append(orphan_data)

        # Summary
        total_orphans = len(catalog_orphans) + len(transform_orphans)
        if not csv_rows:
            csv_rows = [["No orphan records found"]]
        else:
            csv_rows.append([])
            csv_rows.append([f"SUMMARY: {len(catalog_orphans)} catalog orphans, {len(transform_orphans)} transform orphans, {total_orphans} total"])

        self._write_csv_report(csv_rows)

        if not self.cfg.debug:
            self.logger.info(f"Catalog orphans: {len(catalog_orphans)}, Transform orphans: {len(transform_orphans)}, Total: {total_orphans}")
        else:
            self.logger.debug(f"Catalog orphan records ({len(catalog_orphans)}):")
            for row in catalog_orphans:
                entity = f"{row[1]}_{row[2]}" if len(row) > 2 else "unknown"
                title = row[4] if len(row) > 4 else "unknown"
                self.logger.debug(f"  {entity} -> {title}")
            self.logger.debug(f"Transform orphan records ({len(transform_orphans)}):")
            for row in transform_orphans:
                entity = f"{row[1]}_{row[2]}" if len(row) > 2 else "unknown"
                self.logger.debug(f"  {entity}")

    # --------------------------------------------------
    # Helper utilities
    # --------------------------------------------------

    def _split_entity(self, entity: str) -> Tuple[str, str]:
        """Split manifest entity into (county, city).

        Handles multi-word counties like 'miami_dade_unincorporated' or 'st_lucie_port_st_lucie'.
        Strategy:
        1. If the last token is a known suffix (unincorporated/unified/incorporated/countywide) treat it as the city.
        2. Otherwise, iterate from longest possible county prefix to shortest until we find a match in FL_COUNTIES.
           The remainder is considered the city.  Fallback is original heuristic (first token as county).
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

    def _select_entities(self) -> List[str]:
        from fnmatch import fnmatch
        all_entities = self.manifest.get_entities(self.cfg.layer)
        patterns = self.cfg.entities
        if not patterns or patterns == ["*"]:
            return all_entities
        selected: List[str] = []
        for pat in patterns:
            # Convert simple '*' wildcards using fnmatch
            for ent in all_entities:
                if fnmatch(ent, pat):
                    selected.append(ent)
        # Remove duplicates while preserving order
        seen = set()
        uniq = []
        for e in selected:
            if e not in seen:
                seen.add(e)
                uniq.append(e)
        return uniq

    def _write_csv_report(self, rows: List[List[str]]):
        if not self.cfg.generate_CSV:
            return
        csv_path = REPORTS_DIR / f"{self.cfg.layer}_database_check_{get_today_str()}.csv"
        # Load existing rows to support incremental update
        existing: List[List[str]] = []
        if csv_path.exists():
            with csv_path.open(newline="", encoding="utf-8") as fh:
                reader = list(csv.reader(fh))
                existing = reader
        # TODO: merge logic preserving alphabetical order – placeholder just overwrite
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            for row in rows or existing:
                writer.writerow(row)
        self.logger.info(f"CSV report written → {csv_path}")

    # --------------------------------------------------
    # Data fetching helpers
    # --------------------------------------------------

    def _fetch_catalog_rows(self, county: str, city_fmt: str) -> List[Dict[str, Any]]:
        """Return all catalog rows matching county and formatted city."""
        sql = "SELECT * FROM m_gis_data_catalog_main WHERE lower(county) LIKE %s"
        rows = self.db.fetchall(sql, (f"{county.lower()}%",)) or []
        matches = []
        for row in rows:
            title = row.get("title", "")
            lyr, cnty, cty_city, _ = Formatter.format_title_to_entity(title)
            if lyr != self.cfg.layer or not cty_city:
                continue

            parsed_city_norm = norm_city(cty_city)
            city_match = (
                city_fmt == parsed_city_norm or
                city_fmt == f"county_{parsed_city_norm}" or
                parsed_city_norm == f"county_{city_fmt}"
            )

            if city_match and (cnty is None or norm_county(cnty) == norm_county(county)):
                matches.append(dict(row))

        # Fallback: for suffix type entities (unincorporated/unified/incorporated/countywide) accept titles that only mention county
        if not matches and city_fmt in {"unincorporated", "unified", "countywide"}:
            for row in rows:
                title = row.get("title", "")
                lyr, cnty, cty_city, _ = Formatter.format_title_to_entity(title)
                if lyr != self.cfg.layer:
                    continue
                if cty_city is None and norm_county(cnty) == norm_county(county):
                    matches.append(dict(row))
        return matches

    def _fetch_transform_row(self, county: str, city: str) -> Optional[Dict[str, Any]]:
        if not self.cfg.layer in {"zoning", "flu"}:
            return None
        table = f"{self.cfg.layer}_transform"
        sql = f"SELECT city_name, temp_table_name FROM {table} WHERE county=%s AND city_name=%s LIMIT 1"
        params = (county.upper(), city.upper())
        return self.db.fetchone(sql, params)

    def _expected_values(self, entity: str, county: str, city: str) -> Dict[str, Any]:
        """Compute expected values for catalog / transform tables (not yet used for check mode)."""
        layer_group = Formatter.LAYER_GROUP[self.cfg.layer]
        category = Formatter.CATEGORY[self.cfg.layer]
        # Map special suffixes / aliases
        if city == "countywide":
            entity_type = "unified"
            city_std = "unified"
        else:
            entity_type = "city" if city not in {"unincorporated", "unified", "incorporated"} else city
            city_std = city
        title = Formatter.format_entity_to_title(self.cfg.layer, county, city_std, entity_type)
        table_name = Formatter.format_table_name(self.cfg.layer, county, city_std, entity_type)
        sys_raw_folder = str(Formatter.get_sys_raw_folder(category, self.cfg.layer, county, city_std))
        temp_table_name = Formatter.format_temp_table_name(self.cfg.layer, county, city_std)

        return {
            "title": title,
            "county": county.title(),
            "city": city_std.title(),
            "layer_group": layer_group,
            "layer_subgroup": self.cfg.layer,
            "category": category,
            "table_name": table_name,
            "sys_raw_folder": sys_raw_folder,
            "temp_table_name": temp_table_name,
        }

    # --------------------------------------------------
    # Orphan detection helpers
    # --------------------------------------------------

    def _find_catalog_orphans(self, manifest_entities: set[str]) -> List[List[str]]:
        """Return list of full catalog record data for DB rows whose layer matches cfg.layer but entity not in manifest."""
        sql = "SELECT * FROM m_gis_data_catalog_main"
        rows = self.db.fetchall(sql) or []
        orphans: List[List[str]] = []
        
        for row in rows:
            title = row.get("title", "")
            layer_from_title, county_from_title, city_from_title, entity_type = Formatter.format_title_to_entity(title)
            
            # Skip if not matching our layer
            if layer_from_title != self.cfg.layer:
                continue
                
            # Construct the entity name based on parsed title components
            if county_from_title and city_from_title:
                # Normalize county and city names to match manifest format
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
                # Fallback to DB fields if title parsing fails
                county_db = row.get('county') or ''
                city_db = row.get('city') or ''
                
                # Handle case where city is empty/None in DB -> treat as unincorporated
                if county_db and (not city_db or city_db.lower() in {'none', 'null', ''}):
                    county_norm = norm_county(county_db)
                    entity = f"{county_norm}_unincorporated"
                else:
                    county_norm = norm_county(county_db)
                    city_norm = norm_city(city_db)
                    entity = f"{county_norm}_{city_norm}"
                
            # Check if this entity exists in manifest
            if entity not in manifest_entities:
                # Build full row data matching the header structure
                county = county_from_title or (row.get('county') or '')
                city = city_from_title or (row.get('city') or '')
                
                cat_values = [
                    self.cfg.layer,
                    county,
                    city,
                    city.replace('_', ' ').title() if city else "",  # target_city display
                    safe_catalog_val(row.get("title")),
                    safe_catalog_val(row.get("city")),  # catalog_city
                    safe_catalog_val(row.get("src_url_file")),
                    safe_catalog_val(row.get("format")),
                    safe_catalog_val(row.get("download")),
                    safe_catalog_val(row.get("resource")),
                    safe_catalog_val(row.get("layer_group")),
                    safe_catalog_val(row.get("layer_subgroup")),
                    safe_catalog_val(row.get("category")),
                    safe_catalog_val(row.get("sys_raw_folder")),
                    safe_catalog_val(row.get("table_name")),
                    safe_catalog_val(row.get("fields_obj_transform")),
                ]
                
                # Add transform values (empty for catalog orphans)
                transform_values = ["", "", ""] if self.cfg.layer in {"zoning", "flu"} else []
                
                orphans.append(cat_values + transform_values)
                
        return orphans
        
    def _find_transform_orphans(self, manifest_entities: set[str]) -> List[List[str]]:
        """Return list of full transform record data for transform table rows without manifest counterparts."""
        if self.cfg.layer not in {"zoning", "flu"}:
            return []
            
        table = f"{self.cfg.layer}_transform"
        sql = f"SELECT * FROM {table}"
        rows = self.db.fetchall(sql) or []
        orphans: List[List[str]] = []
        
        for row in rows:
            county_raw = row.get("county") or ""
            city_name_raw = row.get("city_name") or ""
            
            # Normalize county and city names to match manifest format
            county_norm = norm_county(county_raw)
            city_name_norm = norm_city(city_name_raw)
            
            # Try to construct entity name from transform table data
            # Handle multi-word counties and special suffixes
            suffixes = {"unincorporated", "unified", "countywide"}
            if city_name_norm in suffixes:
                entity = f"{county_norm}_{city_name_norm}"
            else:
                entity = f"{county_norm}_{city_name_norm}"
                
            # Check if this entity exists in manifest
            if entity not in manifest_entities:
                # Build row data - mostly empty for catalog fields since this is a transform orphan
                cat_values = [
                    self.cfg.layer,
                    county_raw.title(),
                    city_name_raw.title(),
                    city_name_raw.replace('_', ' ').title(),  # target_city display
                    "**TRANSFORM ORPHAN**",  # title
                    "**MISSING**",  # catalog_city
                    "**MISSING**",  # src_url_file
                    "**MISSING**",  # format  
                    "**MISSING**",  # download
                    "**MISSING**",  # resource
                    "**MISSING**",  # layer_group
                    "**MISSING**",  # layer_subgroup
                    "**MISSING**",  # category
                    "**MISSING**",  # sys_raw_folder
                    "**MISSING**",  # table_name
                    "**MISSING**",  # fields_obj_transform
                ]
                
                # Add transform values from the orphan record
                transform_values = [
                    "YES",  # status
                    safe_catalog_val(row.get("city_name")),
                    safe_catalog_val(row.get("temp_table_name")),
                ]
                
                orphans.append(cat_values + transform_values)
                
        return orphans



# --------------------------------------------------
# Placeholder – simple format detection (can be replaced later)
# --------------------------------------------------

def get_format(url: str | None) -> str:
    if not url:
        return "UNKNOWN"
    url_lc = url.lower()
    for ext, fmt in {".shp": "SHP", ".zip": "ZIP", "/rest": "AGS", ".geojson": "GEOJSON", ".kml": "KML"}.items():
        if url_lc.endswith(ext):
            return fmt
    return "UNKNOWN"

# --------------------------------------------------
# CLI parsing
# --------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Standardise DB records to match layer manifest")
    p.add_argument("layer", help="Layer name (e.g. zoning, flu, or 'all')")
    p.add_argument("entities", nargs="*", help="One or more <county>_<city> patterns; use '*' as wildcard. Omit for all entities of the layer.")

    group = p.add_mutually_exclusive_group()
    group.add_argument("--check", action="store_true", help="Run in check-only mode (no DB writes)")
    group.add_argument("--manual-fill", action="store_true", help="Apply edits from missing_fields.json")
    group.add_argument("--create", action="store_true", help="Create a new record")
    group.add_argument("--check-orphans", action="store_true", help="Find DB records lacking manifest entries")

    p.add_argument("--optional-conditions", action="store_true", help="Enable optional condition checks")
    p.add_argument("--no-csv", dest="generate_CSV", action="store_false", help="Disable CSV generation")
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    p.add_argument("--test-mode", action="store_true", help="Run without touching the database")
    return p


def main(argv: List[str] | None = None):
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    mode = "update"
    if args.check_orphans:
        mode = "check-orphans"
    elif args.check:
        mode = "check"
    elif args.manual_fill:
        mode = "manual-fill"
    elif args.create:
        mode = "create"

    cfg = Config(
        layer=args.layer.lower(),
        entities=[e.lower() for e in args.entities] if args.entities else None,
        optional_conditions=args.optional_conditions,
        generate_CSV=args.generate_CSV,
        debug=args.debug,
        test_mode=args.test_mode,
        mode=mode,
    )

    manifest = ManifestManager()
    standardizer = LayerStandardizer(cfg, manifest)
    standardizer.run()


if __name__ == "__main__":
    sys.exit(main())
