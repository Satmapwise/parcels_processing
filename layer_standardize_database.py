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
generate_CSV_default = False
debug_default = False
test_mode_default = True

REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

MISSING_FIELDS_JSON = Path("missing_fields.json")

MANIFEST_PATH = Path("layer_manifest.json")

# --------------------------------------------------
# Helper / formatting utilities
# --------------------------------------------------

def title_case(s: str) -> str:
    """Return string in title-case, but keep words like 'of' lowercase unless first."""
    return " ".join(w.capitalize() if i == 0 or len(w) > 2 else w for i, w in enumerate(s.split()))


def get_today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

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
        import re

        # Split the string into layer part and the remainder.
        try:
            layer_part, rest = title.split(" - ", 1)
        except ValueError:
            return (None, None, None, None)

        # Normalise layer name
        layer_norm = layer_part.strip().lower()
        layer_norm = layer_norm.replace("future land use", "flu")  # Preferred short name

        # The remainder may have extra descriptors after additional dashes, e.g.
        #   "City of Gainesville - AGS"  or  "Alachua Unincorporated - PDF"
        rest_main = rest.split(" - ", 1)[0].strip()

        # Regex patterns for different entity types
        city_re = re.compile(r"^city of\s+(.+)$", re.I)
        county_suffix_re = re.compile(r"^([A-Za-z\s]+?)\s+(unincorporated|unified|countywide)$", re.I)

        m_city = city_re.match(rest_main)
        if m_city:
            city = m_city.group(1).strip().lower()
            return (layer_norm, None, city, "city")

        m_cnty = county_suffix_re.match(rest_main)
        if m_cnty:
            county = m_cnty.group(1).strip().lower()
            suffix = m_cnty.group(2).strip().lower()
            return (layer_norm, county, suffix, suffix)

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
    def get_sys_raw_folder(layer_group: str, layer: str, county: str, city: str) -> Path:
        return Path(
            f"/srv/datascrub/{layer_group}/{layer}/florida/county/{county.lower()}/current/source_data/{city.lower()}"
        )

# --------------------------------------------------
# Layer standardizer core
# --------------------------------------------------

@dataclass
class Config:
    layer: str
    county: str | None = None
    city: str | None = None
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
            "title",
            "src_url_file",
            "format",
            "download",
            "resource",
            "layer_group",
            "category",
            "sys_raw_folder",
            "table_name",
            "fields_obj_transform",
        ]

        header_transform = [
            "transform_city_name",
            "transform_temp_table_name",
        ] if self.cfg.layer in {"zoning", "flu"} else []

        csv_rows: List[List[str]] = [header_catalog + header_transform]

        for entity in sorted(self._select_entities()):
            self.logger.debug(f"Checking entity {entity}")
            county, city = self._split_entity(entity)

            expected = self._expected_values(entity, county, city)

            cat_row = self._fetch_catalog_row(county, city)
            if cat_row is None:
                cat_values = ["MISSING"] * len(header_catalog)
            else:
                cat_values = [
                    self.cfg.layer,
                    cat_row.get("county", "MISSING"),
                    cat_row.get("city", "MISSING"),
                    cat_row.get("title", "MISSING"),
                    cat_row.get("src_url_file", "MISSING"),
                    cat_row.get("format", "MISSING"),
                    cat_row.get("download", "MISSING"),
                    cat_row.get("resource", "MISSING"),
                    cat_row.get("layer_group", "MISSING"),
                    cat_row.get("category", "MISSING"),
                    cat_row.get("sys_raw_folder", "MISSING"),
                    cat_row.get("table_name", "MISSING"),
                    cat_row.get("fields_obj_transform", "MISSING"),
                ]

            transform_values: List[str] = []
            if header_transform:
                tr_row = self._fetch_transform_row(county, city)
                if tr_row is None:
                    transform_values = ["MISSING", "MISSING"]
                else:
                    transform_values = [
                        tr_row.get("city_name", "MISSING"),
                        tr_row.get("temp_table_name", "MISSING"),
                    ]

            csv_rows.append(cat_values + transform_values)

        # Sort rows after header: layer, county
        csv_rows_body = csv_rows[1:]
        csv_rows_body.sort(key=lambda r: (r[0], r[1], r[2]))
        csv_rows = [csv_rows[0]] + csv_rows_body

        # Orphan detection (records in DB but not manifest)
        processed_entities = set(self._select_entities())
        orphan_rows = self._find_db_orphans(processed_entities)

        if orphan_rows:
            csv_rows.append([])  # blank line separator
            csv_rows.append(["ORPHANS", "entity", "title"])  # section header (3 cols)
            csv_rows.extend([["", ent, title] for ent, title in orphan_rows])

        self._write_csv_report(csv_rows)

        # --------------------------------------------------
        # Summary reporting
        # --------------------------------------------------

        processed_entities = set(self._select_entities())

        def _entity_from_row(r: List[str]) -> str | None:
            if len(r) >= 3 and r[0] != "ORPHANS":
                return f"{r[1].lower()}_{r[2].lower()}"
            return None

        present_entities = set()
        for row in csv_rows[1:]:
            if len(row) >= 4 and row[0] != "ORPHANS" and row[3] != "MISSING":
                ent = _entity_from_row(row)
                if ent:
                    present_entities.add(ent)

        missing_records = [e for e in processed_entities if e not in present_entities]

        db_only_entities: set[str] = set()
        try:
            db_only_entities = self._db_entities_not_in_manifest(processed_entities)
        except Exception as exc:
            self.logger.warning(f"Could not fetch DB-only entities: {exc}")

        if not self.cfg.debug:
            self.logger.info("--- Check Summary ---")
            self.logger.info(f"Total entities processed: {len(processed_entities)}")
            self.logger.info(f"Entities missing DB records: {len(missing_records)}")
            self.logger.info(f"Records without manifest entries: {len(db_only_entities)}")
        else:
            self.logger.debug("--- Detailed Check Summary ---")
            self.logger.debug(f"Processed entities: {sorted(processed_entities)}")
            if missing_records:
                self.logger.debug(f"Entities missing a DB record ({len(missing_records)}): {missing_records}")
            # if db_only_entities:
            #     self.logger.debug(f"DB records without manifest entry ({len(db_only_entities)}): {sorted(db_only_entities)}")

    def _run_update_mode(self):
        self.logger.info("Running in UPDATE mode – DB rows will be modified as needed.")
        # TODO: implement update logic
        entities = self._select_entities()
        for entity in entities:
            self.logger.debug(f"Processing entity {entity}")
            # placeholder – real implementation later
            pass

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

        manifest_entities = set(self._select_entities())
        orphan_rows = self._find_db_orphans(manifest_entities)

        if not self.cfg.debug:
            self.logger.info(f"Orphans found: {len(orphan_rows)}")
        else:
            self.logger.debug(f"Orphan records ({len(orphan_rows)}):")
            for ent, title in orphan_rows:
                self.logger.debug(f"  {ent} -> {title}")

        # Build CSV rows with same format as check but only orphan section
        csv_rows = [["ORPHANS", "entity", "title"]]
        csv_rows.extend([["", ent, title] for ent, title in orphan_rows])
        self._write_csv_report(csv_rows)

    # --------------------------------------------------
    # Helper utilities
    # --------------------------------------------------

    def _split_entity(self, entity: str) -> Tuple[str, str]:
        parts = entity.split("_", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid entity format: {entity}")
        return parts[0], parts[1]

    def _select_entities(self) -> List[str]:
        if self.cfg.county and self.cfg.city:
            return [f"{self.cfg.county.lower()}_{self.cfg.city.lower()}"]
        elif self.cfg.county and self.cfg.county.lower() != "all":
            # All entities for this county
            return [e for e in self.manifest.get_entities(self.cfg.layer) if e.startswith(self.cfg.county.lower() + "_")]
        else:
            # All entities in layer
            return self.manifest.get_entities(self.cfg.layer)

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

    def _fetch_catalog_row(self, county: str, city: str) -> Optional[Dict[str, Any]]:
        sql = (
            "SELECT * FROM m_gis_data_catalog_main "
            "WHERE lower(county)=%s AND lower(city)=%s AND lower(layer_group)=%s LIMIT 1"
        )
        params = (county.lower(), city.lower(), Formatter.LAYER_GROUP[self.cfg.layer])
        return self.db.fetchone(sql, params)

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
        entity_type = "city" if city not in {"unincorporated", "unified", "countywide"} else city
        title = Formatter.format_entity_to_title(self.cfg.layer, county, city, entity_type)
        table_name = Formatter.format_table_name(self.cfg.layer, county, city, entity_type)
        sys_raw_folder = str(Formatter.get_sys_raw_folder(layer_group, self.cfg.layer, county, city))
        temp_table_name = Formatter.format_temp_table_name(self.cfg.layer, county, city)

        return {
            "title": title,
            "county": county.title(),
            "city": city.title(),
            "layer_group": layer_group,
            "category": category,
            "table_name": table_name,
            "sys_raw_folder": sys_raw_folder,
            "temp_table_name": temp_table_name,
        }

    # --------------------------------------------------
    # Orphan detection helpers
    # --------------------------------------------------

    def _find_db_orphans(self, manifest_entities: set[str]) -> List[Tuple[str, str]]:
        """Return list of (entity, title) pairs for DB rows whose layer matches cfg.layer but entity not in manifest."""
        sql = "SELECT lower(county) AS county, lower(city) AS city, title FROM m_gis_data_catalog_main"
        rows = self.db.fetchall(sql) or []
        orphans: List[Tuple[str, str]] = []
        for row in rows:
            title = row["title"]
            layer_from_title, county_from_title, city_from_title, _ = Formatter.format_title_to_entity(title)
            if layer_from_title != self.cfg.layer:
                continue
            entity = f"{row['county']}_{row['city']}"
            if entity not in manifest_entities:
                orphans.append((entity, title))
        return orphans

    def _db_entities_not_in_manifest(self, manifest_entities: set[str]) -> set[str]:
        sql = "SELECT lower(county) AS county, lower(city) AS city, title FROM m_gis_data_catalog_main"
        rows = self.db.fetchall(sql) or []
        db_entities = set()
        for row in rows:
            layer_from_title, _, _, _ = Formatter.format_title_to_entity(row["title"])
            if layer_from_title != self.cfg.layer:
                continue
            db_entities.add(f"{row['county']}_{row['city']}")
        return db_entities.difference(manifest_entities)

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
    p.add_argument("county", help="County name or 'all'")
    p.add_argument("city", help="City name or 'all'")

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
        county=None if args.county.lower() == "all" else args.county.lower(),
        city=None if args.city.lower() == "all" else args.city.lower(),
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
