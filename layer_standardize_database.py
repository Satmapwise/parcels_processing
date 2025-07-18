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
PG_CONNECTION: str | None = os.getenv("M_GIS_DATA_CATALOG_MAIN")

# Global defaults – can be overridden by CLI flags
optional_conditions_default = False
generate_CSV_default = True
debug_default = False
test_mode_default = False

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
# Database utilities
# --------------------------------------------------

class DB:
    """Thin wrapper around psycopg2 connection with dict cursors."""

    def __init__(self, conn_str: str):
        self.conn = psycopg2.connect(conn_str)
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def fetchone(self, sql: str, params: Tuple[Any, ...] | None = None):
        self.cur.execute(sql, params)
        return self.cur.fetchone()

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

    @staticmethod
    def format_title_to_entity(title: str) -> str:
        return "placeholder"

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

        # DB connection (unless test_mode)
        self.db: DB | None = None
        if not cfg.test_mode:
            if not PG_CONNECTION:
                self.logger.error("PG_CONNECTION not found in environment. Aborting.")
                sys.exit(1)
            self.db = DB(PG_CONNECTION)

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
        else:
            self._run_update_mode()

        # After operation, write missing-fields JSON if needed
        if self.missing_fields and self.cfg.mode in {"update", "check"}:
            self.logger.info(f"Writing missing field report → {MISSING_FIELDS_JSON}")
            with MISSING_FIELDS_JSON.open("w", encoding="utf-8") as fh:
                json.dump(self.missing_fields, fh, indent=2)

        if self.db:
            self.db.commit()
            self.db.close()

    # --------------------------------------------------
    # Mode implementations (placeholders for now)
    # --------------------------------------------------

    def _run_check_mode(self):
        self.logger.info("Running in CHECK mode – no DB modifications will be made.")
        # TODO: implement full check logic
        # For now, just iterate entities and print basics
        entities = self._select_entities()
        for entity in entities:
            county, city = self._split_entity(entity)
            self.logger.debug(f"Would check {self.cfg.layer}:{county}_{city}")
        self._write_csv_report([])  # placeholder empty list

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

    p.add_argument("--optional-conditions", action="store_true", help="Enable optional condition checks")
    p.add_argument("--no-csv", dest="generate_CSV", action="store_false", help="Disable CSV generation")
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    p.add_argument("--test-mode", action="store_true", help="Run without touching the database")
    return p


def main(argv: List[str] | None = None):
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    mode = "update"
    if args.check:
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
