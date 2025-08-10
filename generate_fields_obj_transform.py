#!/usr/bin/env python3
"""
Conservative generator for fields_obj_transform (FOT).

Behavior:
- Reads current catalog rows (excluding NULL layer_subgroup)
- Builds per-layer_subgroup alias maps from existing non-empty FOT values
- For queued rows, validates field_names; if invalid/missing, attempts extraction from local shapefile
- Proposes new FOT using only exact alias-name matches (normalized) for high precision
- Outputs a CSV summary. With --apply, updates DB for high-confidence rows missing FOT

Flags:
- --include, --exclude: entity string filters (fnmatch-style)
- -r/--restrict: only process records with missing/empty FOT
- --debug: verbose reasoning to stderr
- --apply: write accepted mappings back to catalog

Entity key format:
- Based on layer_subgroup and layer level from layers_helpers.LAYER_CONFIGS
- state_county city rules match layers_scrape._entity_from_parts

Field names validation:
- Expect JSON list of strings, e.g., ["OBJECTID", "ZONING", ...]
- If invalid, attempt fresh extraction via shapefile (pyshp) in resolved layer directory

Notes:
- Strongly conservative: only exact normalized alias matches are used; no fuzzy matching
- Does not overwrite existing non-empty FOT unless --apply and target row is empty
"""

from __future__ import annotations

import argparse
import csv
import fnmatch
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

# Local helpers
sys.path.insert(0, str(Path(__file__).resolve().parent))
from layers_helpers import (
    PG_CONNECTION,
    LAYER_CONFIGS,
    format_name,
    resolve_layer_directory,
)


# ------------------------------
# CLI
# ------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Conservative FOT generator")
    parser.add_argument("--include", action="append", default=[], help="fnmatch pattern(s) to include by entity")
    parser.add_argument("--exclude", action="append", default=[], help="fnmatch pattern(s) to exclude by entity")
    parser.add_argument("-r", "--restrict", action="store_true", help="only process rows with missing fields_obj_transform")
    parser.add_argument("--apply", action="store_true", help="apply updates to DB (otherwise dry-run)")
    parser.add_argument("--debug", action="store_true", help="verbose reasoning logs")
    parser.add_argument("--out", default=None, help="output CSV path (default summaries/auto_fot_<date>.csv)")
    return parser.parse_args()


# ------------------------------
# Utilities
# ------------------------------

def debug_print(enabled: bool, *args: object) -> None:
    if enabled:
        print("[DEBUG]", *args, file=sys.stderr)


def normalize_field_name(name: str) -> str:
    s = str(name or "").strip()
    s = re.sub(r"[^A-Za-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()


def parse_fot_string(fot: str) -> List[Tuple[str, str]]:
    """Parse a fields_obj_transform string like "zon_code:zoning, ord_num:ord_no".

    Returns list of (target_key, source_field) pairs.
    """
    if not fot or not str(fot).strip():
        return []
    pairs: List[Tuple[str, str]] = []
    for piece in str(fot).split(","):
        if ":" not in piece:
            continue
        left, right = piece.split(":", 1)
        left = left.strip()
        right = right.strip()
        if left and right:
            pairs.append((left, right))
    return pairs


def is_valid_field_names_json(value: Optional[str]) -> Tuple[bool, List[str]]:
    """Validate field_names format: must be JSON list of strings."""
    if not value:
        return False, []
    try:
        data = json.loads(value)
        if isinstance(data, list) and all(isinstance(x, str) for x in data):
            return True, data
        return False, []
    except Exception:
        return False, []


def build_entity(layer: str, state: Optional[str], county: Optional[str], city: Optional[str]) -> str:
    """Build entity string similar to layers_scrape._entity_from_parts.

    - For state_county layers: layer_state_county
    - Else: layer_state_county_city (city defaults to 'countywide' if blank)
    """
    county_internal = format_name(county or "", "county", external=False)
    city_internal = format_name(city or "", "city", external=False) if city else ""
    if state and str(state).strip() and str(state).strip().upper() not in ("NULL", "NONE"):
        state_internal = str(state).strip().lower()
    else:
        # Minimal fallback: default to 'fl' (mirrors layers_scrape default)
        state_internal = "fl"
    layer_cfg = LAYER_CONFIGS.get(layer, {})
    level = layer_cfg.get("level", "state_county_city")
    if level == "state_county":
        return f"{layer}_{state_internal}_{county_internal}"
    if not city_internal:
        city_internal = "countywide"
    return f"{layer}_{state_internal}_{county_internal}_{city_internal}"


def try_extract_field_names(
    layer: str,
    state: Optional[str],
    county: Optional[str],
    city: Optional[str],
    sys_raw_file: Optional[str],
    sys_raw_folder: Optional[str],
    debug: bool,
) -> List[str]:
    """Attempt to extract field names from a local shapefile using pyshp.

    Conservative approach: only attempt .shp located in the resolved layer directory.
    """
    # Resolve directory
    state_internal = (state or "").lower() if state else None
    county_internal = format_name(county or "", "county", external=False) if county else None
    city_internal = format_name(city or "", "city", external=False) if city else None
    # Prefer provided sys_raw_folder if available; otherwise standard directory
    if sys_raw_folder and os.path.isdir(sys_raw_folder):
        base_dir = sys_raw_folder
    else:
        base_dir = resolve_layer_directory(layer, state_internal, county_internal, city_internal)
    shp_path: Optional[str] = None

    # If sys_raw_file looks like a .shp, try that first
    if sys_raw_file and sys_raw_file.lower().endswith(".shp"):
        candidate = os.path.join(base_dir, sys_raw_file)
        if os.path.exists(candidate):
            shp_path = candidate

    # Otherwise, try to find any .shp in the base directory (non-recursive)
    if shp_path is None and base_dir and os.path.isdir(base_dir):
        for entry in os.listdir(base_dir):
            if entry.lower().endswith(".shp"):
                shp_path = os.path.join(base_dir, entry)
                break

    if shp_path is None or not os.path.exists(shp_path):
        debug_print(debug, f"No .shp found in {base_dir}")
        return []

    try:
        import shapefile  # pyshp
        sf = shapefile.Reader(shp_path)
        names = [f[0] for f in sf.fields[1:]]  # skip deletion flag
        debug_print(debug, f"Extracted {len(names)} field names via pyshp from {shp_path}")
        return names
    except Exception as e:
        debug_print(debug, f"pyshp extraction failed for {shp_path}: {e}")
        return []


# ------------------------------
# Core logic
# ------------------------------

@dataclass
class CatalogRow:
    layer_subgroup: str
    state: Optional[str]
    county: Optional[str]
    city: Optional[str]
    fields_obj_transform: Optional[str]
    field_names: Optional[str]
    sys_raw_file: Optional[str]
    sys_raw_folder: Optional[str]

    def entity(self) -> str:
        return build_entity(self.layer_subgroup, self.state, self.county, self.city)


def fetch_catalog_rows(conn, restrict_missing: bool) -> List[CatalogRow]:
    where_missing = (
        " AND (fields_obj_transform IS NULL OR btrim(fields_obj_transform) = '')"
        if restrict_missing
        else ""
    )
    sql = (
        "SELECT layer_subgroup, state, county, city, fields_obj_transform, field_names, sys_raw_file, sys_raw_folder "
        "FROM m_gis_data_catalog_main "
        "WHERE status IS DISTINCT FROM 'DELETE' "
        "AND layer_subgroup IS NOT NULL "
        f"{where_missing}"
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    result: List[CatalogRow] = []
    for r in rows:
        result.append(
            CatalogRow(
                layer_subgroup=r.get("layer_subgroup"),
                state=r.get("state"),
                county=r.get("county"),
                city=r.get("city"),
                fields_obj_transform=r.get("fields_obj_transform"),
                field_names=r.get("field_names"),
                sys_raw_file=r.get("sys_raw_file"),
                sys_raw_folder=r.get("sys_raw_folder"),
            )
        )
    return result


def build_alias_index_from_catalog(conn) -> Dict[str, Dict[str, set]]:
    """Build alias index per layer_subgroup from existing non-empty FOT.

    Returns: {layer_subgroup: {target_key: {normalized_source_field_names,...}}}
    """
    sql = (
        "SELECT layer_subgroup, fields_obj_transform "
        "FROM m_gis_data_catalog_main "
        "WHERE layer_subgroup IS NOT NULL "
        "AND fields_obj_transform IS NOT NULL "
        "AND btrim(fields_obj_transform) <> ''"
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    index: Dict[str, Dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for r in rows:
        layer = r.get("layer_subgroup")
        fot = r.get("fields_obj_transform")
        if not layer or not fot:
            continue
        for target, source in parse_fot_string(fot):
            norm_source = normalize_field_name(source)
            if norm_source:
                index[layer][target].add(norm_source)
    return index


def propose_transform_for_row(
    row: CatalogRow,
    alias_index: Dict[str, Dict[str, set]],
    debug: bool,
) -> Tuple[str, str, List[str]]:
    """Return (new_fot_str, confidence, reasons).

    Strategy: only exact normalized alias matches; if conflicts or no matches, return empty.
    """
    layer = row.layer_subgroup
    targets = alias_index.get(layer, {})
    if not targets:
        return "", "", ["no_aliases_for_layer"]

    # Determine field names list
    valid, field_list = is_valid_field_names_json(row.field_names)
    field_src = "existing"
    if not valid:
        field_list = try_extract_field_names(
            layer,
            row.state,
            row.county,
            row.city,
            row.sys_raw_file,
            row.sys_raw_folder,
            debug,
        )
        field_src = "extracted"
    if not field_list:
        return "", "", ["no_field_names"]

    norm_fields = {normalize_field_name(n): n for n in field_list if isinstance(n, str)}
    used_sources: set = set()
    pairs: List[str] = []
    reasons: List[str] = []

    for target_key, alias_names in targets.items():
        # Find exact match: intersection of normalized field names and alias set
        candidates = [nf for nf in norm_fields.keys() if nf in alias_names]
        if len(candidates) == 1:
            nf = candidates[0]
            if nf in used_sources:
                reasons.append(f"conflict:{target_key}:{nf}")
                continue
            used_sources.add(nf)
            original_name = norm_fields[nf]
            pairs.append(f"{target_key}:{original_name}")
        elif len(candidates) > 1:
            # Ambiguous -> skip this target key
            reasons.append(f"ambiguous:{target_key}:{'|'.join(candidates)}")
        else:
            # no candidate for this target
            continue

    if not pairs:
        rsn = ",".join(reasons) if reasons else "no_pairs"
        return "", "", [rsn, field_src]

    fot_str = ", ".join(pairs)
    # Confidence tag reflects conservative exact match approach
    confidence = "exact_alias"
    if reasons:
        reasons.append(field_src)
    else:
        reasons = [field_src]
    return fot_str, confidence, reasons


def entity_matches_filters(entity: str, includes: List[str], excludes: List[str]) -> bool:
    if includes:
        if not any(fnmatch.fnmatch(entity, pat) for pat in includes):
            return False
    if excludes:
        if any(fnmatch.fnmatch(entity, pat) for pat in excludes):
            return False
    return True


def update_catalog_row(conn, row: CatalogRow, new_fot: str) -> None:
    sql = (
        "UPDATE m_gis_data_catalog_main SET fields_obj_transform = %s "
        "WHERE layer_subgroup = %s AND COALESCE(state,'') = COALESCE(%s,'') "
        "AND COALESCE(county,'') = COALESCE(%s,'') AND COALESCE(city,'') = COALESCE(%s,'') "
        "AND (fields_obj_transform IS NULL OR btrim(fields_obj_transform) = '')"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (new_fot, row.layer_subgroup, row.state, row.county, row.city))


def main() -> None:
    args = parse_args()
    out_path = (
        args.out
        if args.out
        else f"{Path(__file__).resolve().parent}/summaries/auto_fot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    conn = psycopg2.connect(PG_CONNECTION or "postgresql://postgres@localhost/gisdev")
    try:
        alias_index = build_alias_index_from_catalog(conn)
        debug_print(args.debug, f"Built alias index for {len(alias_index)} layers")

        rows = fetch_catalog_rows(conn, args.restrict)
        debug_print(args.debug, f"Fetched {len(rows)} catalog rows")

        # Prepare CSV
        with open(out_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "entity",
                    "layer_subgroup",
                    "state",
                    "county",
                    "city",
                    "existing_fields_obj_transform",
                    "new_fields_obj_transform",
                    "confidence",
                    "reasons",
                ],
            )
            writer.writeheader()

            updated_count = 0
            processed_count = 0

            for row in rows:
                entity = row.entity()
                if not entity_matches_filters(entity, args.include, args.exclude):
                    continue

                processed_count += 1

                new_fot, confidence, reasons = propose_transform_for_row(row, alias_index, args.debug)
                writer.writerow(
                    {
                        "entity": entity,
                        "layer_subgroup": row.layer_subgroup,
                        "state": row.state or "",
                        "county": row.county or "",
                        "city": row.city or "",
                        "existing_fields_obj_transform": row.fields_obj_transform or "",
                        "new_fields_obj_transform": new_fot or "",
                        "confidence": confidence,
                        "reasons": ";".join(reasons) if reasons else "",
                    }
                )

                if args.apply and new_fot and not (row.fields_obj_transform or "").strip():
                    update_catalog_row(conn, row, new_fot)
                    updated_count += 1

        if args.apply:
            conn.commit()

        print(f"WROTE:{out_path}")
        print(f"ROWS_PROCESSED:{processed_count}")
        if args.apply:
            print(f"ROWS_UPDATED:{updated_count}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()


