#!/usr/bin/env python3
"""
Generate and optionally apply catalog field transform updates from a transform table.

This script builds proposed `fields_obj_transform` values for the `parcel_geo` layer by
reading from `parcel_shp_fields` and mapping the following headers to their actual
field names when present: shp_pin, shp_pin_clean, shp_pin2, shp_pin2_clean, shp_altkey.

It outputs a CSV of proposed changes and, with --apply, updates the catalog.

CSV columns:
  - entity
  - shp_pin, shp_pin_clean, shp_pin2, shp_pin2_clean, shp_altkey
  - existing_fields_obj_transform
  - new_fields_obj_transform

Entity format used: parcel_geo_<state>_<county>
"""

from __future__ import annotations

import argparse
import csv
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from layers_helpers import PG_CONNECTION, format_name


TRANSFORM_HEADERS = [
    "shp_pin",
    "shp_pin_clean",
    "shp_pin2",
    "shp_pin2_clean",
    "shp_altkey",
]


@dataclass
class TransformRecord:
    state: Optional[str]
    county: Optional[str]
    shp_pin: Optional[str]
    shp_pin_clean: Optional[str]
    shp_pin2: Optional[str]
    shp_pin2_clean: Optional[str]
    shp_altkey: Optional[str]
    import_fields: Optional[str]

    def entity_key(self) -> Optional[str]:
        if not self.state or not self.county:
            return None
        state_internal = format_name(self.state, "state", external=False)
        county_internal = format_name(self.county, "county", external=False)
        if not state_internal or not county_internal:
            return None
        return f"parcel_geo_{state_internal}_{county_internal}"

    def build_new_transform(self) -> str:
        parts: List[str] = []
        values = {
            "shp_pin": self.shp_pin,
            "shp_pin_clean": self.shp_pin_clean,
            "shp_pin2": self.shp_pin2,
            "shp_pin2_clean": self.shp_pin2_clean,
            "shp_altkey": self.shp_altkey,
        }
        for header in TRANSFORM_HEADERS:
            value = values.get(header)
            if value is None:
                continue
            value_str = str(value).strip()
            if value_str:
                parts.append(f"{header}:{value_str}")
        return ", ".join(parts)


def ensure_summaries_dir(path: str) -> None:
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def is_attom_import(import_fields: Optional[str]) -> bool:
    if not import_fields:
        return False
    return "geoid, stack_id" in import_fields.lower()


def fetch_transform_rows(conn) -> Dict[str, TransformRecord]:
    """Fetch rows from parcel_shp_fields and pick one record per (state, county).

    If multiple records exist for the same (state, county), prefer the non-ATTOM record.
    Returns mapping of entity_key -> TransformRecord.
    """
    sql = """
        SELECT
            state,
            county,
            shp_pin,
            shp_pin_clean,
            shp_pin2,
            shp_pin2_clean,
            shp_altkey,
            import_fields
        FROM parcel_shp_fields
        WHERE (shp_pin IS NOT NULL AND btrim(shp_pin) <> '')
           OR (shp_pin_clean IS NOT NULL AND btrim(shp_pin_clean) <> '')
           OR (shp_pin2 IS NOT NULL AND btrim(shp_pin2) <> '')
           OR (shp_pin2_clean IS NOT NULL AND btrim(shp_pin2_clean) <> '')
           OR (shp_altkey IS NOT NULL AND btrim(shp_altkey) <> '')
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    # Group by internalized (state, county) and choose preferred record
    grouped: Dict[Tuple[str, str], List[TransformRecord]] = {}
    for row in rows:
        record = TransformRecord(
            state=row.get("state"),
            county=row.get("county"),
            shp_pin=row.get("shp_pin"),
            shp_pin_clean=row.get("shp_pin_clean"),
            shp_pin2=row.get("shp_pin2"),
            shp_pin2_clean=row.get("shp_pin2_clean"),
            shp_altkey=row.get("shp_altkey"),
            import_fields=row.get("import_fields"),
        )
        state_internal = format_name(record.state or "", "state", external=False)
        county_internal = format_name(record.county or "", "county", external=False)
        if not state_internal or not county_internal:
            continue
        key = (state_internal, county_internal)
        grouped.setdefault(key, []).append(record)

    # Choose non-ATTOM where duplicates exist
    chosen: Dict[str, TransformRecord] = {}
    for (state_internal, county_internal), recs in grouped.items():
        non_attom = [r for r in recs if not is_attom_import(r.import_fields)]
        preferred = non_attom[0] if non_attom else recs[0]
        entity = f"parcel_geo_{state_internal}_{county_internal}"
        chosen[entity] = preferred

    return chosen


def fetch_catalog_map(conn) -> Dict[str, Dict[str, Optional[str]]]:
    """Fetch parcel_geo catalog rows keyed by entity."""
    sql = """
        SELECT ogc_fid,
               layer_subgroup,
               state,
               county,
               city,
               fields_obj_transform
        FROM m_gis_data_catalog_main
        WHERE layer_subgroup = 'parcel_geo'
          AND layer_subgroup IS NOT NULL
          AND status IS DISTINCT FROM 'DELETE'
          AND status IS DISTINCT FROM 'NO'
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    result: Dict[str, Dict[str, Optional[str]]] = {}
    for row in rows:
        state_internal = format_name(row.get("state") or "", "state", external=False)
        county_internal = format_name(row.get("county") or "", "county", external=False)
        # parcel_geo is a county-level layer; city is typically NULL
        if not state_internal or not county_internal:
            continue
        entity = f"parcel_geo_{state_internal}_{county_internal}"
        result[entity] = {
            "ogc_fid": row.get("ogc_fid"),
            "state": state_internal,
            "county": county_internal,
            "existing_transform": row.get("fields_obj_transform") or "",
        }
    return result


def write_csv(out_path: str, rows: List[Dict[str, Optional[str]]]) -> None:
    ensure_summaries_dir(out_path)
    fieldnames = [
        "entity",
        *TRANSFORM_HEADERS,
        "existing_fields_obj_transform",
        "new_fields_obj_transform",
    ]
    rows_sorted = sorted(rows, key=lambda r: r.get("entity") or "")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows_sorted:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def main():
    parser = argparse.ArgumentParser(description="Propose and apply catalog transforms from transform tables")
    parser.add_argument("--apply", action="store_true", help="Apply updates to the catalog")
    parser.add_argument(
        "--out",
        default="summaries/parcel_geo_table_to_catalog.csv",
        help="Output CSV path",
    )
    args = parser.parse_args()

    if not PG_CONNECTION:
        raise RuntimeError("PG_CONNECTION env var must be set (see layers_helpers.py)")

    conn = psycopg2.connect(PG_CONNECTION)
    try:
        transform_map = fetch_transform_rows(conn)
        catalog_map = fetch_catalog_map(conn)

        output_rows: List[Dict[str, Optional[str]]] = []
        updates: List[Tuple[int, str]] = []  # (ogc_fid, new_transform)

        for entity, rec in transform_map.items():
            new_transform = rec.build_new_transform()

            cat_info = catalog_map.get(entity)
            existing = cat_info.get("existing_transform") if cat_info else ""

            row = {
                "entity": entity,
                "shp_pin": rec.shp_pin or "",
                "shp_pin_clean": rec.shp_pin_clean or "",
                "shp_pin2": rec.shp_pin2 or "",
                "shp_pin2_clean": rec.shp_pin2_clean or "",
                "shp_altkey": rec.shp_altkey or "",
                "existing_fields_obj_transform": existing or "",
                "new_fields_obj_transform": new_transform or "",
            }
            output_rows.append(row)

            if args.apply and cat_info and (new_transform or "") != (existing or ""):
                ogc_fid = cat_info.get("ogc_fid")
                if ogc_fid is not None:
                    updates.append((int(ogc_fid), new_transform or ""))

        write_csv(args.out, output_rows)

        print(f"Wrote {len(output_rows)} rows to {args.out}")

        if args.apply and updates:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE m_gis_data_catalog_main SET fields_obj_transform = %s WHERE ogc_fid = %s",
                    [(new_val, ogc_fid) for (ogc_fid, new_val) in updates],
                )
            conn.commit()
            print(f"Applied {len(updates)} updates to catalog")
        elif args.apply:
            print("No updates to apply")

    finally:
        conn.close()


if __name__ == "__main__":
    main()


