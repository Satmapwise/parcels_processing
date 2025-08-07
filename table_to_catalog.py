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
from layers_prescrape import generate_expected_values


TRANSFORM_HEADERS = [
    "shp_pin",
    "shp_pin_clean",
    "shp_pin2",
    "shp_pin2_clean",
    "shp_altkey",
]

CREATE_HEADERS = [
    "entity", "title", "state", "county", "city", "source_org", "data_date", "publish_date", "src_url_file",
    "format", "format_subtype", "download", "resource", "layer_group",
    "layer_subgroup", "category", "sub_category", "sys_raw_folder",
    "table_name", "fields_obj_transform", "source_comments", "processing_comments"
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


def write_create_csv(out_path: str, rows: List[List[str]]) -> None:
    ensure_summaries_dir(out_path)
    rows_sorted = sorted(rows, key=lambda r: r[0] if r and len(r) > 0 else "")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CREATE_HEADERS)
        writer.writerows(rows_sorted)


def _parse_entity(entity: str) -> Tuple[str, str, str]:
    """Return (layer, state, county) from entity like parcel_geo_fl_alachua."""
    parts = entity.split("_")
    # layer can contain an underscore (parcel_geo). Reconstruct layer from first two parts.
    if len(parts) < 4:
        raise ValueError(f"Unexpected entity format: {entity}")
    layer = "_".join(parts[0:2])
    state = parts[2]
    county = "_".join(parts[3:]) if len(parts) > 4 else parts[3]
    return layer, state, county


def _create_catalog_record(
    conn,
    layer: str,
    state_internal: str,
    county_internal: str,
    new_transform: Optional[str],
) -> None:
    # Build base expected record using shared logic
    expected = generate_expected_values(layer, state_internal, county_internal, city=None)
    # Defaults similar to layers_prescrape _create_record
    from datetime import date
    expected.update({
        "publish_date": date.today().strftime("%Y-%m-%d"),
        "download": "AUTO",
        "status": "ACTIVE",
    })
    # Attach fields_obj_transform
    if new_transform:
        expected["fields_obj_transform"] = new_transform
    # Ensure city is NULL for county-level
    if expected.get("city") == "":
        expected["city"] = None

    # Insert
    fields = list(expected.keys())
    placeholders = ", ".join(["%s"] * len(fields))
    field_names = ", ".join(fields)
    values = [expected[f] for f in fields]
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO m_gis_data_catalog_main ({field_names}) VALUES ({placeholders})",
            values,
        )


def main():
    parser = argparse.ArgumentParser(description="Propose and apply catalog transforms from transform tables")
    parser.add_argument("--apply", action="store_true", help="Apply updates to the catalog")
    parser.add_argument("--create", action="store_true", help="Create missing catalog records when applying")
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
        planned_creations: List[str] = []
        applied_creations = 0
        create_rows: List[List[str]] = []

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
            elif args.apply and args.create and not cat_info:
                planned_creations.append(entity)
                # Actually create immediately if applying
                try:
                    layer, st, co = _parse_entity(entity)
                    _create_catalog_record(conn, layer, st, co, new_transform)
                    applied_creations += 1
                except Exception as e:
                    print(f"[CREATE ERROR] Failed creating {entity}: {e}")

            # Build create CSV row for missing catalog records when --create is used (preview or apply)
            if args.create and not cat_info:
                try:
                    layer, st, co = _parse_entity(entity)
                    expected = generate_expected_values(layer, st, co, city=None)
                    row_values = [
                        entity,
                        expected.get("title", ""),
                        expected.get("state", ""),
                        expected.get("county", ""),
                        expected.get("city", ""),
                        "",  # source_org
                        "",  # data_date
                        "",  # publish_date
                        "",  # src_url_file
                        "",  # format
                        "",  # format_subtype
                        "",  # download
                        "",  # resource
                        expected.get("layer_group", ""),
                        expected.get("layer_subgroup", ""),
                        expected.get("category", ""),
                        "",  # sub_category
                        expected.get("sys_raw_folder", ""),
                        expected.get("table_name", ""),
                        new_transform or "",
                        "",  # source_comments
                        "",  # processing_comments
                    ]
                    create_rows.append(row_values)
                except Exception as e:
                    print(f"[CREATE CSV ERROR] Failed composing row for {entity}: {e}")

        write_csv(args.out, output_rows)

        print(f"Wrote {len(output_rows)} rows to {args.out}")

        # When --create is set, also output a create-mode CSV similar to layers_prescrape
        if args.create:
            create_out = os.path.join("summaries", "parcel_geo_table_to_catalog_create.csv")
            write_create_csv(create_out, create_rows)
            print(f"Wrote {len(create_rows)} rows to {create_out}")

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

        if args.apply and args.create:
            conn.commit()
            print(f"Created {applied_creations} new catalog record(s)")
        elif args.create:
            # Preview of which would be created if --apply was also provided
            print(f"Would create {len(planned_creations)} new catalog record(s)")

    finally:
        conn.close()


if __name__ == "__main__":
    main()


