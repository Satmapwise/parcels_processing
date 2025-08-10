#!/usr/bin/env python3
"""
Generate and optionally apply catalog field transform updates from transform tables.

Primary behavior:
  - For `parcel_geo`, build proposed `fields_obj_transform` values from
    `parcel_shp_fields` and output a CSV preview. With --apply, update catalog rows.

Create-mode extension:
  - With `--create`, discover missing catalog records and generate a create CSV.
  - Also supports creating missing catalog records for `zoning` and `flu` based on
    `support.zoning_transform` and `support.flu_transform` tables (Florida only).

CSV columns:
  - entity
  - shp_pin, shp_pin_clean, shp_pin2, shp_pin2_clean, shp_altkey
  - existing_fields_obj_transform
  - new_fields_obj_transform

Entity formats:
  - parcel_geo_<state>_<county>
  - zoning_fl_<county>_<city>
  - flu_fl_<county>_<city>
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


def fetch_catalog_map_parcel(conn) -> Dict[str, Dict[str, Optional[str]]]:
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
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    result: Dict[str, Dict[str, Optional[str]]] = {}
    for row in rows:
        state_internal = format_name(row.get("state") or "", "state", external=False)
        county_internal = format_name(row.get("county") or "", "county", external=False)
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


def fetch_catalog_map_for_layers(conn, layers: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
    """Fetch catalog rows keyed by entity for the given layer_subgroup values.

    Supports city-level layers like `zoning` and `flu`.
    """
    if not layers:
        return {}
    placeholders = ", ".join(["%s"] * len(layers))
    sql = f"""
        SELECT ogc_fid,
               layer_subgroup,
               state,
               county,
               city,
               fields_obj_transform
        FROM m_gis_data_catalog_main
        WHERE layer_subgroup IN ({placeholders})
          AND layer_subgroup IS NOT NULL
          AND status IS DISTINCT FROM 'DELETE'
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, layers)
        rows = cur.fetchall()

    result: Dict[str, Dict[str, Optional[str]]] = {}
    for row in rows:
        layer = (row.get("layer_subgroup") or "").strip().lower()
        state_internal = format_name(row.get("state") or "", "state", external=False)
        county_internal = format_name(row.get("county") or "", "county", external=False)
        city_internal = format_name(row.get("city") or "", "city", external=False)
        if not state_internal or not county_internal:
            continue
        # Default to 'unincorporated' if city is empty for city-level layers
        if layer in {"zoning", "flu"}:
            city_internal = city_internal or "unincorporated"
            entity = f"{layer}_{state_internal}_{county_internal}_{city_internal}"
        else:
            entity = f"{layer}_{state_internal}_{county_internal}"
        result[entity] = {
            "ogc_fid": row.get("ogc_fid"),
            "state": state_internal,
            "county": county_internal,
            "city": city_internal,
            "existing_transform": row.get("fields_obj_transform") or "",
        }
    return result


def _city_from_transform_value(raw_city: Optional[str]) -> str:
    """Normalize city value from transform tables to internal format.

    Returns 'unincorporated' when raw_city is None/empty/'none'.
    """
    if not raw_city or str(raw_city).strip().lower() in {"none", "null", ""}:
        return "unincorporated"
    return format_name(str(raw_city), "city", external=False)


def discover_zoning_entities(conn) -> List[Tuple[str, str, str, str]]:
    """Return list of (layer, state, county, city) discovered from support.zoning_transform."""
    sql = """
        SELECT county, city_name
        FROM support.zoning_transform
        ORDER BY county, city_name
    """
    out: List[Tuple[str, str, str, str]] = []
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            county_internal = format_name(row.get("county") or "", "county", external=False)
            if not county_internal:
                continue
            city_internal = _city_from_transform_value(row.get("city_name"))
            out.append(("zoning", "fl", county_internal, city_internal))
    return out


def discover_flu_entities(conn) -> List[Tuple[str, str, str, str]]:
    """Return list of (layer, state, county, city) discovered from support.flu_transform."""
    sql = """
        SELECT county, city_name
        FROM support.flu_transform
        ORDER BY county, city_name
    """
    out: List[Tuple[str, str, str, str]] = []
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            county_internal = format_name(row.get("county") or "", "county", external=False)
            if not county_internal:
                continue
            city_internal = _city_from_transform_value(row.get("city_name"))
            out.append(("flu", "fl", county_internal, city_internal))
    return out


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


def _parse_entity(entity: str) -> Tuple[str, str, str, Optional[str]]:
    """Return (layer, state, county, city) from entity.

    Supports:
      - parcel_geo_fl_<county>
      - zoning_fl_<county>_<city>
      - flu_fl_<county>_<city>
    """
    parts = entity.split("_")
    if len(parts) < 3:
        raise ValueError(f"Unexpected entity format: {entity}")
    if parts[0] == "parcel" and len(parts) >= 4 and parts[1] == "geo":
        layer = "parcel_geo"
        state = parts[2]
        county = "_".join(parts[3:]) if len(parts) > 4 else parts[3]
        return layer, state, county, None
    else:
        layer = parts[0]
        if len(parts) < 4:
            raise ValueError(f"Unexpected entity format: {entity}")
        state = parts[1]
        county = parts[2]
        city = "_".join(parts[3:]) if len(parts) > 3 else "unincorporated"
        return layer, state, county, city


def _create_catalog_record(
    conn,
    layer: str,
    state_internal: str,
    county_internal: str,
    city_internal: Optional[str],
    new_transform: Optional[str],
) -> None:
    # Build base expected record using shared logic
    expected = generate_expected_values(layer, state_internal, county_internal, city_internal)
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
    # Normalize city for county-level layers
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
        catalog_map = fetch_catalog_map_parcel(conn)

        # Discover zoning/flu entities for create-mode
        zoning_entities = discover_zoning_entities(conn)
        flu_entities = discover_flu_entities(conn)
        catalog_city_layers = fetch_catalog_map_for_layers(conn, ["zoning", "flu"]) if True else {}

        output_rows: List[Dict[str, Optional[str]]] = []
        updates: List[Tuple[int, str]] = []  # (ogc_fid, new_transform)
        planned_creations: List[str] = []
        applied_creations = 0
        create_rows: List[List[str]] = []

        # Precompute which parcel entities are missing from catalog
        missing_parcel_entities = {entity for entity in transform_map.keys() if entity not in catalog_map}

        # Build entity strings for zoning/flu discovered
        def _entity_str(layer: str, state: str, county: str, city: str) -> str:
            return f"{layer}_{state}_{county}_{city}" if layer in {"zoning", "flu"} else f"{layer}_{state}_{county}"

        missing_city_layer_entities: List[Tuple[str, str, str, str]] = []
        for (layer, st, co, ci) in zoning_entities + flu_entities:
            entity = _entity_str(layer, st, co, ci)
            if entity not in catalog_city_layers:
                missing_city_layer_entities.append((layer, st, co, ci))

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
            elif args.apply and args.create and entity in missing_parcel_entities:
                planned_creations.append(entity)
                # Actually create immediately if applying
                try:
                    layer, st, co, ci = _parse_entity(entity)
                    _create_catalog_record(conn, layer, st, co, ci, new_transform)
                    applied_creations += 1
                except Exception as e:
                    print(f"[CREATE ERROR] Failed creating {entity}: {e}")

            # Build create CSV row for missing catalog records when --create is used (preview or apply)
            if args.create and entity in missing_parcel_entities:
                try:
                    layer, st, co, ci = _parse_entity(entity)
                    expected = generate_expected_values(layer, st, co, city=ci)
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
                        "AUTO",  # download
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

        # Handle create-mode for city-level layers (zoning, flu)
        if args.create and missing_city_layer_entities:
            for layer, st, co, ci in missing_city_layer_entities:
                entity = f"{layer}_{st}_{co}_{ci}"
                try:
                    expected = generate_expected_values(layer, st, co, city=ci)
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
                        "AUTO",  # download
                        "",  # resource
                        expected.get("layer_group", ""),
                        expected.get("layer_subgroup", ""),
                        expected.get("category", ""),
                        "",  # sub_category
                        expected.get("sys_raw_folder", ""),
                        expected.get("table_name", ""),
                        "",  # fields_obj_transform (left blank)
                        "",  # source_comments
                        "",  # processing_comments
                    ]
                    create_rows.append(row_values)
                    if args.apply:
                        try:
                            _create_catalog_record(conn, layer, st, co, ci, new_transform=None)
                            applied_creations += 1
                        except Exception as e:
                            print(f"[CREATE ERROR] Failed creating {entity}: {e}")
                except Exception as e:
                    print(f"[CREATE CSV ERROR] Failed composing row for {entity}: {e}")

        print(f"Wrote {len(output_rows)} rows to {args.out}")

        # When --create is set, also output a create-mode CSV similar to layers_prescrape
        if args.create:
            create_out = os.path.join("summaries", "table_to_catalog_create.csv")
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
            print(f"Would create {len(create_rows)} new catalog record(s)")

    finally:
        conn.close()


if __name__ == "__main__":
    main()


