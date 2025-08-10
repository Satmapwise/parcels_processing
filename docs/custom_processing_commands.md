# Custom post-download/metadata processing commands (by layer)

This document lists entity-specific commands mentioned in layer docs that occur after download or after metadata extraction, excluding standard zip operations (`unzip`, `zip_rename_date.sh`). Use this as a reference for adding automated steps (e.g., via processing_comments) or for manual runbooks.

## Address Points

- Pinellas (`address_points_fl_pinellas`)
  - Rename shapefile to a standard basename (renames all sidecar files):
    
    ```bash
    shpmv 'Site_Address_Points_(911).shp' Site_Address_Points.shp
    ```

  - Reproject to EPSG:2882:
    
    ```bash
    ogr2ogr -t_srs "EPSG:2882" -nlt GEOMETRY -f "ESRI Shapefile" Site_Address_Points_2882.shp Site_Address_Points.shp
    ```

- Seminole (`address_points_fl_seminole`)
  - Clean previous FileGDB before re-unzipping:
    
    ```bash
    rm -r addresses.gdb
    ```

- Palm Beach (`address_points_fl_palm_beach`)
  - Special processing flag noted in docs:
    
    ```bash
    update_address_points.py palm_beach true
    ```

## Streets

- Walton (`streets_fl_walton`)
  - Extract target layer from FileGDB to shapefile prior to processing:
    
    ```bash
    ogr2ogr streets.shp PublicData_20230227.gdb
    ```

## Subdivisions

- Hillsborough (`subdivisions_fl_hillsborough`)
  - Fetch plat index via API, convert, and load into Postgres (used to enrich subdivision attributes):
    
    ```bash
    curl -H "Accept: application/json" \
         -H "Content-Type: application/json" \
         -d '{"BookType":"P","BookNum":"110"}' \
         -X POST https://pubrec6.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search \
         > test.json

    in2csv -k "ResultList" test.json > test.csv

    ogr2ogr -append -nlt GEOMETRY -f "PostgreSQL" \
      PG:"user=postgres dbname=gisdev host=localhost port=5432 password=galactic529" \
      -nln temp.plat_index_hillsborough test.csv
    ```

## Sunbiz

- Sunbiz statewide (`sunbiz_fl`)
  - Strip non-ASCII characters from raw files:
    
    ```bash
    tr -c '\11\12\15\40-\176' 'Z' < cordataN.txt > cordataN_strip.txt
    ```

  - Run processing script per chunk N (0..9 as applicable):
    
    ```bash
    /srv/tools/python/lib/sunbiz_corpdata_processing.py N
    ```

  - Load processed text files into Postgres:
    
    ```bash
    psql -d gisdev -U postgres -c "\copy sunbiz_processed from 'sunbiz_processed_N.txt' with delimiter as E'\t' null as ''"
    ```

---

Notes
- Download-phase commands (e.g., `download_data.py`, `ags_extract_data2.py`) and inspection-only commands (e.g., `ogrinfo ... | less`) are intentionally excluded.
- The `shpmv` utility renames a shapefile while keeping sidecar files in sync; ensure it is available in the environment if automating.


