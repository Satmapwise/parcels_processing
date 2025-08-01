# Manual Field Overrides in layers_prescrape.py

## Overview

The `--fill-manual` logic has been enhanced to support dynamic manual field overrides. You can now manually create entity entries with custom field values that will override both existing database values and auto-generated corrections.

## Key Features

### 1. **Dynamic Manual Overrides**
- Add any entity with any field values to the JSON file
- Override existing database values with custom values
- Skip special markers (`MANUAL_REQUIRED`, `***MISSING***`, etc.)
- Support for all field types (URLs, transforms, formats, etc.)

### 2. **Smart Filtering**
- Automatically filters out special markers that indicate missing fields
- Only processes actual field values
- Maintains backward compatibility with existing JSON files

### 3. **Precise Control with `--apply-manual`**
- Only applies manual fields when `--apply-manual` is used
- Skips auto-generated corrections when `--apply-manual` is active
- Ensures manual overrides take precedence over auto-corrections

## JSON File Format

### Structure
```json
{
  "entity_name": {
    "field_name": "field_value",
    "another_field": "another_value"
  }
}
```

### Entity Names
Use the standard entity format: `layer_state_county_city`
- `zoning_fl_alachua_gainesville`
- `flu_fl_broward_unincorporated`
- `streets_fl_miami_dade`
- `address_points_fl_orange`

### Field Names
Common fields you can override:
- `src_url_file`: Source URL for the data
- `fields_obj_transform`: Field mapping for data transformation
- `source_org`: Source organization name
- `format`: Data format (AGS, SHP, etc.)
- `table_name`: Database table name (for AGS layers)
- `resource`: Resource path for non-AGS layers

### Special Markers (Filtered Out)
These values are automatically filtered out and not applied:
- `"MANUAL_REQUIRED"`
- `"URL_DEPRECATED"`
- `"***MISSING***"`
- `"***DEPRECATED***"`

## Usage Examples

### 1. Basic Manual Override
```json
{
  "zoning_fl_alachua_gainesville": {
    "src_url_file": "https://gis.cityofgainesville.org/arcgis/rest/services/Zoning/MapServer/0",
    "fields_obj_transform": "OBJECTID: id, ZONING: zoning_code, ZONING_DESC: zoning_description",
    "source_org": "City of Gainesville GIS Department"
  }
}
```

### 2. AGS Layer with Table Name
```json
{
  "streets_fl_miami_dade": {
    "src_url_file": "https://gis.miamidade.gov/arcgis/rest/services/Transportation/Streets/MapServer/0",
    "format": "AGS",
    "table_name": "streets_fl_miami_dade_ags",
    "source_org": "Miami-Dade County GIS Department"
  }
}
```

### 3. Mixed Valid and Invalid Entries
```json
{
  "flu_fl_broward_unincorporated": {
    "src_url_file": "https://gis.broward.org/arcgis/rest/services/Planning/FutureLandUse/MapServer/0",
    "fields_obj_transform": "OBJECTID: id, FLU_CODE: flu_code, FLU_DESC: flu_description"
  },
  "zoning_fl_invalid_entity": {
    "src_url_file": "MANUAL_REQUIRED",
    "fields_obj_transform": "***MISSING***"
  }
}
```
*Note: The second entry will be filtered out because it contains special markers.*

## Command Usage

### 1. Detect Issues and Generate Manual Fields
```bash
python3 layers_prescrape.py --fill --manual-file missing_fields.json
```
This will:
- Run health checks on all records
- Generate `missing_fields.json` with issues that need manual attention
- Show what needs to be fixed

### 2. Apply Manual Overrides Only
```bash
python3 layers_prescrape.py --fill --apply-manual --manual-file missing_fields.json
```
This will:
- Load manual overrides from the JSON file
- Apply ONLY manual field changes
- Skip all auto-generated corrections
- Update database with your custom values

### 3. Apply Auto Corrections Only
```bash
python3 layers_prescrape.py --fill --apply --manual-file missing_fields.json
```
This will:
- Apply auto-generated corrections
- Skip manual field changes
- Use manual overrides when available

### 4. Apply Both Auto and Manual
```bash
python3 layers_prescrape.py --fill --apply --apply-manual --manual-file missing_fields.json
```
This will:
- Apply auto-generated corrections for non-manual fields
- Apply manual overrides for manual fields
- Give manual overrides precedence over auto-corrections

## Workflow Example

### Step 1: Detect Issues
```bash
python3 layers_prescrape.py --fill --manual-file my_overrides.json
```

### Step 2: Edit JSON File
Add your manual overrides to `my_overrides.json`:
```json
{
  "zoning_fl_alachua_gainesville": {
    "src_url_file": "https://gis.cityofgainesville.org/arcgis/rest/services/Zoning/MapServer/0",
    "fields_obj_transform": "OBJECTID: id, ZONING: zoning_code"
  }
}
```

### Step 3: Apply Manual Overrides
```bash
python3 layers_prescrape.py --fill --apply-manual --manual-file my_overrides.json
```

## Benefits

1. **Flexibility**: Override any field for any entity
2. **Precision**: `--apply-manual` only changes manual fields
3. **Safety**: Special markers are filtered out automatically
4. **Compatibility**: Works with existing JSON files
5. **Control**: Manual overrides take precedence over auto-corrections

## Error Handling

- Invalid JSON files are logged as warnings
- Missing files are handled gracefully
- Special markers are automatically filtered out
- Invalid entity names are skipped with warnings 