# Field Filtering and Title Targeting in layers_prescrape.py

## Overview

The `--fill` mode now supports fine-grained control over which fields to standardize and which records to target. This is particularly useful when dealing with duplicate records that have different data types for the same layer.

## New Command Line Options

### Field Filtering Options

#### `--include-fields FIELD [FIELD ...]`
Include only specific fields for standardization. Only the specified fields will be checked for health and corrected.

**Example:**
```bash
# Only standardize URL and format fields
python3 layers_prescrape.py --fill --include-fields src_url_file format --apply
```

#### `--exclude-fields FIELD [FIELD ...]`
Exclude specific fields from standardization. These fields will be preserved as-is without health checks.

**Example:**
```bash
# Standardize all fields except manual ones
python3 layers_prescrape.py --fill --exclude-fields source_org fields_obj_transform --apply
```

### Record Targeting Options

#### `--title TITLE`
Target a specific record by exact title match (case-insensitive). This is useful when you have duplicate records and want to standardize only one specific record.

**Example:**
```bash
# Target a specific record by title
python3 layers_prescrape.py --fill --title "Zoning - City of Gainesville FL" --include-fields src_url_file format --apply
```

## Use Cases

### 1. **Handling Duplicate Records**
When you have multiple records for the same entity with different data types:

```bash
# Standardize only the AGS version of a record
python3 layers_prescrape.py --fill \
  --title "Zoning - City of Gainesville FL (AGS)" \
  --include-fields src_url_file format table_name \
  --apply
```

### 2. **Selective Field Standardization**
When you want to preserve certain fields while standardizing others:

```bash
# Keep existing manual fields, standardize only auto-correctable fields
python3 layers_prescrape.py --fill \
  --exclude-fields source_org fields_obj_transform \
  --apply
```

### 3. **Combined Filtering**
Combine multiple filters for precise control:

```bash
# Target specific record, specific fields, specific entity
python3 layers_prescrape.py --fill \
  --title "Zoning - City of Gainesville FL" \
  --include-fields src_url_file format table_name \
  --include zoning_fl_alachua_gainesville \
  --apply
```

## Field Filtering Logic

### Include Fields
- **Behavior**: Only specified fields are checked for health and corrected
- **Use Case**: When you want to focus on specific problematic fields
- **Example**: `--include-fields src_url_file format` only checks URL and format fields

### Exclude Fields  
- **Behavior**: Specified fields are preserved as-is without health checks
- **Use Case**: When you want to preserve manual work or specific field values
- **Example**: `--exclude-fields source_org fields_obj_transform` preserves manual fields

### Combined Logic
- **Priority**: Include filters take precedence over exclude filters
- **Logic**: If a field is in both include and exclude lists, it will be **included** (checked)
- **Example**: `--include-fields src_url_file --exclude-fields src_url_file` → `src_url_file` will be checked

## Title Targeting Logic

### Exact Match
- **Case-insensitive**: Title matching is not case-sensitive
- **Exact match**: Must match the entire title exactly
- **Use Case**: Target specific records when duplicates exist

### Example Titles
```
"Zoning - City of Gainesville FL"
"Future Land Use - Broward County Unincorporated FL"
"Streets - Miami-Dade County FL"
```

## Integration with Existing Features

### Entity Filtering
Field filtering and title targeting work alongside existing entity filtering:

```bash
# Target specific entity, specific record, specific fields
python3 layers_prescrape.py --fill \
  --include zoning_fl_alachua_gainesville \
  --title "Zoning - City of Gainesville FL" \
  --include-fields src_url_file format \
  --apply
```

### Manual Overrides
Field filtering respects manual overrides in `missing_fields.json`:

- **Included fields**: Manual overrides are applied if they exist
- **Excluded fields**: Manual overrides are ignored (fields preserved as-is)

### Apply Flags
Field filtering works with both apply flags:

- `--apply`: Applies auto-generated corrections to included fields only
- `--apply-manual`: Applies manual overrides to included fields only

## Best Practices

### 1. **Start with Detection**
Always run detect mode first to understand your data:

```bash
python3 layers_prescrape.py --detect --include-fields src_url_file format
```

### 2. **Use Title Targeting for Duplicates**
When you have duplicate records, use title targeting to focus on one:

```bash
# Find the specific record you want to standardize
python3 layers_prescrape.py --detect --title "Zoning - City of Gainesville FL"
```

### 3. **Preserve Manual Work**
Use exclude fields to preserve manual work:

```bash
# Keep manual fields, standardize auto-correctable ones
python3 layers_prescrape.py --fill --exclude-fields source_org fields_obj_transform --apply
```

### 4. **Incremental Standardization**
Standardize fields in stages:

```bash
# Step 1: Standardize URLs
python3 layers_prescrape.py --fill --include-fields src_url_file --apply

# Step 2: Standardize formats  
python3 layers_prescrape.py --fill --include-fields format --apply

# Step 3: Standardize table names
python3 layers_prescrape.py --fill --include-fields table_name --apply
```

## Error Handling

### Invalid Field Names
- **Behavior**: Invalid field names are ignored with a warning
- **Example**: `--include-fields invalid_field` will show a warning but continue

### No Title Match
- **Behavior**: If `--title` doesn't match any records, script exits with warning
- **Example**: `--title "Nonexistent Title"` will show "No records found matching filters"

### Empty Results
- **Behavior**: If filters result in no records, script exits gracefully
- **Example**: Combining too many restrictive filters may result in no matches

## Examples

### Standardize Only URLs for All Zoning Records
```bash
python3 layers_prescrape.py --fill --include-fields src_url_file --apply
```

### Preserve Manual Fields, Standardize Auto Fields
```bash
python3 layers_prescrape.py --fill --exclude-fields source_org fields_obj_transform --apply
```

### Target Specific Record for Full Standardization
```bash
python3 layers_prescrape.py --fill \
  --title "Zoning - City of Gainesville FL" \
  --apply
```

### Standardize Only Format and Table Name for Specific Entity
```bash
python3 layers_prescrape.py --fill \
  --include zoning_fl_alachua_gainesville \
  --include-fields format table_name \
  --apply
``` 