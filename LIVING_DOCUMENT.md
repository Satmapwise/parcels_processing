# Living Document Functionality for missing_fields.json

## Overview

The `missing_fields.json` file now functions as a **living document** that preserves existing manual work while adding new missing field entries. This prevents the loss of manual changes during batch runs.

## Key Features

### 1. **Preserves Existing Entries**
- Existing entities and their field values are never overwritten
- Manual work is preserved across multiple script runs
- Only new entities and missing fields are added

### 2. **Smart Field Merging**
- **New Entity**: Adds the entity completely with all its fields
- **Existing Entity**: Only adds fields that don't already exist
- **Special Markers**: Replaces `MANUAL_REQUIRED`, `***MISSING***`, etc. with new values
- **Valid Values**: Preserves existing non-marker values

### 3. **Batch-Safe Operations**
- Multiple script runs won't destroy previous work
- Incremental updates are supported
- Manual overrides take precedence over auto-detected values

## How It Works

### **Entity-Level Logic**
```
For each entity in new missing fields:
  If entity doesn't exist in JSON:
    → Add entity completely with all fields
  If entity exists in JSON:
    → For each field in new missing fields:
      If field doesn't exist in entity:
        → Add field to entity
      If field exists in entity:
        If existing value is special marker:
          → Replace with new value
        If existing value is valid:
          → Preserve existing value
```

### **Special Marker Handling**
The following values are considered "special markers" and will be replaced:
- `"MANUAL_REQUIRED"`
- `"URL_DEPRECATED"`
- `"***MISSING***"`
- `"***DEPRECATED***"`

## Examples

### **Scenario 1: New Entity**
**Initial JSON:**
```json
{
  "existing_entity": {
    "src_url_file": "https://example.com/existing"
  }
}
```

**New Missing Fields:**
```json
{
  "new_entity": {
    "src_url_file": "https://example.com/new",
    "fields_obj_transform": "field1: value1"
  }
}
```

**Result:**
```json
{
  "existing_entity": {
    "src_url_file": "https://example.com/existing"
  },
  "new_entity": {
    "src_url_file": "https://example.com/new",
    "fields_obj_transform": "field1: value1"
  }
}
```

### **Scenario 2: Existing Entity - New Field**
**Initial JSON:**
```json
{
  "existing_entity": {
    "src_url_file": "https://example.com/existing"
  }
}
```

**New Missing Fields:**
```json
{
  "existing_entity": {
    "source_org": "New Organization"
  }
}
```

**Result:**
```json
{
  "existing_entity": {
    "src_url_file": "https://example.com/existing",
    "source_org": "New Organization"
  }
}
```

### **Scenario 3: Replace Special Marker**
**Initial JSON:**
```json
{
  "existing_entity": {
    "src_url_file": "MANUAL_REQUIRED",
    "fields_obj_transform": "field1: value1"
  }
}
```

**New Missing Fields:**
```json
{
  "existing_entity": {
    "src_url_file": "https://example.com/new"
  }
}
```

**Result:**
```json
{
  "existing_entity": {
    "src_url_file": "https://example.com/new",
    "fields_obj_transform": "field1: value1"
  }
}
```

### **Scenario 4: Preserve Valid Value**
**Initial JSON:**
```json
{
  "existing_entity": {
    "src_url_file": "https://example.com/existing",
    "fields_obj_transform": "field1: value1"
  }
}
```

**New Missing Fields:**
```json
{
  "existing_entity": {
    "src_url_file": "https://example.com/new"
  }
}
```

**Result:**
```json
{
  "existing_entity": {
    "src_url_file": "https://example.com/existing",  // Preserved!
    "fields_obj_transform": "field1: value1"
  }
}
```

## Usage Workflow

### **Step 1: Initial Run**
```bash
python3 layers_prescrape.py --fill --manual-file missing_fields.json
```
- Creates initial JSON with missing fields
- All entities and fields are added

### **Step 2: Manual Edits**
Edit `missing_fields.json` to add manual values:
```json
{
  "entity_1": {
    "src_url_file": "https://manual-url.com",
    "fields_obj_transform": "manual: transform"
  }
}
```

### **Step 3: Subsequent Runs**
```bash
python3 layers_prescrape.py --fill --manual-file missing_fields.json
```
- Preserves your manual edits
- Only adds new missing fields
- Replaces special markers with new values

### **Step 4: Apply Changes**
```bash
python3 layers_prescrape.py --fill --apply-manual --manual-file missing_fields.json
```
- Applies all manual overrides to database
- Preserves existing manual work

## Benefits

1. **No Data Loss**: Manual work is never overwritten
2. **Incremental Updates**: Add new fields without losing existing ones
3. **Batch Safe**: Multiple script runs won't destroy previous work
4. **Smart Merging**: Only adds what's missing, preserves what's valid
5. **Marker Replacement**: Automatically replaces placeholder markers with real values

## Logging

The script provides detailed logging about what's being preserved vs. added:

```
[INFO] Loaded existing data with 15 entities
[DEBUG] Preserved existing value for src_url_file in entity_1
[DEBUG] Added field source_org to existing entity entity_2
[DEBUG] Replaced special marker for src_url_file in entity_3
[DEBUG] Added new entity: entity_4
[INFO] Updated missing_fields.json: 2 new entities, 5 new fields
```

This ensures you can track exactly what changes are being made to your JSON file. 