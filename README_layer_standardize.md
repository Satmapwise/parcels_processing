# Layer Database Standardization Script

This script standardizes the database and manifest files for Florida GIS layers, ensuring consistency between `layer_manifest.json`, `m_gis_data_catalog_main` table, and transform tables (`zoning_transform`, `flu_transform`).

## Overview

The script performs the following key functions:

1. **Standardization**: Updates database records to match manifest entries
2. **Validation**: Checks for missing fields, duplicates, and orphaned records
3. **Creation**: Creates new entities in all required tables
4. **Manual Fill**: Fills in missing fields with provided data
5. **Reporting**: Generates CSV reports and JSON summaries

## Features

### Core Functionality
- **Entity Processing**: Loops through every entity in the manifest
- **AGS Detection**: Automatically detects AGS vs non-AGS downloads
- **Target City Mapping**: Handles cases where entity names differ from target city names
- **Format Detection**: Automatically determines data format from URLs
- **Directory Creation**: Creates required system directories
- **URL Validation**: Optionally validates source URLs

### Database Updates
- **Catalog Table**: Updates `m_gis_data_catalog_main` with standardized fields
- **Transform Tables**: Updates `zoning_transform` and `flu_transform` tables
- **Field Standardization**: Ensures consistent naming and formatting
- **Missing Field Tracking**: Records fields that need manual input

### Modes of Operation
- **Standard Mode**: Process and update existing records
- **Check Mode**: Validate without making changes
- **Create Mode**: Create new entities
- **Manual Fill Mode**: Fill missing fields with provided data
- **Test Mode**: Show what would be done without making changes

## Installation

### Prerequisites
- Python 3.6+
- PostgreSQL database access
- Required Python packages (see `requirements.txt`)

### Dependencies
```bash
pip install -r requirements.txt
```

Required packages:
- `psycopg2` - PostgreSQL database adapter
- `requests` - HTTP requests for URL validation
- Standard library modules: `json`, `csv`, `argparse`, `logging`, `datetime`

## Configuration

### Database Connection
Edit the `PG_CONNECTION` variable in the script:
```python
PG_CONNECTION = 'host=localhost port=5432 dbname=gisdev user=postgres password=galactic529'
```

### Configuration Variables
Set these at the top of the script:
```python
optional_conditions = True      # Enable optional field validation
generate_CSV = True            # Generate CSV reports
debug = False                  # Enable debug logging
test_mode = False              # Enable test mode
```

### Layer Mappings
The script includes predefined mappings for:
- **Layer Groups**: Maps layers to group names (e.g., 'zoning' → 'flu_zoning')
- **Categories**: Maps layers to categories (e.g., 'zoning' → '08_Land_Use_and_Zoning')
- **Transform Tables**: Maps layers to transform table names
- **Temp Table Prefixes**: Maps layers to temp table naming conventions

## Usage

### Basic Usage

```bash
# Process all entities for a layer
python3 layer_standardize_database.py zoning all

# Process specific entity
python3 layer_standardize_database.py zoning alachua gainesville

# Process all layers
python3 layer_standardize_database.py all
```

### Check Mode (Validation Only)

```bash
# Check all entities for a layer without making changes
python3 layer_standardize_database.py --check zoning

# Check all layers
python3 layer_standardize_database.py --check all
```

### Create Mode (New Entities)

```bash
# Create new entity with manual info
python3 layer_standardize_database.py --create zoning new_county new_city --manual-file manual_data.json

# Create new entity without manual info
python3 layer_standardize_database.py --create zoning new_county new_city
```

### Manual Fill Mode

```bash
# Fill missing fields using default file
python3 layer_standardize_database.py --manual-fill

# Fill missing fields using specific file
python3 layer_standardize_database.py --manual-fill --manual-file custom_missing_fields.json
```

### Test Mode

```bash
# Test mode with any operation
python3 layer_standardize_database.py --test-mode zoning all
python3 layer_standardize_database.py --test-mode --check zoning
python3 layer_standardize_database.py --test-mode --create zoning new_county new_city
```

## File Formats

### Missing Fields JSON
The script generates a JSON file with missing fields:
```json
{
  "zoning_alachua_gainesville": {
    "src_url_file": "MISSING",
    "fields_obj_transform": "MISSING"
  },
  "flu_duval_jacksonville": {
    "source_org": "MISSING"
  }
}
```

### Manual Fill JSON
Format for manual fill data:
```json
{
  "zoning_alachua_gainesville": {
    "src_url_file": "https://example.com/zoning_data.zip",
    "fields_obj_transform": "field1: value1, field2: value2",
    "source_org": "Alachua County GIS"
  }
}
```

### CSV Report
Check mode generates CSV reports with columns for:
- Entity information (layer, county, city, type, download type)
- Catalog table fields (title, county, city, format, etc.)
- Transform table fields (county, city_name, temp_table_name, etc.)
- Summary row with totals

## Database Schema

### m_gis_data_catalog_main Table
Key fields that get updated:
- `title`: Formatted title (e.g., "Zoning - City of Gainesville")
- `county`: County name in uppercase
- `city`: City name in uppercase
- `format`: Auto-detected format (SHP, ZIP, AGS, etc.)
- `download`: Set to "AUTO"
- `layer_group`: Mapped from layer name
- `category`: Mapped from layer name
- `sys_raw_folder`: Generated path
- `table_name`: Generated table name
- `src_url_file`: Source URL (manual input required)
- `fields_obj_transform`: Field mapping (manual input required)

### Transform Tables (zoning_transform, flu_transform)
Key fields that get updated:
- `county`: County name in uppercase
- `city_name`: City name in uppercase
- `temp_table_name`: Generated temp table name

## Field Standardization Rules

### Title Formatting
- **Cities**: `"{Layer} - City of {City}"`
- **Unincorporated**: `"{Layer} - {County} Unincorporated"`
- **Unified**: `"{Layer} - {County} Unified"`

### Table Name Formatting
- **Cities**: `"{layer}_{city}"`
- **Unincorporated/Unified**: `"{layer}_{county}_{entity_type}"`

### Temp Table Name Formatting
- **Zoning**: `"raw_zon_{county}_{city}"`
- **FLU**: `"raw_flu_{county}_{city}"`

### System Raw Folder Formatting
```
/srv/datascrub/{layer_group}/{layer}/florida/county/{county}/current/source_data/{city}
```

## Error Handling

### Common Issues
1. **Database Connection**: Check connection string and credentials
2. **Missing Manifest**: Ensure `layer_manifest.json` exists and is valid
3. **Permission Errors**: Ensure write access for CSV and JSON files
4. **Duplicate Records**: Script reports duplicates but doesn't modify them

### Logging
The script uses Python's logging module with configurable levels:
- **INFO**: Normal operation messages
- **WARNING**: Non-critical issues (missing records, duplicates)
- **ERROR**: Critical issues (database errors, file errors)
- **DEBUG**: Detailed operation information (when debug=True)

## Testing

Run the test script to verify functionality:
```bash
python3 test_layer_standardize.py
```

The test script validates:
- Manifest loading and parsing
- Format detection
- Field formatting functions
- Entity parsing

## Examples

### Example 1: Standardize All Zoning Entities
```bash
python3 layer_standardize_database.py zoning all
```
This will:
- Process all zoning entities in the manifest
- Update catalog and transform records
- Generate missing fields report
- Print operation summary

### Example 2: Check FLU Layer for Issues
```bash
python3 layer_standardize_database.py --check flu
```
This will:
- Validate all FLU entities
- Generate CSV report
- Identify missing fields and orphaned records
- Not make any database changes

### Example 3: Create New Entity
```bash
python3 layer_standardize_database.py --create zoning new_county new_city --manual-file new_entity.json
```
This will:
- Create catalog record
- Create transform record
- Use manual data for required fields

### Example 4: Fill Missing Fields
```bash
# First, run standard mode to identify missing fields
python3 layer_standardize_database.py zoning all

# Edit the generated missing_fields_YYYY-MM-DD.json file
# Then run manual fill mode
python3 layer_standardize_database.py --manual-fill
```

## Troubleshooting

### Database Connection Issues
- Verify PostgreSQL is running
- Check connection string format
- Ensure user has appropriate permissions

### Manifest Issues
- Validate JSON syntax
- Check entity naming conventions
- Verify command structure

### Permission Issues
- Ensure write access to current directory
- Check file permissions for CSV/JSON output

### Missing Dependencies
- Install required packages: `pip install -r requirements.txt`
- Check Python version compatibility

## Contributing

When modifying the script:
1. Update tests in `test_layer_standardize.py`
2. Document new features in this README
3. Follow existing code style and patterns
4. Test with both zoning and FLU layers

## Support

For issues or questions:
1. Check the logs for error messages
2. Run in test mode to see what would be done
3. Use check mode to validate current state
4. Review the generated reports for insights 