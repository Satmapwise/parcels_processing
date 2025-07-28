# layers_scrape.py

## Overview

`layers_scrape.py` is a geospatial data processing pipeline that implements a clean 4-stage architecture for downloading, extracting metadata from, processing, and uploading GIS data layers. The script operates on a data-driven design, dynamically generating commands based on information stored in the PostgreSQL `m_gis_data_catalog_main` table.

## Architecture

The script follows a 4-stage pipeline:

1. **`layer_download`** - Downloads data from AGS servers or direct URLs
2. **`layer_metadata`** - Extracts spatial metadata (EPSG, data_date, field_names)  
3. **`layer_processing`** - Runs pre-processing and layer-specific update scripts
4. **`layer_upload`** - Updates the data catalog with extracted metadata

## Key Features

- **Data-Driven Command Generation**: Commands are built dynamically from database catalog entries
- **Dual NND Detection**: 
  - Download-based: Server returns "no new data" 
  - Metadata-based: Data date unchanged from previous run
- **Living CSV Document**: Persistent CSV files with real-time status updates
- **Configurable Pipeline**: Individual stages can be enabled/disabled
- **Isolated Logging**: Optional entity-specific log files

## Database Dependencies

The script requires specific columns and values in the `m_gis_data_catalog_main` table. Use `layer_standardize_database.py` to ensure proper formatting.

### Required Database Columns

#### **Identification & Location**
- **`layer_subgroup`** (STRING, REQUIRED): Layer name (e.g., 'zoning', 'flu')
- **`county`** (STRING, REQUIRED): County name (lowercase with spaces, e.g., 'hillsborough')  
- **`city`** (STRING, REQUIRED): City name (title case, e.g., 'Plant City', 'Unincorporated')

#### **Download Configuration**
- **`format`** (STRING, REQUIRED): Download type
  - **AGS formats**: `'ags'`, `'arcgis'`, `'esri'`, `'ags_extract'`
  - **Direct download**: `'zip'`, `'url'`, or other non-AGS values
- **`resource`** (STRING, CONDITIONAL): Required for direct downloads
  - URL path or resource identifier (e.g., `/data/zoning/hillsborough/tampa`)
- **`src_url_file`** (STRING, FALLBACK): Alternative to `resource` 
- **`table_name`** (STRING, CONDITIONAL): Required for AGS downloads
  - ArcGIS table/layer name (e.g., `'zoning_plant_city'`)

#### **Processing Configuration**  
- **`processing_comments`** (STRING, OPTIONAL): Pre-processing commands
  - Commands separated by `|` (e.g., `'command1.py|command2.sh'`)
  - Executed before layer-specific update script

#### **Metadata Tracking** (Updated by script)
- **`data_date`** (DATE): Date of source data
- **`publish_date`** (DATE): Processing date  
- **`srs_epsg`** (STRING): Spatial reference system (e.g., `'EPSG:4326'`)
- **`sys_raw_file`** (STRING): Primary shapefile name
- **`sys_raw_file_zip`** (STRING): ZIP file name (non-AGS only)
- **`field_names`** (STRING): Comma-separated field list

### Database Record Requirements by Format

#### **AGS/ArcGIS Downloads** (`format` = 'ags', 'arcgis', 'esri', 'ags_extract')
```sql
-- Required fields
layer_subgroup = 'zoning'                    -- Layer name
county = 'hillsborough'                      -- County (lowercase, spaces)  
city = 'Plant City'                          -- City (title case)
format = 'ags'                               -- Download type
table_name = 'zoning_plant_city'             -- ArcGIS table name

-- Optional fields  
processing_comments = 'command1.py|command2.sh'  -- Pre-processing commands

-- Updated by script
data_date = '2025-07-28'                     -- Extracted from shapefile
srs_epsg = 'EPSG:4326'                       -- Spatial reference
sys_raw_file = 'zoning_plant_city.shp'      -- Shapefile name
field_names = 'FID,PCZONING,Shape_Leng,...' -- Field list
```

#### **Direct Downloads** (`format` != AGS formats)
```sql
-- Required fields
layer_subgroup = 'zoning'                    -- Layer name
county = 'hillsborough'                      -- County (lowercase, spaces)
city = 'Tampa'                               -- City (title case)  
format = 'zip'                               -- Download type
resource = '/data/zoning/hillsborough/tampa' -- URL/path

-- Optional fields
processing_comments = 'preprocess.py'        -- Pre-processing commands

-- Updated by script
data_date = '2024-03-25'                     -- Extracted from shapefile  
srs_epsg = 'EPSG:4326'                       -- Spatial reference
sys_raw_file = 'Zoning_District.shp'        -- Shapefile name
sys_raw_file_zip = 'Zoning_District.zip'    -- ZIP file name
field_names = 'OBJECTID,ZONECLASS,ZONEDESC,,...' -- Field list
```

## Command Generation Logic

### Download Commands

**AGS Downloads**:
```bash
python3 /srv/tools/python/layers_scraping/download_tools/ags_extract_data2.py {table_name} delete 15
```

**Direct Downloads**:  
```bash
python3 /srv/tools/python/layers_scraping/download_tools/download_data.py {resource}
```

### Processing Commands

**Pre-processing** (from `processing_comments`):
```bash
# Commands split by '|' and executed in order
python3 command1.py
python3 command2.sh  
```

**Layer-specific Updates**:
```bash
# For zoning layer
python3 /srv/tools/python/layers_scraping/update_zoning2.py {layer} {entity}

# For other layers  
python3 /srv/tools/python/layers_scraping/update_{layer}.py {layer} {entity}
```

### Upload Commands

**AGS entities**:
```sql
UPDATE m_gis_data_catalog_main SET 
    data_date = '{data_date}', 
    publish_date = '{publish_date}',
    srs_epsg = '{epsg}',
    sys_raw_file = '{shp}',
    field_names = '{field_names}'
WHERE layer_subgroup = '{layer}' AND county = '{county}' AND city = '{city}';
```

**Direct download entities**:
```sql  
UPDATE m_gis_data_catalog_main SET 
    data_date = '{data_date}',
    publish_date = '{publish_date}', 
    srs_epsg = '{epsg}',
    sys_raw_file = '{shp}',
    sys_raw_file_zip = '{raw_zip}',
    field_names = '{field_names}'
WHERE layer_subgroup = '{layer}' AND county = '{county}' AND city = '{city}';
```

## Usage

### Basic Usage
```bash
python3 layers_scrape.py <layer> [entities...]
```

### Examples
```bash
# Process all zoning entities
python3 layers_scrape.py zoning

# Process specific entities  
python3 layers_scrape.py zoning hillsborough_tampa hillsborough_plant_city

# Process with wildcard
python3 layers_scrape.py zoning hillsborough_*

# Debug mode with console output
python3 layers_scrape.py zoning hillsborough_* --debug --no-log-isolation

# Skip specific stages
python3 layers_scrape.py flu orange_* --no-download --no-processing
```

### Command-Line Options

| Option | Description |
|--------|-------------|
| `--test-mode` | Skip actual command execution |
| `--debug` | Enable debug logging and granular output |
| `--no-log-isolation` | Show all logs in console |
| `--no-download` | Skip download phase |
| `--no-metadata` | Skip metadata extraction |  
| `--no-processing` | Skip processing phase |
| `--no-upload` | Skip database upload |
| `--no-summary` | Skip CSV summary generation |
| `--process-anyway` | Continue processing despite NND |

## Output Files

### CSV Summary Files
- **Filename**: `{layer}_summary.csv` (e.g., `zoning_summary.csv`)
- **Type**: Living document (persistent, incrementally updated)
- **Columns**: `county`, `city`, `data_date`, `download_status`, `processing_status`, `upload_status`, `error_message`, `timestamp`
- **Status Values**: `SUCCESS`, `FAILED`, `NND` (No New Data), or empty
- **Organization**: Alphabetical by county, then city

### Log Files (when `isolate_logs=True`)
- **Location**: `{work_dir}/{entity}_processing.log`
- **Content**: Entity-specific processing details

## NND (No New Data) Detection

### Download-based NND
- **Trigger**: Download tool exits with code 1
- **Error Message**: `"Download command: no new data"`
- **Status**: `download_status = 'NND'`, other statuses cleared

### Metadata-based NND  
- **Trigger**: Extracted `data_date` matches existing CSV `data_date`
- **Error Message**: `"Metadata check: data date unchanged"`
- **Status**: `download_status = 'NND'`, other statuses cleared

## Constants and Configuration

### Work Directory Patterns
```python
WORK_DIR_PATTERNS = {
    'zoning': '/srv/datascrub/08_Land_Use_and_Zoning/zoning/florida/county/{county}/current/source_data/{city}',
    'flu': '/srv/datascrub/08_Land_Use_and_Zoning/future_land_use/florida/county/{county}/current/source_data/{city}',
}
```

### Database Connection
```python
PG_CONNECTION = "host=gisdb.manatee.org port=5433 dbname=gis user=smay sslmode=require"
```

### Skip Entities
```python  
SKIP_ENTITIES = ['hillsborough_temple_terrace']  # Blacklisted entities
```

## Integration with layer_standardize_database.py

Before running `layers_scrape.py`, ensure your database catalog is properly formatted:

```bash
# Standardize database for specific county
python3 layer_standardize_database.py hillsborough

# Standardize entire database  
python3 layer_standardize_database.py --all
```

This ensures:
- Column names and data types are correct
- Required fields are populated  
- Format values are standardized
- Naming conventions are consistent

## Error Handling

The script implements a comprehensive exception hierarchy:

- **`LayerProcessingError`**: Base exception class
- **`DownloadError`**: Download failures  
- **`ProcessingError`**: Processing script failures
- **`UploadError`**: Database update failures
- **`SkipEntityError`**: NND conditions (not actual errors)

Failed entities are logged but don't stop processing of other entities.