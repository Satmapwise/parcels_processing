# Florida GIS Layers Processing Pipeline

A comprehensive geospatial data processing system for downloading, standardizing, and processing Florida GIS layers with state-aware architecture and robust data validation.

## 🏗️ System Architecture

This system consists of three core components working together:

- **`layers_helpers.py`** - Shared utilities, constants, and entity parsing logic
- **`layers_scrape.py`** - Production data processing pipeline (4-stage architecture)  
- **`layers_prescrape.py`** - Database preparation and quality assurance
- **`layer_standardize_database.py`** - Legacy database standardization (being phased out)

## 📁 Project Structure

```
layers_scraping/
├── layers_helpers.py           # Shared utilities and constants
├── layers_scrape.py            # Main processing pipeline 
├── layers_prescrape.py         # Database preparation tool
├── layer_standardize_database.py  # Legacy standardization
├── requirements.txt            # Python dependencies
├── download_tools/             # Download utilities
├── processing_tools/           # Processing scripts  
├── test/                       # Layer manifests and test data
├── reports/                    # Generated CSV reports
└── documentation/              # Additional documentation
```

---

## 🤝 Shared Foundation (`layers_helpers.py`)

### Purpose
Central hub for all shared logic, ensuring consistency across scripts and eliminating code duplication.

### Key Components

#### **🌍 Geographic Data**
- **`FL_COUNTIES`** - Set of all 67 Florida counties in internal format
- **`VALID_STATES`** - State abbreviation mappings (`'fl': 'FL'`, etc.)

#### **🎯 Layer Configuration**  
- **`LAYER_CONFIGS`** - Metadata for all 10 layers (category, level, special entities)
- **`LAYERS`** - Set of available layer names
- **`FULL_PIPELINE_FORMATS`** / **`METADATA_ONLY_FORMATS`** - Processing rules by file format

#### **🔧 Core Utilities**
- **`format_name(name, type, external=bool)`** - Bidirectional name formatting
  - Internal: `"miami_dade"`, `"flu"` (code-friendly)
  - External: `"Miami-Dade"`, `"Future Land Use"` (human-readable)
- **`parse_entity_pattern(pattern)`** - Robust entity parsing with multi-part name support
  - Handles: `"zoning_fl_palm_beach_west_palm_beach"` → `('zoning', 'fl', 'palm_beach', 'west_palm_beach')`
- **`validate_state_abbreviation()`** - State validation and normalization
- **`safe_catalog_val()`** - Database null value handling

---

## 🏭 Production Pipeline (`layers_scrape.py`)

### Purpose
Main production script that processes geospatial data through a clean 4-stage pipeline with intelligent error handling and status tracking.

### 🔄 4-Stage Pipeline

#### **Stage 1: Download** (`layer_download`)
- **AGS/ArcGIS**: Extract from ArcGIS servers using table names
- **Direct URLs**: Download ZIP files or direct resources
- **NND Detection**: Detects "no new data" from server responses
- **HTML Content Validation**: Detects PDF viewer pages masquerading as files
- **AGS Validation**: Validates GeoJSON files for empty/corrupt content

#### **Stage 2: Metadata** (`layer_metadata`)  
- **Spatial Metadata**: Extracts EPSG, field names, geometry type
- **Data Date Detection**: Reads modification dates from shapefiles
- **Conservative PDF Extraction**: Smart date extraction for PDF files
- **NND Check**: Compares data dates to prevent unnecessary processing

#### **Stage 3: Processing** (`layer_processing`)
- **Dynamic Scripts**: Generates update commands based on layer name
- **Pre-processing**: Runs optional preprocessing commands from database
- **Warning-Only Commands**: Commands prefixed with `WARNING:` continue on failure
- **Format Control**: Skips processing for metadata-only formats (PDF)

#### **Stage 4: Upload** (`layer_upload`)
- **Dynamic SQL**: Builds UPDATE queries based on available metadata
- **Batch Updates**: Updates database catalog with extracted information

### 🎛️ Features

#### **Smart Entity Management**
```bash
# New flexible pattern-based filtering
python3 layers_scrape.py --include "zoning_fl_*"           # All FL zoning
python3 layers_scrape.py --include "*_miami_dade_*"        # All Miami-Dade layers  
python3 layers_scrape.py --exclude "*_temple_terrace"      # Skip problem entities
```

#### **Pipeline Control**
```bash
# Skip specific stages
python3 layers_scrape.py --include "flu_*" --no-download --no-processing

# Test mode (simulation)
python3 layers_scrape.py --include "streets_fl_alachua" --test

# Debug with full console output
python3 layers_scrape.py --include "zoning_*" --debug --no-log-isolation

# Force processing even when no new data detected
python3 layers_scrape.py --include "zoning_*" --process-anyway
```

#### **Advanced Command Processing**
- **Source Comments**: Pre-metadata commands from database with `WARNING:` support
- **Processing Comments**: Post-metadata commands with multiple format support
  - Bracketed: `[command1] [command2] [command3]`
  - JSON array: `["command1", "command2", "command3"]`
  - Legacy: newlines/semicolons
- **Warning-Only Commands**: Commands starting with `WARNING:` generate warnings but don't fail the entity

#### **State-Aware Processing**
- **Multi-State Support**: Ready for Florida, Georgia, Delaware expansion
- **State Inference**: Automatically determines state from county names
- **Directory Paths**: Generates state-specific work directories

### 📊 Output & Monitoring

#### **Living CSV Documents**
- **Persistent Status**: `{layer}_summary.csv` files track processing history
- **Real-time Updates**: Status updated after each pipeline stage
- **Status Values**: `SUCCESS`, `FAILED`, `NND` (No New Data), `SKIPPED`

#### **Error Handling**
- **Graceful Degradation**: Failed entities don't stop processing
- **Detailed Logging**: Entity-specific logs with isolation options  
- **Exception Hierarchy**: Custom exceptions for different failure types
- **HTML Content Detection**: Prevents PDF viewer pages from being processed as data files

---

## 📋 Database Preparation (`layers_prescrape.py`)

### Purpose
Quality assurance and database preparation tool that ensures clean, validated metadata before production processing.

### 🔍 Operation Modes

#### **DETECT Mode**
```bash
# Find all issues across layers
python3 layers_prescrape.py --detect

# Focus on specific entities  
python3 layers_prescrape.py --include "zoning_fl_alachua_*" --detect
```
- **Duplicate Detection**: Groups records by entity to find duplicates
- **Missing Fields**: Identifies incomplete database records
- **CSV Reports**: Detailed field-by-field analysis with completion statistics

#### **FILL Mode**  
```bash
# Auto-generate missing fields
python3 layers_prescrape.py --include "streets_*" --fill

# Health check with field validation
python3 layers_prescrape.py --fill --all-layers
```
- **Auto-Generation**: Derives `sys_raw_folder`, `table_name`, titles from entity patterns
- **URL Validation**: Batch validates source URLs with caching and concurrent processing
- **Field Health**: Validates 19 critical database fields
- **Manifest Integration**: Extracts commands from legacy manifest files

#### **CREATE Mode**
```bash
# Create a new record using comma-separated input (no positional args)
# Format depends on layer level:
#   national:            layer
#   state:               layer, state
#   state_county:        layer, state, county
#   state_county_city:   layer, state, county, city

# Examples:
python3 layers_prescrape.py --create "zoning, fl, alachua, gainesville" --debug
python3 layers_prescrape.py --create "parcel_geo, fl, alachua" --debug
python3 layers_prescrape.py --create "traffic_counts, fl" --debug
python3 layers_prescrape.py --create "fema_flood" --debug
```
- **New Records**: Inserts into `m_gis_data_catalog_main` with standardized values (`title`, `layer_group`, `category`, `layer_subgroup`, `sys_raw_folder`, `table_name`, `download='AUTO'`).
- **Manual Inputs (Prompted)**: `format`, `download_method`, `src_url_file`, and `fields_obj_transform` are prompted; pressing Enter leaves them null. `resource` is auto-generated only when `download_method` is `WGET`.
- **Auto-Generated (Same as FILL)**: Suggests `new_title` and applies to `title`; computes `sys_raw_folder`, `table_name`, `layer_group`, and `category` from config.
- **Input Parts by Level**: The required number of comma-separated parts is inferred from the layer’s `level` in `LAYER_CONFIGS`.

#### **Combined Mode**
```bash
# Run detect then fill in sequence
python3 layers_prescrape.py --detect --fill --include "zoning_*"
```

### 🎯 Advanced Features

#### **URL Validation Engine**
- **Concurrent Processing**: Uses ThreadPoolExecutor for batch URL validation
- **Caching**: Validates URLs once and caches results for performance
- **ArcGIS Service Detection**: Specialized validation for ArcGIS REST services
- **Deprecated URL Detection**: Identifies URLs that require authentication or are no longer accessible
- **Status Codes**: `OK`, `MISSING`, `DEPRECATED` with detailed reasoning

#### **Title Parsing Engine**
- **Complex Extraction**: Parses layer/county/city from database titles
- **Special Handling**: Hardcoded logic for state/national layers (`fdot_tc`, `sunbiz`, `flood_zones`)
- **Fallback Logic**: Multiple parsing strategies for edge cases

#### **Entity Generation**
- **Format Standards**: Enforces `layer_state_county_city` format
- **State Inference**: Determines state from county when missing
- **Multi-Part Names**: Handles `palm_beach`, `west_palm_beach`, etc.

#### **Data Quality Reporting**
- **Field Statistics**: Completion rates across all database fields
- **Issue Tracking**: Categorizes and counts different types of problems
- **CSV Export**: Machine-readable reports for analysis

#### **Manifest Integration**
- **Legacy Support**: Extracts preprocessing commands from old manifest files
- **Command Phasing**: Separates source_comments (pre-metadata) from processing_comments (post-metadata)
- **Format Conversion**: Converts between new entity format and legacy manifest format

---

## 🗃️ Database Schema & Requirements

### Core Entity Standard
All entities follow `layer_state_county_city` format with flexible components:
- **National**: `flood_zones` (1 part)
- **State**: `fdot_tc_fl` (2 parts)  
- **County**: `streets_fl_alachua` (3 parts)
- **City**: `zoning_fl_alachua_gainesville` (4 parts)

### Required Database Columns (`m_gis_data_catalog_main`)

#### **🆔 Entity Identification**
- **`layer_subgroup`** (REQUIRED) - Layer name: `'zoning'`, `'flu'`, `'streets'`, etc.
- **`state`** (REQUIRED) - State abbreviation: `'FL'`, `'GA'`, `'DE'`
- **`county`** (REQUIRED) - County name: `'Alachua'`, `'Miami-Dade'`, `'St. Johns'`
- **`city`** (CONDITIONAL) - City name: `'Gainesville'`, `'Unincorporated'`, `'Unified'`

#### **📥 Download Configuration**
- **`format`** (REQUIRED) - Download type: `'ags'`, `'zip'`, `'url'`, `'pdf'`
- **`src_url_file`** (REQUIRED) - Source URL or ArcGIS service endpoint
- **`table_name`** (AGS only) - ArcGIS layer/table name
- **`resource`** (Direct downloads) - Resource path or identifier

#### **⚙️ Processing Configuration**
- **`processing_comments`** (OPTIONAL) - Post-metadata processing commands
  - Bracketed format: `[command1] [command2] [command3]`
  - JSON array: `["command1", "command2", "command3"]`
  - Legacy: newlines/semicolons
- **`source_comments`** (OPTIONAL) - Pre-metadata processing commands
  - Same format support as processing_comments
  - Commands prefixed with `WARNING:` continue on failure
- **`fields_obj_transform`** (OPTIONAL) - Field mapping transformations

#### **📊 Metadata (Auto-populated)**
- **`data_date`** - Source data modification date
- **`publish_date`** - Processing completion date  
- **`srs_epsg`** - Spatial reference system (e.g., `'EPSG:4326'`)
- **`sys_raw_file`** - Primary shapefile name
- **`field_names`** - Comma-separated field list

### Format-Specific Requirements

#### **AGS/ArcGIS Downloads** (`format` = 'ags', 'arcgis', 'esri')
```sql
-- Required
layer_subgroup = 'zoning'           -- Layer name
state = 'FL'                        -- State abbreviation
county = 'Hillsborough'             -- County name
city = 'Plant City'                 -- City name
format = 'ags'                      -- Download type
table_name = 'zoning_plant_city'    -- ArcGIS table

-- Optional  
processing_comments = '[prep1.py] [prep2.sh]'
source_comments = '[WARNING:cleanup.sh] [validate.py]'
```

#### **Direct Downloads** (`format` != AGS)
```sql
-- Required
layer_subgroup = 'zoning'           -- Layer name
state = 'FL'                        -- State abbreviation
county = 'Hillsborough'             -- County name
city = 'Tampa'                      -- City name
format = 'zip'                      -- Download type
src_url_file = 'https://...'        -- Source URL

-- Optional
resource = '/data/zoning/tampa'     -- Resource identifier
```

---

## 🚀 Installation & Setup

### Prerequisites
- **Python 3.8+**
- **PostgreSQL** database with `m_gis_data_catalog_main` table
- **System Dependencies**: GDAL/OGR for spatial data processing

### Installation
```bash
# Clone repository
git clone <repository-url>
cd layers_scraping

# Install Python dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your database connection details
```

### Database Connection
Create `.env` file with:
```bash
PG_CONNECTION="host=your-host port=5432 dbname=gis user=username password=password sslmode=require"
```

---

## 📖 Usage Examples

### 🎯 Complete Workflow

#### 1. Database Preparation
```bash
# Detect issues across all layers
python3 layers_prescrape.py --detect

# Fix issues for specific layers
python3 layers_prescrape.py --include "zoning_fl_*" --fill

# Combined detect and fill
python3 layers_prescrape.py --detect --fill --include "streets_*"
```

#### 2. Production Processing  
```bash
# Process all zoning entities
python3 layers_scrape.py --include "zoning_*"

# Process specific county
python3 layers_scrape.py --include "*_fl_alachua_*"

# Test mode first
python3 layers_scrape.py --include "flu_fl_orange_*" --test

# Force processing even with no new data
python3 layers_scrape.py --include "zoning_fl_*" --process-anyway
```

#### 3. Monitoring & Analysis
```bash
# Check processing status
cat zoning_summary.csv | grep -E "(FAILED|NND)"

# Review detected issues
head -20 reports/zoning_prescrape_detect.csv
```

### 🎛️ Advanced Usage

#### **Multi-Layer Processing**
```bash
# Process multiple layers with different patterns
python3 layers_scrape.py --include "zoning_fl_*" "flu_fl_*" --exclude "*_temple_terrace"
```

#### **Staged Pipeline Control**
```bash
# Download only (for bulk downloads)
python3 layers_scrape.py --include "*_fl_*" --no-metadata --no-processing --no-upload

# Processing only (skip download)
python3 layers_scrape.py --include "streets_fl_alachua_*" --no-download
```

#### **State Expansion**
```bash
# Future Georgia support
python3 layers_scrape.py --include "*_ga_*"

# Multi-state processing
python3 layers_scrape.py --include "zoning_fl_*" "zoning_ga_*"
```

#### **Advanced Command Processing**
```bash
# Commands with warning-only support
# In database: source_comments = "[WARNING:cleanup.sh] [validate.py]"
# WARNING: commands continue processing even if they fail

# Multiple command formats supported
# Bracketed: [cmd1] [cmd2] [cmd3]
# JSON: ["cmd1", "cmd2", "cmd3"]  
# Legacy: cmd1|cmd2|cmd3
```

---

## 🛠️ Dependencies & Tools

### Core Processing Scripts
- **`update_zoning2.py`** - Zoning layer processing logic
- **`update_flu.py`** - Future Land Use processing
- **`update_streets.py`** - Streets layer processing
- **Additional `update_*.py`** - Layer-specific processing scripts

### Download Tools (`download_tools/`)
- **`ags_extract_data2.py`** - ArcGIS Server data extraction
- **`download_data.py`** - Direct URL downloads with NND detection

### Processing Tools (`processing_tools/`)
- **Layer-specific preprocessing scripts**
- **Data transformation utilities**
- **Quality assurance tools**

### Test Data (`test/`)
- **`layer_manifest.json`** - Layer configuration manifest
- **Sample data files for testing**

---

## 📊 Monitoring & Troubleshooting

### Status Tracking
- **CSV Summaries**: `{layer}_summary.csv` files show processing history
- **JSON Reports**: `missing_fields.json` tracks required manual input
- **Log Files**: Entity-specific processing logs when enabled

### Common Issues

#### **Database Connection**
```bash
# Test connection
python3 -c "from layers_helpers import PG_CONNECTION; print('DB OK' if PG_CONNECTION else 'Check .env')"
```

#### **Entity Parsing**
```bash
# Test entity parsing
python3 -c "from layers_helpers import parse_entity_pattern; print(parse_entity_pattern('zoning_fl_palm_beach_west_palm_beach'))"
```

#### **Format Detection**
```bash
# Check format categories
python3 -c "from layers_helpers import FULL_PIPELINE_FORMATS, METADATA_ONLY_FORMATS; print(f'Full: {FULL_PIPELINE_FORMATS}'); print(f'Metadata: {METADATA_ONLY_FORMATS}')"
```

### Error Types
- **NND (No New Data)**: Normal - server indicates no updates available
- **FAILED**: Actual errors requiring investigation
- **SKIPPED**: Intentionally excluded (format rules, blacklist)

### Advanced Error Handling
- **HTML Content Detection**: Prevents PDF viewer pages from being processed as data files
- **AGS Validation**: Checks GeoJSON files for empty/corrupt content
- **Conservative PDF Extraction**: Smart date extraction that avoids fake freshness
- **URL Validation**: Batch validation with caching for performance

---

## 🔮 Future Enhancements

### Multi-State Expansion
- **Georgia**: Add GA counties to `layers_helpers.py`
- **Delaware**: Add DE counties and state-specific processing rules
- **Generalized Logic**: Abstract state-specific logic for easy expansion

### Enhanced Processing
- **Format Support**: Additional file formats beyond AGS/SHP/PDF
- **Parallel Processing**: Multi-threading for batch operations
- **Cloud Integration**: S3/cloud storage support

### Quality Assurance
- **Automated Testing**: Unit tests for entity parsing and formatting
- **Data Validation**: Enhanced field validation and business rules
- **Performance Monitoring**: Processing time tracking and optimization

---

## 📚 Additional Documentation

- **Layer Manifests**: See `test/layer_manifest.json` for entity definitions
- **Processing Scripts**: Individual `update_*.py` files have layer-specific documentation
- **Database Schema**: Full schema documentation in `documentation/` directory
- **API Reference**: Function-level documentation in source code

---

## 🤝 Contributing

1. **Follow Standards**: Use existing entity format (`layer_state_county_city`)
2. **Update Tests**: Add test cases for new functionality
3. **Documentation**: Update this README for new features
4. **Shared Logic**: Add reusable code to `layers_helpers.py`

---

## 📞 Support

For issues or questions:
1. **Check Logs**: Review entity-specific log files and CSV status
2. **Test Mode**: Run with `--test` to see what would happen
3. **Database Validation**: Use `layers_prescrape.py --detect` to find issues
4. **Entity Debugging**: Test entity parsing with `layers_helpers.py` functions

The system is designed for **reliability**, **consistency**, and **ease of maintenance** across Florida's 67 counties and 400+ municipalities! 🎯