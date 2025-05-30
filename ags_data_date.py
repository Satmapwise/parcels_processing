# Pre-check: Ensure all required modules are available
required_modules = [
    "requests", 
    "logging", 
    "argparse", 
    "datetime", 
    "re", 
    "json", 
    "io", 
    "xml.etree.ElementTree"
]

missing_modules = []
for module in required_modules:
    try:
        __import__(module)
    except ImportError:
        missing_modules.append(module)

if missing_modules:
    print("ERROR: Missing required Python modules!")
    print("The following modules need to be installed:")
    for module in missing_modules:
        print(f"  - {module}")
    print("\nTo install missing modules, run:")
    print("  pip install " + " ".join(missing_modules))
    exit(1)

import requests
import datetime
import logging
import xml.etree.ElementTree as ET
import io
import argparse
from typing import Optional, Dict, List, Tuple
from urllib.parse import urljoin

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DateFinding:
    """Represents a date found by one of the methods"""
    def __init__(self, method: str, source: str, raw_value: str, converted_date: Optional[str] = None, 
                 reliable: bool = False, notes: str = ""):
        self.method = method
        self.source = source  # field name, tag name, etc.
        self.raw_value = raw_value
        self.converted_date = converted_date
        self.reliable = reliable
        self.notes = notes
    
    def __str__(self):
        status = "✓ RELIABLE" if self.reliable else "⚠ IGNORED"
        if self.converted_date and self.converted_date != self.raw_value:
            return f"{self.method} | {self.source}: '{self.raw_value}' → '{self.converted_date}' | {status}"
        else:
            return f"{self.method} | {self.source}: '{self.raw_value}' | {status}"

def get_most_reliable_date(findings: List[DateFinding]) -> Optional[DateFinding]:
    """Determine the most reliable date from all findings"""
    # Priority order for methods (most to least reliable)
    method_priority = {
        "Method 1": 1,  # Direct field queries
        "Method 2": 2,  # Editing info
        "Method 3": 3,  # Service metadata
        "Method 4": 4,  # ArcGIS.com item
        "Method 5": 5   # XML metadata
    }
    
    # Filter to only reliable dates
    reliable_findings = [f for f in findings if f.reliable]
    
    if not reliable_findings:
        return None
    
    # Sort by method priority (lower number = higher priority)
    reliable_findings.sort(key=lambda x: method_priority.get(x.method, 999))
    
    return reliable_findings[0]

def print_date_summary(findings: List[DateFinding], most_reliable: Optional[DateFinding]):
    """Print a comprehensive summary of all date findings"""
    print("\n" + "="*80)
    print("DATE DETECTION SUMMARY")
    print("="*80)
    
    if not findings:
        print("No dates found by any method.")
        return
    
    print(f"Total dates discovered: {len(findings)}")
    reliable_count = len([f for f in findings if f.reliable])
    print(f"Reliable dates: {reliable_count}")
    print(f"Ignored dates: {len(findings) - reliable_count}")
    
    print(f"\nALL DISCOVERED DATES:")
    print("-" * 60)
    for finding in findings:
        print(f"  {finding}")
        if finding.notes:
            print(f"    Notes: {finding.notes}")
    
    print(f"\nMOST RELIABLE DATE:")
    print("-" * 60)
    if most_reliable:
        print(f"  ★ {most_reliable.converted_date or most_reliable.raw_value}")
        print(f"    Source: {most_reliable.method} ({most_reliable.source})")
    else:
        print("  No reliable date could be determined")
    
    print("="*80)

def to_date_string(ms) -> Optional[str]:
    """Convert epoch milliseconds to ISO string"""
    try:
        if ms is None:
            return None
        return datetime.datetime.utcfromtimestamp(ms / 1000).isoformat()
    except (ValueError, TypeError, OSError) as e:
        logger.warning(f"Failed to convert timestamp {ms}: {e}")
        return None

def make_request(url: str, params: dict = None, timeout: int = 30) -> Optional[dict]:
    """Make HTTP request with proper error handling"""
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()  # Raises HTTPError for bad status codes
        return response.json()
    except requests.exceptions.Timeout:
        logger.error(f"Request timeout for URL: {url}")
        return None
    except requests.exceptions.ConnectionError:
        logger.error(f"Connection error for URL: {url}")
        return None
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error {e.response.status_code} for URL: {url}")
        return None
    except requests.exceptions.JSONDecodeError:
        logger.error(f"Invalid JSON response from URL: {url}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error for URL {url}: {e}")
        return None

def query_max_date_field(service_url: str, field: str) -> List[DateFinding]:
    """Query for the max value of a specific date field"""
    logger.info(f"Method 1: Querying max value for date field '{field}'")
    findings = []
    
    # Properly construct query URL
    query_url = urljoin(service_url.rstrip('/') + '/', 'query')
    logger.debug(f"Query URL: {query_url}")
    
    params = {
        "where": "1=1",
        "outFields": field,
        "orderByFields": f"{field} DESC",
        "resultRecordCount": 1,
        "f": "json",
        "returnGeometry": False
    }
    logger.debug(f"Query parameters: {params}")
    
    data = make_request(query_url, params=params)
    if not data:
        logger.warning(f"Method 1: Failed to get response from query endpoint")
        return findings
        
    try:
        features = data.get("features", [])
        logger.debug(f"Method 1: Received {len(features)} features in response")
        
        if features and len(features) > 0:
            attributes = features[0].get("attributes", {})
            raw_date = attributes.get(field)
            logger.debug(f"Method 1: Raw date value from field '{field}': {raw_date}")
            
            if raw_date is None:
                logger.warning(f"Method 1: Field '{field}' exists but contains null value")
                findings.append(DateFinding("Method 1", field, "null", None, False, "Field contains null value"))
            else:
                converted_date = to_date_string(raw_date)
                if converted_date:
                    logger.info(f"Method 1: Successfully converted date: {converted_date}")
                    findings.append(DateFinding("Method 1", field, str(raw_date), converted_date, True))
                else:
                    logger.warning(f"Method 1: Failed to convert raw date value: {raw_date}")
                    findings.append(DateFinding("Method 1", field, str(raw_date), None, False, "Failed to convert timestamp"))
        else:
            logger.warning(f"Method 1: No features returned in query response")
            
    except (KeyError, IndexError, TypeError) as e:
        logger.warning(f"Method 1: Error parsing query response for field {field}: {e}")
    
    return findings

def get_editing_info_date(layer_url: str) -> List[DateFinding]:
    """Get last edit date from editing info"""
    logger.info(f"Method 2: Checking editingInfo.lastEditDate from layer metadata")
    findings = []
    
    data = make_request(layer_url, params={"f": "json"})
    if not data:
        logger.warning(f"Method 2: Failed to get layer metadata")
        return findings
        
    try:
        editing_info = data.get("editingInfo", {})
        logger.debug(f"Method 2: editingInfo present: {bool(editing_info)}")
        
        if not editing_info:
            logger.warning(f"Method 2: No editingInfo found in layer metadata")
            return findings
            
        last_edit_date = editing_info.get("lastEditDate")
        logger.debug(f"Method 2: Raw lastEditDate value: {last_edit_date}")
        
        if last_edit_date is None:
            logger.warning(f"Method 2: editingInfo exists but lastEditDate is null")
            findings.append(DateFinding("Method 2", "lastEditDate", "null", None, False, "lastEditDate is null"))
        else:
            converted_date = to_date_string(last_edit_date)
            if converted_date:
                logger.info(f"Method 2: Successfully converted date: {converted_date}")
                findings.append(DateFinding("Method 2", "lastEditDate", str(last_edit_date), converted_date, True))
            else:
                logger.warning(f"Method 2: Failed to convert raw date value: {last_edit_date}")
                findings.append(DateFinding("Method 2", "lastEditDate", str(last_edit_date), None, False, "Failed to convert timestamp"))
                
    except (KeyError, TypeError) as e:
        logger.warning(f"Method 2: Error parsing editing info: {e}")
        
    return findings

def find_date_field(layer_url: str) -> List[str]:
    """Try to identify relevant date fields"""
    logger.info(f"Method 1: Searching for date fields in layer metadata")
    
    data = make_request(layer_url, params={"f": "json"})
    if not data:
        logger.warning(f"Method 1: Failed to get layer metadata for field discovery")
        return []
        
    # Extended list with case variations
    candidates = [
        "DT_CHG", "dt_chg", "Dt_Chg",
        "edit_date", "EDIT_DATE", "Edit_Date",
        "last_edited_date", "LAST_EDITED_DATE", "Last_Edited_Date",
        "MOD_DATE", "mod_date", "Mod_Date",
        "UPDATE_DATE", "update_date", "Update_Date",
        "modified_date", "MODIFIED_DATE", "Modified_Date",
        "created_date", "CREATED_DATE", "Created_Date"
    ]
    
    fields = data.get("fields", [])
    if not isinstance(fields, list):
        logger.warning("Method 1: Fields is not a list in layer metadata")
        return []
    
    logger.debug(f"Method 1: Found {len(fields)} total fields in layer")
    
    # Log all date fields found
    date_fields = []
    matching_fields = []
    
    for field in fields:
        if not isinstance(field, dict):
            continue
            
        field_name = field.get("name")
        field_type = field.get("type")
        
        if field_type == "esriFieldTypeDate":
            date_fields.append(field_name)
            
            # Check if it matches our candidate list
            if field_name in candidates:
                matching_fields.append(field_name)
                logger.info(f"Method 1: Found matching date field: {field_name}")
    
    logger.debug(f"Method 1: All date fields found: {date_fields}")
    
    if matching_fields:
        return matching_fields
    elif date_fields:
        logger.warning(f"Method 1: Found date fields but none match known patterns: {date_fields}")
        # Return all date fields for comprehensive checking
        return date_fields
    else:
        logger.warning(f"Method 1: No date fields found in layer")
        return []

def get_service_metadata_date(layer_url: str) -> List[DateFinding]:
    """Get last update date from service-level documentInfo metadata"""
    logger.info(f"Method 3: Checking documentInfo.LastSaved from service metadata")
    findings = []
    
    # Strip layer ID from URL to get parent service URL
    if layer_url.endswith('/'):
        layer_url = layer_url.rstrip('/')
    
    last_slash_idx = layer_url.rfind('/')
    if last_slash_idx == -1:
        logger.warning(f"Method 3: Could not parse layer URL structure: {layer_url}")
        return findings
    
    potential_layer_id = layer_url[last_slash_idx + 1:]
    if potential_layer_id.isdigit():
        service_url = layer_url[:last_slash_idx]
        logger.debug(f"Method 3: Stripped layer ID '{potential_layer_id}' from URL")
    else:
        service_url = layer_url
        logger.debug(f"Method 3: No layer ID detected, using full URL")
    
    logger.debug(f"Method 3: Service URL: {service_url}")
    
    data = make_request(service_url, params={"f": "json"})
    if not data:
        logger.warning(f"Method 3: Failed to get service metadata")
        return findings
        
    try:
        document_info = data.get("documentInfo", {})
        logger.debug(f"Method 3: documentInfo present: {bool(document_info)}")
        
        if not document_info:
            logger.warning(f"Method 3: No documentInfo found in service metadata")
            return findings
            
        last_saved = document_info.get("LastSaved")
        logger.debug(f"Method 3: Raw LastSaved value: {last_saved}")
        
        if last_saved is None:
            logger.warning(f"Method 3: documentInfo exists but LastSaved is null")
            findings.append(DateFinding("Method 3", "LastSaved", "null", None, False, "LastSaved is null"))
        else:
            converted_date = to_date_string(last_saved)
            if converted_date:
                logger.info(f"Method 3: Successfully converted date: {converted_date}")
                findings.append(DateFinding("Method 3", "LastSaved", str(last_saved), converted_date, True))
            else:
                logger.warning(f"Method 3: Failed to convert raw date value: {last_saved}")
                findings.append(DateFinding("Method 3", "LastSaved", str(last_saved), None, False, "Failed to convert timestamp"))
                
    except (KeyError, TypeError) as e:
        logger.warning(f"Method 3: Error parsing document info: {e}")
        
    return findings

def get_service_item_date(layer_url: str) -> List[DateFinding]:
    """Get last update date from ArcGIS.com service item metadata"""
    logger.info(f"Method 4: Checking ArcGIS.com service item metadata")
    findings = []
    
    # First, get the layer metadata to extract serviceItemId
    data = make_request(layer_url, params={"f": "json"})
    if not data:
        logger.warning(f"Method 4: Failed to get layer metadata")
        return findings
        
    try:
        service_item_id = data.get("serviceItemId")
        logger.debug(f"Method 4: serviceItemId found: {service_item_id}")
        
        if not service_item_id:
            logger.warning(f"Method 4: No serviceItemId found in layer metadata")
            return findings
            
        # Request service item metadata from ArcGIS.com
        item_url = f"https://www.arcgis.com/sharing/rest/content/items/{service_item_id}"
        logger.debug(f"Method 4: Requesting item metadata from: {item_url}")
        
        item_data = make_request(item_url, params={"f": "json"})
        if not item_data:
            logger.warning(f"Method 4: Failed to get service item metadata from ArcGIS.com")
            return findings
        
        # Try modified date first, then created date
        modified = item_data.get("modified")
        created = item_data.get("created")
        
        logger.debug(f"Method 4: Raw modified value: {modified}")
        logger.debug(f"Method 4: Raw created value: {created}")
        
        # Process both dates if available
        if modified is not None:
            converted_date = to_date_string(modified)
            if converted_date:
                logger.info(f"Method 4: Successfully converted modified date: {converted_date}")
                findings.append(DateFinding("Method 4", "modified", str(modified), converted_date, True))
            else:
                logger.warning(f"Method 4: Failed to convert modified value: {modified}")
                findings.append(DateFinding("Method 4", "modified", str(modified), None, False, "Failed to convert timestamp"))
                
        if created is not None:
            converted_date = to_date_string(created)
            if converted_date:
                logger.info(f"Method 4: Successfully converted created date: {converted_date}")
                # Created date is less reliable than modified date
                findings.append(DateFinding("Method 4", "created", str(created), converted_date, True, "Created date (less reliable than modified)"))
            else:
                logger.warning(f"Method 4: Failed to convert created value: {created}")
                findings.append(DateFinding("Method 4", "created", str(created), None, False, "Failed to convert timestamp"))
        
        if not modified and not created:
            logger.warning(f"Method 4: No modified or created timestamp found in service item")
            
    except (KeyError, TypeError) as e:
        logger.warning(f"Method 4: Error parsing service item metadata: {e}")
        
    return findings

def convert_yyyymmdd_to_iso(date_str: str) -> Optional[str]:
    """Convert YYYYMMDD format to ISO date string"""
    try:
        if len(date_str) == 8 and date_str.isdigit():
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:8]
            # Validate the date components
            datetime.datetime(int(year), int(month), int(day))
            return f"{year}-{month}-{day}"
    except (ValueError, TypeError):
        pass
    return None

def get_metadata_xml_date(layer_url: str) -> List[DateFinding]:
    """Get date from XML metadata endpoint with namespace awareness"""
    logger.info(f"Method 5: Checking XML metadata for date stamps")
    findings = []
    
    # Construct metadata URL
    metadata_url = layer_url.rstrip('/') + '/metadata'
    logger.debug(f"Method 5: Requesting XML metadata from: {metadata_url}")
    
    try:
        # Make request with XML accept header
        response = requests.get(metadata_url, headers={"Accept": "application/xml"}, timeout=30)
        response.raise_for_status()
        
        logger.debug(f"Method 5: Received XML response, length: {len(response.text)} characters")
        
        # Parse XML with namespace awareness using iterparse
        try:
            # Extract namespaces dynamically
            namespaces = {}
            xml_string = io.StringIO(response.text)
            
            # Use iterparse to extract namespaces
            for event, elem in ET.iterparse(xml_string, events=['start-ns']):
                prefix, uri = event
                if prefix:  # Only store prefixed namespaces
                    namespaces[prefix] = uri
            
            logger.debug(f"Method 5: Namespaces found: {namespaces}")
            
            # Parse the XML again to get the root element
            root = ET.fromstring(response.text)
            
        except ET.ParseError as e:
            logger.warning(f"Method 5: Failed to parse XML metadata: {e}")
            return findings
        
        # Define date-related tags to search for
        date_candidates = [
            'dateStamp', 
            'gmd:dateStamp', 
            'gmd:date', 
            'date', 
            'modifiedDate', 
            'lastUpdate'
        ]
        
        logger.debug(f"Method 5: Searching for date elements: {date_candidates}")
        
        # Search for date elements with proper namespace handling
        for tag in date_candidates:
            try:
                elem = None
                search_error = None
                
                if ':' in tag:
                    # Handle namespaced tags
                    prefix, local_name = tag.split(':', 1)
                    if prefix in namespaces:
                        namespace_uri = namespaces[prefix]
                        try:
                            # Search using namespace URI
                            elem = root.find(f".//{{{namespace_uri}}}{local_name}")
                            logger.debug(f"Method 5: Searching for namespaced tag {tag} -> {{{namespace_uri}}}{local_name}")
                        except Exception as ns_error:
                            search_error = f"namespace search error: {ns_error}"
                            logger.debug(f"Method 5: Error in namespace search for {tag}: {ns_error}")
                    else:
                        logger.debug(f"Method 5: Namespace prefix '{prefix}' not found, trying fallback search")
                        try:
                            # Fallback to searching without namespace if prefix not found
                            elem = root.find(f".//{tag}")
                        except Exception as fallback_error:
                            search_error = f"fallback search error: {fallback_error}"
                            logger.debug(f"Method 5: Error in fallback search for {tag}: {fallback_error}")
                else:
                    # Handle non-namespaced tags
                    try:
                        elem = root.find(f".//{tag}")
                        logger.debug(f"Method 5: Searching for non-namespaced tag: {tag}")
                    except Exception as simple_error:
                        search_error = f"simple search error: {simple_error}"
                        logger.debug(f"Method 5: Error in simple search for {tag}: {simple_error}")
                
                if elem is not None and elem.text and elem.text.strip():
                    date_text = elem.text.strip()
                    logger.debug(f"Method 5: Found {tag}: '{date_text}'")
                    
                    # Try standard ISO format first
                    if len(date_text) >= 10 and ('-' in date_text or 'T' in date_text):
                        logger.info(f"Method 5: Found ISO date: {date_text}")
                        findings.append(DateFinding("Method 5", tag, date_text, date_text, True))
                        continue
                    
                    # Try YYYYMMDD format conversion
                    iso_date = convert_yyyymmdd_to_iso(date_text)
                    if iso_date:
                        logger.info(f"Method 5: Found YYYYMMDD date converted to ISO: {iso_date} (from {date_text})")
                        findings.append(DateFinding("Method 5", tag, date_text, iso_date, True, "Converted from YYYYMMDD format"))
                        continue
                        
                    # If we reach here, the date format wasn't recognized
                    findings.append(DateFinding("Method 5", tag, date_text, None, False, "Unrecognized date format"))
                    
                elif search_error:
                    logger.debug(f"Method 5: Skipped {tag} due to {search_error}")
                        
            except Exception as tag_error:
                logger.debug(f"Method 5: Error processing tag {tag}: {tag_error}")
                continue
        
        # Log summary of findings
        if findings:
            reliable_count = len([f for f in findings if f.reliable])
            logger.debug(f"Method 5: Found {len(findings)} date elements, {reliable_count} reliable")
        else:
            logger.warning(f"Method 5: No date elements found in XML metadata")
        
    except requests.exceptions.Timeout:
        logger.warning(f"✗ METHOD 5 FAILED: Request timeout for metadata URL: {metadata_url}")
    except requests.exceptions.ConnectionError:
        logger.warning(f"✗ METHOD 5 FAILED: Connection error for metadata URL: {metadata_url}")
    except requests.exceptions.HTTPError as e:
        logger.warning(f"✗ METHOD 5 FAILED: HTTP error {e.response.status_code} for metadata URL: {metadata_url}")
    except Exception as e:
        logger.warning(f"✗ METHOD 5 FAILED: Error parsing metadata: {e}")
        
    return findings

def validate_arcgis_url(url: str) -> bool:
    """Basic validation for ArcGIS service URLs"""
    if not url or not isinstance(url, str):
        return False
    
    # Check for basic ArcGIS service patterns
    required_patterns = ['/rest/services/', '/MapServer/']
    return any(pattern in url for pattern in required_patterns)

def get_arcgis_data_date(layer_url: str) -> Tuple[Optional[str], List[DateFinding]]:
    """
    Determine the last update date of ArcGIS layer data using all available methods.
    
    Args:
        layer_url: URL to ArcGIS layer endpoint
        
    Returns:
        Tuple of (most_reliable_date, all_findings)
    """
    if not validate_arcgis_url(layer_url):
        logger.error(f"Invalid ArcGIS URL format: {layer_url}")
        return None, []
        
    logger.info(f"=== Checking ArcGIS layer: {layer_url} ===")
    logger.info(f"Running all methods to collect comprehensive date information...")
    
    all_findings = []
    
    # Method 1: Try known date fields
    logger.info(f"\n--- METHOD 1: Query date fields ---")
    date_fields = find_date_field(layer_url)
    if date_fields:
        for field in date_fields:
            method1_findings = query_max_date_field(layer_url, field)
            all_findings.extend(method1_findings)
            
            # Log results for this field
            reliable_findings = [f for f in method1_findings if f.reliable]
            if reliable_findings:
                logger.info(f"✓ METHOD 1 SUCCESS: Found max({field}) = {reliable_findings[0].converted_date}")
            else:
                logger.warning(f"✗ METHOD 1 PARTIAL: Found field '{field}' but couldn't get valid date")
    else:
        logger.warning(f"✗ METHOD 1 FAILED: No suitable date fields found")

    # Method 2: Try editingInfo > lastEditDate
    logger.info(f"\n--- METHOD 2: Check editing info ---")
    method2_findings = get_editing_info_date(layer_url)
    all_findings.extend(method2_findings)
    
    reliable_findings = [f for f in method2_findings if f.reliable]
    if reliable_findings:
        logger.info(f"✓ METHOD 2 SUCCESS: Found editingInfo.lastEditDate = {reliable_findings[0].converted_date}")
    else:
        logger.warning(f"✗ METHOD 2 FAILED: No valid lastEditDate in editingInfo")

    # Method 3: Try service-level documentInfo > LastSaved
    logger.info(f"\n--- METHOD 3: Check service metadata ---")
    method3_findings = get_service_metadata_date(layer_url)
    all_findings.extend(method3_findings)
    
    reliable_findings = [f for f in method3_findings if f.reliable]
    if reliable_findings:
        logger.info(f"✓ METHOD 3 SUCCESS: Found documentInfo.LastSaved = {reliable_findings[0].converted_date}")
    else:
        logger.warning(f"✗ METHOD 3 FAILED: No valid LastSaved in service documentInfo")

    # Method 4: Try ArcGIS.com service item metadata
    logger.info(f"\n--- METHOD 4: Check service item metadata ---")
    method4_findings = get_service_item_date(layer_url)
    all_findings.extend(method4_findings)
    
    reliable_findings = [f for f in method4_findings if f.reliable]
    if reliable_findings:
        logger.info(f"✓ METHOD 4 SUCCESS: Found service item metadata date = {reliable_findings[0].converted_date}")
    else:
        logger.warning(f"✗ METHOD 4 FAILED: No valid date found in service item metadata")

    # Method 5: Try XML metadata
    logger.info(f"\n--- METHOD 5: Check XML metadata ---")
    method5_findings = get_metadata_xml_date(layer_url)
    all_findings.extend(method5_findings)
    
    reliable_findings = [f for f in method5_findings if f.reliable]
    if reliable_findings:
        logger.info(f"✓ METHOD 5 SUCCESS: Found date in XML metadata = {reliable_findings[0].converted_date}")
    else:
        logger.warning(f"✗ METHOD 5 FAILED: No valid date found in XML metadata")

    # Determine most reliable date from all findings
    most_reliable = get_most_reliable_date(all_findings)
    most_reliable_date = most_reliable.converted_date if most_reliable else None
    
    # Summary logging
    logger.info(f"\n--- METHODS COMPLETE ---")
    if most_reliable_date:
        logger.info(f"Most reliable date determined: {most_reliable_date}")
    else:
        logger.warning("Could not determine a reliable data date using any available method.")
        logger.info("Consider checking the service manually or adding additional detection methods.")
    
    return most_reliable_date, all_findings

# Example usage:
if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Detect last update date of ArcGIS layer data")
    parser.add_argument("--debug", action="store_true",
                       help="Enable detailed debug logging")
    parser.add_argument("url", nargs="?", help="ArcGIS layer URL to check")
    
    args = parser.parse_args()
    
    # Set up logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Get URL from command line or prompt user
    if args.url:
        layer_url = args.url
    else:
        layer_url = input("Enter the layer URL: ")
    
    print(f"\nRunning comprehensive date detection on all methods...")
    
    print("="*60)
    
    # Run the detection
    result, all_findings = get_arcgis_data_date(layer_url)
    
    print("="*60)
    
    # Show comprehensive summary
    most_reliable = get_most_reliable_date(all_findings)
    print_date_summary(all_findings, most_reliable) 