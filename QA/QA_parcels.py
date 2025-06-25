import json
import os
from datetime import datetime
import csv
import psycopg2
from dotenv import load_dotenv
import requests

# Suppress only the single InsecureRequestWarning from urllib3 needed for this script
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)


def get_db_connection():
    """Establishes a database connection using an environment variable."""
    conn_string = os.environ.get('PG_CONNECTION_STRING')
    if not conn_string:
        print("Warning: PG_CONNECTION_STRING environment variable not set. Cannot perform FDOR checks.")
        return None
    try:
        connection = psycopg2.connect(conn_string)
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def get_config():
    """Reads the configuration from the JSON file."""
    config_path = os.path.join(os.path.dirname(__file__), 'QA_config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

def get_api_data(county_name, params={}):
    """Queries the API for a given county using the requests library."""
    base_url = "https://wms1.mapwise.com/api_v1/parcels_v2/"

    user = os.environ.get('MAPWISE_API_USER')
    password = os.environ.get('MAPWISE_API_PASS')

    if not user or not password:
        print("Warning: MAPWISE_API_USER or MAPWISE_API_PASS not set. API requests will likely fail.")
        return None

    # Build parameters for the GET request
    all_params = params.copy()
    all_params['searchCounty'] = county_name.upper()
    all_params['format'] = 'json'

    # Set headers for the request
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'curl/7.68.0'  # Mimic curl to avoid potential server-side blocking
    }

    try:
        response = requests.get(
            base_url,
            params=all_params,
            auth=(user, password),
            headers=headers,
            timeout=30  # Add a timeout to prevent indefinite hanging
        )
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"API request failed for {county_name}.")
        print(f"  Status Code: {e.response.status_code}")
        print(f"  Response: {e.response.text}")
        return None
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error for {county_name}: {e}")
        return None
    except requests.exceptions.Timeout as e:
        print(f"Timeout error for {county_name}: {e}")
        return None
    except requests.exceptions.SSLError as e:
        print(f"SSL error for {county_name}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        # This catches other request-related errors like timeouts, connection errors, etc.
        print(f"An unexpected error occurred during API request for {county_name}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"Failed to decode JSON from API response for {county_name}.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during API request for {county_name}: {e}")
        return None

def check_record_number(county_config, api_record_count, raw_data_path, db_connection=None):
    """Checks if the record number from the API is within the allowed margin of error."""
    raw_file_name = county_config.get('raw_file_name')
    file_format = county_config.get('file_format', 'delimited') # Default to delimited

    if raw_file_name == "FDOR":
        if not db_connection:
            return True, "SKIPPED: DB connection not available for FDOR check."
        
        id_type = county_config.get('fdor_identifier_type')
        id_value = county_config.get('fdor_identifier_value')

        if not id_type or id_value is None:
            return False, "FDOR county is missing identifier type or value in config."

        try:
            with db_connection.cursor() as cursor:
                query = f"SELECT COUNT(DISTINCT parcel_id) FROM parcels_fdor_2024 WHERE {id_type} = %s;"
                cursor.execute(query, (id_value,))
                raw_record_count = cursor.fetchone()[0]
        except psycopg2.Error as e:
            return False, f"Database error during FDOR count: {e}"

    elif raw_file_name == "UNAVAILABLE":
        return True, "SKIPPED: Raw file source is UNAVAILABLE."
    
    elif file_format == 'fixed-width':
        try:
            parcel_ids = set()
            start = county_config.get('parcel_id_start')
            length = county_config.get('parcel_id_length')
            if start is None or length is None:
                return False, "Fixed-width config missing start or length."
            
            with open(raw_data_path, 'r', newline='', errors='ignore') as f:
                for line in f:
                    if len(line) >= start + length:
                        parcel_id = line[start:start+length].strip()
                        if parcel_id:
                            parcel_ids.add(parcel_id)
            raw_record_count = len(parcel_ids)
        except FileNotFoundError:
            return False, f"Raw data file not found at {raw_data_path}"
        except Exception as e:
            return False, f"Error reading raw data file: {e}"

    else: # Default to delimited
        try:
            parcel_ids = set()
            delimiter = county_config.get('delimiter', ',')
            has_header = county_config.get('has_header', False)
            id_column = county_config.get('parcel_id_column_index', 0)

            with open(raw_data_path, 'r', newline='', errors='ignore') as f:
                reader = csv.reader(f, delimiter=delimiter)
                
                if has_header:
                    next(reader, None)  # Skip header row
                
                for row in reader:
                    if row and len(row) > id_column:
                        parcel_id = row[id_column].strip()
                        if parcel_id:
                            parcel_ids.add(parcel_id)
            
            raw_record_count = len(parcel_ids)

        except FileNotFoundError:
            return False, f"Raw data file not found at {raw_data_path}"
        except Exception as e:
            return False, f"Error reading raw data file: {e}"

    margin = county_config['record_number_error_margin_percent'] / 100
    lower_bound = raw_record_count * (1 - margin)
    upper_bound = raw_record_count * (1 + margin)

    if not (lower_bound <= api_record_count <= upper_bound):
        return False, f"Record count mismatch. Raw: {raw_record_count}, API: {api_record_count}"
    return True, ""

def check_most_recent_sale_date(county_config, most_recent_sale_date_str, data_date):
    """Checks if the most recent sale date is not too old."""
    if not most_recent_sale_date_str:
        return False, "Most recent sale date is null."
        
    most_recent_sale_date = datetime.strptime(most_recent_sale_date_str, '%Y-%m-%d').date()
    
    days_difference = county_config['sale_date_days_difference']
    if (data_date - most_recent_sale_date).days > days_difference:
        return False, f"Most recent sale date {most_recent_sale_date} is too old."
    return True, ""

def check_empty_columns(county_name, columns_to_check):
    """Checks for empty values in specified columns for the 10 most recent records."""
    data = get_api_data(county_name, params={'limit': 10})
    if not data or 'data' not in data or not data['data']:
        return False, "Could not retrieve sample data for empty column check."

    errors = []
    for record in data['data']:
        attributes = record['attributes']
        for item in columns_to_check:
            if isinstance(item, str):
                # Simple check for a single column
                if attributes.get(item) is None or attributes.get(item) == '':
                    errors.append(f"Empty value in column '{item}' for parcel {attributes.get('parcelid')}")
            elif isinstance(item, dict):
                # Complex check for a rule-based item
                if item.get('rule') == 'any':
                    # Check if any of the fields have a value
                    found = False
                    for field in item.get('fields', []):
                        if attributes.get(field) is not None and attributes.get(field) != '':
                            found = True
                            break
                    if not found:
                        errors.append(f"No value in any of the specified square footage fields for parcel {attributes.get('parcelid')}")

    if errors:
        return False, ". ".join(list(set(errors)))
    return True, ""

def main():
    """Main function to run the QA checks."""
    load_dotenv() # Load environment variables from .env file
    config = get_config()
    db_connection = get_db_connection()
    results_path = os.path.join(os.path.dirname(__file__), 'QA_results.csv')
    raw_data_dir_template = "/srv/mapwise_dev/county/{county_name}/processing/database/current"

    success_count = 0
    failure_count = 0

    with open(results_path, 'w', newline='') as csvfile:
        fieldnames = ['county', 'status', 'error_description']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for county_config in config['counties']:
            county_name = county_config['name']
            print(f"Processing {county_name}...")

            # API calls now use the original county_name. The path needs a formatted version.
            path_county_name = county_name.lower().replace(" ", "_")
            
            most_recent_data = get_api_data(county_name, params={'limit': 1})
            if not most_recent_data or 'data' not in most_recent_data or not most_recent_data['data']:
                error_description = 'Could not retrieve data from API.'
                print(f"  -> FAILED: {error_description}\n")
                writer.writerow({'county': county_name, 'status': 'Failure', 'error_description': error_description})
                failure_count += 1
                continue
            
            attributes = most_recent_data['data'][0]['attributes']
            prodate_str = attributes.get('d_date')
            
            if not prodate_str:
                error_description = 'Could not retrieve d_date from API.'
                print(f"  -> FAILED: {error_description}\n")
                writer.writerow({'county': county_name, 'status': 'Failure', 'error_description': error_description})
                failure_count += 1
                continue
            
            data_date = datetime.strptime(prodate_str, '%Y%m%d').date()

            raw_data_dir = raw_data_dir_template.format(county_name=path_county_name)
            raw_data_path = os.path.join(raw_data_dir, county_config['raw_file_name'])

            api_record_count = most_recent_data['meta']['record_count']

            error_messages = []

            # 1. Record number check
            print("  - Checking record count...", end="")
            success, msg = check_record_number(county_config, api_record_count, raw_data_path, db_connection)
            if not success:
                error_messages.append(msg)
                print(f" FAILED: {msg}")
            elif "SKIPPED" in msg:
                print(f" {msg}")
            else:
                print(" OK")


            # 2. Most recent sale date check
            print("  - Checking most recent sale date...", end="")
            most_recent_sale_date_str = attributes.get('sale1_date')
            success, msg = check_most_recent_sale_date(county_config, most_recent_sale_date_str, data_date)
            if not success:
                error_messages.append(msg)
                print(f" FAILED: {msg}")
            else:
                print(" OK")

            # 3. Empty columns check
            print("  - Checking for empty columns...", end="")
            success, msg = check_empty_columns(county_name, config['columns_to_check'])
            if not success:
                error_messages.append(msg)
                print(f" FAILED: {msg}")
            else:
                print(" OK")

            if error_messages:
                writer.writerow({'county': county_name, 'status': 'Failure', 'error_description': ". ".join(error_messages)})
                print(f"  -> RESULT: Failure\n")
                failure_count += 1
            else:
                writer.writerow({'county': county_name, 'status': 'Success', 'error_description': ''})
                print(f"  -> RESULT: Success\n")
                success_count += 1

    if db_connection:
        db_connection.close()

    print("\n" + "="*40)
    print("QA Run Summary")
    print("="*40)
    print(f"Total Counties Processed: {success_count + failure_count}")
    print(f"  Success: {success_count}")
    print(f"  Failure: {failure_count}")
    print("="*40)
    print(f"\nFull results saved to: {results_path}")


if __name__ == "__main__":
    main()
