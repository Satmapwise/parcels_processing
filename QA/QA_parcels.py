import json
import os
from datetime import datetime, timedelta
import csv
import psycopg2
from dotenv import load_dotenv
import requests

# Suppress only the single InsecureRequestWarning from urllib3 needed for this script
# requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

global test_mode
global record_check
global recent_sale_check
global empty_columns_check

test_mode = True
record_check = False
recent_sale_check = True
empty_columns_check = False


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

def get_county_config(config, county_name):
    """Finds the county configuration by name from the counties array."""
    for county in config['counties']:
        if county['name'] == county_name:
            return county
    return None

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
    all_params['searchCounty'] = county_name.upper().replace('.', '')
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
        if test_mode:
            print(f"\n  TEST MODE: Querying {response.url}...")
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
        print(f"  Response content: {response.text[:200]}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during API request for {county_name}: {e}")
        return None

def get_api_record_count(county_name):
    """Gets the total record count for a county from the API."""
    # Get total count without limiting results
    data = get_api_data(county_name, params={})
    if not data or 'meta' not in data:
        return None
    return data['meta'].get('record_count', 0)

def get_api_most_recent_record(county_name, days_tolerance, initial_records):
    """Gets a record with recent sale date for a county from the API."""
    # Extract data date from any record in the initial batch
    if not initial_records or 'data' not in initial_records or not initial_records['data']:
        if test_mode:
            print(f"  TEST MODE: No initial records found. Returning None.")
        return False, "No initial records found"
    
    # Get d_date from the first record in initial_records
    first_record = initial_records['data'][0]
    prodate_str = first_record['attributes'].get('d_date')
    if not prodate_str:
        if test_mode:
            print(f"  TEST MODE: No data date found. Returning None.")
        return False, "No data date found"
    
    # Parse the d_date and calculate threshold date
    data_date = datetime.strptime(prodate_str, '%Y%m%d').date()
    threshold_date = (data_date - timedelta(days=days_tolerance)).strftime('%m/%d/%Y')
    if test_mode:
        print(f"  TEST MODE: Data date: {data_date}")
        print(f"  TEST MODE: Threshold date: {threshold_date}")
    
    # Get records with sale date from threshold_date onwards and minimum sale amount of 100
    data = get_api_data(county_name, params={'limit': 100, 'searchSaleAmt1': 100, 'searchDate1': threshold_date})
    if not data or 'data' not in data or not data['data']:
        # Run another query 30 days earlier
        if test_mode:
            print(f"  TEST MODE: No recent sale date found. Running query 30 days earlier.")
        retry_date = (data_date - timedelta(days=days_tolerance+30)).strftime('%m/%d/%Y')
        data = get_api_data(county_name, params={'limit': 100, 'searchSaleAmt1': 100, 'searchDate1': retry_date})
        if not data or 'data' not in data or not data['data']:
            # Run a final query from September 1st of the previous year
            retry_date = datetime(data_date.year - 1, 9, 1).strftime('%m/%d/%Y')
            if test_mode:
                print(f"  TEST MODE: No recent sale date found after retry. Running final query from {retry_date}.")
            data = get_api_data(county_name, params={'limit': 100, 'searchSaleAmt1': 100, 'searchDate1': retry_date})
            if not data or 'data' not in data or not data['data']:
                if test_mode:
                    print(f"  TEST MODE: No recent sale date found after final query. Returning None.")
            return False, "No recent sale date found after all retries."
    
    most_recent_record = None
    most_recent_sale_date = None

    for record in data['data']:
        sale_date_str = record['attributes'].get('sale1_date')
        if sale_date_str:
            # Assuming YYYY-MM-DD format, string comparison is sufficient
            if most_recent_sale_date is None or sale_date_str > most_recent_sale_date:
                most_recent_sale_date = sale_date_str
                most_recent_record = record
    
    return most_recent_record, None

def check_record_number(county_config, api_record_count, raw_data_path, db_connection=None):
    """Checks if the record number from the API is within the allowed margin of error."""
    raw_file_name = county_config.get('raw_file_name')
    file_format = county_config.get('file_format', 'delimited') # Default to delimited
    raw_record_count = None

    if raw_file_name == "FDOR":
        if not db_connection:
            return True, "SKIPPED: DB connection not available for FDOR check.", None
        
        id_type = county_config.get('fdor_identifier_type')
        id_value = county_config.get('fdor_identifier_value')

        if not id_type or id_value is None:
            return False, "FDOR county is missing identifier type or value in config.", None

        try:
            with db_connection.cursor() as cursor:
                query = f"SELECT COUNT(DISTINCT parcel_id) FROM parcels_fdor_2024 WHERE {id_type} = %s;"
                cursor.execute(query, (id_value,))
                raw_record_count = cursor.fetchone()[0]
        except psycopg2.Error as e:
            return False, f"Database error during FDOR count: {e}", None

    elif raw_file_name == "UNAVAILABLE":
        return True, "SKIPPED: Raw file source is UNAVAILABLE.", None
    
    elif file_format == 'fixed-width':
        try:
            parcel_ids = set()
            start = county_config.get('parcel_id_start')
            length = county_config.get('parcel_id_length')
            if start is None or length is None:
                return False, "Fixed-width config missing start or length.", None
            
            with open(raw_data_path, 'r', newline='', errors='ignore') as f:
                for line in f:
                    if len(line) >= start + length:
                        parcel_id = line[start:start+length].strip()
                        if parcel_id:
                            parcel_ids.add(parcel_id)
            raw_record_count = len(parcel_ids)
        except FileNotFoundError:
            return False, f"Raw data file not found at {raw_data_path}", None
        except Exception as e:
            return False, f"Error reading raw data file: {e}", None

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
            return False, f"Raw data file not found at {raw_data_path}", None
        except Exception as e:
            return False, f"Error reading raw data file: {e}", None

    margin = county_config['record_number_error_margin_percent'] / 100
    lower_bound = raw_record_count * (1 - margin)
    upper_bound = raw_record_count * (1 + margin)

    if not (lower_bound <= api_record_count <= upper_bound):
        return False, f"Record count mismatch. Raw: {raw_record_count}, API: {api_record_count}", raw_record_count
    return True, "", raw_record_count

def check_most_recent_sale_date(county_config, most_recent_sale_date_str, data_date):
    """Checks if the most recent sale date is not too old."""
    if not most_recent_sale_date_str:
        return True, "SKIPPED: Most recent sale date is null."
    
    most_recent_sale_date = datetime.strptime(most_recent_sale_date_str, '%Y-%m-%d').date()
    days_difference = county_config['sale_date_days_difference']
    if (data_date - most_recent_sale_date).days > days_difference:
        return False, f"Most recent sale date {most_recent_sale_date} is too old."
    return True, "The most recent sale date is within the allowed margin of error."

def check_empty_columns(county_name, columns_to_check):
    """
    Checks for empty values in specified columns for a sample of recent records.
    Returns a tuple containing:
    - A boolean indicating if the check passed (True if no empty columns found).
    - A list of error messages.
    - A dictionary with details about empty columns, including counts and parcel IDs.
    """
    # Get searchDate1 from current date minus 3 months
    search_date = (datetime.now() - timedelta(days=90)).strftime('%m/%d/%Y')
    data = get_api_data(county_name, params={'limit': 10, 'searchSaleAmt1': 100, 'searchDate1': search_date})
    
    if not data or 'data' not in data or not data['data']:
        return False, ["Could not retrieve sample data for empty column check."], {}

    sample_size = len(data['data'])
    empty_column_details = {}

    # Initialize details for all columns to check
    for item in columns_to_check:
        if isinstance(item, str):
            empty_column_details[item] = {'count': 0, 'parcels': []}
        elif isinstance(item, dict) and item.get('rule') == 'any':
            key = f"any_of_{'_'.join(item.get('fields', []))}"
            empty_column_details[key] = {'count': 0, 'parcels': []}

    for record in data['data']:
        attributes = record['attributes']
        pin = attributes.get('pin')
        ogc_fid = attributes.get('ogc_fid')
        # Use PIN if available, otherwise OGC_FID. Add prefix for clarity.
        if pin:
            parcel_id_display = f"pin:{pin}"
        elif ogc_fid:
            parcel_id_display = f"ogc_fid:{ogc_fid}"
        else:
            parcel_id_display = "Unknown"
        
        for item in columns_to_check:
            if isinstance(item, str):
                if attributes.get(item) is None or attributes.get(item) == '':
                    empty_column_details[item]['count'] += 1
                    empty_column_details[item]['parcels'].append(parcel_id_display)
            elif isinstance(item, dict) and item.get('rule') == 'any':
                key = f"any_of_{'_'.join(item.get('fields', []))}"
                found = any(attributes.get(field) is not None and attributes.get(field) != '' for field in item.get('fields', []))
                if not found:
                    empty_column_details[key]['count'] += 1
                    empty_column_details[key]['parcels'].append(parcel_id_display)

    has_empty_columns = any(details['count'] > 0 for details in empty_column_details.values())
    errors = []
    if has_empty_columns:
        for col, details in empty_column_details.items():
            if details['count'] > 0:
                errors.append(f"Column '{col}' is empty in {details['count']}/{sample_size} sample records.")

    return not has_empty_columns, errors, empty_column_details

def main():
    """Main function to run the QA checks."""
    load_dotenv() # Load environment variables from .env file
    config = get_config()
    db_connection = get_db_connection()

    # Setup paths for results
    qa_results_dir = os.path.join(os.path.dirname(__file__), 'QA_results')
    county_reports_dir = os.path.join(qa_results_dir, 'county_reports')
    os.makedirs(county_reports_dir, exist_ok=True)
    summary_path = os.path.join(qa_results_dir, 'QA_summary.csv')
    
    success_count = 0
    failure_count = 0

    # Write results to CSV
    with open(summary_path, 'w', newline='') as summary_csv_file:
        summary_fieldnames = ['county', 'data_date']
        if record_check:
            summary_fieldnames.append('record_count_check')
        if recent_sale_check:
            summary_fieldnames.append('most_recent_sale_check')
        if empty_columns_check:
            summary_fieldnames.append('empty_columns_check')
            summary_fieldnames.append('missing_columns_count')
        
        summary_writer = csv.DictWriter(summary_csv_file, fieldnames=summary_fieldnames)
        summary_writer.writeheader()

        # Get counties to process
        if test_mode:
            QA_counties = ['Nassau', 'Okaloosa', 'Seminole', 'Miami-Dade']
        else:
            for county_config_item in config['counties']:
                if county_config_item['name'] not in QA_counties:
                    QA_counties.append(county_config_item['name'])
        
        # Process each county
        for county_name in QA_counties:
            county_config = get_county_config(config, county_name)
            
            # Check if county configuration exists
            if not county_config:
                print(f"  -> FAILED: County configuration not found for {county_name}.\n")
                summary_writer.writerow({
                    'county': county_name, 'status': 'Failure', 
                    'error_description': 'County configuration not found.'
                })
                failure_count += 1
                continue

            print(f"Processing {county_name}...")

            # API calls now use the original county_name. The path needs a formatted version.
            path_county_name = county_name.lower().replace(" ", "_").replace(".", "")
            raw_data_dir = f"/srv/mapwise_dev/county/{path_county_name}/processing/database/current"
            raw_data_path = os.path.join(raw_data_dir, county_config.get('raw_file_name', ''))

            # Setup county-specific report
            county_report_path = os.path.join(county_reports_dir, f"{path_county_name}_QA.csv")
            with open(county_report_path, 'w', newline='') as county_csv_file:
                county_writer = csv.writer(county_csv_file)
                county_writer.writerow(['check_name', 'value', 'details'])

                summary_row = {'county': county_name}
                error_messages = []

                # Get initial batch for data_date
                initial_records = get_api_data(county_name, params={'limit': 1})
                if not initial_records or not initial_records.get('data'):
                    error_messages.append('Could not retrieve initial records to determine data date.')
                
                prodate_str = initial_records['data'][0]['attributes'].get('d_date') if initial_records and initial_records.get('data') else None
                data_date = datetime.strptime(prodate_str, '%Y%m%d').date() if prodate_str else None
                summary_row['data_date'] = data_date
                if not data_date:
                    error_messages.append('Could not determine data date.')
                    county_writer.writerow(['data_date', 'ERROR', 'Could not determine data date.'])
                else:
                    county_writer.writerow(['data_date', data_date, 'The date of the most recent update to the server.'])

                # 1. Record number check
                if record_check:
                    print("  - Checking record count...", end="", flush=True)
                    api_record_count = get_api_record_count(county_name)
                    county_writer.writerow(['api_record_count', api_record_count, 'Total records available from the server.'])
                    
                    rec_num_success, rec_num_msg, raw_record_count = check_record_number(county_config, api_record_count, raw_data_path, db_connection)
                    county_writer.writerow(['raw_file_parcel_count', raw_record_count, 'Distinct parcel IDs found in the raw source file.'])
                    county_writer.writerow(['record_count_check', 'SUCCESS' if rec_num_success else 'FAILURE', rec_num_msg])
                    
                    summary_row['record_count_check'] = 'SUCCESS' if rec_num_success else 'FAILURE'
                    if not rec_num_success:
                        error_messages.append(f"Record count: {rec_num_msg}")
                        print(f" FAILED: {rec_num_msg}")
                    elif "SKIPPED" in rec_num_msg:
                        print(f" {rec_num_msg}")
                        summary_row['record_count_check'] = 'SKIPPED'
                    else:
                        print(" OK")

                # 2. Most recent sale date check
                if recent_sale_check:
                    print("  - Checking most recent sale date...", end="", flush=True)
                    most_recent_record, sale_date_msg = get_api_most_recent_record(county_name, county_config['sale_date_days_difference'], initial_records)
                    if most_recent_record:
                        most_recent_sale_date = most_recent_record['attributes'].get('sale1_date')
                        sale_date_success, sale_date_msg = check_most_recent_sale_date(county_config, most_recent_sale_date, data_date) if data_date and most_recent_sale_date else (False, "Unexpected error.")
                        county_writer.writerow(['most_recent_sale', most_recent_sale_date, sale_date_msg])
                    else:
                        sale_date_success = False
                        sale_date_msg = sale_date_msg
                        county_writer.writerow(['most_recent_sale', 'ERROR', sale_date_msg])

                    summary_row['most_recent_sale_check'] = 'SUCCESS' if sale_date_success else 'FAILURE'
                    if not sale_date_success:
                        error_messages.append(f"Sale date: {sale_date_msg}")
                        print(f" FAILED: {sale_date_msg}")
                    elif "SKIPPED" in sale_date_msg:
                        print(f" {sale_date_msg}")
                        summary_row['most_recent_sale_check'] = 'SKIPPED'
                    else:
                        print(" OK")

                # 3. Empty columns check
                if empty_columns_check:
                    print("  - Checking for empty columns...", end="", flush=True)
                    empty_col_success, empty_col_errors, empty_col_details = check_empty_columns(county_name, config['columns_to_check'])
                    
                    missing_cols_count = sum(1 for details in empty_col_details.values() if details['count'] > 0)
                    summary_row['missing_columns_count'] = missing_cols_count
                    summary_row['empty_columns_check'] = 'SUCCESS' if empty_col_success else 'FAILURE'

                    for col, details in empty_col_details.items():
                        count = details['count']
                        parcels = details['parcels']
                        details_msg = f"Missing in parcels: {', '.join(parcels)}" if parcels else "All sample records have a value."
                        county_writer.writerow([f'empty_column: {col}', f"{count}/10", details_msg])

                    if not empty_col_success:
                        error_messages.extend(empty_col_errors)
                        print(" FAILED")
                    else:
                        print(" OK")

                # Finalize and write summary
                if error_messages:
                    print(f"  -> RESULT: Failure\n")
                    failure_count += 1
                else:
                    print(f"  -> RESULT: Success\n")
                    success_count += 1
                
                summary_writer.writerow(summary_row)

    if db_connection:
        db_connection.close()

    print("\n" + "="*40)
    print("QA Run Summary")
    print("="*40)
    print(f"Total Counties Processed: {success_count + failure_count}")
    print(f"  Success: {success_count}")
    print(f"  Failure: {failure_count}")
    print("="*40)
    print(f"\nFull results saved to: {summary_path}")
    print(f"County-specific reports saved in: {county_reports_dir}")


if __name__ == "__main__":
    main()
