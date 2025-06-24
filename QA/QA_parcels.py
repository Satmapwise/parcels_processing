import json
import os
import requests
from datetime import datetime
import csv
import psycopg2
from dotenv import load_dotenv


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
    """Queries the API for a given county."""
    url = f"https://wms1.mapwise.com/api/v1/parcels/{county_name.lower()}"
    
    user = os.environ.get('MAPWISE_API_USER')
    password = os.environ.get('MAPWISE_API_PASS')
    
    auth = (user, password) if user and password else None

    if not auth:
        print("Warning: MAPWISE_API_USER or MAPWISE_API_PASS not set. API requests will likely fail.")

    try:
        response = requests.get(url, params=params, auth=auth)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed for {county_name}: {e}")
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
    data = get_api_data(county_name, params={'sort': '-saledate', 'limit': 10})
    if not data or 'features' not in data or not data['features']:
        return False, "Could not retrieve sample data for empty column check."

    errors = []
    for record in data['features']:
        props = record['properties']
        for item in columns_to_check:
            if isinstance(item, str):
                # Simple check for a single column
                if props.get(item) is None or props.get(item) == '':
                    errors.append(f"Empty value in column '{item}' for parcel {props.get('parcelid')}")
            elif isinstance(item, dict):
                # Complex check for a rule-based item
                if item.get('rule') == 'any':
                    # Check if any of the fields have a value
                    found = False
                    for field in item.get('fields', []):
                        if props.get(field) is not None and props.get(field) != '':
                            found = True
                            break
                    if not found:
                        errors.append(f"No value in any of the specified square footage fields for parcel {props.get('parcelid')}")

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

    with open(results_path, 'w', newline='') as csvfile:
        fieldnames = ['county', 'status', 'error_description']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for county_config in config['counties']:
            county_name = county_config['name']
            print(f"Processing {county_name}...")
            
            most_recent_data = get_api_data(county_name, params={'sort': '-saledate', 'limit': 1})
            if not most_recent_data or 'features' not in most_recent_data or not most_recent_data['features']:
                writer.writerow({'county': county_name, 'status': 'Failure', 'error_description': 'Could not retrieve data from API.'})
                continue
            
            properties = most_recent_data['features'][0]['properties']
            prodate_str = properties.get('prodate')
            
            if not prodate_str:
                writer.writerow({'county': county_name, 'status': 'Failure', 'error_description': 'Could not retrieve prodate from API.'})
                continue
            
            data_date = datetime.strptime(prodate_str, '%Y-%m-%d').date()

            raw_data_dir = raw_data_dir_template.format(county_name=county_name.lower())
            raw_data_path = os.path.join(raw_data_dir, county_config['raw_file_name'])

            api_record_count = most_recent_data['totalfeatures']

            error_messages = []

            # 1. Record number check
            success, msg = check_record_number(county_config, api_record_count, raw_data_path, db_connection)
            if not success:
                error_messages.append(msg)

            # 2. Most recent sale date check
            most_recent_sale_date_str = properties.get('saledate')
            success, msg = check_most_recent_sale_date(county_config, most_recent_sale_date_str, data_date)
            if not success:
                error_messages.append(msg)

            # 3. Empty columns check
            success, msg = check_empty_columns(county_name, config['columns_to_check'])
            if not success:
                error_messages.append(msg)

            if error_messages:
                writer.writerow({'county': county_name, 'status': 'Failure', 'error_description': ". ".join(error_messages)})
            else:
                writer.writerow({'county': county_name, 'status': 'Success', 'error_description': ''})

            print(f"Finished processing {county_name}.")

    if db_connection:
        db_connection.close()

if __name__ == "__main__":
    main()
