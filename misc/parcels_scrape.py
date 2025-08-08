#!/usr/bin/env python3
import os
import logging
import time
import argparse
import re
import fnmatch
import csv
import sys
import json
import textwrap
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (TimeoutException, NoSuchElementException)
from selenium.webdriver.common.by import By
from dataclasses import dataclass
from parcels_scrape_functions import (initialize_all, start_county_logging, 
                                      log_county_info, log_county_error, 
                                      end_county_logging, initialize_logging)

# Import helper functions
from parcels_scrape_functions import (
                                      load_county_config, safe_find, 
                                      safe_click, safe_input, split_date_range, 
                                      debug_screenshot, wait_for_download, rename_file,
                                      transfer_files, run_command)


# Define county categories in the global scope
QPUBLIC = ["madison","holmes","okaloosa","flagler","walton","hardee","washington",
            "hendry","levy","gilchrist","calhoun","liberty","dixie","jefferson","gulf",
            "hamilton","taylor","gadsden","jackson","bay","glades","sumter"]
GRIZZLY = ["union","columbia","suwannee","okeechobee","desoto","bradford","lafayette"]
GSACORP = ["wakulla","nassau","franklin"]
OPENDATA = ["palm_beach"]
WGET = ['polk', 'brevard', 'volusia', 'seminole', 'manatee', 'st_lucie', 'leon', 
        'alachua', 'clay_wget', 'st_johns', 'hernando', 'charlotte', 'martin', 'citrus', 
        'highlands', 'hillsborough', 'okaloosa_wget']
OTHER = ["indian_river","santa_rosa","duval","pinellas","escambia","putnam","collier"]

# Consolidate Selenium counties into a single list for easier checking
SELENIUM_COUNTIES = QPUBLIC + GRIZZLY + GSACORP + OPENDATA + OTHER
ALL_COUNTIES = SELENIUM_COUNTIES + WGET


# Control variables (defaults)
local = False # Set directories for local testing
manual = False # Set to True to manually enter counties
isolate_county_logs = True # Set to True to isolate county logs from main log (reduces verbosity of output)
chromium = True # Set to True to use Chromium
headless = True # Set to True to run headless
attempts = 10 # Set the number of attempts per county

# ADDED from attributes_download_handler.py
COLUMNS = ['county', 'data_date', 'download_status', 'processing_status', 'QA_status', 'error_message', 'download_date', 'processing_date', 'QA_date']

# PRESET:
prod = False # For production runs (DEPRECATED AFTER ARG UPDATE)
# local = False
# manual = False
# isolate_county_logs = True
# headless = True
# attempts = 10

# Module-level variables to track sessions
_active_sessions = {}
_summary_file = None
_batch_initialized = False

def parse_arguments():
    """
    Parse command-line arguments to override default configuration.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Download parcel data from various county websites",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    Examples:
    python parcel_selenium_download.py                    # Use default settings
    python parcel_selenium_download.py --local            # Run in local mode
    python parcel_selenium_download.py --headful          # Run with browser visible
    python parcel_selenium_download.py --chrome --no-retry # Use Chrome, no retries
    python parcel_selenium_download.py --full-logs        # Show all county logs
        """
    )
    
    # Add county arguments (existing functionality)
    parser.add_argument("counties", nargs="*", help="Names of counties to process")
    
    # Add configuration flags
    parser.add_argument("--local", action="store_true", 
                       help="Set local mode (use local directories for testing)")
    parser.add_argument("--full-logs", action="store_true", 
                       help="Show full county logs (don't isolate county logs)")
    parser.add_argument("--chrome", action="store_true", 
                       help="Use Chrome instead of Chromium")
    parser.add_argument("--headful", action="store_true", 
                       help="Run with browser visible (not headless)")
    parser.add_argument("--no-retry", action="store_true", 
                       help="Set attempts to 1 (no retries on failure)")
    parser.add_argument("--no-update", dest="update_csv", action="store_false",
                       help="Do not update the parcels_data_status.csv file with the results")
    
    return parser.parse_args()

# Function to expand county categories into a list of counties
def expand_county_categories(counties_and_categories):
    """
    Expands a list of county names and category names into a full list of counties.

    Args:
        counties_and_categories (list): A list containing county names and/or category names.

    Returns:
        list: A list of all counties corresponding to the provided names and categories.
    """
    expanded_counties = set()
    
    # Create a mapping from category names to county lists
    category_map = {
        "qpublic": QPUBLIC,
        "grizzly": GRIZZLY,
        "gsacorp": GSACORP,
        "opendata": OPENDATA,
        "wget": WGET,
        "other": OTHER
    }

    all_valid_counties = set(SELENIUM_COUNTIES + WGET)

    for item in counties_and_categories:
        item_lower = item.lower()
        if item_lower in category_map:
            # If the item is a category, add all counties from that category
            expanded_counties.update(category_map[item_lower])
        elif item_lower in all_valid_counties:
            # If the item is a valid county, add it to the set
            expanded_counties.add(item_lower)
        else:
            # If the item is neither a category nor a valid county, log a warning
            logging.warning(f"Unknown county or category: {item}")

    return sorted(list(expanded_counties))

# Function to get counties to process
def get_counties_to_process(manual, args=None):
    """
    Determines which counties to process based on user input or command-line arguments.

    If `manual` is True, it prompts the user to enter a comma- or space-separated list of counties.
    If `manual` is False, it uses the counties from parsed command-line arguments.
    If no counties are provided either way, it defaults to a predefined list of all valid counties.

    Args:
        manual (bool): A flag to determine the input method.
        args (argparse.Namespace, optional): Parsed command-line arguments.

    Returns:
        list: A list of lowercase county names to be processed. Returns an empty list if invalid
              county names are provided via command-line arguments.
    """
    # Define valid counties from the global lists
    valid_counties = SELENIUM_COUNTIES + WGET
    
    if manual:
        input_counties = input("Enter the counties to process, separated by commas or spaces: ")
        # First split by commas, then split each part by spaces
        counties = []
        # If no counties specified anywhere, return full list
        if not input_counties:
            print("[INFO] No counties specified, defaulting to all counties.")
            return valid_counties
        
        for part in input_counties.split(","):
            counties.extend([county.strip().lower() for county in part.split()])
        return counties
    
    # Use counties from parsed arguments
    if args and args.counties:
        logging.info(f"Using counties and categories from command line arguments: {args.counties}")
        # Expand categories and validate counties
        counties = expand_county_categories(args.counties)
        if 'okaloosa' in counties and 'okaloosa_wget' not in counties:
            counties.append('okaloosa_wget')
        
        
        invalid_counties = [
            county for county in counties 
            if county not in valid_counties
        ]

        if invalid_counties:
            print(f"[ERROR] Invalid counties found after expansion: {invalid_counties}")
            print("[INFO] Valid counties are: " + ", ".join(valid_counties))
            return []
        
        logging.info(f"Expanded to the following counties: {counties}")
        return counties
    
    # If no counties specified anywhere, return full list
    print("[INFO] No counties specified, defaulting to all counties.")
    return valid_counties

# Function to initialize CSV data
def initialize_data(csv_name, base_dir, all_counties, logger):
    """
    Load existing CSV into a list of dictionaries or create a new one if it doesn't exist.
    
    Args:
        csv_name (str): Filename of the CSV file relative to BASE_LOG_DIR
    
    Returns:
        tuple: (data, full_path) where data is list of dictionaries and full_path is the complete file path
    """
    data = []
    if not os.path.exists(base_dir):
        raise Exception(f"Base log directory {base_dir} does not exist")
    
    full_path = os.path.join(base_dir, csv_name)
    
    if os.path.exists(full_path):
        # Load existing CSV
        try:
            with open(full_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                data = list(reader)
            if data == []:
                logger.debug(f"CSV exists but is empty")
            else:
                logger.debug(f"Loaded existing CSV with {len(data)} rows")
                # Ensure all counties are present
                existing_counties = {row['county'] for row in data}
                for county in all_counties:
                    if county not in existing_counties:
                        data.append({
                            'county': county,
                            'data_date': '',
                            'download_status': '',
                            'processing_status': '',
                            'QA_status': '',
                            'error_message': ''
                        })
                        logger.debug(f"Added missing county {county} to CSV data.")
        except Exception as e:
            logger.error(f"Error reading existing CSV: {e}, will create a new one.")
            data = [] # Reset data to create new file
    else:
        logger.debug(f"No existing CSV found at {full_path}")
    
    # Create new CSV with specified columns if CSV is empty or nonexistent
    if not data:
        for county in all_counties:
            data.append({
                'county': county,
                'data_date': '',
                'download_status': '',
                'processing_status': '',
                'QA_status': '',
                'error_message': ''
            })
        logger.debug(f"Generated new CSV data structure.")
    
    logger.info(f"Initialized data for {csv_name}")
    return data, full_path

# Function to save CSV data to file
def save_csv(data, full_path, logger, runtime_seconds=0):
    """
    Save list of dictionaries to CSV file, sorted alphabetically by county,
    and add a summary row at the bottom.
    
    Args:
        data (list): List of dictionaries to save
        full_path (str): Complete path to the CSV file
        logger: Logger instance
        runtime_seconds (float): Runtime in seconds for summary row
    """
    
    if not data:
        # If no data, create empty CSV with headers
        with open(full_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(COLUMNS)
        logger.debug(f"Created empty CSV with headers: {COLUMNS}")
        return
    
    # Find existing summary row to preserve other scripts' timestamp values (BEFORE filtering)
    existing_summary = next((row for row in data if row.get('county') == 'summary'), None)
    
    # Sort data by county name alphabetically
    # Filter out summary row if it exists before sorting
    data = sorted([row for row in data if row.get('county') != 'summary'], key=lambda x: x.get('county', ''))

    # Ensure all rows have all columns to prevent DictWriter errors
    for row in data:
        for col in COLUMNS:
            row.setdefault(col, '')

    try:
        with open(full_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(data)
            
            # Calculate summary statistics for all status columns
            successful_downloads = sum(1 for row in data if row.get('download_status') == 'SUCCESS')
            failed_downloads = sum(1 for row in data if row.get('download_status') == 'FAILED')
            total_downloads = successful_downloads + failed_downloads
            
            successful_processing = sum(1 for row in data if row.get('processing_status') == 'SUCCESS')
            failed_processing = sum(1 for row in data if row.get('processing_status') == 'FAILED')
            total_processing = successful_processing + failed_processing
            
            successful_qa = sum(1 for row in data if row.get('QA_status') == 'SUCCESS')
            failed_qa = sum(1 for row in data if row.get('QA_status') == 'FAILED')
            total_qa = successful_qa + failed_qa
            
            # Format runtime string
            def format_runtime(seconds):
                if seconds <= 0:
                    return ''
                hours = int(seconds // 3600)
                minutes = int((seconds % 3600) // 60)
                secs = int(seconds % 60)
                
                parts = []
                if hours > 0:
                    parts.append(f"{hours}h")
                if minutes > 0:
                    parts.append(f"{minutes}m")
                if secs > 0 or not parts:  # Always show seconds if no other parts
                    parts.append(f"{secs}s")
                return ' '.join(parts)
            
            # Create and write the summary row
            summary_row = {
                'county': 'summary',
                'data_date': 'last updated: ' + time.strftime("%Y-%m-%d %H:%M:%S"),
                'download_status': f"{successful_downloads}/{total_downloads}" if total_downloads > 0 else '0/0',
                'processing_status': f"{successful_processing}/{total_processing}" if total_processing > 0 else '0/0',
                'QA_status': f"{successful_qa}/{total_qa}" if total_qa > 0 else '0/0',
                'error_message': '',
                'download_date': format_runtime(runtime_seconds),
                'processing_date': existing_summary.get('processing_date', '') if existing_summary else '',
                'QA_date': existing_summary.get('QA_date', '') if existing_summary else ''
            }
            writer.writerow(summary_row)
            
        logger.info(f"Saved sorted CSV with {len(data)} rows and a summary row to {full_path}")
    except Exception as e:
        logger.error(f"Failed to save CSV to {full_path}: {e}")

# Function to wrap error messages for better CSV display
def wrap_error_message(error_msg, width=200):
    """
    Wraps error messages to specified width with line breaks for better CSV cell display.
    
    Args:
        error_msg (str): The error message to wrap
        width (int): Maximum characters per line (default: 60)
    
    Returns:
        str: Wrapped error message with \n line breaks
    """
    if not error_msg:
        return error_msg
    # Use textwrap to break long lines, preserving existing line breaks
    wrapped_lines = []
    for line in str(error_msg).splitlines():
        wrapped_lines.extend(textwrap.wrap(line, width=width, break_long_words=True))
    return '\n'.join(wrapped_lines)

# Function to safely parse date strings
def format_date(date_str):
    if not date_str:
        return ""
    
    # Try parsing MM/DD/YYYY
    try:
        return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
    except ValueError:
        pass
    
    # Add other formats here if needed, e.g. from opendata
    # 'March 31, 2025'
    try:
        return datetime.strptime(date_str, '%B %d, %Y').strftime('%Y-%m-%d')
    except ValueError:
        pass

    # Return original string if no format matches
    return date_str

# Function to check if a download is necessary based on data date
def should_download(county_row, new_data_date_str, county_logger):
    """
    Checks if a download is necessary by comparing the new data date with the last recorded date.
    Returns True if a download should proceed, False otherwise.
    """

    # Helper to normalize various date string formats into date objects
    def parse_date(date_str):
        # Handles formats like '2024-07-26', '07/26/2024', and 'July 26, 2024'
        for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%B %d, %Y'):
            try:
                return datetime.strptime(date_str, fmt).date()
            except (ValueError, TypeError):
                continue
        return None

    if not new_data_date_str:
        county_logger.warning("No new data date provided for comparison. Proceeding with download.")
        return True

    last_data_date_str = county_row.get('data_date')
    if not last_data_date_str:
        county_logger.info("No previous data date found. Proceeding with download.")
        return True

    last_date = parse_date(last_data_date_str)
    new_date = parse_date(new_data_date_str)

    if not last_date or not new_date:
        county_logger.warning(f"Could not parse dates for comparison (last: '{last_data_date_str}', new: '{new_data_date_str}'). Proceeding with download.")
        return True

    if new_date == last_date:
        county_logger.info(f"Data is up to date. Last downloaded date ({last_date.strftime('%Y-%m-%d')}) matches current source date.")
        return False
    else:
        county_logger.info(f"New data available. Last downloaded date: {last_date.strftime('%Y-%m-%d')}, Current source date: {new_date.strftime('%Y-%m-%d')}.")
        return True

# Function to define and map download functions
def download_county(county_name, county_name_formatted, driver, county_logger, download_dir, main_window, initial_window_count, csv_data):
    """
    Orchestrates the download of parcel data for a specific county by calling the appropriate
    download function based on the county's category (e.g., QPublic, Grizzly).

    Args:
        county_name (str): The name of the county to process.
        driver: The Selenium WebDriver instance.
        county_logger: The logger instance for the specific county.
        download_dir (str): The directory where files will be downloaded.
        main_window: The handle for the main browser window.
        initial_window_count (int): The number of browser windows open at the start.
        csv_data: list # Pass the full CSV data into the context

    Returns:
        dict: A result dictionary containing the status of the download ('SUCCESS' or 'FAILED'),
              an error message if applicable, the data date, and file count information.
    """
    # Define county categories are now global

    # Define critical error class
    class CriticalError(Exception):
        """
        Custom exception for critical errors that should halt execution.
        """
        pass


    # Set context to package common variables needed for the download functions
    county_config = load_county_config(county_name, county_name_formatted)
    county_logger.info(f"Loaded configuration for {county_name_formatted}")
    
    @dataclass
    class CountyContext:
        driver: object
        county_name: str
        county_name_formatted: str
        county_logger: object  # add more as needed
        county_config: object
        download_dir: str
        local: bool
        manual: bool
        main_window: object
        initial_window_count: int
        csv_data: list # Pass the full CSV data into the context

    context = CountyContext(driver, county_name, county_name_formatted, county_logger, county_config, download_dir, local, manual, main_window, initial_window_count, csv_data)

    
    # Define download functions
    def download_qpublic(context):
        """
        Downloads parcel data for counties using the QPublic platform.

        This function automates the process of navigating the QPublic website,
        accepting disclaimers, determining the appropriate date range, setting search filters,
        and downloading the data in chunks to avoid server timeouts.

        Args:
            context (CountyContext): An object containing all necessary parameters and objects
                                     for the download, such as the driver, logger, and configuration.

        Returns:
            dict: A dictionary containing the download status, data date, and file count.

        Raises:
            CriticalError: If a step in the process fails in a way that prevents continuation.
        """
            
        # Helper: open the page and handle disclaimers
        def open_page_and_accept_disclaimer(context):
            """Navigates to the county's QPublic URL and handles disclaimer pop-ups."""
            url = context.county_config["url"]
            driver = context.driver
            county_name = context.county_name
            selectors = context.county_config["selectors"]
            county_logger = context.county_logger

            # Navigate to the page
            county_logger.info(f"Attempting to navigate to URL: {url}")
            driver.get(url)

            # Click the disclaimer acceptance button
            disclaimer_button_1 = safe_find(context, "disclaimer_button")
            safe_click(context, disclaimer_button_1, "disclaimer_button", specific_click="click")

            # Click the second disclaimer button if it exists
            if "disclaimer_button_2" in selectors:
                county_logger.info(f"Attempting to click second disclaimer button for {county_name.capitalize()}.")
                disclaimer_button_2 = safe_find(context, "disclaimer_button_2")
                safe_click(context, disclaimer_button_2, "disclaimer_button_2", specific_click="click")
            else:
                county_logger.info(f"No second disclaimer button needed for {county_name.capitalize()}.")

            # Wait for the search button to confirm page load
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selectors["search_button"]))
            )
            county_logger.info("Successfully opened page and handled disclaimer.")

        # Helper: get the date range from the page
        def get_date_range(context):
            """
            Reads the 'data date' from the QPublic webpage and calculates a date range for the search.

            The end date is the 'data date' displayed on the page. The start date is calculated
            as 90 days prior to the end date, then set to the first day of that month.

            Args:
                context (CountyContext): The context object for the county.

            Returns:
                tuple: A tuple (start_date_str, end_date_str) with dates formatted as 'MM/DD/YYYY'.
            
            Raises:
                CriticalError: If the data date element cannot be found or the date cannot be parsed.
            """
            driver = context.driver
            county_logger = context.county_logger
            
            try:
                # Locate the element that displays the 'Last Data Upload' text COMMENT OUT FOR MANUAL
                data_date_element = driver.find_element(By.CSS_SELECTOR, "#hlkLastUpdated")
                data_date_text = data_date_element.text.strip()
                county_logger.info(f"Data date text found on page: {data_date_text}")

                # Extract the date portion from the string (before the comma) COMMENT OUT FOR MANUAL
                data_date_str = data_date_text.split(":")[1].split(",")[0].strip()
                end_date = datetime.strptime(data_date_str, '%m/%d/%Y')

                # Calculate the start date: first day of the month, 3 months prior COMMENT OUT FOR MANUAL
                three_months_prior = end_date - timedelta(days=90)  # Approximation for 3 months
                start_date = datetime(three_months_prior.year, three_months_prior.month, 1)

                # Format the dates as strings
                start_date_str = start_date.strftime('%m/%d/%Y')
                end_date_str = end_date.strftime('%m/%d/%Y')

                county_logger.info(f"Calculated date range: {start_date_str} to {end_date_str}")
                return start_date_str, end_date_str

            except Exception as e:
                county_logger.error(f"Error reading or calculating date range: {e}")
                debug_screenshot(context, "error_date_range")
                raise CriticalError(f"Could not read or calculate date range: {e}")
        
        # Helper: set filters
        def set_filters(context, start_date_str, end_date_str):
            """Sets the search filters on the QPublic sales search page."""
            county_logger = context.county_logger
            price_low = 0
            price_high = 9999999999
            qualified_sales_selector = context.county_config["selectors"].get("qualified_sales_button")
            date_range_button_selector = context.county_config["selectors"].get("date_range_button")

            try:
                # Optional step: Click the date range button, if provided
                if date_range_button_selector:
                    county_logger.info(f"Trying to click the date range button with selector: {date_range_button_selector}")
                    date_range_button = safe_find(context, "date_range_button", critical=False)
                    if date_range_button:
                        safe_click(context, date_range_button, "date_range_button")
                else:
                    county_logger.info("No date range button specified for this county, skipping step.")

                # Set the date range
                start_date_input = safe_find(context, "start_date_input")
                end_date_input = safe_find(context, "end_date_input")
                if start_date_input and end_date_input:
                    safe_input(context, start_date_input, "start_date_input", start_date_str, specific_method="regular")
                    safe_input(context, end_date_input, "end_date_input", end_date_str, specific_method="regular")
                else:
                    raise CriticalError("No start or end date input found.")

                # Set the price range
                price_low_input = safe_find(context, "price_low_input")
                price_high_input = safe_find(context, "price_high_input")
                if price_low_input and price_high_input:
                    safe_input(context, price_low_input, "price_low_input", str(price_low), specific_method="regular")
                    safe_input(context, price_high_input, "price_high_input", str(price_high), specific_method="regular")
                else:
                    raise CriticalError("No price low or high input found.")
                #time.sleep(3)

                # Only click the qualified sales checkbox if the selector is provided
                if qualified_sales_selector:
                    qualified_sales_button = safe_find(context, "qualified_sales_button")
                    if qualified_sales_button:
                        safe_click(context, qualified_sales_button, "qualified_sales_button")
                    county_logger.info("Qualified sales filter set.")
                else:
                    county_logger.info("No qualified sales button for this county.")

                county_logger.info("Filters set successfully.")
            except Exception as e:
                county_logger.error(f"Error setting filters: {e}")
                debug_screenshot(context, "error_screenshot")
                raise CriticalError(f"Error setting filters: {e}")
        
        # Helper: perform sale search
        def perform_sale_search(context, chunk_start_date, chunk_end_date):
            """Initiates the search and downloads the resulting CSV file."""
            county_logger = context.county_logger
            
            try:
                # Click the search button
                search_button = safe_find(context, "search_button")
                county_logger.info(f"Clicking search button for {context.county_name.capitalize()}.")
                safe_click(context, search_button, "search_button", specific_click="js")
                county_logger.info(f"Search for sales between {chunk_start_date} and {chunk_end_date} initiated.")

                # Wait for the dropdown menu to be clickable
                dropdown_button = safe_find(context, "dropdown_menu", critical=False)
                safe_click(context, dropdown_button, "dropdown_menu", specific_click="click")
                county_logger.info("Dropdown menu opened.")

                # Wait for the CSV option to be clickable
                csv_option = safe_find(context, "csv_option")
                safe_click(context, csv_option, "csv_option", specific_click="click")
                county_logger.info("CSV option selected.")

                # Click the download button
                download_button = safe_find(context, "download_button")
                safe_click(context, download_button, "download_button", specific_click="click")
                county_logger.info("Download initiated.")

                # Set input and target filenames
                input_filename = f"{context.county_name.capitalize()}CountyFL-*.csv"
                # Convert dates from MM/DD/YYYY to YYYY-MM-DD format
                start_dt = datetime.strptime(chunk_start_date, '%m/%d/%Y')
                end_dt = datetime.strptime(chunk_end_date, '%m/%d/%Y')
                start_date_formatted = start_dt.strftime('%Y-%m-%d')
                end_date_formatted = end_dt.strftime('%Y-%m-%d')
                target_filename = f"{context.county_name.capitalize()}CountyFL_{start_date_formatted}_{end_date_formatted}.csv"

                # Wait for download
                full_path, file_name = wait_for_download(context, input_filename, timeout=120)

                # Rename file
                renamed_file = rename_file(context, file_name, target_filename)
                temp_files.append(os.path.basename(renamed_file))
                county_logger.info(f"Added {os.path.basename(renamed_file)} to temporary file list")

            except Exception as e:
                county_logger.error(f"Error performing sale search: {e}")
                raise CriticalError(f"Error performing sale search: {e}")

        # Helper: download mailing list
        def download_mailing_list(context):
            """Downloads the mailing list from the results page."""
            county_logger = context.county_logger

            try:
                # Click the results tab
                results_tab = safe_find(context, "results_tab")
                county_logger.info(f"Clicking results tab for {context.county_name.capitalize()}.")
                safe_click(context, results_tab, "results_tab", specific_click="click")
                county_logger.info("Results tab clicked.")

                # If a show all owners button is provided, make sure it is clicked
                if "show_all_owners_button" in context.county_config["selectors"]:
                    show_all_owners_button = safe_find(context, "show_all_owners_button", critical=False)
                    if show_all_owners_button and not show_all_owners_button.is_selected():
                        safe_click(context, show_all_owners_button, "show_all_owners_button", specific_click="click")
                        county_logger.info("Show all owners button clicked.")

                # Wait for the dropdown menu to be clickable
                dropdown_button = safe_find(context, "dropdown_menu_results", critical=False)
                safe_click(context, dropdown_button, "dropdown_menu_results", specific_click="click")
                county_logger.info("Dropdown menu opened.")

                # Wait for the CSV option to be clickable
                csv_option = safe_find(context, "csv_option_results")
                safe_click(context, csv_option, "csv_option_results", specific_click="click")
                county_logger.info("CSV option selected.")

                # Click the download button
                download_button = safe_find(context, "download_button_results")
                safe_click(context, download_button, "download_button_results", specific_click="click")
                county_logger.info("Download initiated.")

                # Set input and target filenames
                input_filename = f"{context.county_name.capitalize()}CountyFL-*.csv"
                # Convert dates from MM/DD/YYYY to YYYY-MM-DD format
                start_dt = datetime.strptime(chunk_start_date, '%m/%d/%Y')
                end_dt = datetime.strptime(chunk_end_date, '%m/%d/%Y')
                start_date_formatted = start_dt.strftime('%Y-%m-%d')
                end_date_formatted = end_dt.strftime('%Y-%m-%d')
                target_filename = f"{context.county_name.capitalize()}CountyFL_MailingList_{start_date_formatted}_{end_date_formatted}.csv"


                # Wait for download
                full_path, file_name = wait_for_download(context, input_filename, timeout=120)

                # Rename file
                renamed_file = rename_file(context, file_name, target_filename)
                temp_files.append(os.path.basename(renamed_file))
                county_logger.info(f"Added {os.path.basename(renamed_file)} to temporary file list")

            except Exception as e:
                county_logger.error(f"Error downloading mailing list: {e}")
                raise CriticalError(f"Error downloading mailing list: {e}")

        # Main logic
        try:
            # Navigate to the page and handle the disclaimer
            open_page_and_accept_disclaimer(context)

            # Get date range
            start_date_str, end_date_str = get_date_range(context)

            # Check if we need to download
            county_row = next((row for row in context.csv_data if row['county'] == context.county_name), None)
            if not should_download(county_row, end_date_str, context.county_logger):
                return {'status': 'NND', 'data_date': end_date_str}

            # start_date_str = '09/01/2024'
            # end_date_str = datetime.now().strftime('%m/%d/%Y')

            start_date = datetime.strptime(start_date_str, '%m/%d/%Y')
            end_date = datetime.strptime(end_date_str, '%m/%d/%Y')
            date_chunks = split_date_range(context, start_date, end_date, 8)
            county_logger.info(f"Date chunks: {date_chunks}")

            temp_files = []  # Local list to collect renamed files

            # Loop through date chunks
            for chunk_start_date, chunk_end_date in date_chunks:
                county_logger.info(f"Processing date range: {chunk_start_date} to {chunk_end_date}")

                try:
                    # Navigate to the page
                    driver.get(context.county_config["url"])

                    # Set filters
                    set_filters(context, chunk_start_date, chunk_end_date)

                    # Perform sale search
                    perform_sale_search(context, chunk_start_date, chunk_end_date)

                    # Download mailing list
                    download_mailing_list(context)
                    
                    county_logger.info(f"Successfully processed date range: {chunk_start_date} to {chunk_end_date}")

                except CriticalError as e:
                    county_logger.error(f"Failed to process chunk {chunk_start_date}-{chunk_end_date}: {e}. Skipping chunk.")
                    continue

            county_logger.info(f"Script finished. {county_name.capitalize()} data date: {end_date_str}")

            # After the loop, transfer all files
            transfer_files(context, temp_files)

            return {
                'status': 'SUCCESS', 
                'data_date': end_date_str,
                'file_count': len(temp_files),
                'file_count_status': 'SUCCESS' if len(temp_files) > 5 else 'FAILED'}

        except Exception as e:
            if len(temp_files) > 0: # If files were downloaded, transfer them
                transfer_files(context, temp_files)
            raise CriticalError(f"Unexpected error: {type(e).__name__} - {e}")
    
    def download_grizzly(context):
        """
        Downloads parcel and sales data for counties using the Grizzly platform.

        This function navigates through the Grizzly web interface, which often uses frames and pop-up windows.
        It extracts the data date to determine which years of data to download, sets the necessary filters,
        and downloads both mailing list and sales data files.

        Args:
            context (CountyContext): An object containing all necessary parameters.

        Returns:
            dict: A dictionary containing the download status, data date, and file count.

        Raises:
            CriticalError: For failures in critical steps like navigation or file download.
        """

        # Helper: handle popup windows
        def handle_popup(context):
            """Waits for and switches to a new pop-up window, then switches to the main frame within it."""
            
            # Define variables
            wait_time = 10
            download_window = None
            driver = context.driver
            county_logger = context.county_logger
            main_window = context.main_window
            initial_window_count = context.initial_window_count

            # Check if popup window has appeared
            for i in range(wait_time):
                time.sleep(0.5)
                current_handles = driver.window_handles
                if len(current_handles) > initial_window_count:
                    # Find the new window handle
                    download_window = [handle for handle in current_handles if handle != main_window][0]
                    county_logger.info(f"New window detected after {i+1} seconds")
                    break
                county_logger.info(f"Waiting for new window... ({i+1}/{wait_time})")

            if download_window:
                # Switch to the new window
                driver.switch_to.window(download_window)
                county_logger.info(f"Switched to new window. Title: {driver.title}")

                # Switch to main frame
                driver.switch_to.default_content()
                county_logger.info(f"Switched to main frame.")
                return download_window
            else:
                county_logger.warning("No new window detected.")

        # Function to wait for download
        def wait_and_convert(context, type, year, timeout=10, check_interval=0.5):
            """
            Waits for a specific .tab file to be downloaded, converts it to UTF-8, and renames it.

            Grizzly downloads files with a generic name. This function identifies the correct
            downloaded file based on its expected name pattern, waits for it to complete,
            reads its content, saves it as a new UTF-8 encoded file with a descriptive name,
            and then deletes the original.

            Args:
                context (CountyContext): The context object for the county.
                type (str): The type of file to download (e.g., "mailing", "detailed").
                year (int): The year of the data being downloaded.
                timeout (int): Maximum time to wait for the download in seconds.
                check_interval (float): Interval between checks for the downloaded file.

            Returns:
                str: The path to the newly created and renamed file.

            Raises:
                CriticalError: If the download does not complete within the specified timeout.
            """
            download_dir = context.download_dir
            county_name = context.county_name
            county_name_debug = context.county_name.capitalize()
            county_logger = context.county_logger
        
            # Get file pattern
            if county_name != "bradford":
                if type == "mailing":
                    file_pattern = f"{county_name_debug}PA_Mailing_List.tab"
                elif type == "detailed":
                    file_pattern = f"{county_name_debug}PA_Detailed_Sales.tab"
                    type = "sales"
                elif type == "brief":
                    file_pattern = f"{county_name_debug}PA_Brief_Sales.tab"
                    type = "briefsales"
            else:
                if type == "mailing":
                    file_pattern = f"{county_name_debug}Appraiser_Mailing_List.tab"
                elif type == "detailed":
                    file_pattern = f"{county_name_debug}Appraiser_Detailed_Sales.tab"
                    type = "sales"
                elif type == "brief":
                    file_pattern = f"{county_name_debug}Appraiser_Brief_Sales.tab"
                    type = "briefsales"
            
            county_logger.info(f"Waiting for {file_pattern}.")
            
            start_time = time.time()
            while time.time() - start_time < timeout:
                # Wait for specified interval
                time.sleep(check_interval)
                
                # Filter by pattern if specified
                if file_pattern:
                    # Get current time to calculate file age
                    current_time = time.time()
                    
                    # Filter files that match pattern AND were created in the last minute (60 seconds)
                    recent_matching_files = []
                    for filename in os.listdir(download_dir):
                        if file_pattern in filename:
                            file_path = os.path.join(download_dir, filename)
                            file_creation_time = os.path.getctime(file_path)
                            # Only include files created in the last minute (60 seconds)
                            if current_time - file_creation_time <= 60:
                                recent_matching_files.append(filename)
                    
                    if recent_matching_files:
                        # Get the newest file based on creation time
                        newest_file = max(
                            recent_matching_files, 
                            key=lambda f: os.path.getctime(os.path.join(download_dir, f))
                        )
                        full_path = os.path.join(download_dir, newest_file)
                        county_logger.info(f"Download detected: {file_pattern} after {time.time() - start_time:.1f} seconds")
                        
                        # Check if file is completely downloaded (not a temporary file)
                        if not newest_file.endswith('.crdownload') and not newest_file.endswith('.part'):
                                
                            # Create new filename with county name, type, and year
                            new_filename = f"{county_name.replace(' ', '_')}_{type}_{year}.txt"
                            new_path = os.path.join(download_dir, new_filename)

                            # Wait a moment to ensure file is fully written
                            time.sleep(0.5)
                            
                            # Convert content and save as new file
                            with open(full_path, 'r', encoding='utf-8', errors='replace') as src_file:
                                content = src_file.read()

                            # Write to new file
                            with open(new_path, 'w', encoding='utf-8') as dest_file:
                                dest_file.write(content)
                            
                            # Delete the original file after successful conversion
                            os.remove(full_path)
                            
                            county_logger.info(f"Converted and renamed file to: {new_filename}")
                            return new_path
                
                # Log progress occasionally
                if int((time.time() - start_time) * 2) % 4 == 0:  # Log every 2 seconds
                    county_logger.info(f"Still waiting for download... ({time.time() - start_time:.1f}s elapsed)")
            
            county_logger.warning(f"Download timeout after {timeout} seconds")
            raise CriticalError(f"Download timeout after {timeout} seconds")

        # Helper: extract data date
        def find_data_date(context):
            """
            Finds and extracts the data date from the Grizzly page to determine which years to download.

            It searches the page source for date patterns (MM/DD/YYYY). If the month of the found
            date is March or earlier, it concludes that data for the previous year should also
            be downloaded.

            Args:
                context (CountyContext): The context object for the county.

            Returns:
                tuple: A tuple containing:
                    - data_date_text (str): The full text of the found data date.
                    - main_year (int): The primary year extracted from the data date.
                    - prev_year (int or None): The previous year, if it needs to be processed.

            Raises:
                CriticalError: If no date can be found or parsed from the page.
            """
            driver = context.driver
            county_logger = context.county_logger
            
            data_date_text = ""
            main_year = None
            prev_year = None
            
            try:
                # Search for date patterns in the page source
                page_text = driver.page_source
                
                # First, look for any text containing the date pattern MM/DD/YYYY
                date_patterns = re.findall(r'(\d{1,2}/\d{1,2}/\d{4})', page_text)
                if date_patterns:
                    # Use the first date pattern found
                    data_date_text = date_patterns[0]
                    county_logger.info(f"Found date in page: {data_date_text}")
                    
                    # Extract the year from the date
                    year_match = re.search(r'/(\d{4})', data_date_text)
                    if year_match:
                        main_year = int(year_match.group(1))
                        county_logger.info(f"Extracted year {main_year} from date {data_date_text}")
                    else:
                        # If year can't be extracted, use current year
                        raise CriticalError("Couldn't extract year from date")
                # Commented out to reduce overhead, remove after testing all grizzly counties
                #else:
                    # If no date found, look for elements with "updated" or "date" text
                    #date_elements = []
                    #try:
                        # Look in spans, divs, and paragraphs that might contain date info
                        #for tag in ['span', 'div', 'p', 'td']:
                            #elements = driver.find_elements(By.TAG_NAME, tag)
                            #for element in elements:
                                #try:
                                    #text = element.text.strip()
                                    #if text and ('date' in text.lower() or 'update' in text.lower()):
                                        #date_elements.append(text)
                                        #logging.info(f"Found potential date element: {text}")
                                #except Exception as elem_e:
                                    #continue
                    #except Exception as tag_e:
                        #logging.error(f"Error searching for date elements: {tag_e}")
                    
                    #if date_elements:
                        # Try to find a date pattern in the collected date elements
                        #for text in date_elements:
                            #date_matches = re.findall(r'(\d{1,2}/\d{1,2}/\d{4})', text)
                            #if date_matches:
                                #data_date_text = date_matches[0]
                                #logging.info(f"Found date in element: {data_date_text}")
                                
                                # Extract the year from the date
                                #year_match = re.search(r'/(\d{4})', data_date_text)
                                #if year_match:
                                    #main_year = int(year_match.group(1))
                                    #logging.info(f"Extracted year {main_year} from date {data_date_text}")
                                    #break

                # Extract year from data_date_text if no year found
                #if main_year is None and data_date_text:
                    # Try to find a 4-digit year in the text
                    #import re
                    #match = re.search(r"\d{4}", data_date_text)
                    #if match:
                        #main_year = int(match.group(0))
                        #logging.info(f"Extracted year {main_year} from data date text")
                    #else:
                        # Default to current year
                        #main_year = datetime.now().year
                        #logging.info(f"Defaulting to current year: {main_year}")
                
                # Final fallback
                #if main_year is None:
                    #main_year = datetime.now().year
                    #logging.info(f"Final fallback to current year: {main_year}")
                
                #if not data_date_text:
                    #data_date_text = f"Default year: {main_year}"

                # Parse month from data_date_text to decide which years to download (move to find_data_date)
                try:
                    # Try to get month (either as word or number)
                    month_num = None
                    # Check for numeric month (e.g. 02/2023 or 2/2023 etc.)
                    date_match = re.search(r"(\d{1,2})\D+(\d{1,2})?\D+(\d{4})", data_date_text)
                    # This matches patterns like 3/10/2025 or 3-10-2025 or 3.10.2025
                    if date_match:
                        # month is the first group
                        month_num = int(date_match.group(1))
                        county_logger.info(f"Month found: {month_num}")
                    else:
                        raise CriticalError("Could not parse month from data_date text")
                    # If month is found and <= 3 (Jan, Feb, Mar), include previous year
                    if month_num and month_num <= 3 and main_year is not None:
                        prev_year = main_year - 1
                        county_logger.info(f"Month number {month_num} <= 3, previous year added: {prev_year}")
                except Exception:
                    raise CriticalError("Error parsing data_date text")

                if prev_year:
                    county_logger.info(f"Years found: {main_year}, {prev_year}")
                else:
                    county_logger.info(f"Year found: {main_year}")

            except Exception:
                raise CriticalError("Error in find_data_date")
            
            return data_date_text, main_year, prev_year

        # Helper: access search page (switches to sales report iframe)#
        def access_search_page(context, specific_click):
            """Navigates to the sales report search page, handling disclaimers and iframes."""
            # Define variables
            driver = context.driver
            county_logger = context.county_logger
            county_name = context.county_name
            url = context.county_config["url"]
            frames = context.county_config["frames"]

            # Step 1: Navigate to the county's sales data website
            county_logger.info(f"Navigating to {county_name.capitalize()}")
            driver.get(url)

            time.sleep(1)

            # Step 4: Handle disclaimer pop-up if it appears
            disclaimer_button = safe_find(context, "disclaimer_button")
            if disclaimer_button:
                safe_click(context, disclaimer_button, "disclaimer_button", specific_click=specific_click)
            else:
                county_logger.info(f"Disclaimer button not found.")
                raise CriticalError(f"Disclaimer button not found.")

            # Switch to sales report iframe
            driver.switch_to.frame(frames["sales_report_frame"])
            county_logger.info(f"Switched to sales iframe.")

            # Step 5: Click sales report button if required to reach search page
            sales_report_button = safe_find(context, "sales_report")
            safe_click(context, sales_report_button, "sales_report", specific_click=specific_click)

        # Helper: set filters
        def set_filters(context, specific_click):
            """Sets the search filters on the Grizzly sales report page."""
            # Define variables
            county_logger = context.county_logger

            # Check for last sale only checkbox
            county_logger.info(f"Checking for last sale only checkbox...")
            last_sale_only = safe_find(context, "last_sale_box")
            if last_sale_only and last_sale_only.is_selected():
                safe_click(context, last_sale_only, "last_sale_only", specific_click=specific_click)
                county_logger.info("Last Sale Only checkbox unchecked.")
            else:
                county_logger.info("Last Sale Only checkbox already unchecked.")

            # Check sales price checkbox
            county_logger.info(f"Checking for sales price checkbox...")
            sales_price_box = safe_find(context, "sales_price_box")
            if sales_price_box and sales_price_box.is_selected():
                safe_click(context, sales_price_box, "sales_price_box", specific_click=specific_click)
                county_logger.info("Sales Price checkbox unchecked.")
            else:
                county_logger.info("Sales Price checkbox already unchecked.")

            # Check entire year radio button
            county_logger.info(f"Checking for entire year radio button...")
            entire_year_radio = safe_find(context, "entire_year")
            if entire_year_radio and not entire_year_radio.is_selected():
                safe_click(context, entire_year_radio, "entire_year", specific_click=specific_click)
                county_logger.info("Entire Year button checked.")
            else:
                county_logger.info("Entire Year button already checked.")

        # Helper: perform search and download
        def perform_search(context, year, specific_click):
            """Performs the search for a given year and downloads the mailing and sales files."""
            # Define variables
            driver = context.driver
            county_logger = context.county_logger
            county_name = context.county_name
            selectors = context.county_config["selectors"]
            download_dir = context.download_dir
            main_window = context.main_window
            initial_window_count = context.initial_window_count

            # Set search year
            county_logger.info(f"Checking for {year} year radio button...")
            year_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, f"input[name='SaleReport_Year'][value='{year}']"))
            )
            if year_btn and not year_btn.is_selected():
                county_logger.info(f"{year} button not toggled, clicking...")
                safe_click(context, year_btn, f"{year} button", specific_click=specific_click)
            else:
                county_logger.info(f"{year} button already toggled.")
                
            # Click generate report button
            county_logger.info(f"Checking for generate report button...")
            generate_report_btn = safe_find(context, "generate_report")
            if generate_report_btn:
                safe_click(context, generate_report_btn, "generate_report", specific_click=specific_click)
            else:
                raise CriticalError(f"Generate report button not found.")

            # Click download/print report button
            county_logger.info(f"Checking for download/print report button...")
            download_print_report_btn = safe_find(context, "download_print_report")
            if download_print_report_btn:
                time.sleep(1)
                safe_click(context, download_print_report_btn, "download_print_report", specific_click=specific_click)

            # Access next page
            download_window = handle_popup(context)

            # Check tab delim acsii button
            county_logger.info(f"Checking for tab delim ascii button...")
            tab_delim_ascii_btn = safe_find(context, "tab_delim_ascii")
            if tab_delim_ascii_btn and not tab_delim_ascii_btn.is_selected():
                county_logger.info("Tab Delim ASCII button not selected, clicking...")
                safe_click(context, tab_delim_ascii_btn, "tab_delim_ascii", specific_click=specific_click)
            else:
                county_logger.info("Tab Delim ASCII button already selected.")

            # Click get mailing button
            county_logger.info(f"Checking for get mailing button...")
            get_mailing_btn = safe_find(context, "get_mailing")
            if get_mailing_btn:
                safe_click(context, get_mailing_btn, "get_mailing", specific_click=specific_click)

            # Wait for mailing list download
            downloaded_file = wait_and_convert(context, "mailing", year, timeout=10, check_interval=0.5)
            if downloaded_file:
                temp_files.append(os.path.basename(downloaded_file))
                
            # Click get sales button
            county_logger.info(f"Checking for get sales button...")
            if selectors.get("get_det_sales"):
                get_det_sales_btn = safe_find(context, "get_det_sales")
                if get_det_sales_btn:
                    safe_click(context, get_det_sales_btn, "get_det_sales", specific_click=specific_click)

                # Wait for sales download
                downloaded_file = wait_and_convert(context, "detailed", year, timeout=10, check_interval=0.5)
                if downloaded_file:
                    temp_files.append(os.path.basename(downloaded_file))
                        
            elif selectors.get("get_brief_sales"):
                get_brief_sales_btn = safe_find(context, "get_brief_sales")
                if get_brief_sales_btn:
                    safe_click(context, get_brief_sales_btn, "get_brief_sales", specific_click=specific_click)

                    # Wait for sales download
                    downloaded_file = wait_and_convert(context, "brief", year, timeout=10, check_interval=0.5)
                    if downloaded_file:
                        temp_files.append(os.path.basename(downloaded_file))

            # Close download window
            driver.close()
            driver.switch_to.window(main_window)
            county_logger.info(f"Sales search for {year} completed.")

            return True



        # Define variables for Grizzly
        specific_click = "js"
        temp_files = []

        try:

            # Steps 1-5: Access search page
            access_search_page(context, specific_click)

            # Step 6: Extract the data date text from the page, determine year(s) to download
            data_date_text, main_year, prev_year = find_data_date(context)

            # Check if we need to download
            county_row = next((row for row in context.csv_data if row['county'] == context.county_name), None)
            if not should_download(county_row, data_date_text, context.county_logger):
                return {'status': 'NND', 'data_date': data_date_text}

            # Perform search and download
            county_logger.info(f"Processing {main_year}...")

            # Step 7: Set filters for search
            set_filters(context, specific_click)
                
            # Step 8: Perform search and download
            perform_search(context, main_year, specific_click)
            
            # Iteration if previous year is needed
            if prev_year:
                county_logger.info(f"Processing {prev_year}...")

                # Access search page
                access_search_page(context, specific_click)

                # Set filters for search
                set_filters(context, specific_click)
                    
                # Perform search and download
                perform_search(context, prev_year, specific_click)

            county_logger.info("Download process completed successfully.")
            
            # Transfer files
            transfer_files(context, temp_files)
            
            if prev_year:
                county_logger.info(f"{county_name.capitalize()} county successfully downloaded for {main_year} and {prev_year}. Data date: {data_date_text}")
                return {
                    'status': 'SUCCESS', 
                    'data_date': data_date_text,
                    'file_count': len(temp_files),
                    'file_count_status': 'SUCCESS' if len(temp_files) > 3 else "FAILED"
                }
            else:
                county_logger.info(f"{county_name.capitalize()} county successfully downloaded for {main_year}. Data date: {data_date_text}")
                return {
                    'status': 'SUCCESS', 
                    'data_date': data_date_text,
                    'file_count': len(temp_files),
                    'file_count_status': 'SUCCESS' if len(temp_files) > 1 else "FAILED"
                }

        except Exception as e:
            county_logger.error(f"Script encountered an error: {e}", exc_info=True)
            # Capture final state screenshot
            if len(temp_files) > 0: # If files were downloaded, transfer them
                transfer_files(context, temp_files)
            debug_screenshot(context, "final_state")
            raise CriticalError(f"Unexpected error: {str(e)}")
    
    def download_gsacorp(context):
        """
        Downloads parcel data for counties using the GSACorp platform.

        This function automates navigating the GSACorp website, calculating a rolling
        date range (from September 1st of the previous or current year to yesterday),
        setting search filters for sales data, and downloading the results as a CSV file.

        Args:
            context (CountyContext): An object containing all necessary parameters.

        Returns:
            dict: A dictionary containing the download status and file count.

        Raises:
            CriticalError: For failures in critical steps like navigation or file download.
        """

        # Helper: get the date range
        def get_date_range():
            """Calculates the date range for the search."""
            
            # Determine the year for the start date
            current_date = datetime.now()
            
            # If current date is before October, use previous year
            # Otherwise, use current year
            if current_date.month < 10:  # Before October
                start_year = current_date.year - 1
            else:  # October or later
                start_year = current_date.year
                
            # Format the start date as September 1st of the determined year
            start_date = datetime(start_year, 9, 1)
            formatted_start_date = start_date.strftime("%m/%d/%Y")

            # Get end date
            current_date = datetime.now() - timedelta(days=1)
            formatted_date = current_date.strftime("%m/%d/%Y")
        
            county_logger.info(f"Date range: {formatted_start_date} - {formatted_date}")

            start_date = formatted_start_date
            end_date = formatted_date

            return start_date, end_date
        
        try:
            # Define variables
            driver = context.driver
            county_logger = context.county_logger
            county_name = context.county_name
            url = context.county_config["url"]
            selectors = context.county_config["selectors"]
            filename = context.county_config["filename"]
            temp_files = []

            # Open the URL  
            driver.get(url)

            # Get date range
            start_date, end_date = get_date_range()

            # Wait for page to load
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, selectors["sale_search"])))

            # Set filters
            start_date_input = driver.find_element(By.CSS_SELECTOR, selectors["start_date_input"])
            start_date_input.send_keys(start_date)
            county_logger.info(f"Set start date to {start_date}")
            end_date_input = driver.find_element(By.CSS_SELECTOR, selectors["end_date_input"])
            end_date_input.send_keys(end_date)
            county_logger.info(f"Set end date to {end_date}")

            price_low_input = driver.find_element(By.CSS_SELECTOR, selectors["price_low_input"])
            price_low_input.send_keys("0")
            price_high_input = driver.find_element(By.CSS_SELECTOR, selectors["price_high_input"])
            price_high_input.send_keys("9999999999")
            county_logger.info(f"Set price range")
            
            # Click search button
            search_button = driver.find_element(By.CSS_SELECTOR, selectors["sale_search"])
            search_button.click()
            county_logger.info(f"Clicked search button")

            # Wait for next page to load
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, selectors["csv_option"])))

            # Click CSV option
            csv_option = driver.find_element(By.CSS_SELECTOR, selectors["csv_option"])
            csv_option.click()
            county_logger.info(f"Clicked CSV option")
            
            # Click export button
            export_button = driver.find_element(By.CSS_SELECTOR, selectors["export"])
            export_button.click()
            county_logger.info(f"Clicked export button")

            # Wait for download to complete
            download_path, newest_file = wait_for_download(context, filename["sale_file"])

            # Convert dates from MM/DD/YYYY to YYYY-MM-DD
            start_date_formatted = datetime.strptime(start_date, "%m/%d/%Y").strftime("%Y-%m-%d")
            end_date_formatted = datetime.strptime(end_date, "%m/%d/%Y").strftime("%Y-%m-%d")

            # Rename file
            renamed_file = rename_file(context, newest_file, f"{county_name.capitalize()}CountyFL_{start_date_formatted}_{end_date_formatted}.csv")
            temp_files.append(os.path.basename(renamed_file))
            
            # Transfer files
            transfer_files(context, temp_files)
            
            # Finish function
            county_logger.info(f"{county_name.capitalize()} county successfully downloaded: {start_date} to {end_date}")
            if len(temp_files) < 1:
                file_count_status = "FAILED"
            else:
                file_count_status = "SUCCESS"
            return {
                'status': 'SUCCESS', 
                'data_date': end_date,
                'file_count': len(temp_files),
                'file_count_status': file_count_status
            }
        
        except Exception as e:
            county_logger.error(f"Unexpected error: {e}", exc_info=True)
            if len(temp_files) > 0: # If files were downloaded, transfer them
                transfer_files(context, temp_files)
            debug_screenshot(context, "Exception")
            raise CriticalError(f"Unexpected error: {str(e)}")
    
    def download_opendata(context):
        """
        Downloads parcel data for counties using an OpenData platform (ArcGIS Hub).

        This function handles modern web applications that heavily use Shadow DOM. It navigates
        to the data page, extracts the data update date, clicks through a series of tabs and
        buttons (some within nested shadow roots) to trigger the download of a shapefile.

        Args:
            context (CountyContext): An object containing all necessary parameters.

        Returns:
            dict: A dictionary containing the download status, data date, and file count.

        Raises:
            CriticalError: For failures in critical steps like finding elements or file download.
        """

        # Helper: extract data date
        def find_data_date(context):
            """Extracts the data date from the OpenData page."""
            driver = context.driver
            county_logger = context.county_logger
            selectors = context.county_config["palm_beach"]["parcels"]["selectors"]
            info_tab = context.county_config["opendata"]["selectors"]["info_tab"]

            try:
                # Get data date text
                try:
                    WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selectors["data_date"])))
                    data_date_text = driver.find_element("css selector", selectors["data_date"]).text
                    county_logger.info(f"Data date text: \"{data_date_text}\"")
                except TimeoutException:
                    county_logger.error("Error: Timeout occurred while waiting for element to be visible.")
                    county_logger.error(f"Attempting to open info tab...")
                    try:
                        info_tab = driver.find_element(By.CSS_SELECTOR, info_tab)
                        info_tab.click()
                        info_tab.click()
                        county_logger.info(f"Clicked info tab")
                    except Exception as e_1:
                        county_logger.error(f"Failed to click info tab: {e_1}")
                        debug_screenshot(context, "info_tab_not_found")
                        raise CriticalError(f"Failed to click info tab: {e_1}")
                    try:
                        WebDriverWait(driver, 3).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selectors["data_date"])))
                        data_date_text = driver.find_element("css selector", selectors["data_date"]).text
                        county_logger.info(f"Data date text: \"{data_date_text}\"")
                    except Exception as e_2:
                        county_logger.error(f"Failed to extract data date text: {e_2}")
                        debug_screenshot(context, "data_date_text_not_found")
                        raise CriticalError(f"Failed to extract data date text: {e_2}")
                except Exception as e_3:
                    county_logger.error(f"Failed to extract data date text: {e_3}")
                    debug_screenshot(context, "data_date_text_not_found")
                    raise CriticalError(f"Failed to extract data date text: {e_3}")

                # Extract date and time
                # Extract date from text (format like "March 31, 2025")
                date_pattern = r'([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})'
                match = re.search(date_pattern, data_date_text)

                if not match:
                    county_logger.warning(f"Could not extract date from text: \"{data_date_text}\"")
                    county_logger.warning(f"Attempting alternate date source...")
                    try:
                        WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selectors["data_date_alt"])))
                        data_date_text = driver.find_element("css selector", selectors["data_date_alt"]).text
                        county_logger.info(f"Data date text: \"{data_date_text}\"")
                        date_pattern = r'([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})'
                        match = re.search(date_pattern, data_date_text)
                    except Exception as e:
                        county_logger.error(f"Failed to extract data date text: {e}")
                        debug_screenshot(context, "date_extraction_failed")
                        raise CriticalError(f"Failed to extract data date text: {e}")
                    
                if match:
                    month_name, day, year = match.groups()
                    
                    # Convert month name to number
                    month_dict = {
                        'January': '01', 'February': '02', 'March': '03', 'April': '04',
                        'May': '05', 'June': '06', 'July': '07', 'August': '08',
                        'September': '09', 'October': '10', 'November': '11', 'December': '12'
                    }
                    
                    # Get month number
                    month = month_dict.get(month_name, '01')  # Default to 01 if not found
                    
                    # Format day with leading zero if needed
                    day = day.zfill(2)
                    
                    # Format as MM/DD/YYYY
                    data_date = f"{month}/{day}/{year}"
                    county_logger.info(f"Extracted date: {data_date}")
                    
                    return data_date
                else:
                    county_logger.warning(f"Failed to extract date from text: \"{data_date_text}\"")
                    debug_screenshot(context, "date_extraction_failed")
                    raise CriticalError(f"Failed to extract date from text: {data_date_text}")

            except NoSuchElementException:
                county_logger.error("Error: Element not found.")
                debug_screenshot(context, "date_extraction_failed")
                raise CriticalError(f"An unexpected error occurred: {e}")
            
            except Exception as e:
                county_logger.error(f"An unexpected error occurred: {e}")
                debug_screenshot(context, "date_extraction_failed")
                raise CriticalError(f"An unexpected error occurred: {e}")

        # Function to click download button
        def find_download_button(context, selectors, critical=True):
            """
            Traverses nested Shadow DOMs to locate and return the download button.

            This is necessary for modern web UIs where elements are encapsulated. It uses
            JavaScript execution to pierce through the shadow boundaries and find the
            target button element.

            Args:
                context (CountyContext): The context object for the county.
                selectors (dict): A dictionary of CSS selectors for the shadow DOM path.
                critical (bool): If True, raise a CriticalError if the button is not found.

            Returns:
                WebElement: The located download button element, or None.

            Raises:
                CriticalError: If the button cannot be found and `critical` is True.
            """
            try:
                js_code = """
                // Start with the container element
                const container = document.querySelector(arguments[0]);
                if (!container) return null;
                
                // Get its shadow root
                const shadowRoot1 = container.shadowRoot;
                if (!shadowRoot1) return null;
                
                // Find the next element in the first shadow DOM
                const nestedElement = shadowRoot1.querySelector(arguments[1]);
                if (!nestedElement) return null;
                
                // Get the second shadow root
                const shadowRoot2 = nestedElement.shadowRoot;
                if (!shadowRoot2) return null;
                
                // Find the button container in the second shadow DOM
                const buttonContainer = shadowRoot2.querySelector(arguments[2]);
                if (!buttonContainer) return null;
                
                // Get the third shadow root
                const shadowRoot3 = buttonContainer.shadowRoot;
                if (!shadowRoot3) return null;
                
                // Finally, find the button in the deepest shadow DOM
                const button = shadowRoot3.querySelector(arguments[3]);
                return button;
                """
                
                download_button = driver.execute_script(
                    js_code, 
                    selectors["outer_shadow_selector"],
                    selectors["nested_shadow_selector"], 
                    selectors["nested_shadow_selector_2"],
                    selectors["download_button"]
                )
                
                if download_button:
                    county_logger.info("Found download button using JavaScript traversal")
                    return download_button
                else:
                    raise Exception("Could not find download button with JavaScript traversal")
                    
            except Exception as e:
                county_logger.error(f"Error finding download button in shadow DOM: {e}")
                debug_screenshot(context, "download_button_not_found")
                if critical:
                    raise CriticalError(f"Error finding download button in shadow DOM: {e}")
                return None



        # Define variables
        driver = context.driver
        county_logger = context.county_logger
        county_name_formatted = context.county_name.replace("_", " ").title()
        url = context.county_config["palm_beach"]["parcels"]["url"]
        selectors = context.county_config["palm_beach"]["parcels"]["selectors"]
        filename = context.county_config["palm_beach"]["parcels"]["filename"]
        temp_files = []

        # Navigate to the URL
        try: 
            driver.get(url)

            # Wait for page to load
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, selectors["wait_for_selector"])))

            # Get data date
            data_date = find_data_date(context)

            # Check if we need to download
            county_row = next((row for row in context.csv_data if row['county'] == context.county_name), None)
            if not should_download(county_row, data_date, context.county_logger):
                return {'status': 'NND', 'data_date': data_date}

            # Click download tab
            try:
                download_tab = driver.find_element(By.CSS_SELECTOR, selectors["download_tab"])
                driver.execute_script("arguments[0].click();", download_tab)
                county_logger.info(f"Clicked download tab")
            except Exception as e_3:
                county_logger.error(f"Error clicking download tab, attempting alternate selector...")
                try:
                    WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selectors["wait_for_selector"])))
                    download_tab = driver.find_element(By.CSS_SELECTOR, selectors["DOWNLOAD_TAB_OLD"])
                    driver.execute_script("arguments[0].click();", download_tab)
                    county_logger.info(f"Clicked alternate download tab")
                except Exception as e_4:
                    county_logger.error(f"Failed to click download tab: {e_4}")
                    debug_screenshot(context, "download_tab_not_found")
                    raise CriticalError(f"Failed to click download tab: {e_4}")

            # Wait for tab to load
            WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selectors["outer_shadow_selector"])))
            
            # Click download button
            download_button = find_download_button(context, selectors, critical=True)
            safe_click(context, download_button, "download button", specific_click='action')
                
            # Wait for download
            download_path, download_file = wait_for_download(context, f"{filename}", timeout=90, log_interval=10)

            county_logger.info(f"Download complete for {county_name_formatted}. Data date: {data_date}")
            temp_files.append(download_file)

            # Transfer files
            transfer_files(context, temp_files)

            return {
                'status': 'SUCCESS', 
                'data_date': data_date,
                'file_count': len(temp_files),
                'file_count_status': 'SUCCESS' if len(temp_files) > 0 else "FAILED"
            }

        except TimeoutException as te:
            county_logger.error(f"Timeout: {te}")
            if len(temp_files) > 0: # If files were downloaded, transfer them
                transfer_files(context, temp_files)
            debug_screenshot(context, "timeout")
            raise CriticalError(f"Timeout: {te}")
        except Exception as e:
            county_logger.error(f"Unexpected error: {e}", exc_info=True)
            if len(temp_files) > 0: # If files were downloaded, transfer them
                transfer_files(context, temp_files)
            debug_screenshot(context, "Exception")
            raise CriticalError(f"Unexpected error: {str(e)}")
        
    def download_indian_river(context):
        """
        Downloads CAMA data extract for Indian River county.

        This is a simple download process that navigates to a specific page
        and clicks a direct download link for a ZIP file.

        Args:
            context (CountyContext): An object containing all necessary parameters.

        Returns:
            dict: A dictionary containing the download status and file count.

        Raises:
            CriticalError: If the download link cannot be found or the download fails.
        """
        # Define variables
        driver = context.driver
        county_logger = context.county_logger
        county_name = context.county_name
        county_config = context.county_config
        url = county_config["url"]
        selectors = county_config["selectors"]
        temp_files = []

        try:
            # Navigate to the URL
            driver.get(url)

            # Click cama_extract
            cama_extract = safe_find(context, "cama_extract")
            if not cama_extract:
                county_logger.error(f"Failed to find cama_extract")
                debug_screenshot(context, "cama_extract_not_found")
                raise CriticalError(f"Failed to find cama_extract")

            safe_click(context, cama_extract, "cama_extract")
            county_logger.info(f"Clicked cama_extract")

            # Wait for downloaded file
            download_path, newest_file = wait_for_download(context, "DataDownload*.zip")

            # Close function
            county_logger.info(f"{context.county_name_formatted} county successfully downloaded")
            temp_files.append(newest_file)

            # Transfer files
            data_date = transfer_files(context, temp_files)

            return {
                'status': 'SUCCESS', 
                'data_date': data_date,
                'file_count': len(temp_files),
                'file_count_status': 'SUCCESS' if len(temp_files) > 0 else "FAILED"
            }
            
        except Exception as e:
            county_logger.error(f"Unexpected error: {e}", exc_info=True)
            if len(temp_files) > 0: # If files were downloaded, transfer them
                transfer_files(context, temp_files)
            debug_screenshot(context, "Exception")
            raise CriticalError(f"Unexpected error: {str(e)}")

    def download_santa_rosa(context):
        """
        Downloads parcel sales data for Santa Rosa county.

        The website only allows downloading data in small date ranges. This function
        calculates a total date range (last 3 months), splits it into 31-day chunks,
        and then iterates through each chunk to download the data as separate CSV files.

        Args:
            context (CountyContext): An object containing all necessary parameters.

        Returns:
            dict: A dictionary containing the download status and file count.

        Raises:
            CriticalError: If a download chunk fails repeatedly or a critical error occurs.
        """

        # Function to process a chunk
        def process_chunk(context, chunk_start_date, chunk_end_date):
            """Processes a single date chunk for Santa Rosa county."""
            # Define variables
            driver = context.driver
            url = context.county_config["url"]
            selectors = context.county_config["selectors"]
            frames = context.county_config["frames"]

            # Navigate to the URL
            driver.get(url)

            # Switch to frame
            iframe = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, f"iframe#{frames['frame0']}"))
            )
            driver.switch_to.frame(iframe)
            
            # Click sales tab
            sales_tab = safe_find(context, "sales_tab")
            safe_click(context, sales_tab, "sales_tab", specific_click='click')
            time.sleep(0.5)  # test pause

            # Re-find and interact with each element in every iteration
            start_date_input = safe_find(context, "start_date_input")
            end_date_input = safe_find(context, "end_date_input")
            safe_input(context, start_date_input, "start_date_input", chunk_start_date)
            safe_input(context, end_date_input, "end_date_input", chunk_end_date)

            # Click search button - use JavaScript for better reliability
            search_button = safe_find(context, "search_button")
            safe_click(context, search_button, "search_button", specific_click='js')
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, selectors["dropdown_menu"])))
            time.sleep(1)  # Give time for results to load

            # Open dropdown and export data
            dropdown_menu = safe_find(context, "dropdown_menu")
            safe_click(context, dropdown_menu, "dropdown_menu", specific_click='js')
            
            # Select CSV option
            csv_option = safe_find(context, "csv_option")
            safe_click(context, csv_option, "csv_option", specific_click='click')
            
            # Export - Use special handling to avoid popup blocking
            export_all = safe_find(context, "export_all")
            safe_click(context, export_all, "export_all", specific_click='js')

            # Wait for download and rename
            download_path, download_file = wait_for_download(context, "SRCPA Data*.csv", timeout=90, log_interval=10)

            # Format and rename file
            start_dt = datetime.strptime(chunk_start_date, '%m/%d/%Y')
            end_dt   = datetime.strptime(chunk_end_date,   '%m/%d/%Y')
            start_date_formatted = start_dt.strftime('%Y-%m-%d')
            end_date_formatted   = end_dt.strftime('%Y-%m-%d')
            renamed_file = rename_file(context, download_file, f"SantaRosaCountyFL_{start_date_formatted}_{end_date_formatted}.csv")

            if renamed_file:
                return renamed_file
            else:
                return None

        # Function to get date range
        def get_date_range(context):
            """
            Calculates the date range for the entire download process.

            The end date is today. The start date is the first day of the month,
            three months prior to the end date.

            Args:
                context (CountyContext): The context object for the county.

            Returns:
                tuple: A tuple (start_date_str, end_date_str) with dates formatted as 'MM/DD/YYYY'.
            
            Raises:
                CriticalError: If an error occurs during date calculation.
            """
            driver = context.driver
            county_logger = context.county_logger

            try:
                # Define end_date as the current date
                end_date = datetime.now().date()

                # Calculate the start date: first day of the month, 3 months prior
                three_months_prior = end_date - relativedelta(months=3)  # Approximation for 3 months
                start_date = datetime(three_months_prior.year, three_months_prior.month, 1)

                # Format the dates as strings
                start_date_str = start_date.strftime('%m/%d/%Y')
                end_date_str = end_date.strftime('%m/%d/%Y')

                county_logger.info(f"Calculated date range: {start_date_str} to {end_date_str}")
                return start_date_str, end_date_str

            except Exception as e:
                county_logger.error(f"Error reading or calculating date range: {e}")
                raise CriticalError(f"Error reading or calculating date range: {e}")
        
        # Main logic
        temp_files = []
        try:
            # Get date range input
            start_date_str, end_date_str = get_date_range(context)
            if not start_date_str or not end_date_str:
                county_logger.error("Failed to retrieve date range.")
                raise CriticalError("Failed to retrieve date range.")

            # Parse the date range into datetime objects
            start_date = datetime.strptime(start_date_str, '%m/%d/%Y')
            end_date = datetime.strptime(end_date_str, '%m/%d/%Y')

            # Split the date range into chunks
            date_chunks = split_date_range(context, start_date, end_date, 31)
            county_logger.info(f"Date chunks: {date_chunks}")

            # Loop through each chunk and perform actions
            for chunk_start_date, chunk_end_date in date_chunks:
                county_logger.info(f"Processing date range: {chunk_start_date} to {chunk_end_date}")
                for attempt in range(2): # 1 initial attempt + 1 retry
                    try:
                        renamed_file = process_chunk(context, chunk_start_date, chunk_end_date)
                        county_logger.info(f"Download successful for {chunk_start_date} to {chunk_end_date}")
                        temp_files.append(renamed_file)
                        break # success
                    except CriticalError as e:
                        county_logger.error(f"Attempt {attempt + 1} failed for chunk {chunk_start_date} to {chunk_end_date}: {e}")
                        if attempt == 1: # last attempt
                            county_logger.error(f"All attempts failed for chunk. Aborting Santa Rosa.")
                            raise # re-raise the final CriticalError
                        time.sleep(1) # wait before retry

            # Transfer files
            transfer_files(context, temp_files)

            if len(temp_files) > 2:
                county_logger.info(f"{context.county_name_formatted} county download complete.")
                
                return {
                    'status': 'SUCCESS', 
                    'data_date': end_date_str,
                    'file_count': len(temp_files),
                    'file_count_status': 'SUCCESS' if len(temp_files) > 2 else "FAILED"
                }
            else:
                county_logger.error(f"{context.county_name_formatted} download missing files: {len(temp_files)}")
                return {
                    'status': 'FAILED', 
                    'error': 'Missing files',
                    'data_date': end_date_str,
                    'file_count': len(temp_files),
                    'file_count_status': 'FAILED'
                }
        except Exception as e:
            county_logger.error(f"Unexpected error: {e}", exc_info=True)
            if len(temp_files) > 0: # If files were downloaded, transfer them
                transfer_files(context, temp_files)
            debug_screenshot(context, "Exception")
            raise CriticalError(f"Unexpected error: {str(e)}")
    
    def download_duval(context):
        """
        Downloads parcel data for Duval county.

        The process involves navigating to a page with two separate download links
        and clicking both to download two distinct files. The function then waits
        for both files to be downloaded completely.

        Args:
            context (CountyContext): An object containing all necessary parameters.

        Returns:
            dict: A dictionary containing the download status and file count.

        Raises:
            CriticalError: If a download button isn't found or a download fails.
        """

        # Define variables
        driver = context.driver
        county_logger = context.county_logger
        county_config = context.county_config
        url = county_config["url"]
        selectors = county_config["selectors"]
        filename = county_config["filename"]
        temp_files = []

        # Helper: wait for both downloads
        def wait_for_download_II(context, file_pattern_I, description_I, file_pattern_II, description_II, timeout=120, check_interval=0.5, log_interval=10):
            """
            Waits for two separate files to be downloaded completely.

            Args:
                context (CountyContext): The context object for the county.
                file_pattern_I (str): The filename pattern for the first file.
                description_I (str): A description of the first file for logging.
                file_pattern_II (str): The filename pattern for the second file.
                description_II (str): A description of the second file for logging.
                timeout (int): The maximum time to wait for both downloads.
                check_interval (float): The interval between checks.
                log_interval (int): The interval for logging progress.

            Returns:
                tuple: A tuple containing the full paths to the two downloaded files.
                       If a file did not download, its path will be None.
            """
            # Define variables
            download_dir = context.download_dir
            county_logger = context.county_logger

            # Get initial files in directory
            initial_files = set(os.listdir(download_dir))
            county_logger.info(f"Initial files in directory: {list(initial_files)}")
            county_logger.info(f"Waiting for both {description_I} and {description_II}.")
            county_logger.info(f"Pattern I: {file_pattern_I}")
            county_logger.info(f"Pattern II: {file_pattern_II}")
            
            start_time = time.time()
            last_log_time = start_time
            download_started = False
            
            # Track completion status and paths for both downloads
            first_file_path = None
            second_file_path = None
            
            # First, check if any matching files already exist (race condition handling)
            current_time = time.time()
            recent_threshold = 60  # 60 seconds
            
            # Check for recently downloaded files that match our patterns
            for filename in initial_files:
                file_path = os.path.join(download_dir, filename)
                if os.path.isfile(file_path):
                    file_mod_time = os.path.getmtime(file_path)
                    if (current_time - file_mod_time) <= recent_threshold:
                        if fnmatch.fnmatch(filename, file_pattern_I) and not first_file_path:
                            first_file_path = file_path
                            county_logger.info(f"Found recently downloaded file matching pattern I: {filename}")
                        elif fnmatch.fnmatch(filename, file_pattern_II) and not second_file_path:
                            second_file_path = file_path
                            county_logger.info(f"Found recently downloaded file matching pattern II: {filename}")
            
            if first_file_path and second_file_path:
                county_logger.info("Both files already downloaded recently!")
                return (first_file_path, second_file_path)
            
            while time.time() - start_time < timeout:
                # Wait for specified interval
                time.sleep(check_interval)
                
                # Get current files
                current_files = set(os.listdir(download_dir))
                
                # Find new files
                new_files = current_files - initial_files
                
                if new_files:
                    # Convert to list for easier processing
                    new_files_list = list(new_files)
                    #county_logger.info(f"New files detected: {new_files_list}")
                    
                    # Find files matching first pattern
                    if not first_file_path:
                        matching_files_I = [f for f in new_files_list if fnmatch.fnmatch(f, file_pattern_I)]
                        if matching_files_I:
                            county_logger.info(f"Found files matching pattern I: {matching_files_I}")
                        complete_files_I = [f for f in matching_files_I if not f.endswith('.crdownload') and not f.endswith('.part')]
                        
                        if complete_files_I:
                            newest_file_I = max(complete_files_I, key=lambda f: os.path.getctime(os.path.join(download_dir, f)))
                            first_file_path = os.path.join(download_dir, newest_file_I)
                            county_logger.info(f"{description_I} download complete: {newest_file_I} after {time.time() - start_time:.1f} seconds")
                    
                    # Find files matching second pattern
                    if not second_file_path:
                        matching_files_II = [f for f in new_files_list if fnmatch.fnmatch(f, file_pattern_II)]
                        if matching_files_II:
                            county_logger.info(f"Found files matching pattern II: {matching_files_II}")
                        complete_files_II = [f for f in matching_files_II if not f.endswith('.crdownload') and not f.endswith('.part')]
                        
                        if complete_files_II:
                            newest_file_II = max(complete_files_II, key=lambda f: os.path.getctime(os.path.join(download_dir, f)))
                            second_file_path = os.path.join(download_dir, newest_file_II)
                            county_logger.info(f"{description_II} download complete: {newest_file_II} after {time.time() - start_time:.1f} seconds")
                    
                    # Track if downloads have started
                    temp_files = [f for f in new_files_list if f.endswith('.crdownload') or f.endswith('.part')]
                    if temp_files and not download_started:
                        download_started = True
                        county_logger.info(f"Downloads started - temporary files: {temp_files}")
                    
                    # Check if we've found both files
                    if first_file_path and second_file_path:
                        county_logger.info(f"All downloads complete after {time.time() - start_time:.1f} seconds")
                        return (first_file_path, second_file_path)
                
                # Log "waiting" messages (but not too often)
                current_time = time.time()
                if current_time - last_log_time >= log_interval:
                    if not download_started:
                        county_logger.info(f"Still waiting for download initiation... ({current_time - start_time:.1f}s elapsed)")
                    else:
                        # Report status of each download
                        status_I = "Complete" if first_file_path else "In progress"
                        status_II = "Complete" if second_file_path else "In progress"
                        county_logger.info(f"First download: {status_I}, Second download: {status_II} ({current_time - start_time:.1f}s elapsed)")
                    
                    last_log_time = current_time
            
            county_logger.warning(f"Download timeout after {timeout} seconds")
            county_logger.warning(f"Final status - First: {first_file_path}, Second: {second_file_path}")
            
            # Final fallback: check all files in directory for matches
            if not first_file_path or not second_file_path:
                county_logger.info("Performing final fallback check of all files in directory...")
                all_files = os.listdir(download_dir)
                for filename in all_files:
                    file_path = os.path.join(download_dir, filename)
                    if os.path.isfile(file_path):
                        if fnmatch.fnmatch(filename, file_pattern_I) and not first_file_path:
                            first_file_path = file_path
                            county_logger.info(f"Fallback found file matching pattern I: {filename}")
                        elif fnmatch.fnmatch(filename, file_pattern_II) and not second_file_path:
                            second_file_path = file_path
                            county_logger.info(f"Fallback found file matching pattern II: {filename}")
            
            return (first_file_path, second_file_path)

        # Main logic:
        try:
            # Open the URL  
            driver.get(url)

            # Wait for and click download button I
            download_button_I = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selectors["download_button_I"])))
            county_logger.info("Found download button I, clicking...")
            driver.execute_script("arguments[0].click();", download_button_I)
            county_logger.info("Clicked download button I")

            # Small delay to reduce race condition
            #time.sleep(1)

            # Wait for and click download button II
            download_button_II = driver.find_element(By.CSS_SELECTOR, selectors["download_button_II"])
            county_logger.info("Found download button II, clicking...")
            driver.execute_script("arguments[0].click();", download_button_II)
            county_logger.info("Clicked download button II")

            # Wait for both downloads simultaneously
            first_path, second_path = wait_for_download_II(context, filename["download_I"], "download_I", 
                                                        filename["download_II"], "download_II", timeout=90, log_interval=15)
            
            county_logger.info(f"Both downloads complete. Duval parcels downloaded.")
            if first_path:
                temp_files.append(os.path.basename(first_path))
            if second_path:
                temp_files.append(os.path.basename(second_path))

            # Transfer files and get data date from unzipped files
            data_date = transfer_files(context, temp_files)

            # Check if data is new before returning
            county_row = next((row for row in context.csv_data if row['county'] == context.county_name), None)
            if not should_download(county_row, data_date, context.county_logger):
                return {'status': 'NND', 'data_date': data_date, 'file_count': len(temp_files)}

            return {
                'status': 'SUCCESS', 
                'data_date': data_date,
                'file_count': len(temp_files),
                'file_count_status': 'SUCCESS' if len(temp_files) > 1 else "FAILED"
            }

        except Exception as e:
            county_logger.error(f"Error in main: {e}")
            if len(temp_files) > 0: # If files were downloaded, transfer them
                transfer_files(context, temp_files)
            debug_screenshot(context, "Exception")
            raise CriticalError(f"Error in main: {e}")
    
    def download_pinellas(context):
        """
        Downloads five distinct parcel data files for Pinellas county.

        This function navigates to a data download portal, expands all sections,
        and clicks five separate download links. It then waits for all five files
        to complete downloading before finishing. It also extracts a data date
        from the page.

        Args:
            context (CountyContext): An object containing all necessary parameters.

        Returns:
            dict: A dictionary containing the download status, data date, and file count.

        Raises:
            CriticalError: If a download button isn't found or a download fails.
        """

        # Define variables
        driver = context.driver
        county_logger = context.county_logger
        county_config = context.county_config
        download_dir = context.download_dir
        filename = county_config["filename"]
        selectors = county_config["selectors"]
        url = county_config["url"]
        temp_files = []

        # Helper: wait for all 5 downloads
        def wait_for_download(download_dir, 
                            file_pattern_I, description_I,
                            file_pattern_II, description_II,
                            file_pattern_III, description_III,
                            file_pattern_IV, description_IV,
                            file_pattern_V, description_V,
                            timeout=180, check_interval=0.5, log_interval=10, 
                            recent_file_threshold=300):  # 300 seconds = 5 minutes
            """
            Waits for five separate files to download, handling both new and recent downloads.

            This function checks for files matching five distinct patterns. It first checks if any
            of the files have already been downloaded within a recent threshold (5 minutes). For
            any remaining files, it monitors the download directory until they appear.

            Args:
                download_dir (str): The directory to monitor for downloads.
                file_pattern_I-V (str): Filename patterns for the five files.
                description_I-V (str): Descriptions for each file for logging.
                timeout (int): The maximum time to wait for all downloads.
                check_interval (float): The interval between checks.
                log_interval (int): The interval for logging progress.
                recent_file_threshold (int): The time in seconds to consider a file "recent".

            Returns:
                list: A list containing the full paths to the five downloaded files.

            Raises:
                CriticalError: If one or more downloads fail to complete within the timeout.
            """
            # Get initial files in directory
            initial_files = os.listdir(download_dir)
            county_logger.info(f"Waiting for {description_I}, {description_II}, {description_III}, {description_IV}, and {description_V}.")
            
            start_time = time.time()
            current_time = start_time
            last_log_time = start_time
            download_started = False
            
            # Track completion status and paths for all downloads
            file_paths = [None, None, None, None, None]
            patterns = [file_pattern_I, file_pattern_II, file_pattern_III, file_pattern_IV, file_pattern_V]
            descriptions = [description_I, description_II, description_III, description_IV, description_V]
            
            # First check for recently downloaded files
            for i in range(5):
                if not file_paths[i]:
                    matching_files = [f for f in initial_files if fnmatch.fnmatch(f, patterns[i])]
                    complete_files = [f for f in matching_files if not f.endswith('.crdownload') and not f.endswith('.part')]
                    
                    if complete_files:
                        # Find the newest file that matches the pattern and was modified within threshold
                        recent_complete_files = []
                        for f in complete_files:
                            file_path = os.path.join(download_dir, f)
                            file_mod_time = os.path.getmtime(file_path)
                            if (current_time - file_mod_time) <= recent_file_threshold:
                                recent_complete_files.append(f)
                        
                        if recent_complete_files:
                            newest_file = max(recent_complete_files, key=lambda f: os.path.getmtime(os.path.join(download_dir, f)))
                            file_paths[i] = os.path.join(download_dir, newest_file)
                            file_age = current_time - os.path.getmtime(file_paths[i])  # type: ignore
                            county_logger.info(f"{descriptions[i]} recently downloaded ({file_age:.1f}s ago): {newest_file}")
            
            # Convert to set for tracking new files
            initial_files_set = set(initial_files)
            
            # If all files are already downloaded, return immediately
            if all(file_paths):
                county_logger.info("All files already downloaded within the last 5 minutes")
                return file_paths
            
            while time.time() - start_time < timeout:
                # Wait for specified interval
                time.sleep(check_interval)
                
                # Get current files
                current_files = set(os.listdir(download_dir))
                
                # Find new files
                new_files = current_files - initial_files_set
                
                if new_files:
                    # Convert to list for easier processing
                    new_files_list = list(new_files)
                    
                    # Check each download pattern
                    for i in range(5):
                        if not file_paths[i]:
                            matching_files = [f for f in new_files_list if fnmatch.fnmatch(f, patterns[i])]
                            complete_files = [f for f in matching_files if not f.endswith('.crdownload') and not f.endswith('.part')]
                            
                            if complete_files:
                                newest_file = max(complete_files, key=lambda f: os.path.getctime(os.path.join(download_dir, f)))
                                file_paths[i] = os.path.join(download_dir, newest_file)
                                county_logger.info(f"{descriptions[i]} download complete: {newest_file} after {time.time() - start_time:.1f} seconds")
                    
                    # Track if downloads have started
                    temp_files = [f for f in new_files_list if f.endswith('.crdownload') or f.endswith('.part')]
                    if temp_files and not download_started:
                        download_started = True
                    
                    # Check if we've found all files
                    if all(file_paths):
                        county_logger.info(f"All downloads complete after {time.time() - start_time:.1f} seconds")
                        return file_paths
                
                # Log "waiting" messages (but not too often)
                current_time = time.time()
                if current_time - last_log_time >= log_interval:
                    if not download_started:
                        county_logger.info(f"Still waiting for download initiation... ({current_time - start_time:.1f}s elapsed)")
                    else:
                        # Report status of each download
                        statuses = ["Complete" if path else "In progress" for path in file_paths]
                        status_message = ", ".join([f"\n{desc}: {status}" for desc, status in zip(descriptions, statuses)])
                        county_logger.info(f"Download status ({current_time - start_time:.1f}s elapsed): \n{status_message}\n")
                    
                    last_log_time = current_time
            
            county_logger.warning(f"Download timeout after {timeout} seconds")
            
            # After timeout, check which files failed
            failed_downloads = []
            for i in range(5):
                if not file_paths[i]:
                    failed_downloads.append(descriptions[i])
            if failed_downloads:
                msg = f"The following downloads failed: {', '.join(failed_downloads)}"
                county_logger.error(msg)
                raise CriticalError(msg)
            
            return file_paths

        # Helper: extract data date from a selector
        def extract_data_date(driver, selector, format_str="%m/%d/%Y"):
            """
            Extracts and formats the data date from a given element on the webpage.

            Args:
                driver: The Selenium WebDriver instance.
                selector (str): The CSS selector for the element containing the date text.
                format_str (str): The desired output date format string.

            Returns:
                str: The formatted date string, or None if extraction fails.

            Raises:
                CriticalError: If the date element is not found or the date cannot be parsed.
            """
            try:
                # Find the element containing the date
                date_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                
                # Get the text content
                date_text = date_element.text.strip()
                county_logger.info(f"Found date text: {date_text}")
                
                # Extract the date using regex (looking for month and day pattern)
                # Pattern matches formats like "Apr 01, 2:22 AM" or "Apr 01"
                date_match = re.search(r'([A-Za-z]{3})\s+(\d{1,2})(?:,?\s+.*)?', date_text)
                
                if date_match:
                    month_str = date_match.group(1)  # e.g., "Apr"
                    day_str = date_match.group(2)    # e.g., "01"
                    
                    # Get current date for year determination
                    current_date = datetime.now()
                    current_year = current_date.year
                    current_month = current_date.month
                    
                    # Convert month abbreviation to month number (1-12)
                    month_num = datetime.strptime(month_str, "%b").month
                    
                    # Handle year boundary case
                    # If current month is January and data date month is December, use previous year
                    year_to_use = current_year
                    if current_month == 1 and month_num == 12:
                        year_to_use = current_year - 1
                        county_logger.info(f"Year boundary detected: Using previous year ({year_to_use})")
                    
                    # Create date object with determined year
                    date_obj = datetime(year_to_use, month_num, int(day_str))
                    
                    # Format according to the specified format
                    formatted_date = date_obj.strftime(format_str)
                    county_logger.info(f"Extracted and formatted date: {formatted_date}")
                    return formatted_date
                    
                county_logger.warning(f"No date pattern found in text: {date_text}")
                raise CriticalError(f"No date pattern found in text: {date_text}")
            except TimeoutException:
                county_logger.error(f"Timeout waiting for date element with selector: {selector}")
                raise CriticalError(f"Timeout waiting for date element with selector: {selector}")
            except NoSuchElementException:
                county_logger.error(f"Date element not found with selector: {selector}")
                raise CriticalError(f"Date element not found with selector: {selector}")
            except Exception as e:
                county_logger.error(f"Error extracting date: {str(e)}")
                raise CriticalError(f"Error extracting date: {str(e)}")


        # Main logic
        try:
            # Open the URL  
            driver.get(url)

            # Wait for and click expand all
            expand_all = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selectors["expand_all"]))
            )
            county_logger.info(f"Waited for page to load")
            driver.execute_script("arguments[0].click();", expand_all)
            time.sleep(1)

            # Extract data date
            data_date = extract_data_date(driver, selectors["data_date"])

            # Check if we need to download
            county_row = next((row for row in context.csv_data if row['county'] == context.county_name), None)
            if not should_download(county_row, data_date, context.county_logger):
                return {'status': 'NND', 'data_date': data_date}

            # Click RP_BUILDING
            rp_building = driver.find_element(By.CSS_SELECTOR, selectors["RP_BUILDING"])
            driver.execute_script("arguments[0].click();", rp_building)
            county_logger.info(f"Clicked RP_BUILDING")

            # Click RP_LAND
            rp_land = driver.find_element(By.CSS_SELECTOR, selectors["RP_LAND"])
            driver.execute_script("arguments[0].click();", rp_land)
            county_logger.info(f"Clicked RP_LAND")

            # Click RP_PROPERTY_INFO
            rp_property_info = driver.find_element(By.CSS_SELECTOR, selectors["RP_PROPERTY_INFO"])
            driver.execute_script("arguments[0].click();", rp_property_info)
            county_logger.info(f"Clicked RP_PROPERTY_INFO")

            # Click RP_SALES
            rp_sales = driver.find_element(By.CSS_SELECTOR, selectors["RP_SALES"])
            driver.execute_script("arguments[0].click();", rp_sales)
            county_logger.info(f"Clicked RP_SALES")

            # Click os_sales_tab
            os_sales_tab = driver.find_element(By.CSS_SELECTOR, selectors["os_sales_tab"])
            driver.execute_script("arguments[0].click();", os_sales_tab)
            county_logger.info(f"Clicked os_sales_tab")

            # Click RP_OS_SALES
            rp_os_sales = driver.find_element(By.CSS_SELECTOR, selectors["RP_OS_SALES"])
            driver.execute_script("arguments[0].click();", rp_os_sales)
            county_logger.info(f"Clicked RP_OS_SALES")

            # # Clicking each dataset defensively (better error handling, but seems less reliable)
            # for key in ("RP_BUILDING", "RP_LAND", "RP_PROPERTY_INFO", "RP_SALES", "RP_OS_SALES"):
            #     try:
            #         btn = WebDriverWait(driver, 10).until(
            #             EC.element_to_be_clickable((By.CSS_SELECTOR, selectors[key])))
            #         driver.execute_script("arguments[0].click();", btn)
            #         county_logger.info(f"Clicked {key}")
            #     except TimeoutException:
            #         county_logger.error(f"{key} not clickable")
            #         raise CriticalError(f"{key} not clickable")

            # Wait for all downloads to complete
            downloaded_files = wait_for_download(
                download_dir,
                filename["RP_BUILDING"], "RP_BUILDING", 
                filename["RP_LAND"], "RP_LAND", 
                filename["RP_PROPERTY_INFO"], "RP_PROPERTY_INFO", 
                filename["RP_SALES"], "RP_SALES", 
                filename["RP_OS_SALES"], "RP_OS_SALES",
                timeout=180, 
                log_interval=10
            )
            
            # Check if all downloads completed successfully
            county_logger.info(f"Pinellas downloads complete. Data date: {data_date}")
            temp_files.extend([os.path.basename(f) for f in downloaded_files if f])

            # Transfer files
            transfer_files(context, temp_files)

            return {
                'status': 'SUCCESS', 
                'data_date': data_date,
                'file_count': len(temp_files),
                'file_count_status': 'SUCCESS' if len(temp_files) > 4 else "FAILED"
            }

        except Exception as e:
            county_logger.error(f"Error in main: {e}")
            if len(temp_files) > 0: # If files were downloaded, transfer them
                transfer_files(context, temp_files)
            debug_screenshot(context, "Exception")
            raise CriticalError(f"Error in main: {e}")
    
    def download_escambia(context):
        """
        Downloads sales grid data and mailing addresses for Escambia county.

        This function automates filling out a sales search form with a calculated
        date range (from June of the previous year to today), downloads the main
        grid data, and then clicks another button to download the associated
        mailing addresses.

        Args:
            context (CountyContext): An object containing all necessary parameters.

        Returns:
            dict: A dictionary containing the download status and file count.

        Raises:
            CriticalError: If a required form element is not found or a download fails.
        """

        # Define variables
        driver = context.driver
        county_logger = context.county_logger
        county_config = context.county_config
        filename = county_config["filename"]
        url = county_config["url"]
        temp_files = []

        # Helper: get the date range
        def get_date_range(month_number):
            """
            Generates a date range from a specific month in the previous year to today.

            Args:
                month_number (int): The month (1-12) to use for the start date.

            Returns:
                tuple: A tuple (start_date, end_date) formatted as MM/DD/YYYY.
            """

            # Get current date
            current_date = datetime.now()
            
            # Calculate the 1st of the selected month of the previous year
            if current_date.month > month_number:
                # If current month is later than the selected month, use the selected month of the current year
                start_date_obj = datetime(current_date.year, month_number, 1)
            else:
                # If current month is the selected month or earlier, use the selected month of last year
                start_date_obj = datetime(current_date.year - 1, month_number, 1)
            
            # Format dates as MM/DD/YYYY
            start_date = start_date_obj.strftime("%m/%d/%Y")
            end_date = current_date.strftime("%m/%d/%Y")
            
            county_logger.info(f"Using date range: {start_date} to {end_date}")

            return start_date, end_date

        # Main logic
        try:
            # Open the URL
            driver.get(url)
            
            # Get date range
            start_date, end_date = get_date_range(6)

            # Set the date range
            start_date_input = safe_find(context, "start_date_input")
            safe_input(context, start_date_input, "Start date input", start_date)
            end_date_input = safe_find(context, "end_date_input")
            safe_input(context, end_date_input, "End date input", end_date)

            # Set the price range
            price_low_input = safe_find(context, "price_low_input")
            safe_input(context, price_low_input, "Price low input", "0")
            price_high_input = safe_find(context, "price_high_input")
            safe_input(context, price_high_input, "Price high input", "999999999")

            # Ensure other parameters are set properly
            improvement_status = safe_find(context, "improvement_status")
            if improvement_status and not improvement_status.is_selected():
                county_logger.info(f"Improvement status not selected, clicking...")
                safe_click(context, improvement_status, "Improvement status")
            elif improvement_status and improvement_status.is_selected():
                county_logger.info(f"Improvement status already selected, skipping...")
            else:
                county_logger.error(f"Improvement status not found")
                raise CriticalError(f"Improvement status not found")

            qualification_status = safe_find(context, "qualification_status")
            if qualification_status and not qualification_status.is_selected():
                county_logger.info(f"Qualification status not selected, clicking...")
                safe_click(context, qualification_status, "Qualification status")
            elif qualification_status and qualification_status.is_selected():
                county_logger.info(f"Qualification status already selected, skipping...")
            else:
                county_logger.error(f"Qualification status not found")
                raise CriticalError(f"Qualification status not found")
            
            property_use = safe_find(context, "property_use")
            if property_use and property_use.is_selected():
                county_logger.info(f"Property use already selected, skipping...")
            elif property_use and not property_use.is_selected():
                county_logger.info(f"Property use already unselected, skipping...")
            else:
                county_logger.error(f"Property use not found")
                raise CriticalError(f"Property use not found")
            
            instrument_type = safe_find(context, "instrument_type")
            if instrument_type and instrument_type.is_selected():
                county_logger.info(f"Instrument type selected, clicking...")
                safe_click(context, instrument_type, "Instrument type")
            elif instrument_type and not instrument_type.is_selected():
                county_logger.info(f"Instrument type already unselected, skipping...")
            else:
                county_logger.error(f"Instrument type not found")
                raise CriticalError(f"Instrument type not found")
            
            # Click the search button
            search_button = safe_find(context, "search_button")
            safe_click(context, search_button, "Search button")

            # Click the grid data button
            grid_data_button = safe_find(context, "grid_data_button")
            safe_click(context, grid_data_button, "Grid data button")

            # Wait for the download to complete
            grid_file_path, grid_file_nm = wait_for_download(context, "*.csv")

            # Rename the file
            renamed_file = rename_file(context, grid_file_nm, filename["grid_file_name"])
            temp_files.append(os.path.basename(renamed_file))
            
            # Click the mailing addresses button
            mailing_addresses_button = safe_find(context, "mailing_addresses_button")
            safe_click(context, mailing_addresses_button, "Mailing addresses button")
                
            # Wait for the download to complete
            mailing_file_path, mailing_file_nm = wait_for_download(context, "*.csv", ex_file_pattern=filename["grid_file_name"])

            # Rename the file
            renamed_file = rename_file(context, mailing_file_nm, filename["mailing_file_name"])
            temp_files.append(os.path.basename(renamed_file))

            # Transfer files
            data_date = transfer_files(context, temp_files)

            # Check if data is new before returning
            county_row = next((row for row in context.csv_data if row['county'] == context.county_name), None)
            if not should_download(county_row, data_date, context.county_logger):
                return {'status': 'NND', 'data_date': data_date, 'file_count': len(temp_files)}

            # Close the driver
            if len(temp_files) > 1:
                county_logger.info(f"Escambia download complete.")
                return {
                    'status': 'SUCCESS', 
                    'data_date': data_date,
                    'file_count': len(temp_files),
                    'file_count_status': 'SUCCESS' if len(temp_files) > 1 else "FAILED"
                }
            else:
                county_logger.error(f"Escambia download failed.")
                debug_screenshot(context, "download_failed")
                return {
                    'status': 'FAILED', 
                    'error': 'Missing files',
                    'data_date': data_date,
                    'file_count': len(temp_files),
                    'file_count_status': 'FAILED'
                }
        except Exception as e:
            county_logger.error(f"Unexpected error: {e}")
            raise CriticalError(f"Unexpected error: {e}")

    def download_putnam(context):
        """
        Downloads parcel sales data for Putnam county.

        The website is prone to timeouts on large queries. This function implements a robust
        download strategy that splits a 3-month date range into smaller 15-day chunks.
        It then processes each chunk individually. If a chunk download fails (times out),
        it is recursively split into smaller chunks until the download succeeds or the
        chunk size becomes too small.

        Args:
            context (CountyContext): An object containing all necessary parameters.

        Returns:
            dict: A dictionary containing the download status and file count.

        Raises:
            CriticalError: For unrecoverable errors during the download process.
        """

        # Define variables
        driver = context.driver
        county_logger = context.county_logger
        county_config = context.county_config
        url = county_config["url"]

        # Helper: get the date range
        def get_date_range(end_date, date_format, month_number=None, months_ago=None):
            """
            Calculates a start and end date for a search.

            This flexible helper can calculate a date range based on a fixed starting month
            or a relative number of months in the past.

            Args:
                end_date (datetime): The end date of the range.
                date_format (str): The desired output format for the date strings.
                month_number (int, optional): A specific month (1-12) to set the start date to.
                months_ago (int, optional): A number of months to go back for the start date.

            Returns:
                tuple: A tuple containing the start and end date strings in the specified format.

            Raises:
                CriticalError: If input arguments are invalid.
            """

            # Set variables
            year_number = end_date.year
            county_logger.info(f"Calculating start date from {end_date} in {date_format} format...")

            # If a specific number of months ago is selected, set the month number
            if months_ago:
                # Validate input
                if months_ago < 1:
                    county_logger.error("Invalid argument: months_ago must be greater than 0")
                    raise CriticalError("Invalid argument: months_ago must be greater than 0")

                # Break the offset into years and remaining months
                years_back      = months_ago // 12
                leftover_months = months_ago % 12  # 0-11

                year_number -= years_back

                if leftover_months == 0:
                    # Exact multiple of 12 → same calendar month one or more years ago
                    month_number = end_date.month
                else:
                    # Calculate target month, wrapping the year boundary if necessary
                    if leftover_months > end_date.month:
                        year_number -= 1
                        month_number = 12 - (leftover_months - end_date.month)
                    else:
                        month_number = end_date.month - leftover_months
            else:
                month_number = end_date.month

            # Calculate the start date (first day of the computed month)
            start_date_obj = datetime(year_number, month_number, 1)

            # Format dates as MM/DD/YYYY
            start_date = start_date_obj.strftime(date_format)
            end_date = end_date.strftime(date_format)

            county_logger.info(f"Calculated date range: {start_date} to {end_date}")

            return start_date, end_date

        # Helper: click download button
        def click_download_button(context):
            """Clicks the download button, bypassing page load timeouts."""
            
            try:
                driver = context.driver
                county_logger = context.county_logger
                download_button = driver.find_element(By.CSS_SELECTOR, context.county_config["selectors"]["download_button"])

                # Set page load timeout to minimal value before clicking
                driver.set_page_load_timeout(1)  # 1 second
                
                try:
                    download_button.click()  # This will fail quickly due to timeout
                    county_logger.info(f"Download button clicked.")
                except:
                    county_logger.info(f"Download button clicked.")
                    pass  # Ignore the timeout exception
                
                # Restore original timeout
                driver.set_page_load_timeout(60)
            except Exception as e:
                county_logger.error(f"Error clicking download button: {e}")
                debug_screenshot(context, "download button error")
                raise CriticalError(f"Error clicking download button: {e}")

        # Helper: check the record number
        def get_record_number(context):
            """Retrieves the number of records found from the search results text."""
            driver = context.driver
            county_logger = context.county_logger
            selectors = context.county_config["selectors"]
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, selectors["record_number"])))
                # Wait for element to contain text
                WebDriverWait(driver, 5).until(lambda d: d.find_element(By.CSS_SELECTOR, selectors["record_number"]).text.strip() != "")
                record_number = driver.find_element(By.CSS_SELECTOR, selectors["record_number"]).text
                return record_number
            except Exception as e_1:
                county_logger.error(f"Error checking record number: {e_1}")
                try:
                    county_logger.error("Retrying...")
                    time.sleep(1)
                    record_number = driver.find_element(By.CSS_SELECTOR, selectors["record_number"]).text
                    return record_number
                except Exception as e_2:
                    county_logger.error(f"Failed to get record number: {e_2}")
                    debug_screenshot(context, "record number error")
                    raise CriticalError(f"Failed to get record number: {e_2}")

        # Helper: set initial search parameters
        def set_initial_parameters(context, max_retries=3):
            """Handles the initial page setup: disclaimer, sales tab, and price range."""
            county_logger = context.county_logger
            # Track retry attempts
            attempts = 0
            success = False
            
            while attempts <= max_retries and not success:
                if attempts > 0:
                    county_logger.warning(f"Retrying initial parameter initialization (retry {attempts}/{max_retries})")
                    
                # Click disclaimer
                try:
                    disclaimer_button = safe_find(context, "disclaimer_button", critical=False)
                    if disclaimer_button:
                        time.sleep(1)
                        safe_click(context, disclaimer_button, "Disclaimer button")
                        try:
                            WebDriverWait(driver, 1).until(EC.element_to_be_clickable((By.CSS_SELECTOR, context.county_config["selectors"]["disclaimer_button"])))
                            county_logger.info(f"Disclaimer button not clicked.")
                            county_logger.info(f"Retrying...")
                            time.sleep(1)
                            attempts += 1
                            continue
                        except:
                            county_logger.info(f"Disclaimer button clicked.")
                            
                except Exception as e:
                    county_logger.warning(f"Error clicking disclaimer button: {e}")
                    county_logger.info(f"Retrying...")
                    attempts += 1
                    time.sleep(1)
                    continue
                    
                # Click sales tab
                try:
                    sales_tab = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, context.county_config["selectors"]["sales_tab"])))
                    safe_click(context, sales_tab, "sales tab")
                    county_logger.info(f"Sales tab clicked.")
                    time.sleep(1)  # Short pause to ensure tab switches
                except Exception as e:
                    county_logger.warning(f"Error clicking sales tab: {e}")
                    
                # Try to set sales price (this will determine if previous steps worked)
                try:
                    sales_price_low = safe_find(context, "sales_price_low")
                    safe_input(context, sales_price_low, "Sales price low", "0")
                    time.sleep(0.5)
                    
                    sales_price_high = safe_find(context, "sales_price_high")
                    safe_input(context, sales_price_high, "Sales price high", "99999999")
                    time.sleep(0.5)
                    
                    success = True  # If we got here, everything worked
                except Exception as e:
                    attempts += 1
                    county_logger.warning(f"Error setting sales prices (attempt {attempts}): {e}")
                    if attempts > max_retries:
                        county_logger.error("Max retries exceeded. Exiting.")
                        debug_screenshot(context, "initialization error")
                        raise CriticalError("Max retries exceeded. Exiting.")
                    # Small pause before retry
                    time.sleep(1)

        # Helper: set date range
        def perform_search(context, start_date, end_date):
            """Sets the date range, performs the search, and returns the estimated download time."""
            county_logger = context.county_logger

            # Set date range
            try:
                sales_date_low = safe_find(context, "sales_date_low")
                safe_input(context, sales_date_low, "Sales date low", start_date)
                county_logger.info(f"Sales date low set to {start_date}.")
                time.sleep(0.5)
            except Exception as e:
                county_logger.error(f"Error setting sales date low: {e}.")
                debug_screenshot(context, "sales date low error")
                raise CriticalError(f"Error setting sales date low: {e}")
            try:
                sales_date_high = safe_find(context, "sales_date_high")
                safe_input(context, sales_date_high, "Sales date high", end_date)
                county_logger.info(f"Sales date high set to {end_date}.")
                time.sleep(0.5)
            except Exception as e:
                county_logger.error(f"Error setting sales date high: {e}")
                debug_screenshot(context, "sales date high error")
                raise CriticalError(f"Error setting sales date high: {e}")
            
            # Click search button
            #debug_screenshot(context, "search range set")
            search_button = safe_find(context, "search_button")
            safe_click(context, search_button, "Search button")
            county_logger.info(f"Search button clicked.")
            time.sleep(1)

            # Get record number
            record_number = get_record_number(context)
            if record_number == "NO":
                county_logger.info(f"No records found for {start_date} to {end_date}.")
                return 0

            # Estimate download time
            est_download_time = download_time(record_number)
            county_logger.info(f"{record_number} records found for {start_date} to {end_date}.")
            county_logger.info(f"Estimated download time: {est_download_time} seconds.")

            return est_download_time

        # Helper: estimate download time
        def download_time(record_number):
            """Estimates the download time based on the number of records."""
            # Calculate download time based on average of 4.5 seconds per 50 records
            try:
                record_count = int(record_number.replace(',', ''))
                seconds_per_50_records = 4.5
                if round((record_count / 50) * seconds_per_50_records, 1) < 3:
                    total_seconds = 3
                else:
                    total_seconds = round((record_count / 50) * seconds_per_50_records, 1)
                return total_seconds
            except (ValueError, AttributeError, TypeError):
                # If record_number is not a valid number, return a default estimate
                county_logger.warning("Could not calculate download time, using default estimate")
                return None

        # Helper: perform search, download, wait, rename, and close
        def download_wait_rename(context, start_date, end_date, final=False):
            """Manages the process of downloading, waiting, and renaming for a single chunk."""
            
            # Set sales date
            est_download_time = perform_search(context, start_date, end_date)
            if est_download_time == 0:
                county_logger.info(f"No records found for {start_date} to {end_date}, skipping download for this chunk.")
                return None, 0 # Skip this chunk

            # Click download button
            click_download_button(context)
            # download_button = safe_find(context, "download_button")
            # safe_click(context, download_button, "download button")

            # Wait for download
            try:
                download_file, download_file_nm = wait_for_download(context, context.county_config["filename"], ex_file_pattern=None, timeout=60, check_interval=0.5, log_interval=5)
            except CriticalError:
                raise TimeoutException

            # Rename file
            new_filename = rename_file(context, download_file_nm, f"PutnamCountyFL_{start_date}_{end_date}.xlsx")

            # Click download close button
            if final == False:
                download_close_button = safe_find(context, "download_close_button")
                safe_click(context, download_close_button, "download close button")
                time.sleep(3)

            return os.path.basename(new_filename), est_download_time
    
        # Main logic
        temp_files = []
        try:
            # Set date range
            date_format = "%Y-%m-%d"
            start_date, end_date = get_date_range(datetime.now(), date_format, month_number=None, months_ago=3)

            # Split date range into chunks
            date_chunks = split_date_range(context, start_date, end_date, 15, date_format)
            county_logger.info(f"{len(date_chunks)} chunks generated.")
            queue = date_chunks
            county_logger.info(f"Queue remaining: {len(queue)}")

            # Open the page
            driver.get(url)
            time.sleep(2)
            #WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, context.county_config["selectors"]["disclaimer_button"])))

            # Set initial search parameters
            set_initial_parameters(context)
            
            # Perform initial search and get estimated download time
            est_total_download_time = perform_search(context, start_date, end_date)
            if est_total_download_time is not None:
                est_total_download_time = est_total_download_time + (len(date_chunks) * 7)
            # response = input(f"Estimated overall download time: {est_total_download_time} seconds. Proceed? (y/n): ")
            # if response == "y":
            #     county_logger.info(f"Proceeding with download.")
            # else:
            #     county_logger.info(f"Exiting.")
            #     raise CriticalError("User did not approve download.")

            # Reopen the page
            county_logger.info(f"Reopening the page...")

            # Initialize iteration counter
            iteration = 0
            has_retried = False

            # Loop through date chunks
            while queue:
                
                chunk_start_date, chunk_end_date = queue.pop(0)  # Get and remove first chunk

                # Refresh the page
                driver.refresh()
                time.sleep(2)

                # debug: Scroll to top of page
                # driver.execute_script("window.scrollTo(0, 0);")
                # time.sleep(1)  # Brief pause to allow scroll to complete
                # debug_screenshot(context, "initial parameters set")

                # Set iteration counter
                iteration += 1
                county_logger.info(f"Iteration {iteration}: {chunk_start_date} to {chunk_end_date}")
                is_final_chunk = (len(queue) == 0)  # Changed from len(queue) == 1 since we pop before checking
                if is_final_chunk:
                    county_logger.info(f"Final iteration.")

                # Perform download procedure with retries
                try:
                    # Download file and rename
                    new_filename, est_download_time = download_wait_rename(context, chunk_start_date, chunk_end_date, is_final_chunk)
                    if new_filename: # If a file was downloaded, add it to the temp_files list
                        temp_files.append(new_filename)
                    if not new_filename and est_download_time == 0:
                        county_logger.info(f"No records found for {chunk_start_date} to {chunk_end_date}, skipping download...")
                        continue
                    county_logger.info(f"Queue remaining: {len(queue)}")
                    has_retried = False

                except (TimeoutException, Exception) as e:
                    county_logger.error(f"Download timed out: {e}")
                    
                    # If this is the first failure and we haven't retried yet, try one more time
                    if not has_retried and est_download_time is not None and est_download_time < 45: # If the estimated download time is less than 45 seconds, try one more time
                        county_logger.info("First failure - attempting one retry...")
                        has_retried = True
                        queue.insert(0, (chunk_start_date, chunk_end_date))  # Add the failed chunk back to the front of the queue
                        county_logger.info(f"Queue remaining: {len(queue)}")
                        continue  # Try again with the same chunk
                    
                    # If we've already retried or this is a subsequent failure, try to split
                    if has_retried: # Max retries reached
                        # Calculate days in chunk
                        try:
                            # Convert dates from YYYY-MM-DD to MM/DD/YYYY format
                            chunk_start = datetime.strptime(chunk_start_date, '%Y-%m-%d')
                            chunk_end = datetime.strptime(chunk_end_date, '%Y-%m-%d')
                            days_in_chunk = (chunk_end - chunk_start).days + 1  # +1 to include both start and end dates
                            if days_in_chunk < 6: # If the chunk is too small to split, exit
                                county_logger.info(f"Chunk is too small to split: {chunk_start_date} to {chunk_end_date} ({days_in_chunk} days)")
                                county_logger.info(f"Queue remaining: {len(queue)}")
                                debug_screenshot(context, "chunk too small to split")
                            else:
                                county_logger.info(f"Splitting chunk {chunk_start_date} to {chunk_end_date}...")
                            
                                # Split the chunk in half
                                mid_date = chunk_start + (chunk_end - chunk_start) / 2
                                
                                # Calculate days in each new chunk
                                first_half_days = (mid_date - chunk_start).days + 1
                                second_half_days = (chunk_end - mid_date).days + 1
                                
                                # Add both halves to the front of the queue
                                queue.insert(0, ((mid_date + timedelta(days=1)).strftime('%Y-%m-%d'), chunk_end_date))
                                queue.insert(0, (chunk_start_date, mid_date.strftime('%Y-%m-%d')))

                                county_logger.info(f"Split chunk into two smaller chunks: {chunk_start_date} to {mid_date.strftime('%Y-%m-%d')} ({first_half_days} days) and {(mid_date + timedelta(days=1)).strftime('%Y-%m-%d')} to {chunk_end_date} ({second_half_days} days)")
                                county_logger.info(f"Queue remaining: {len(queue)}")
                                has_retried = False

                        except ValueError as e:
                            county_logger.error(f"Error parsing dates: {e}")
                            raise CriticalError(f"Error parsing dates: {e}")

                        continue # Try again with the new chunks

            # Transfer files
            transfer_files(context, temp_files)
            # End function

            if len(temp_files) < 6:
                file_count_status = "FAILED"
            else:
                file_count_status = "SUCCESS"

            return {
                'status': 'SUCCESS', 
                'data_date': end_date,
                'file_count': len(temp_files),
                'file_count_status': file_count_status
            }
        except TimeoutException as e:
            county_logger.error(f"Unexpected timeout error: {e}")
            if len(temp_files) > 0: # If files were downloaded, transfer them
                transfer_files(context, temp_files)
            debug_screenshot(context, "unexpected timeout error")
            raise CriticalError(f"Unexpected timeout error: {e}")
        except Exception as e:
            county_logger.error(f"Unexpected error: {e}")
            if len(temp_files) > 0: # If files were downloaded, transfer them
                transfer_files(context, temp_files)
            debug_screenshot(context, "unexpected error")
            raise CriticalError(f"Unexpected error: {e}")
    
    def download_collier(context):

        # Define variables
        driver = context.driver
        county_logger = context.county_logger
        url = context.county_config["url"]

        # Helper: switch to new tab
        def switch_to_new_tab(context):
            driver = context.driver
            county_logger = context.county_logger

            # Wait for new tab to open
            county_logger.info("Waiting for new tab to open...")
            original_window = driver.current_window_handle
            wait_timeout = 10
            start_time = time.time()

            while time.time() - start_time < wait_timeout:
                if len(driver.window_handles) > 1:
                    county_logger.info("New tab detected")
                    break
                time.sleep(0.5)
                
            # Switch to the new tab
            if len(driver.window_handles) > 1:
                # Get all window handles
                window_handles = driver.window_handles
                
                # Switch to the new tab (the one that's not the original window)
                for handle in window_handles:
                    if handle != original_window:
                        driver.switch_to.window(handle)
                        county_logger.info(f"Switched to new tab. URL: {driver.current_url}")
                        return True
            else:
                county_logger.warning("No new tab was opened")
                return False

        try:
            # Open the page
            driver.get(url)

            # Click privacy disclaimer button
            privacy_disclaimer = safe_find(context, "privacy_disclaimer", specific_frame=3)
            safe_click(context, privacy_disclaimer, "privacy disclaimer")

            # # Click disclaimer button (2025-07-21: stopped appearing)
            # disclaimer_button = safe_find(context, "disclaimer_button", specific_frame=3)
            # safe_click(context, disclaimer_button, "disclaimer button")
            # time.sleep(1)

            # Click download tab
            download_tab = safe_find(context, "download_tab", specific_frame=3)
            safe_click(context, download_tab, "download tab")
            time.sleep(1)

            # Click csv files
            csv_files = safe_find(context, "csv_files")
            safe_click(context, csv_files, "csv files")

            # Switch to new tab and click download disclaimer if it appears
            if switch_to_new_tab(context):   
                download_disclaimer = safe_find(context, "download_disclaimer")
                safe_click(context, download_disclaimer, "download disclaimer")

            # Wait for download
            full_path, filename = wait_for_download(context, "int_values_rp_history_csv*.zip")

            if full_path:
                data_date = transfer_files(context, [filename])

                # Check if data is new before returning
                county_row = next((row for row in context.csv_data if row['county'] == context.county_name), None)
                if not should_download(county_row, data_date, context.county_logger):
                    return {'status': 'NND', 'data_date': data_date, 'file_count': 1}
                    
                county_logger.info("Collier download complete")
                return {
                    'status': 'SUCCESS', 
                    'data_date': data_date,
                    'file_count': 1,
                    'file_count_status': "SUCCESS"
                }
            else:
                county_logger.error("Collier download failed")
                return {
                    'status': 'FAILED', 
                    'error': "Collier download failed",
                    'data_date': None
                }
        except Exception as e:
            county_logger.error(f"Error in main: {e}")
            raise CriticalError(f"Error in main: {e}")

    def download_wget(context):
        """
        Downloads data for a county using a series of wget/bash commands.
        """
        county_logger = context.county_logger
        county_name = context.county_name
        ignore_file_not_found = ['rmdir', 'rm']

        # Helper: validate download command output
        def validate_download_output(return_code, output, county, logger):
            # Check if the script ran successfully
            if return_code != 0:
                raise Exception(f"Error downloading attributes for {county}: {output}")
            
            # Check for various success indicators
            has_progress_bar = ('100%[' in output and ']' in output)
            file_saved = 'saved [' in output
            
            if has_progress_bar or file_saved:
                logger.debug(f"Downloaded attributes for {county}")
                return True
            elif re.search(r'Omitting download|already up-to-date|not retrieving', output):
                logger.debug(f"No new records found for {county}")
                return False
            else:
                raise Exception(f"Error downloading attributes for {county}: {output}")

        try:
            # The county_config for wget counties is passed directly in the context
            wget_config = context.county_config
            if not wget_config or 'cwd' not in wget_config: # Basic validation
                raise CriticalError(f"Wget configuration for {county_name} is missing or invalid in parcels_scrape.json")

            working_dir = wget_config['cwd']
            county_logger.debug(f"Changing to working directory: {working_dir}")
            
            # Run pre-download commands
            for item in wget_config.get('pre_commands', []):
                logging.info(f"Running pre-download command: {item}")
                return_code, output = run_command(item, working_dir, county_logger)
                if return_code != 0:
                    # Allow rmdir cleanup commands to fail gracefully if directory doesn't exist
                    if any(cmd in item for cmd in ignore_file_not_found) and ('No such file or directory' in output):
                        county_logger.info(f"Pre-command failed as expected (directory doesn't exist): {item}")
                        continue
                    raise CriticalError(f"Error executing pre-command '{item}': {output}")
            
            # Run download commands
            successful_downloads = []
            for item in wget_config.get('download_commands', []):
                logging.info(f"Running download command: {item}")
                return_code, output = run_command(item, working_dir, county_logger)
                if validate_download_output(return_code, output, county_name, county_logger):
                    successful_downloads.append(item)
            
            if not successful_downloads:
                logging.info(f"No new files downloaded for {county_name}. Data is up-to-date.")
                county_row = next((row for row in context.csv_data if row['county'] == context.county_name), None)
                last_data_date_str = county_row.get('data_date')
                if not last_data_date_str:
                    logging.info("No previous data date found.")

                    # Find and run the post-command that contains the data date
                    for item in wget_config.get('post_commands', []):
                        if "get_file_date.py" in item:
                            return_code, output = run_command(item, working_dir, county_logger)
                            if return_code != 0:
                                raise CriticalError(f"Error executing post-command '{item}': {output}")
                            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', output)
                            if date_match:
                                data_date = date_match.group(1)
                                logging.info(f"Extracted data date: {data_date}")
                            else:
                                logging.error(f"Could not extract date from post-command output: {output}")
                                raise CriticalError(f"Could not extract date from post-command output: {output}")

                    logging.info(f"Using data date: {data_date}")
                else:
                    data_date = last_data_date_str
                return {'status': 'NND', 'data_date': data_date, 'file_count': 0, 'file_count_status': 'N/A'}
        
            # Run post-download commands
            data_date = None
            for item in wget_config.get('post_commands', []):
                logging.info(f"Running post-download command: {item}")
                return_code, output = run_command(item, working_dir, county_logger)
                if return_code != 0:
                    raise CriticalError(f"Error executing post-command '{item}': {output}")
                
                if "get_file_date.py" in item:
                    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', output)
                    if date_match:
                        data_date = date_match.group(1)
                        logging.info(f"Extracted data date: {data_date}")
                    else:
                        raise CriticalError(f"Could not extract date from post-command output: {output}")

            # Check if data is new before returning
            county_row = next((row for row in context.csv_data if row['county'] == context.county_name), None)
            if not should_download(county_row, data_date, context.county_logger):
                return {'status': 'NND', 'data_date': data_date, 'file_count': len(successful_downloads)}

            return {
                'status': 'SUCCESS',
                'data_date': data_date,
                'file_count': len(successful_downloads),
                'file_count_status': 'SUCCESS' if len(successful_downloads) > 0 else "FAILED"
            }
        except Exception as e:
            raise CriticalError(f"Error during wget download for {county_name}: {str(e)}")


    # Main function
    try:
        if county_name in OTHER:
            if county_name == "indian_river":
                result = download_indian_river(context)
            elif county_name == "santa_rosa":
                result = download_santa_rosa(context)
            elif county_name == "duval":
                result = download_duval(context)
            elif county_name == "pinellas":
                result = download_pinellas(context)
            elif county_name == "escambia":
                result = download_escambia(context)
            elif county_name == "putnam":
                result = download_putnam(context)
            elif county_name == "collier":
                result = download_collier(context)
        elif county_name in QPUBLIC:
            result = download_qpublic(context)
        elif county_name in GRIZZLY:
            result = download_grizzly(context)
        elif county_name in GSACORP:
            result = download_gsacorp(context)
        elif county_name in OPENDATA:
            result = download_opendata(context)
        elif county_name in WGET:
            result = download_wget(context)
        else:
            raise CriticalError(f"County {county_name_formatted} not found in any download category.")
        if result:
            return result
        else:
            county_logger.error(f"Unexpected error: No result returned from {county_name_formatted} script.")
            return {
                'status': 'FAILED', 
                'error': 'No result returned',
                'data_date': None}
    except CriticalError as e:
        county_logger.error(f"Critical error: {e}")
        return {
            'status': 'FAILED', 
            'error': str(e),
            'data_date': None}

# Main orchestration function for batch downloads
def main():
    """
    Main orchestration function for the entire batch download process.

    This function initializes the system, determines which counties to process,
    clears the download directory, and then iterates through the list of counties.
    For each county, it calls the main `download_county` function and handles retries
    on failure. Finally, it generates a summary report and cleans up resources.
    """
    global local
    global manual
    global isolate_county_logs
    global chromium
    global headless
    global prod
    global attempts

    # Parse command-line arguments
    args = parse_arguments()
    
    # Apply command-line flag overrides
    if args.local:
        local = True
    if args.full_logs:
        isolate_county_logs = False
    if args.chrome:
        chromium = False
    if args.headful:
        headless = False
    if args.no_retry:
        attempts = 1

    # Check control variables
    if prod:
        local = False
        manual = False
        isolate_county_logs = True
        headless = True
        attempts = 10
    else:
        if attempts < 1:
            print("ATTEMPTS must be at least 1.")
            exit(1)
        print("----- RUNNING IN TEST MODE -----")
        print(f"LOCAL: {local}")
        print(f"MANUAL: {manual}")
        print(f"ISOLATE_COUNTY_LOGS: {isolate_county_logs}")
        print(f"CHROMIUM: {chromium}")
        print(f"HEADLESS: {headless}")
        print(f"ATTEMPTS: {attempts}")
        print("--------------------------------")

    # Set directories
    if local:
        _persistent_data_dir = "/Users/seanmay/Downloads/batch_logs"
        _log_dir = os.path.join(_persistent_data_dir, "download_logs")
        download_dir = "/Users/seanmay/Downloads/batch_downloads"
    else:
        _persistent_data_dir = "/srv/mapwise_dev/county/_batch_logs"
        _log_dir = os.path.join(_persistent_data_dir, "download_logs")
        download_dir = "/srv/mapwise_dev/county/_batch_downloads"
        os.makedirs(download_dir, exist_ok=True)
        os.makedirs(_log_dir, exist_ok=True)
    
    # Get all possible counties for the CSV from the source of truth
    all_counties = get_counties_to_process(False, None) # Get all valid counties

    # Initialize CSV data from the persistent location
    csv_data, csv_path = initialize_data('parcels_data_status.csv', _persistent_data_dir, all_counties, logging.getLogger())

    # Determine which counties to process for this run
    counties_to_process = get_counties_to_process(manual, args)
    driver = None
    main_window = None
    initial_window_count = None

    # Check if any of the counties to be processed require Selenium
    needs_selenium = any(county in SELENIUM_COUNTIES for county in counties_to_process)

    # Track runtime for summary row
    start_time = time.time()

    try:
        # Always initialize logging, regardless of whether Selenium is needed
        initialize_logging(_log_dir)

        # Initialize Selenium only if needed
        if needs_selenium:
            # Initialize batch system with the specific log location
            driver, main_window, initial_window_count = initialize_all(_log_dir, download_dir, chromium, local, headless)
        else:
            logging.info("No Selenium counties in this run. Skipping browser initialization.")


        # Clear download directory
        try:
            # Get list of all files in download directory
            if not os.path.exists(download_dir):
                pass
            else:
                files = os.listdir(download_dir)
                logging.info(f"Download directory: {download_dir}")
                
                # Delete each file
                if len(files) > 0:
                    logging.info(f"Files in download directory: {files}")
                    if manual:
                        response = input(f"Clear download directory? (y/n): ")
                        if response == "y":
                            for file in files:
                                file_path = os.path.join(download_dir, file)
                                if os.path.isfile(file_path):
                                    os.remove(file_path)
                            logging.info(f"Download directory cleared.")
                        else:
                            logging.info(f"Download directory not cleared.")
                    else:
                        for file in files:
                            file_path = os.path.join(download_dir, file)
                            if os.path.isfile(file_path):
                                os.remove(file_path) # Delete each file
                    
                    for file in files:
                        file_path = os.path.join(download_dir, file)
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                            logging.info(f"Deleted file: {file}")
                    
                    logging.info(f"Cleared download directory: {download_dir}")

        except Exception as e:
            logging.error(f"Error clearing download directory: {e}")
            raise Exception(f"Error clearing download directory: {e}")
        
        # Loop through counties
        for county_name in counties_to_process:
            # Format the county name for logging
            county_name_formatted = county_name.replace('_', ' ').title()
            if "Wget" in county_name_formatted:
                county_name_formatted = county_name_formatted.replace("Wget", "(WGET)")

            # Find the row for the current county
            county_row = next((row for row in csv_data if row['county'] == county_name), None)
            if not county_row:
                # This case should be handled by initialize_data, but as a fallback:
                logging.error(f"County {county_name} not found in CSV data. Skipping.")
                continue

            # Set download start timestamp (only updated, never cleared)
            if args.update_csv:
                county_row['download_date'] = time.strftime("%-m/%-d/%y %-I:%M %p")

            # Attempt each county up to the number of attempts
            max_attempts = attempts
            for attempt in range(1, max_attempts + 1):
                county_logger = start_county_logging(county_name, county_name_formatted, isolate_county_logs, chromium, local)
                result = None # Initialize result
                try:
                    # Call the function with shared resources
                    result = download_county(county_name, county_name_formatted, driver, county_logger, download_dir, main_window, initial_window_count, csv_data)

                    if result.get('status') == 'SUCCESS':
                        # Success – record information and stop retrying
                        log_county_info(county_name, f"Data date: {result['data_date']}")
                        end_county_logging(county_name, county_name_formatted, 'SUCCESS',
                                           attempt=attempt,
                                           file_count=result.get('file_count'),
                                           file_count_status=result.get('file_count_status'))
                        
                        # Update CSV data for success
                        if args.update_csv:
                            county_row['download_status'] = 'SUCCESS'
                            county_row['processing_status'] = 'PENDING' if not 'wget' in county_name else ''
                            county_row['data_date'] = format_date(result.get('data_date'))
                            county_row['QA_status'] = ''
                            county_row['error_message'] = ''
                        break
                    elif result.get('status') == 'NND':
                        # No new data - update CSV and stop
                        log_county_info(county_name, f"No new data detected. Data date: {result['data_date']}")
                        end_county_logging(county_name, county_name_formatted, 'NND', attempt=attempt)

                        if args.update_csv:
                            county_row['download_status'] = 'NND'
                            county_row['data_date'] = format_date(result.get('data_date'))
                            county_row['processing_status'] = ''
                            county_row['QA_status'] = ''
                            county_row['error_message'] = ''
                        break
                    else:
                        # The function returned FAILED – log the error
                        error_msg = result.get('error', 'Unknown error')
                        log_county_error(county_name, f"Attempt {attempt} error: {error_msg}")
                        end_county_logging(county_name, county_name_formatted, 'FAILED',
                                           attempt=attempt,
                                           error_message=error_msg,
                                           file_count=result.get('file_count'),
                                           file_count_status=result.get('file_count_status'))
                        
                        # Update CSV on last attempt failure
                        if attempt == max_attempts:
                            if args.update_csv:
                                county_row['download_status'] = 'FAILED'
                                county_row['processing_status'] = ''
                                county_row['QA_status'] = ''
                                # Use data date from result if available, even on exception
                                if result and result.get('data_date'):
                                    county_row['data_date'] = format_date(result.get('data_date'))
                                county_row['error_message'] = wrap_error_message(str(error_msg))
                                # Update the processing status of the main county row if county_name is a wget county
                                if 'wget' in county_name:
                                    county_part = county_name.split('_wget')[0]
                                    main_county_row = next((row for row in csv_data if row['county'] == county_part), None)
                                    if main_county_row:
                                        main_county_row['processing_status'] = ''
                                        main_county_row['QA_status'] = ''
                                        main_county_row['error_message'] = wrap_error_message("WGET download failed")
                except Exception as e:
                    # Unexpected exception during the attempt – log it
                    log_county_error(county_name, f"Attempt {attempt} unexpected error: {str(e)}")
                    end_county_logging(county_name, county_name_formatted, 'FAILED', attempt=attempt, error_message=str(e))
                    
                    # Update CSV on last attempt failure
                    if attempt == max_attempts:
                        if args.update_csv:
                            county_row['download_status'] = 'FAILED'
                            county_row['processing_status'] = ''
                            county_row['QA_status'] = ''
                            # Use data date from result if available, even on exception
                            if result and result.get('data_date'):
                                county_row['data_date'] = format_date(result.get('data_date'))
                            county_row['error_message'] = wrap_error_message(str(e))
                            # Update the processing status of the main county row if county_name is a wget county
                            if 'wget' in county_name:
                                county_part = county_name.split('_wget')[0]
                                main_county_row = next((row for row in csv_data if row['county'] == county_part), None)
                                if main_county_row:
                                    main_county_row['processing_status'] = ''
                                    main_county_row['QA_status'] = ''
                                    main_county_row['error_message'] = wrap_error_message("WGET download failed")


                # If we reach here, the attempt failed. Decide whether to retry.
                if attempt < max_attempts:
                    county_logger.info(f"Retrying {county_name_formatted} (attempt {attempt + 1}/{max_attempts}) after reinitializing browser…")
                    if driver: # Only quit driver if it exists
                        driver.quit()
                    
                    if needs_selenium:
                        driver, main_window, initial_window_count = initialize_all(_log_dir, download_dir, chromium, local, headless)
                else:
                    county_logger.info(f"All attempts failed for {county_name_formatted}.")

            # Optional delay between counties (regardless of success/failure)
            time.sleep(3)

        # DEPRECATED: add_batch_summary(counties_to_process)
        
    except KeyboardInterrupt:
        print('\nReceived keyboard interrupt. Cleaning up...')
        logging.info("Script interrupted by user")
    finally:
        # Save the final CSV data if requested
        if 'csv_data' in locals() and 'csv_path' in locals():
            if args.update_csv:
                # Calculate runtime for summary row
                runtime_seconds = time.time() - start_time if 'start_time' in locals() else 0
                save_csv(csv_data, csv_path, logging.getLogger(), runtime_seconds)

        # Cleanup - this runs whether we complete normally or get interrupted
        if driver:
            try:
                driver.quit()
                logging.info("Browser driver cleaned up")
            except Exception as e:
                logging.error(f"Error during cleanup: {e}")
        
        logging.info("Batch download completed")

if __name__ == "__main__":
    main()