#!/usr/bin/env python3
import os
import logging
import time
import argparse
import re
import fnmatch
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (TimeoutException, NoSuchElementException)
from selenium.webdriver.common.by import By
from dataclasses import dataclass
from python_batch_functions import (initialize_all, start_county_logging, 
                                    log_county_info, log_county_error, 
                                    end_county_logging, add_batch_summary)

# Import helper functions
from python_batch_functions import (
                                    load_county_config, safe_find, 
                                    safe_click, safe_input, split_date_range, 
                                    debug_screenshot, wait_for_download, rename_file,
                                    transfer_files)


# Control variables (defaults)
local = False # Set directories for local testing
manual = False # Set to True to manually enter counties
isolate_county_logs = True # Set to True to isolate county logs from main log (reduces verbosity of output)
chromium = True # Set to True to use Chromium
headless = True # Set to True to run headless
attempts = 10 # Set the number of attempts per county

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
    
    return parser.parse_args()

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
    # Define valid counties
    valid_counties = [
        "palm_beach", "duval", "pinellas", "collier", "escambia", "okaloosa", "bay", "santa_rosa",
        "indian_river", "sumter", "flagler", "nassau", "walton", "putnam", "columbia",
        "jackson", "gadsden", "suwannee", "levy", "okeechobee", "hendry", "desoto",
        "wakulla", "bradford", "hardee", "washington", "taylor", "holmes", "madison",
        "gilchrist", "dixie", "union", "jefferson", "gulf", "hamilton", "calhoun",
        "franklin", "glades", "lafayette", "liberty"
    ]
    
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
        logging.info(f"Using counties from command line arguments: {args.counties}")
        # Convert to lowercase and validate
        counties = [county.lower() for county in args.counties]
        invalid_counties = [county for county in counties if county not in valid_counties]
        if invalid_counties:
            print(f"[ERROR] Invalid counties found in arguments: {invalid_counties}")
            print("[INFO] Valid counties are: " + ", ".join(valid_counties))
            return []
        return counties
    
    # If no counties specified anywhere, return full list
    print("[INFO] No counties specified, defaulting to all counties.")
    return valid_counties

# Function to define and map download functions
def download_county(county_name, driver, county_logger, download_dir, main_window, initial_window_count):
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

    Returns:
        dict: A result dictionary containing the status of the download ('SUCCESS' or 'FAILED'),
              an error message if applicable, the data date, and file count information.
    """
    # Define county categories
    qpublic = ["madison","holmes","okaloosa","flagler","walton","hardee","washington",
               "hendry","levy","gilchrist","calhoun","liberty","dixie","jefferson","gulf",
               "hamilton","taylor","gadsden","jackson","bay","glades","sumter"]
    grizzly = ["union","columbia","suwannee","okeechobee","desoto","bradford","lafayette"]
    gsacorp = ["wakulla","nassau","franklin"]
    opendata = ["palm_beach"]
    other = ["indian_river","santa_rosa","duval","pinellas","escambia","putnam","collier"]

    # Define critical error class
    class CriticalError(Exception):
        """
        Custom exception for critical errors that should halt execution.
        """
        pass


    # Set context to package common variables needed for the download functions
    county_config = load_county_config(county_name)
    county_logger.info(f"Loaded configuration for {county_name.replace('_', ' ').title()}")
    
    @dataclass
    class CountyContext:
        driver: object
        county_name: str
        county_logger: object  # add more as needed
        county_config: object
        download_dir: str
        local: bool
        manual: bool
        main_window: object
        initial_window_count: int

    context = CountyContext(driver, county_name, county_logger, county_config, download_dir, local, manual, main_window, initial_window_count)

    
    # Define download functions

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
        


    # Main function
    try:
        result = download_opendata(context)
        
        if result:
            if result.get('status') == 'SUCCESS' and result.get('data_date') == None:
                result['data_date'] = datetime.now().strftime("%m/%d/%Y")
            return result
        else:
            county_logger.error(f"Unexpected error: No result returned from {county_name.replace('_', ' ').title()} script.")
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
        _base_log_dir = os.path.join("/Users/seanmay/Downloads/batch_logs", datetime.now().strftime("%Y-%m-%d"))
        download_dir = "/Users/seanmay/Downloads/batch_downloads"
    else:
        _base_log_dir = os.path.join("/srv/mapwise_dev/county/_batch_logs", datetime.now().strftime("%Y-%m-%d"))
        download_dir = "/srv/mapwise_dev/county/_batch_downloads"
        os.makedirs(download_dir, exist_ok=True)
    
    # Determine which counties to process
    counties_to_process = get_counties_to_process(manual, args)
    driver = None
    main_window = None
    initial_window_count = None

    try:
        # Initialize batch system
        driver, main_window, initial_window_count = initialize_all(_base_log_dir, download_dir, chromium, local, headless)

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
            # Attempt each county up to the number of attempts
            max_attempts = attempts
            for attempt in range(1, max_attempts + 1):
                county_logger = start_county_logging(county_name, isolate_county_logs, chromium, local)
                try:
                    # Call the function with shared resources
                    result = download_county(county_name, driver, county_logger, download_dir, main_window, initial_window_count)

                    if result.get('status') == 'SUCCESS':
                        # Success – record information and stop retrying
                        log_county_info(county_name, f"Data date: {result['data_date']}")
                        end_county_logging(county_name, 'SUCCESS',
                                           attempt=attempt,
                                           file_count=result.get('file_count'),
                                           file_count_status=result.get('file_count_status'))
                        break
                    else:
                        # The function returned FAILED – log the error
                        log_county_error(county_name, f"Attempt {attempt} error: {result.get('error')}")
                        end_county_logging(county_name, 'FAILED',
                                           attempt=attempt,
                                           error_message=result.get('error'),
                                           file_count=result.get('file_count'),
                                           file_count_status=result.get('file_count_status'))
                except Exception as e:
                    # Unexpected exception during the attempt – log it
                    log_county_error(county_name, f"Attempt {attempt} unexpected error: {str(e)}")
                    end_county_logging(county_name, 'FAILED', attempt=attempt, error_message=str(e))

                # If we reach here, the attempt failed. Decide whether to retry.
                if attempt < max_attempts:
                    county_logger.info(f"Retrying {county_name.replace('_', ' ').title()} (attempt {attempt + 1}/{max_attempts}) after reinitializing browser…")
                    driver.quit()
                    driver, main_window, initial_window_count = initialize_all(_base_log_dir, download_dir, chromium, local, headless)
                else:
                    county_logger.info(f"All attempts failed for {county_name.replace('_', ' ').title()}.")

            # Optional delay between counties (regardless of success/failure)
            time.sleep(3)

        # Add batch summary
        add_batch_summary(counties_to_process)
        
    except KeyboardInterrupt:
        print('\nReceived keyboard interrupt. Cleaning up...')
        logging.info("Script interrupted by user")
    finally:
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