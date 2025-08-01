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

def get_arcgis_url(url):
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

    # Navigate to the URL
    try: 
        driver.get(url)

        # Wait for page to load
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, selectors["wait_for_selector"])))

        # Click full details
        try:
            full_details = driver.find_element(By.CSS_SELECTOR, selectors["full_details"])
            driver.execute_script("arguments[0].click();", full_details)
            county_logger.info(f"Clicked full details")
        except Exception as e_3:
            county_logger.error(f"Error clicking full details, attempting alternate selector...")
            try:
                WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selectors["wait_for_selector"])))
                full_details = driver.find_element(By.CSS_SELECTOR, selectors["full_details"])
                driver.execute_script("arguments[0].click();", full_details)
                county_logger.info(f"Clicked alternate full details")
            except Exception as e_4:
                county_logger.error(f"Failed to click full details: {e_4}")
                debug_screenshot(context, "full_details_not_found")
                raise CriticalError(f"Failed to click full details: {e_4}")

        # Wait for tab to load
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selectors["data_source"])))
        
        # Get destination link from source
        data_source = driver.find_element(By.CSS_SELECTOR, selectors["data_source"])
        destination_link = data_source.get_attribute("href")


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
    try:
        # Load selectors from county_config.json
        with open("county_config.json", "r") as f:
            county_config = json.load(f)
            selectors = county_config["arcgis"]["selectors"]
            
        # Initialize driver
        driver = webdriver.Chrome()
        
        # Get the ArcGIS URL
        arcgis_url = get_arcgis_url(url)
        
       
    except Exception as e:
        county_logger.error(f"Unexpected error: {e}", exc_info=True)
        raise CriticalError(f"Unexpected error: {str(e)}")