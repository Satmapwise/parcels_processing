#!/usr/bin/env python3
import os
import sys
import json
import logging
import subprocess
import time
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (TimeoutException, NoSuchElementException)

# Set up Selenium WebDriver with anti-detection options
def initialize_driver(download_dir, chromium, local, headless):
    """
    Initializes and configures a shared Selenium WebDriver instance.

    This function uses `undetected-chromedriver` to create a driver instance that is
    less likely to be detected as a bot. It automatically finds the browser binary,
    sets various options to mimic a real user, and configures download preferences.

    Args:
        download_dir (str): The default directory for browser downloads.
        chromium (bool): If True, use Chromium; otherwise, use Google Chrome.
        local (bool): A flag for environment-specific settings (currently unused here).
        headless (bool): If True, run the browser without a UI.

    Returns:
        tuple: A tuple containing:
            - driver: The initialized WebDriver instance.
            - main_window: The handle of the main browser window.
            - initial_window_count (int): The number of open browser windows.
    
    Raises:
        Exception: If the specified browser binary cannot be found.
    """
    options = uc.ChromeOptions()
    browser_executable_path = None

    if chromium:
        logging.info("Chromium mode enabled. Searching for binary...")
        if os.path.exists("/usr/bin/chromium-browser"):
            logging.info("Linux/Docker detected. Using Chromium binary: /usr/bin/chromium-browser")
            browser_executable_path = "/usr/bin/chromium-browser"
        elif os.path.exists("/Applications/Chromium.app/Contents/MacOS/Chromium"):
            logging.info("Mac detected. Using Chromium binary: /Applications/Chromium.app/Contents/MacOS/Chromium")
            browser_executable_path = "/Applications/Chromium.app/Contents/MacOS/Chromium"
        else:
            logging.error("Chromium binary not found. Please install Chromium.")
            raise Exception("Chromium binary not found. Please install Chromium.")
    else: # Use Chrome
        logging.info("Chrome mode enabled. Searching for binary...")
        if os.path.exists("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"):
            logging.info("Mac detected. Using Chrome binary: /Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
            browser_executable_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        elif os.path.exists("/usr/bin/google-chrome"):
            logging.info("Linux/Docker detected. Using Chrome binary: /usr/bin/google-chrome")
            browser_executable_path = "/usr/bin/google-chrome"
        else:
            logging.error("Google Chrome binary not found. Please install Google Chrome.")
            raise Exception("Google Chrome binary not found. Please install Google Chrome.")

    options.binary_location = browser_executable_path

    # Get browser version
    try:
        if chromium:
            if os.path.exists("/Applications/Chromium.app/Contents/MacOS/Chromium"):
                result = subprocess.run(["/Applications/Chromium.app/Contents/MacOS/Chromium", "--version"], 
                                        capture_output=True, text=True)
            else:
                result = subprocess.run(["/usr/bin/chromium-browser", "--version"], 
                                        capture_output=True, text=True)
        else:
            if os.path.exists("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"):
                result = subprocess.run(["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "--version"], 
                                        capture_output=True, text=True)
            else:
                result = subprocess.run(["/usr/bin/google-chrome", "--version"], 
                                        capture_output=True, text=True)
        
        browser_version = result.stdout.strip()
        logging.info(f"Raw browser version output: {browser_version}")
        
        # Handle different version string formats
        if "Chromium" in browser_version:
            # Format: "Chromium 137.0.7104.0"
            browser_major_version = browser_version.split()[1].split('.')[0]
        elif "Google Chrome" in browser_version:
            # Format: "Google Chrome 137.0.7104.0"
            browser_major_version = browser_version.split()[2].split('.')[0]
        else:
            # Try to extract version number directly
            import re
            version_match = re.search(r'(\d+)\.', browser_version)
            if version_match:
                browser_major_version = version_match.group(1)
            else:
                raise Exception(f"Could not parse browser version from: {browser_version}")
        
        logging.info(f"Parsed browser version: {browser_major_version}")
    except Exception as e:
        logging.error(f"Failed to get browser version: {e}")
        raise

    # # Debug Xvfb
    # if not local:
    #     try:
    #         xvfb_process = subprocess.check_output(['ps', 'aux']).decode()
    #         logging.info(f"Running processes: {xvfb_process}")
    #     except Exception as e:
    #         logging.error(f"Failed to check processes: {e}")

    # # Debug DISPLAY
    # if not local:
    #     logging.info(f"DISPLAY environment variable: {os.environ.get('DISPLAY')}")
    #     logging.info(f"X11 socket exists: {os.path.exists('/tmp/.X11-unix/X99')}")

    # Anti-detection arguments (similar to undetected-chromedriver)
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-first-run")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-popup-blocking")  # Explicitly disable popup blocking
    options.add_argument("--allow-popups-during-page-unload")
    options.add_argument("--allow-popups")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    if headless:
        options.add_argument("--headless")
    
    # Set user agent to match browser version
    options.add_argument(f"--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{browser_major_version}.0.0.0 Safari/537.36")
    
    # Set download preferences
    prefs = {
        "profile.default_content_settings.popups": 0,  # 0 allows popups
        "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.images": 1,  # 1 allows images
        "download.default_directory": download_dir,  # Keep the download directory setting
        "download.prompt_for_download": False,  # Keep download prompt setting
        "plugins.always_open_pdf_externally": True,  # Keep PDF handling setting
        "safebrowsing.disable_download_protection": True
    }
    options.add_experimental_option("prefs", prefs)
    options.set_capability("pageLoadStrategy", "none")
    
    # ------------------------------------------------------------------
    #  Launch browser via undetected_chromedriver letting it manage the
    #  driver download/cache.  This works the same whether we run locally
    #  or on a server / inside Docker, so we no longer branch on *local*.
    # ------------------------------------------------------------------
    try:
        driver = uc.Chrome(
            browser_executable_path=browser_executable_path,
            options=options,
            version_main=int(browser_major_version)
        )
        
        # Execute script to hide webdriver property
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        # Set window context
        main_window = driver.current_window_handle
        initial_window_count = len(driver.window_handles)
        # driver.maximize_window()  # open browser in max window for visibility
        
        logging.info("Connected to shared WebDriver with anti-detection")
        return driver, main_window, initial_window_count
    except Exception as e:
        logging.error(f"Failed to initialize webdriver: {e}")
        raise



def get_arcgis_url(driver, url, selectors):
    """
    Extract ArcGIS service URL from an opendata portal using Selenium.

    This function navigates to an opendata portal page, clicks through the interface
    to find the "Data Source" link that points to the actual ArcGIS REST service.

    Args:
        driver: Selenium WebDriver instance
        url (str): The opendata portal URL to extract from
        selectors (dict): CSS selectors for navigation elements

    Returns:
        str: The extracted ArcGIS service URL, or None if extraction failed

    Raises:
        Exception: For critical failures in navigation or element finding
    """
    try:
        logging.info(f"Navigating to: {url}")
        driver.get(url)

        # Check if data source is already present
        try:
            data_source = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selectors["data_source"]))
            )
            arcgis_url = data_source.get_attribute("href")
            logging.info(f"Found ArcGIS URL: {arcgis_url}")
            return arcgis_url
        except Exception as e:
            logging.error(f"Failed to find data source link: {e}")
            return None

        # Click "Full Details" or "View Full Details" button
        try:
            full_details = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selectors["full_details"]))
            )
            if full_details:
                driver.execute_script("arguments[0].click();", full_details)
                logging.info("Clicked full details button")
        except Exception as e:
            logging.error(f"Failed to click full details: {e}")
            return None

        # Wait for the data source link to appear and extract it
        try:
            data_source = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selectors["data_source"]))
            )
            arcgis_url = data_source.get_attribute("href")
            logging.info(f"Found ArcGIS URL: {arcgis_url}")
            return arcgis_url
        except Exception as e:
            logging.error(f"Failed to find data source link: {e}")
            return None

    except TimeoutException as te:
        logging.error(f"Timeout while loading page: {te}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return None
    


def extract_arcgis_url_from_opendata(opendata_url, headless=True):
    """
    Main function to extract ArcGIS service URL from an opendata portal.
    
    Args:
        opendata_url (str): The opendata portal URL to extract from
        headless (bool): Whether to run browser in headless mode
        
    Returns:
        str: The extracted ArcGIS service URL, or None if extraction failed
    """
    driver = None
    try:
        # Load selectors from county_config.json
        with open("county_config.json", "r") as f:
            county_config = json.load(f)
            selectors = county_config["arcgis"]["selectors"]
            
        # Initialize driver with minimal settings
        download_dir = "/tmp"  # We don't need downloads for URL extraction
        try:
            driver, main_window, initial_window_count = initialize_driver(
                download_dir=download_dir, 
                chromium=True,   # Use Chromium
                local=False,     # Server environment
                headless=headless
            )
        except Exception as init_e:
            logging.error(f"Failed to initialize driver: {init_e}")
            return None
        
        # Extract the ArcGIS URL with timeout protection
        try:
            arcgis_url = get_arcgis_url(driver, opendata_url, selectors)
            return arcgis_url
        except Exception as extract_e:
            logging.error(f"Failed to extract URL: {extract_e}")
            return None
        
    except Exception as e:
        logging.error(f"Error extracting ArcGIS URL: {e}")
        return None
    finally:
        # Robust driver cleanup
        if driver:
            try:
                # Force close all windows first
                driver.close()
            except:
                pass
            try:
                # Then quit the driver
                driver.quit()
            except:
                pass
            try:
                # Kill any remaining processes
                driver.service.stop()
            except:
                pass


if __name__ == "__main__":
    # Simple test
    test_url = "https://ocgis-datahub-ocfl.hub.arcgis.com/datasets/ocfl::orange-county-zoning/explore"
    result = extract_arcgis_url_from_opendata(test_url, headless=False)
    print(f"Extracted URL: {result}")