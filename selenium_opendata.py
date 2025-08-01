#!/usr/bin/env python3
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (TimeoutException, NoSuchElementException)
import json

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
        driver = initialize_driver(download_dir, chromium, local, headless)
        
        # Get the ArcGIS URL
        arcgis_url = get_arcgis_url(url)
        
       
    except Exception as e:
        county_logger.error(f"Unexpected error: {e}", exc_info=True)
        raise CriticalError(f"Unexpected error: {str(e)}")