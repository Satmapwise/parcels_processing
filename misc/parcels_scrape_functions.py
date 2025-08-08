#!/usr/bin/env python3
# Generic automation functions
import os
import json
import logging
import csv
import time
import fnmatch
import shutil
import re
import zipfile
import glob
from datetime import datetime, timedelta
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchWindowException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import undetected_chromedriver as uc
import subprocess

SELENIUM_COUNTIES = [
    "palm_beach", "duval", "pinellas", "collier", "escambia", "okaloosa", "bay", "santa_rosa",
    "indian_river", "sumter", "flagler", "nassau", "walton", "putnam", "columbia",
    "jackson", "gadsden", "suwannee", "levy", "okeechobee", "hendry", "desoto",
    "wakulla", "bradford", "hardee", "washington", "taylor", "holmes", "madison",
    "gilchrist", "dixie", "union", "jefferson", "gulf", "hamilton", "calhoun",
    "franklin", "glades", "lafayette", "liberty"
]
ALL_COUNTIES = [
    "miami_dade", "broward", "palm_beach", "hillsborough", "orange", "duval", "pinellas", "lee", 
    "polk", "brevard", "pasco", "volusia", "seminole", "sarasota", "manatee", "osceola", "lake", 
    "marion", "collier", "st_lucie", "escambia", "leon", "alachua", "st_johns", "clay", "okaloosa", 
    "hernando", "bay", "santa_rosa", "charlotte", "martin", "indian_river", "citrus", "sumter", 
    "flagler", "highlands", "nassau", "monroe", "walton", "putnam", "columbia", "jackson", "gadsden", 
    "suwannee", "levy", "okeechobee", "hendry", "desoto", "wakulla", "bradford", "baker", "hardee", 
    "washington", "taylor", "holmes", "madison", "gilchrist", "dixie", "union", "jefferson", "gulf", 
    "hamilton", "calhoun", "franklin", "glades", "lafayette", "liberty"
]

VALID_COUNTIES = ALL_COUNTIES

# Module-level variables to track sessions
_active_sessions = {}
_summary_file = None
_batch_initialized = False


# General functions
# Helper: Set up batch logging, directories, and summary CSV
def initialize_logging(log_dir):
    """
    Creates log directories and the summary CSV file for the batch run.

    This function sets up the directory structure for logs (including a general
    batch log, individual county logs, and debug screenshots) and creates a CSV
    file to summarize the results of each county's download attempt. It also
    configures the root logger for the application.
    
    Args:
        log_dir (str or Path): The root directory for process logs.

    Returns:
        tuple: A tuple containing a dictionary of log directory paths and the
                path to the batch summary CSV file.
    """
    global _log_dir, _summary_file
    
    # Create directory structure
    _log_dir = Path(log_dir)
    _log_dir.mkdir(parents=True, exist_ok=True)  # Ensure all parent dirs are created
    
    county_logs_dir = _log_dir / "county_logs"
    debug_screenshots_dir = _log_dir / "debug_screenshots" 
    county_logs_dir.mkdir(parents=True, exist_ok=True)  # Ensure all parent dirs are created
    debug_screenshots_dir.mkdir(parents=True, exist_ok=True)  # Ensure all parent dirs are created
    
    # Initialize summary CSV
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    _summary_file = _log_dir / f"batch_summary_{current_time}.csv"
    
    # New headers
    HEADERS = ["county", "status", "data_date", "file_count", "file_count_status", "attempts", "duration", "start_time", "end_time", "error_message", "processed?", "qa_status", "qa_error_message"]

    if not _summary_file.exists():
        with open(_summary_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(HEADERS)
            # Populate with all counties
            for county in ALL_COUNTIES:
                # county, status='N/A', rest blank
                if county in SELENIUM_COUNTIES:
                    writer.writerow([county, 'N/A', '', '', '', '', '', '', '', '', 'FALSE', '', ''])
                else:
                    writer.writerow([county, 'N/S', '', '', '', '', '', '', '', '', 'TRUE', '', ''])
    
    # ---------------------------------------------------------------
    #  Configure (or re-configure) root logging.
    #  A stray logging call that fires before this function can cause
    #  Python to install its default WARNING-level StreamHandler.
    #  We use force=True so our INFO-level console + file handlers
    #  replace anything that might already exist.
    # ---------------------------------------------------------------
    file_path = _log_dir / f"main_batch_{current_time}.log"
    console_h = logging.StreamHandler()
    # Overwrite main log file at the start of every run
    file_h    = logging.FileHandler(file_path, mode='w', encoding='utf-8')

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[console_h, file_h],
        force=True  # replace any previous configuration
    )
    
    log_dirs = {
        'base': _log_dir,
        'county_logs': county_logs_dir,
        'debug_screenshots': debug_screenshots_dir
    }
    
    logging.info(f"Batch logging system initialized. Base dir: {_log_dir}")
    return log_dirs, _summary_file

# Function for general initialization
def initialize_all(_log_dir, download_dir, chromium, local, headless):
    """
    Initializes the web driver, logging configurations, and directories for a batch run.

    This function sets up the entire environment for the scraping process. It configures
    a shared Selenium WebDriver with anti-detection measures, establishes logging for both
    the main batch process and individual counties, and ensures the necessary directories
    for logs and downloads exist.

    Args:
        base_log_dir (str): The root directory where all log files will be stored.
        download_dir (str): The directory where the browser will save downloaded files.
        chromium (bool): If True, use the Chromium browser; otherwise, use Google Chrome.
        local (bool): If True, configures paths for a local environment.
        headless (bool): If True, runs the browser in headless mode without a visible UI.

    Returns:
        tuple: A tuple containing:
            - driver: The initialized Selenium WebDriver instance.
            - main_window: The handle of the main browser window.
            - initial_window_count (int): The number of browser windows at startup.
    """
    global _batch_initialized, _summary_file
    
    # Helper: Set up Selenium WebDriver with anti-detection options
    def initialize_shared_driver(download_dir, chromium, local, headless):
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

    # Main logic for initialization
    if not _batch_initialized:
        # Batch mode: initialize once
        initialize_logging(_log_dir)
        _batch_initialized = True
    if not os.path.exists(download_dir):
        if local:
            logging.warning(f"Download directory {download_dir} does not exist. Creating it...")
            os.makedirs(download_dir)
        else:
            raise Exception(f"WARNING: Download directory {download_dir} does not exist")
    driver, main_window, initial_window_count = initialize_shared_driver(download_dir, chromium, local, headless)
    return driver, main_window, initial_window_count

# Starts a logging session for a specific county.
# Creates a logger and log file, and tracks session data.
def start_county_logging(county_name, county_name_formatted, isolate_county_logs, chromium, local):
    """
    Initializes a dedicated logging session for a single county.

    This creates a new logger and a unique log file for the county. It also
    initializes a session tracking dictionary to store metadata about the
    download process, such as start time, browser type, and environment.

    Args:
        county_name (str): The name of the county.
        isolate_county_logs (bool): If True, county log messages will not be
            propagated to the main batch logger, keeping the main log cleaner.
        chromium (bool): The browser type being used for this session.
        local (bool): The environment type (e.g., local or server).

    Returns:
        logging.Logger: The configured logger instance for the specific county.
    """
    global _active_sessions

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"{county_name.lower()}_{timestamp}.log"
    log_file_path = _log_dir / "county_logs" / log_filename
    
    # Create county-specific logger
    county_logger = logging.getLogger(f"county.{county_name.lower()}")
    county_logger.setLevel(logging.DEBUG)
    county_logger.propagate = not isolate_county_logs   # <— one-liner switch
    
    # Clear any existing handlers
    for handler in county_logger.handlers[:]:
        county_logger.removeHandler(handler)
    
    # Add file handler
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    county_logger.addHandler(file_handler)
    
    # Track session data
    session_data = {
        'county_name': county_name,
        'start_time': datetime.now(),
        'start_timestamp': time.time(),
        'log_file_path': log_file_path,
        'data_date': None,
        'error_message': None,
        'file_count': 0,
        'file_count_status': 'PENDING'
    }
    
    _active_sessions[county_name] = session_data

    logging.info(f"Starting download session for {county_name_formatted}")
    
    return county_logger

# Logs an info message for a county, and optionally updates the data date.
def log_county_info(county_name, message):
    """
    Logs an informational message for a county and updates its session data.

    If the message contains a date (e.g., "Data date: 01/01/2024"), this function
    parses that date and stores it in the active session for the county.

    Args:
        county_name (str): The county to which the log message applies.
        message (str): The informational message to log.
    """
    if county_name in _active_sessions:
        logger = logging.getLogger(f"county.{county_name.lower()}")
        logger.info(message)
        
        # Check if message contains a data date
        if "Data date:" in message or "Extracted date:" in message:
            # Extract the date from the message
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', message)
            if date_match:
                # Parse the date and reformat it to M/DD/YYYY
                try:
                    date_str = date_match.group(1)
                    parsed_date = datetime.strptime(date_str, "%m/%d/%Y")
                    formatted_date = parsed_date.strftime("%-m/%-d/%Y")  # Use - to remove leading zeros
                    _active_sessions[county_name]['data_date'] = formatted_date
                    logger.info(f"Updated session data_date to: {formatted_date}")
                except ValueError as e:
                    logger.error(f"Failed to parse date {date_str}: {e}")

# Logs an error message for a county, and updates the error message in session data.
def log_county_error(county_name, message):
    """
    Logs an error message for a county and records it in the session data.

    Args:
        county_name (str): The county where the error occurred.
        message (str): The error message to log and record.
    """
    if county_name in _active_sessions:
        logger = logging.getLogger(f"county.{county_name.lower()}")
        logger.error(message)
        _active_sessions[county_name]['error_message'] = message

# Takes a debug screenshot for a county and logs the file path.
def debug_screenshot(context, description):
    """
    Saves a screenshot of the current browser state for debugging purposes.

    The screenshot is saved in a dedicated directory for the county, with a
    timestamp and descriptive name for easy identification.

    Args:
        context (CountyContext): The context object containing the driver and county info.
        description (str): A brief description of the state being captured, used
            in the filename.

    Returns:
        str or None: The full path to the saved screenshot file, or None if saving fails.
    """
    county_name = context.county_name
    driver = context.driver

    if county_name not in _active_sessions:
        logging.warning(f"No active session for {context.county_name_formatted}")
        return None
    
    # Create county debug directory
    county_debug_dir = _log_dir / "debug_screenshots" / county_name.lower()
    county_debug_dir.mkdir(exist_ok=True)
    
    # Generate filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_description = "".join(c for c in description if c.isalnum() or c in (' ', '_')).replace(' ', '_')
    screenshot_path = county_debug_dir / f"{county_name.lower()}_{timestamp}_{safe_description}.png"
    
    try:
        driver.save_screenshot(str(screenshot_path))
        log_county_info(county_name, f"Debug screenshot saved: {screenshot_path}")
        return str(screenshot_path)
    except Exception as e:
        log_county_error(county_name, f"Failed to save screenshot: {e}")
        return None

# Ends the logging session for a county, writes a summary row to the CSV, and cleans up.
def end_county_logging(county_name, county_name_formatted, status, attempt, error_message=None, file_count=None, file_count_status=None):
    """
    Finalizes a county's logging session and records the outcome.

    This function calculates the duration of the session, writes a summary row
    to the main batch CSV file (including status, data date, error messages, etc.),
    and closes the county-specific log file handlers.

    Args:
        county_name (str): The name of the county whose session is ending.
        status (str): The final status of the download ('SUCCESS' or 'FAILED').
        attempt (int): The attempt number for this run.
        error_message (str, optional): The final error message, if any.
        file_count (int, optional): The total number of files downloaded.
        file_count_status (str, optional): The status based on the file count.
    """
    global _active_sessions, _summary_file
    
    if county_name not in _active_sessions:
        logging.error(f"No active session for {county_name_formatted}")
        return
    
    session = _active_sessions[county_name]
    end_time = datetime.now()
    duration_seconds = int(time.time() - session['start_timestamp'])
    
    # Update session data
    if error_message:
        session['error_message'] = error_message
    if file_count is not None:
        session['file_count'] = file_count
    if file_count_status:
        session['file_count_status'] = file_count_status
    
    # Log final status
    county_logger = logging.getLogger(f"county.{county_name.lower()}")
    logging.info(f"{county_name_formatted} session completed: {status} ({duration_seconds}s)")
    if session['data_date']:
        logging.info(f"Final data date: {session['data_date']}")
    if file_count:
        logging.info(f"Files downloaded: {file_count} ({file_count_status})")
    
    # Truncate and sanitize error message
    error_msg = (error_message or session['error_message'] or "")
    if error_msg:
        if '(Session info:' in error_msg:
            error_msg = error_msg.split('(Session info:')[0].strip()
        error_msg = error_msg.replace('"', "'").replace('\n', ' ')[:200]

    # Prepare new row data as a dictionary
    new_row_data = {
        "county": county_name,
        "status": status,
        "data_date": session['data_date'] or "",
        "file_count": session.get('file_count', 0),
        "file_count_status": session.get('file_count_status', 'PENDING'),
        "attempts": attempt,
        "duration": f"{duration_seconds}s",
        "start_time": session['start_time'].strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "error_message": error_msg,
        "processed?": "FALSE"
    }

    # Read existing data, update, and write back
    try:
        if _summary_file is None:
            raise CriticalError("Summary file not initialized")
        with open(_summary_file, 'r', newline='') as f:
            rows = list(csv.DictReader(f))

        # Separate data rows from a potential summary row
        data_rows = [row for row in rows if row['county'] != 'TOTALS']
        summary_rows = [row for row in rows if row['county'] == 'TOTALS']
        
        # Find the row to update
        row_found = False
        for i, row in enumerate(data_rows):
            if row['county'] == county_name:
                # Preserve existing qa fields if they exist
                new_row_data['qa_status'] = row.get('qa_status', '')
                new_row_data['qa_error_message'] = row.get('qa_error_message', '')
                data_rows[i] = new_row_data
                row_found = True
                break
        
        if not row_found: # Should not happen if initialized correctly
            data_rows.append(new_row_data)

        # Write all data rows and the summary row back
        headers = ["county", "status", "data_date", "file_count", "file_count_status", "attempts", "duration", "start_time", "end_time", "error_message", "processed?", "qa_status", "qa_error_message"]
        
        if _summary_file is None:
            raise CriticalError("Summary file not initialized")
        with open(_summary_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data_rows)
            if summary_rows:
                writer.writerows(summary_rows)

    except Exception as e:
        logging.error(f"Failed to update summary CSV: {e}")

    # Cleanup
    for handler in county_logger.handlers[:]:
        handler.close()
        county_logger.removeHandler(handler)
    
    del _active_sessions[county_name]

# Download functions

# Define critical error class
class CriticalError(Exception):
    """
    Custom exception for critical errors that should halt execution.
    """
    pass

# Function to load config for a specific county from JSON
def load_county_config(county_name, county_name_formatted):
    """
    Loads and returns the configuration for a specific county from the JSON config file.

    Args:
        county_name (str): The name of the county for which to load configuration.

    Returns:
        dict: The configuration dictionary for the specified county.

    Raises:
        CriticalError: If the `parcels_scrape.json` file cannot be found or if the
            specified county is not present in the configuration.
    """
    config_path = os.path.join(os.path.dirname(__file__), "parcels_scrape.json")
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
    except Exception as e:
        logging.error(f"Unable to load {config_path}. Make sure the file exists.")
        raise CriticalError(f"Could not load {config_path}") from e

    if county_name not in config:
        logging.error(f"County '{county_name_formatted}' not found in configuration.")
        raise CriticalError(f"County '{county_name_formatted}' not found in configuration.")

    # Special handling for palm_beach which has a nested structure
    if county_name == "palm_beach":
        return config
    else:
        # For all other counties, return their specific configuration block.
        return config[county_name]

# Function to find an element with waits, iframe handling, and error handling (5 main arguments, 2 config arguments)
def safe_find(
    context,
    description,
    check_frames=True,
    specific_frame=None,
    critical=True
    ):
    """
    Finds a web element safely, with robust handling for frames and timeouts.

    This function looks for an element using a CSS selector retrieved from the
    county's configuration. It can search within the main document, a specific iframe,
    or all iframes on the page.

    Args:
        context (CountyContext): The context object containing the driver, logger, and config.
        description (str): A key used to look up the CSS selector in the county's
            configuration and for logging purposes.
        check_frames (bool): If True, searches inside all frames on the page if the
            element is not found in the main document. Defaults to True.
        specific_frame (str, optional): The name or ID of a specific frame to
            search within first. Defaults to None.
        critical (bool): If True, raises a CriticalError if the element is not found.
            Otherwise, logs an error and returns None. Defaults to True.

    Returns:
        WebElement or None: The found Selenium WebElement, or None if not found and
                            `critical` is False.

    Raises:
        CriticalError: If the element is not found and `critical` is True.
    """
    element = None
    original_context = None
    timeout = 40

    county_logger = context.county_logger
    driver = context.driver
    county_name = context.county_name
    css_selector = context.county_config["selectors"][description]

    # Function to detect frames
    def detect_frames(driver, county_name, save_page_source=True):
        """
        Detects frames/iframes on the current page and logs their details.
        
        Args:
            driver: Selenium WebDriver instance
            county_name: Name of the county being processed
            save_page_source: Whether to save the page HTML for debugging
            
        Returns:
            list: Combined list of frames and iframes found
        """
        # Debug: Check for frames on the page
        frames = driver.find_elements(By.TAG_NAME, "frame")
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        county_logger.info(f"Found {len(frames)} frames and {len(iframes)} iframes on the page")
        
        all_frames = []
        # If frames/iframes exist, log their details
        if frames or iframes:
            all_frames = frames + iframes
            for i, frame in enumerate(all_frames):
                try:
                    frame_id = frame.get_attribute("id") or "no-id"
                    frame_name = frame.get_attribute("name") or "no-name"
                    frame_src = frame.get_attribute("src") or "no-src"
                    county_logger.info(f"Frame/iframe #{i}: id='{frame_id}', name='{frame_name}', src='{frame_src}'")
                except:
                    county_logger.info(f"Frame/iframe #{i}: (failed to get attributes)")
        
        # # Debug: Save page source for analysis
        # if save_page_source:
        #     with open(os.path.join(debug_dir, f"{county_name}_page_source.html"), "w") as f:
        #         f.write(driver.page_source)
        
        return all_frames

    try:
        # Check current context
        if specific_frame is None:
            try:
                if "contains" in css_selector:
                    tag, text = css_selector.split(":contains('")
                    text = text.rstrip("')")
                    elements = driver.find_elements(By.TAG_NAME, tag)
                    for elem in elements:
                        if text in elem.text:
                            element = elem
                            break
                else:
                    # Use shorter timeout for specific frame
                    frame_timeout = min(3, timeout)
                    element = WebDriverWait(driver, frame_timeout).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
                    )
                
                if element:
                    county_logger.info(f"Found {description} in current context")
                    return element
            except Exception as e:
                county_logger.error(f"Error checking current context for {description}: {e}")
                pass

        # Save original context to return to it later if needed
        driver.switch_to.default_content()
        original_context = "default"

        # Check specified frame if provided
        if specific_frame is not None:
            try:
                county_logger.info(f"Checking specified frame: {specific_frame}")
                driver.switch_to.default_content()
                driver.switch_to.frame(specific_frame)
                original_context = "specific_frame"
                
                # Try to find element in the specific frame
                if "contains" in css_selector:
                    tag, text = css_selector.split(":contains('")
                    text = text.rstrip("')")
                    elements = driver.find_elements(By.TAG_NAME, tag)
                    for elem in elements:
                        if text in elem.text:
                            element = elem
                            break
                else:
                    # Use shorter timeout for specific frame
                    frame_timeout = min(3, timeout)
                    element = WebDriverWait(driver, frame_timeout).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
                    )
                
                if element:
                    county_logger.info(f"Found {description} in specified frame")
                    return element
            except TimeoutException:
                # If not found in specified frame, that's okay. Log it and proceed.
                county_logger.info(f"'{description}' not found in specified frame '{specific_frame}'.")
                driver.switch_to.default_content()
                original_context = "default"
        
        # First check main document
        try:
            # Handle special case for text-containing elements
            if "contains" in css_selector:
                county_logger.info(f"Checking as special case in main document for {description}")
                tag, text = css_selector.split(":contains('")
                text = text.rstrip("')")
                elements = driver.find_elements(By.TAG_NAME, tag)
                for elem in elements:
                    if text in elem.text:
                        element = elem
                        county_logger.info(f"Found {description} as special case in main document")
                        return element
            else:
                # Use shorter timeout for main document check to move to frames faster if needed
                county_logger.info(f"Checking main document for {description}")
                main_timeout = min(3, timeout)
                element = WebDriverWait(driver, main_timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
                )
                county_logger.info(f"Found {description} in main document")
                return element
        except TimeoutException:
            # Element not found in main document, will check frames if requested
            pass
            
        # If element not found in main document and frames should be checked
        if element is None and check_frames:
            county_logger.info(f"Element {description} not found in main document, checking frames...")

            all_frames = detect_frames(driver, county_name, save_page_source=False)
            
            if all_frames:
                for i, frame in enumerate(all_frames):
                    try:
                        county_logger.info(f"Trying frame #{i} for {description}...")
                        driver.switch_to.default_content()
                        driver.switch_to.frame(frame)
                        original_context = f"frame_{i}"
                        
                        # Try to find element in this frame
                        if "contains" in css_selector:
                            tag, text = css_selector.split(":contains('")
                            text = text.rstrip("')")
                            elements = driver.find_elements(By.TAG_NAME, tag)
                            for elem in elements:
                                if text in elem.text:
                                    element = elem
                                    break
                        else:
                            # Use shorter timeout for each frame
                            frame_timeout = min(2, timeout)
                            element = WebDriverWait(driver, frame_timeout).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
                            )
                            
                        if element:
                            county_logger.info(f"Found {description} in frame #{i}")
                            return element
                        
                        # If not found in this frame, continue to next
                        driver.switch_to.default_content()
                        original_context = "default"
                    except TimeoutException:
                        # Element not in this frame, continue to next. This is expected.
                        driver.switch_to.default_content()
                        original_context = "default"
        
        # If we've searched everywhere and still haven't found the element
        if element is None:
            msg = f"Could not find element: {description} ({css_selector})"
            county_logger.error(msg)
        
            if critical:
                # Take screenshot for debugging
                debug_screenshot(context, f"{description}_not_found")
                raise CriticalError(msg)
            else:
                return None
            
    except Exception as e:
        county_logger.error(f"Unexpected error in safe_find for {description}: {e}")
        if critical:
            raise CriticalError(f"Unexpected error in safe_find for {description}: {e}")
        else:
            return None
    finally:
        # Always return to default content unless we found the element in a frame
        if element is None and original_context != "default":
            try:
                driver.switch_to.default_content()
            except:
                pass
    
    return element

# Function to safely input text into a field
def safe_input(
    context,
    element,
    description,
    text,
    specific_method=None,
    return_to_default=False,
    critical=True,
    debug=False
    ):
    """
    Inputs text into a field safely, trying multiple methods as fallbacks.

    This function attempts to send text to a web element, verifying that the input
    was successful. It tries a standard `send_keys`, then `ActionChains`, and finally
    a JavaScript-based input as fallbacks.

    Args:
        context (CountyContext): The context object for the current county.
        element (WebElement): The input field element.
        description (str): A descriptive name for the element for logging.
        text (str): The text to be entered into the field.
        specific_method (str, optional): Forces a specific input method ('regular',
            'action', 'js', 'scroll'). Defaults to None.
        return_to_default (bool): If True, switches the driver context back to the
            default content after the operation. Defaults to False.
        critical (bool): If True, raises a CriticalError on failure. Defaults to True.
        debug (bool): If True, saves a screenshot on failure. Defaults to False.

    Returns:
        bool: True if the input was successful, False otherwise.

    Raises:
        CriticalError: If all input methods fail and `critical` is True.
    """
    county_logger = context.county_logger
    driver = context.driver

    def _value_matches(driver, element, expected, wait_time=1.0):
        """
        Returns True if the element's value matches *expected*.

        Match hierarchy:
        1. Exact string equality
        2. Same calendar date (ignores formatting)
        3. Same number after stripping separators / currency symbols
        4. Truncated input with same character (e.g. "99999999" matches "9999999999")
        """
        # --- helper format sets ----------------------------------------
        _DATE_FORMATS = (
            "%m/%d/%Y", "%m/%d/%y",
            "%m-%d-%Y", "%m-%d-%y",
            "%Y-%m-%d", "%Y/%m/%d",
        )

        def _parse_date(s: str):
            s = s.strip()
            for fmt in _DATE_FORMATS:
                try:
                    return datetime.strptime(s, fmt).date()
                except ValueError:
                    continue
            return None

        def _parse_number(s: str):
            """Return a float parsed from any formatted number string or None."""
            cleaned = re.sub(r"[^0-9.\-]", "", s)
            try:
                return float(cleaned)
            except ValueError:
                return None

        def _is_truncated_match(actual: str, expected: str) -> bool:
            """Check if actual is a truncated version of expected with same character."""
            if not actual or not expected:
                return False
            # Check if all characters in expected are the same
            if not all(c == expected[0] for c in expected):
                return False
            # Check if actual is at least 7 characters and all characters match expected[0]
            return len(actual) >= 7 and all(c == expected[0] for c in actual)

        end = time.time() + wait_time
        while time.time() < end:
            actual = driver.execute_script("return arguments[0].value;", element).strip()

            if not actual:
                time.sleep(0.1)
                continue

            if actual == expected.strip():
                return actual

            # Date-tolerant comparison
            ad, ed = _parse_date(actual), _parse_date(expected)
            if ad and ed and ad == ed:
                return actual

            # Numeric-tolerant comparison
            an, en = _parse_number(actual), _parse_number(expected)
            if an is not None and en is not None and an == en:
                return actual

            # Truncated input check
            if _is_truncated_match(actual, expected):
                return actual

            time.sleep(0.1)  # small poll interval

        # Log for debugging
        county_logger.info(f"Expected: {expected}, Actual: {actual}")
        return None

    success = False
    if element is None:
        county_logger.error(f"Cannot input text into {description}: Element is None")
        if critical:
            raise CriticalError(f"Cannot input text into {description}: Element is None")
        return False
    # If specific method is provided, use it
    if specific_method:
        try:
            if specific_method == "regular":
                element.clear()
                element.send_keys(text)
            elif specific_method == "action":
                element.clear()
                ActionChains(driver).move_to_element(element).send_keys(text).perform()
            elif specific_method == "js":
                driver.execute_script("""
                    arguments[0].value = arguments[1];
                    arguments[0].dispatchEvent(new Event('input', {bubbles:true}));
                    arguments[0].dispatchEvent(new Event('change', {bubbles:true}));
                """, element, text)
            elif specific_method == "scroll":
                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                time.sleep(1)  # Short pause after scrolling
                driver.execute_script("arguments[0].value = arguments[1];", element, text)
            else:
                county_logger.error(f"Invalid specific method: {specific_method}")
                raise CriticalError(f"Invalid specific method: {specific_method}")
            
            # Check if the input was successful
            actual = _value_matches(driver, element, text)
            if actual:
                county_logger.info(f"Input {description} with text: {actual} using specific method \"{specific_method}\".")
                success = True
            else:
                county_logger.error(f"Attempt to input {description} with text: {text} using specific method \"{specific_method}\" failed.")
                if debug:
                    debug_screenshot(context, f"{description}_input_fail")
                raise CriticalError(f"Attempt to input {description} with text: {text} using specific method \"{specific_method}\" failed.")

        except Exception as specific_method_e:
            county_logger.error(f"Error inputting {description} using specific method \"{specific_method}\": {specific_method_e}")
            if debug:
                debug_screenshot(context, f"{description}_input_fail")
            county_logger.info(f"Attempting alternative methods...")

    # Run through input methods
    if success == False:
        try:
            # Try regular input first
            element.clear()
            element.send_keys(text)
            actual = _value_matches(driver, element, text)
            if actual:
                county_logger.info(f"Input {description} with text: {actual} using regular method.")
                success = True
            else:
                county_logger.error(f"Attempt to input {description} with text: {text} using regular method failed.")
                if debug:
                    debug_screenshot(context, f"{description}_input_fail")
                raise Exception
        except Exception as regular_e:
            try:
                # Fallback to ActionChains input
                element.clear()
                ActionChains(driver).move_to_element(element).send_keys(text).perform()
                actual = _value_matches(driver, element, text)
                if actual:
                    county_logger.info(f"Input {description} with text: {actual} using ActionChains.")
                    success = True
                else:
                    county_logger.error(f"Attempt to input {description} with text: {text} using ActionChains failed.")
                    if debug:
                        debug_screenshot(context, f"{description}_input_fail")
                    raise Exception
            except Exception as action_e:
                try:
                    # Try JavaScript input
                    driver.execute_script("""
                        arguments[0].value = arguments[1];
                        arguments[0].dispatchEvent(new Event('input', {bubbles:true}));
                        arguments[0].dispatchEvent(new Event('change', {bubbles:true}));
                    """, element, text)
                    actual = _value_matches(driver, element, text)
                    if actual:
                        county_logger.info(f"Input {description} with text: {actual} using JavaScript.")
                        if debug:  
                            debug_screenshot(context, f"{description}_input_success")
                        success = True
                    else:
                        county_logger.error(f"Attempt to input {description} with text: {text} using JavaScript failed.")
                        if debug:
                            debug_screenshot(context, f"{description}_input_fail")
                        raise Exception
                except Exception as js_e:
                    try:
                        # Try JavaScript input with scroll as last resort
                        driver.execute_script("arguments[0].scrollIntoView(true);", element)
                        time.sleep(1)  # Short pause after scrolling
                        driver.execute_script("""
                            arguments[0].value = arguments[1];
                            arguments[0].dispatchEvent(new Event('input', {bubbles:true}));
                            arguments[0].dispatchEvent(new Event('change', {bubbles:true}));
                        """, element, text)
                        actual = _value_matches(driver, element, text)
                        if actual:
                            county_logger.info(f"Input {description} with text: {actual} using JavaScript and scroll.")
                            success = True
                        else:
                            county_logger.error(f"Attempt to input {description} with text: {text} using JavaScript and scroll failed.")
                            if debug:
                                debug_screenshot(context, f"{description}_input_fail")
                            raise Exception
                    except Exception as scroll_e:
                        county_logger.error(f"All attempts to input {description} with text: {text} failed.")
                        if critical:
                            debug_screenshot(context, f"{description}_input_fail")
                            raise CriticalError(f"All attempts to input {description} with text: {text} failed.")
                        elif debug:
                            debug_screenshot(context, f"{description}_input_fail")
                            return False
                        else:
                            return False

    # Make sure we're back in the main document after clicking if requested
    if return_to_default:
        try:
            driver.switch_to.default_content()
            county_logger.info(f"Switched back to main document after {description} interaction")
        except Exception as switch_e:
            county_logger.error(f"Error switching to default content after clicking {description}: {switch_e}")

    return success

# Function to safely click an element
def safe_click(
    context,
    element,
    description,
    return_to_default=False,
    specific_click=None,
    critical=True
    ):
    """
    Clicks an element safely, trying multiple methods as fallbacks.

    This function attempts to click a web element. It tries a standard `.click()`,
    then an `ActionChains` click, and finally a JavaScript-based click as fallbacks
    to handle various website behaviors and element states.

    Args:
        context (CountyContext): The context object for the current county.
        element (WebElement): The element to be clicked.
        description (str): A descriptive name for the element for logging.
        return_to_default (bool): If True, switches the driver context back to the
            default content after the operation. Defaults to False.
        specific_click (str, optional): Forces a specific click method ('click',
            'action', 'js', 'scroll'). Defaults to None.
        critical (bool): If True, raises a CriticalError on failure. Defaults to True.

    Returns:
        bool: True if the click was successful, False otherwise.

    Raises:
        CriticalError: If all click methods fail and `critical` is True.
    """
    county_logger = context.county_logger
    driver = context.driver

    success = False
    if element is None:
        county_logger.error(f"Cannot click {description}: Element is None")
        if critical:
            raise CriticalError(f"Cannot click {description}: Element is None")
        return False
    # If specific click is provided, use it
    if specific_click:
        try:
            if specific_click == "click":
                element.click()
            elif specific_click == "action":
                ActionChains(driver).move_to_element(element).click().perform()
            elif specific_click == "js":
                driver.execute_script("arguments[0].click();", element)
            elif specific_click == "scroll":
                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                time.sleep(1)  # Short pause after scrolling
                driver.execute_script("arguments[0].click();", element)
            else:
                county_logger.error(f"Invalid specific click method: {specific_click}")
                raise CriticalError(f"Invalid specific click method: {specific_click}")
            county_logger.info(f"Clicked {description} using specific click \"{specific_click}\".")
            success = True
        except Exception as specific_click_e:
            county_logger.error(f"Error clicking {description} using specific click \"{specific_click}\": {specific_click_e}")
            county_logger.info(f"Attempting alternative methods...")

    # Run through click        
    if success == False:
        try:
            # Try regular click first
            element.click()
            county_logger.info(f"Clicked {description} using regular click.")
            success = True
        except Exception as click_e:
            county_logger.error(f"Error clicking {description} using regular click: {click_e}")
            county_logger.info(f"Attempting ActionChains click...")
            try:
                # Fallback to ActionChains click
                ActionChains(driver).move_to_element(element).click().perform()
                county_logger.info(f"Clicked {description} using ActionChains.")
                success = True
            except Exception as action_e:
                county_logger.error(f"Error clicking {description} using ActionChains: {action_e}")
                county_logger.info(f"Attempting JavaScript click...")
                try:
                    # Try JavaScript click
                    driver.execute_script("arguments[0].click();", element)
                    county_logger.info(f"Clicked {description} using JavaScript.")
                    success = True
                except Exception as js_e:
                    county_logger.error(f"Error clicking {description} using JavaScript: {js_e}")
                    county_logger.info(f"Attempting JavaScript click with scroll...")
                    try:
                        # Try JavaScript click with scroll as last resort
                        driver.execute_script("arguments[0].scrollIntoView(true);", element)
                        time.sleep(1)  # Short pause after scrolling
                        driver.execute_script("arguments[0].click();", element)
                        county_logger.info(f"Clicked {description} using JavaScript and scroll.")
                        success = True
                    except Exception as scroll_e:
                        county_logger.error(f"All attempts to click {description} failed.")
                        if critical:
                            debug_screenshot(context, f"{description}_click_fail")
                            raise CriticalError(f"All attempts to click {description} failed.")
                        else:
                            return False

    # Make sure we're back in the main document after clicking if requested
    if return_to_default:
        try:
            driver.switch_to.default_content()
            county_logger.info(f"Switched back to main document after {description} interaction")
        except Exception as switch_e:
            county_logger.error(f"Error switching to default content after clicking {description}: {switch_e}")

    return success

# Function to split the date range into chunks of no more than selected number of days
def split_date_range(context, start_date, end_date, chunk_length, date_format = "%m/%d/%Y"):
    """
    Splits a given date range into smaller chunks of a specified maximum length.

    This is useful for websites that limit the amount of data that can be queried
    at once. The function is also careful to not create chunks that cross over
    month boundaries, as some sites also have this limitation.

    Args:
        context (CountyContext): The context object for logging.
        start_date (datetime or str): The beginning of the date range.
        end_date (datetime or str): The end of the date range.
        chunk_length (int): The maximum number of days for any single chunk.
        date_format (str): The string format for the output dates (e.g., "%m/%d/%Y").

    Returns:
        list: A list of tuples, where each tuple contains the start and end date
              strings for a chunk.
    """
    county_logger = context.county_logger

    county_logger.info(f"Splitting date range {start_date} to {end_date} into chunks of {chunk_length} days...")

    # Convert start and end dates to datetime objects if not already
    if not isinstance(start_date, datetime):
        start_date = datetime.strptime(start_date, date_format)
    if not isinstance(end_date, datetime):
        end_date = datetime.strptime(end_date, date_format)

    date_chunks = []
    chunk_start_date = start_date
    
    while chunk_start_date <= end_date:
        # Calculate the last day of the current month
        if chunk_start_date.month == 12:
            # For December, manually set the last day of the month to December 31st
            last_day_of_current_month = datetime(chunk_start_date.year, 12, 31)
        else:
            next_month = chunk_start_date.month + 1
            last_day_of_current_month = datetime(chunk_start_date.year, next_month, 1) - timedelta(days=1)
        
        # The end date for the current chunk is either the selected number of days ahead or the last day of the month, whichever is earlier
        chunk_end_date = min(chunk_start_date + timedelta(days=chunk_length), last_day_of_current_month, end_date)
        
        # Add the chunk to the list
        date_chunks.append((chunk_start_date.strftime(date_format), chunk_end_date.strftime(date_format)))
        
        # Move to the next chunk's start date (the day after the current chunk's end date)
        chunk_start_date = chunk_end_date + timedelta(days=1)

    return date_chunks

# Function to wait for download
def wait_for_download(context, file_pattern, ex_file_pattern=None, timeout=90, check_interval=0.5, log_interval=10, max_age_seconds=None):
    """
    Waits for a file matching a specific pattern to appear in the download directory.

    This function monitors the download directory for a new file. It includes a
    pre-cleanup step to remove any old, partially downloaded, or completed files
    that match the pattern, which prevents issues in iterative downloads.

    Args:
        context (CountyContext): The context object for the current county.
        file_pattern (str): A glob-style pattern for the expected filename (e.g., "*.csv").
        ex_file_pattern (str, optional): A glob-style pattern for filenames to exclude.
        timeout (int): Maximum time in seconds to wait for the download.
        check_interval (float): Time in seconds between checks of the directory.
        log_interval (int): How often to log "still waiting" messages.
        max_age_seconds (int, optional): Only consider files modified within this many
            seconds. Defaults to None.

    Returns:
        tuple: A tuple containing the full path and the filename of the downloaded file.

    Raises:
        CriticalError: If the download does not complete within the specified timeout.
    """
    download_dir = context.download_dir
    county_logger = context.county_logger

    # -----------------------------------------------------------------
    #  Pre-cleanup: remove any lingering partial or completed files that
    #  match the requested pattern. This prevents previous failed or
    #  cancelled downloads from blocking detection in loops where the
    #  same base filename is reused (Putnam, Santa Rosa, QPublic, etc.).
    # -----------------------------------------------------------------
    try:
        for fname in os.listdir(download_dir):

            # Skip if the file matches the exclude pattern
            if ex_file_pattern and fnmatch.fnmatch(fname, ex_file_pattern):
                continue

            # Match complete and temporary files
            if fnmatch.fnmatch(fname, file_pattern) or fnmatch.fnmatch(fname, f"{file_pattern}*"):
                if fname.endswith(('.crdownload', '.part')) or fnmatch.fnmatch(fname, file_pattern):
                    try:
                        os.remove(os.path.join(download_dir, fname))
                        county_logger.info(f"Pruned old artefact before wait: {fname}")
                    except Exception as e:
                        county_logger.warning(f"Could not remove stale file {fname}: {e}")
    except Exception as e:
        county_logger.warning(f"Pre-cleanup step failed: {e}")

    # Get initial files in directory
    initial_files = set(os.listdir(download_dir))
    county_logger.info(f"Waiting for {file_pattern}.")
    
    start_time = time.time()
    last_log_time = start_time
    download_started = False
    temp_file_path = None
    
    while time.time() - start_time < timeout:
        # Wait for specified interval
        time.sleep(check_interval)
        
        # Get current files
        current_files = set(os.listdir(download_dir))
        
        # Find new files
        new_files = current_files - initial_files
        
        if new_files:
            # Filter by pattern if specified
            if file_pattern:
                # Check for temporary files
                temp_pattern = f"{file_pattern}*"
                temp_files = [f for f in new_files if fnmatch.fnmatch(f, temp_pattern) and (f.endswith('.crdownload') or f.endswith('.part'))]

                # Then check for completed files
                matching_files = [f for f in new_files if fnmatch.fnmatch(f, file_pattern)]
                exclude_condition = (lambda f: not fnmatch.fnmatch(f, ex_file_pattern)) if ex_file_pattern else (lambda f: True)
                complete_files = [f for f in matching_files if not f.endswith('.crdownload') and not f.endswith('.part') and exclude_condition(f)]
                
                # Optionally filter by file age (modification time instead of creation time).
                # This is disabled by default (max_age_seconds=None) because a long download can
                # legitimately take tens of seconds and its ctime is the *start* of the download.
                if max_age_seconds is not None:
                    now = time.time()
                    complete_files = [
                        f for f in complete_files
                        if now - os.path.getmtime(os.path.join(download_dir, f)) <= max_age_seconds
                    ]
                
                # Case 1: Download is in progress (temporary file exists)
                if temp_files:
                    if download_started == False:
                        download_started = True
                        county_logger.info(f"Download started: {temp_files}")
                        temp_file = max(temp_files, key=lambda f: os.path.getctime(os.path.join(download_dir, f)))
                        temp_file_path = os.path.join(download_dir, temp_file)
                    
                    # Log download in progress message (but not too often)
                    current_time = time.time()
                    if current_time - last_log_time >= log_interval:
                        county_logger.info(f"Waiting for download completion... ({current_time - start_time:.1f}s elapsed)")
                        last_log_time = current_time
                
                # Case 2: Download is complete
                if complete_files:
                    newest_file = max(complete_files, key=lambda f: os.path.getctime(os.path.join(download_dir, f)))
                    full_path = os.path.join(download_dir, newest_file)
                    county_logger.info(f"Download complete: {newest_file} after {time.time() - start_time:.1f} seconds")
                    return full_path, newest_file
        
        # Log "waiting for initiation" if no download has started yet
        current_time = time.time()
        if not download_started and current_time - last_log_time >= log_interval:
            county_logger.info(f"Still waiting for download initiation... ({current_time - start_time:.1f}s elapsed)")
            last_log_time = current_time
    
    county_logger.warning(f"Download timeout after {timeout} seconds for pattern: {file_pattern}")
    raise CriticalError(f"Download timeout after {timeout} seconds for pattern: {file_pattern}")

# Function to rename a file
def rename_file(context, input_filename, target_filename, check_dir=None):
    """
    Renames a downloaded file to a more descriptive target filename.

    If a file with the target name already exists, it will be overwritten.

    Args:
        context (CountyContext): The context object for the current county.
        input_filename (str): The original name of the file to be renamed.
        target_filename (str): The new, desired filename.
        check_dir (str, optional): The directory to perform the operation in. If None,
            the default download directory from the context is used.

    Returns:
        str: The full path to the newly renamed file.

    Raises:
        CriticalError: If the source file doesn't exist or if renaming fails.
    """
    if check_dir is None:
        download_dir = context.download_dir
    else:
        download_dir = check_dir

    county_logger = context.county_logger

    # Create full paths for both input and output files
    input_file_path = os.path.join(download_dir, input_filename)
    new_file_path = os.path.join(download_dir, target_filename)
    
    try:
        # Check if the input file exists
        if not os.path.exists(input_file_path):
            raise CriticalError(f"Cannot rename: source file {input_filename} not found in {download_dir}.")
            
        # Check if target file already exists and remove it
        if os.path.exists(new_file_path):
            county_logger.warning(f"Target file {target_filename} already exists. Overwriting.")
            os.remove(new_file_path)
            
        # Rename the file using os.rename
        os.rename(input_file_path, new_file_path)
        county_logger.info(f"File {input_filename} renamed to: {target_filename}")
        
        # Return the path to the renamed file
        return new_file_path
        
    except Exception as e:
        msg = f"Error renaming {input_filename} to {target_filename}: {e}"
        county_logger.error(msg)
        raise CriticalError(msg)

# Function to transfer files to the target directory
def transfer_files(context, input_files):
    """
    Transfers downloaded files to their final destination directory on the server.

    This function handles the logic for moving files from the temporary download
    directory to the county-specific source data directory. For non-local environments,
    it also includes logic to archive old files and unzip new ones based on the
    county's specific data format and requirements.

    Args:
        context (CountyContext): The context object containing county info and paths.
        input_files (list): A list of filenames (not full paths) to be transferred.
    
    Returns:
        bool: True if the transfer process completes successfully.
    """
    download_dir = context.download_dir
    county_name = context.county_name
    local = context.local
    county_logger = context.county_logger

    # Function to move files to the source_data directory
    def move_files(source_dir, target_dir, file_list):
        """
        Transfers files from the batch_downloads directory to the source_data directory
        for a given county.

        Parameters:
            source_dir (str): Path to the source directory
            target_dir (str): Path to the target directory
            file_list (list): List of filenames to transfer (not full paths).

        Raises:
            FileNotFoundError: If a source file doesn't exist.
            Exception: For other unexpected errors.
        """
        moved_files = []
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        for filename in file_list:
            src_path = os.path.join(source_dir, filename)
            dst_path = os.path.join(target_dir, filename)

            if not os.path.isfile(src_path):
                raise FileNotFoundError(f"Source file not found: {src_path}")

            shutil.move(src_path, dst_path)
            moved_files.append(filename)

        return moved_files

    # Function to archive files by pattern
    def archive_files(county_name, pattern):
        """
        Archives files matching a given filename pattern by moving them to an x_old directory.
        Wildcards like '*' are supported.

        Parameters:
            county_name (str): The county name.
            pattern (str): The filename pattern to match, e.g., '*.zip', '*_old.shp'.

        Returns:
            list: List of archived file paths.
        """
        base_dir = f"/srv/mapwise_dev/county/{county_name}/processing/database/current/source_data"
        archive_dir = os.path.join(base_dir, "x_old")
        
        # Create archive directory if it doesn't exist
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)
            county_logger.info(f"Created archive directory: {archive_dir}")

        search_path = os.path.join(base_dir, pattern)
        matched_files = glob.glob(search_path)
        archived_files = []

        for file_path in matched_files:
            if os.path.isfile(file_path):
                try:
                    # Get just the filename without the path
                    filename = os.path.basename(file_path)
                    # Create destination path in archive directory
                    dest_path = os.path.join(archive_dir, filename)
                    
                    # Move the file to archive directory (will overwrite if exists)
                    shutil.move(file_path, dest_path)
                    archived_files.append(filename)
                except Exception as e:
                    county_logger.error(f"Failed to archive {filename}: {e}")
            else:
                county_logger.error(f"Skipped (not a file): {file_path}")

        return archived_files

    # Function to unzip files
    def unzip_files(county_name, pattern, flatten=True, overwrite=True):
        """
        Unzips files matching a given filename pattern inside a base directory.
        Wildcards like '*' are supported.

        Parameters:
            county_name (str): The county name.
            pattern (str): Filename pattern to match, e.g. '*.zip'.
            flatten (bool, optional): If True, discards any directory structure inside
                the archive (similar to the `-j` flag for the Unix `unzip` command).
                Defaults to False.
            overwrite (bool, optional): If True, existing destination files will be
                replaced. Defaults to True.

        Returns:
            list: A list of full paths to the extracted files.
        """
        base_dir = f"/srv/mapwise_dev/county/{county_name}/processing/database/current/source_data"
        search_path = os.path.join(base_dir, pattern)
        matched_archives = glob.glob(search_path)

        extracted_files = []

        for archive_path in matched_archives:
            if not zipfile.is_zipfile(archive_path):
                county_logger.error(f"Skipped (not a valid zip file): {archive_path}")
                continue

            try:
                with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                    if flatten:
                        # Extract each member to base_dir, discarding any internal path
                        for member in zip_ref.infolist():
                            if member.is_dir():
                                continue  # Skip directories

                            member_name = os.path.basename(member.filename)
                            dest_path = os.path.join(base_dir, member_name)

                            if overwrite or not os.path.exists(dest_path):
                                with open(dest_path, 'wb') as dest_file:
                                    dest_file.write(zip_ref.read(member))
                            extracted_files.append(dest_path)
                    else:
                        # Extract with full directory structure preserved
                        zip_ref.extractall(path=base_dir)
                        extracted_files.extend([
                            os.path.join(base_dir, name) for name in zip_ref.namelist() if not name.endswith('/')
                        ])

            except Exception as e:
                county_logger.error(f"Failed to extract {archive_path}: {e}")

        return matched_archives, extracted_files

    # Function to get data date from a file
    def get_data_date(context, filename, target_dir, zip_rename_date=False):
        """
        Gets the data date from a file using get_file_date.py.
        """
        county_logger = context.county_logger

        if zip_rename_date:
            # Run zip_rename_date.sh
            county_logger.info(f"Running zip_rename_date.sh for {filename}")
            result = subprocess.run(["/srv/tools/bash/zip_rename_date.sh", filename], capture_output=True, text=True, cwd=target_dir)
            county_logger.info(f"zip_rename_date.sh output: {result.stdout}")
        else:
            # Run get_file_date.py
            county_logger.info(f"Running get_file_date.py for {filename}")
            result = subprocess.run(["python3", "/srv/tools/python/lib/get_file_date.py", filename], capture_output=True, text=True, cwd=target_dir)
            county_logger.info(f"get_file_date.py output: {result.stdout}")
        
        # Check for errors
        if result.stderr:
            county_logger.error(f"zip_rename_date.sh error: {result.stderr} (return code: {result.returncode})")
            raise CriticalError(f"zip_rename_date.sh error: {result.stderr} (return code: {result.returncode})")
        if result.returncode != 0:
            raise CriticalError(f"zip_rename_date.sh return code: {result.returncode}")
        
        # Get data date from output
        raw_date = result.stdout.strip()
        data_date = datetime.strptime(raw_date, "%Y-%m-%d").strftime("%m/%d/%Y")
        county_logger.info(f"Data date: {data_date}")

        return data_date

    # Main logic
    try:
        # Extract filenames from full paths
        input_files = [os.path.basename(file) for file in input_files]
    except Exception as e:
        county_logger.error(f"Error extracting filenames from full paths: {e}")
        raise CriticalError(f"Error extracting filenames from full paths: {e}")

    # Set source and target directories
    source_dir = download_dir
    if local:
        target_dir = os.path.join(download_dir, county_name)
        try:
            move_files(source_dir, target_dir, input_files)
            county_logger.info(f"Transferred {input_files}.")
        except Exception as e:
            county_logger.error(f"Error moving files: {e}")
            raise CriticalError(f"Error moving files: {e}")
    else:
        target_dir = f"/srv/mapwise_dev/county/{county_name}/processing/database/current/source_data"

        try:
            qpublic = ["madison","holmes","okaloosa","flagler","walton","hardee","washington",
                "hendry","levy","gilchrist","calhoun","liberty","dixie","jefferson","gulf",
                "hamilton","taylor","gadsden","jackson","bay","glades","sumter"]
            qpublic_other = ["putnam","santa_rosa"]
            grizzly = ["union","columbia","suwannee","okeechobee","desoto","bradford","lafayette"]
            gsacorp = ["wakulla","nassau","franklin"]
            opendata = ["palm_beach"]
            other = ["indian_river","duval","pinellas","escambia","collier"]

            if county_name in qpublic or county_name in qpublic_other or county_name in gsacorp:
                
                # Get patterns of files to archive
                patterns = set()
                for filename in input_files:
                    # Extract the county name and year-month part
                    parts = filename.split('_')
                    if len(parts) >= 2:
                        if "MailingList" in filename:
                            county_part = parts[0]  # e.g., 'WaltonCountyFL'
                            # parts[1] = 'MailingList'
                            date_part = parts[2][:7]  # e.g., '2025-03'
                            pattern = f"{county_part}_MailingList_{date_part}*.csv"
                        else:
                            county_part = parts[0]  # e.g., 'WaltonCountyFL'
                            date_part = parts[1][:7]  # e.g., '2025-03'
                            pattern = f"{county_part}_{date_part}*.csv"
                        patterns.add(pattern)
                        #county_logger.info(f"---DEBUG---Pattern: {pattern}")

                # Archive existing files that start with the same month
                for pattern in patterns:
                    archived_files = archive_files(county_name, pattern)
                    county_logger.info(f"Archived {archived_files}.")

                # Transfer files to source_data directory
                moved_files = move_files(source_dir, target_dir, input_files)
                county_logger.info(f"Transferred {moved_files}.")

                return "True"
            
            elif county_name in grizzly:
                
                # Transfer files to source_data directory
                moved_files = move_files(source_dir, target_dir, input_files)
                county_logger.info(f"Transferred {moved_files}.")

                return "True"
            
            elif county_name in opendata:

                # Transfer files to source_data directory
                moved_files = move_files(source_dir, target_dir, input_files)
                county_logger.info(f"Transferred {moved_files}.")

                return "True"
            
            elif county_name in other:
                if county_name == "duval":

                    # Archive existing files that start with the same month
                    archived_files = archive_files(county_name, "DCPAO-REAL-ESTATE*")
                    county_logger.info(f"Archived {archived_files}.")

                    # Transfer files to source_data directory
                    moved_files = move_files(source_dir, target_dir, input_files)
                    county_logger.info(f"Transferred {moved_files}.")

                    # Unzip files
                    matched_archives, extracted_files = unzip_files(county_name, "DCPAO-REAL-ESTATE-SALES-FIXED-FORMAT-TEXT-FILE-*.zip", flatten=True, overwrite=True)
                    county_logger.info(f"Extracted {len(extracted_files)} files from {matched_archives}.")

                    matched_archives, extracted_files = unzip_files(county_name, "DCPAO-REAL-ESTATE-PIPE-DELIMITED-TEXT-UNCERTIFIED-AS-OF*.zip", flatten=True, overwrite=True)
                    county_logger.info(f"Extracted {len(extracted_files)} files from {matched_archives}.")

                    # Get filenames to rename
                    sales_files = glob.glob(os.path.join(target_dir, "DCPAO REAL ESTATE SALES FIXED FORMAT TEXT FILE*.txt"))
                    combined_files = glob.glob(os.path.join(target_dir, "DCPAO REAL ESTATE PIPE DELIMITED TEXT UNCERTIFIED AS OF*.txt"))

                    # Extract date from filename in MM/DD/YYYY format
                    match = re.search(r'(\d{2}-\d{2}-\d{4})', os.path.basename(sales_files[0]))
                    if match:
                        date_str = match.group(1)  # "07-01-2025"
                        data_date = f"{date_str[:2]}/{date_str[3:5]}/{date_str[6:]}" # "07/01/2025"
                    else:
                        data_date = None
                    county_logger.info(f"Data date: {data_date}")

                    # Rename files (using just the first match if multiple files exist)
                    if sales_files:
                        rename_file(context, os.path.basename(sales_files[0]), "sales.txt", check_dir=target_dir)
                    if combined_files:
                        rename_file(context, os.path.basename(combined_files[0]), "2025_COMBINED_PRELIM.txt", check_dir=target_dir)
                    county_logger.info(f"Renamed files in {target_dir}.")

                    return data_date
                
                elif county_name == "pinellas":

                    # Move to source_data directory
                    moved_files = move_files(source_dir, target_dir, input_files)
                    county_logger.info(f"Transferred {moved_files}.")

                    # Unzip files
                    matched_archives, extracted_files = unzip_files(county_name, "*.zip", flatten=False, overwrite=True)
                    county_logger.info(f"Extracted {len(extracted_files)} files from {matched_archives}.")

                    return "True"
                
                elif county_name == "escambia":

                    # Transfer files to source_data directory
                    moved_files = move_files(source_dir, target_dir, input_files)
                    county_logger.info(f"Transferred {moved_files}.")

                    data_date = get_data_date(context, "sales_2015.csv", target_dir)

                    return data_date
                
                elif county_name == "indian_river":

                    # Transfer files to source_data directory
                    moved_files = move_files(source_dir, target_dir, input_files)
                    county_logger.info(f"Transferred {moved_files}.")

                    # Unzip files
                    matched_archives, extracted_files = unzip_files(county_name, "DataDownload.zip", flatten=False, overwrite=True)
                    county_logger.info(f"Extracted {len(extracted_files)} files from {matched_archives}.")

                    # Run rename_files.sh
                    county_logger.info(f"Running rename_files.sh")
                    result = subprocess.run(["/srv/tools/bash/parcels/scripts_county/indian_river/rename_files.sh"], capture_output=True, text=True, cwd=target_dir)
                    county_logger.info(f"rename_files.sh output: {result.stdout}")
                    if result.stderr:
                        county_logger.error(f"rename_files.sh error: {result.stderr} (return code: {result.returncode})")
                        raise CriticalError(f"rename_files.sh error: {result.stderr} (return code: {result.returncode})")
                    if result.returncode != 0:
                        raise CriticalError(f"rename_files.sh return code: {result.returncode}")
                    
                    # Run zip_rename_date.sh
                    data_date = get_data_date(context, "DataDownload.zip", target_dir, zip_rename_date=True)

                    return data_date
                    
                elif county_name == "collier":

                    # Normalize filename
                    input_filename = "int_values_rp_history_csv.zip"
                    input_files[0] = os.path.basename(input_files[0]) # "int_values_rp_history_csv.zip"
                    input_files[0] = input_filename

                    # Transfer files to source_data directory
                    moved_files = move_files(source_dir, target_dir, input_files)
                    county_logger.info(f"Transferred {moved_files}.")

                    # Unzip files
                    matched_archives, extracted_files = unzip_files(county_name, input_filename, flatten=False, overwrite=True)
                    county_logger.info(f"Extracted {len(extracted_files)} files from {matched_archives}.")

                    # Run zip_rename_date.sh
                    data_date = get_data_date(context, input_filename, target_dir, zip_rename_date=True)

                    return data_date
                    
            else:
                raise Exception(f"County {context.county_name_formatted} not found in any download category.")

        except Exception as e:
            county_logger.error(f"Error transferring files: {e}")
            raise CriticalError(f"Error transferring files: {e}")
    
    return "True"

# Function to run a command with real time output
def run_command(command, cwd, logger):
    
    process = subprocess.Popen(
        command,
        shell=True,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # Combine streams
        universal_newlines=True,
        bufsize=1
    )
    
    output_lines = []
    for line in process.stdout:
        logger.debug(line.rstrip())  # Real-time display
        output_lines.append(line)  # Still capture for validation

    return_code = process.wait()
    full_output = ''.join(output_lines)
    
    return return_code, full_output


