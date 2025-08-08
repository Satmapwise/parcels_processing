import json
import os
import time
import logging
import re
import fnmatch
import zipfile
import undetected_chromedriver as uc
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Function to load county config
def load_county_config(county_name):
    try:
        with open("county_config.json", "r") as f:
            config = json.load(f)
    except Exception as e:
        logging.error("Unable to load county_config.json. Make sure the file exists.")
        exit(1)

    if county_name in config:
        return config[county_name]
    else:
        logging.error(f"County '{county_name}' not found in configuration.")
        exit(1)

# Function to initialize debug directory
def initialize_debug(county_name):
    # Setup logging for info and error messages
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # Create the debug screenshot directory if not exists
    debug_dir = os.path.expanduser(f"~/Downloads/debug_screenshots_{county_name.lower()}")
    os.makedirs(debug_dir, exist_ok=True)
    logging.info(f"Debug screenshots will be saved to: {debug_dir}")  # Make usage explicit for linter

    return debug_dir

# Function to wait for download
def wait_for_download(download_dir, file_pattern, ex_file_pattern=None, timeout=90, check_interval=0.5, log_interval=10, max_age_seconds=1):
    """
    Wait for a file matching the pattern to appear in the download directory.
    
    Args:
        download_dir: Directory to check for downloads
        file_pattern: Pattern to match in filenames
        ex_file_pattern: Pattern to exclude from filenames
        timeout: Maximum time to wait for file (default 90 seconds)
        check_interval: Time between checks (default 0.5 seconds)
        log_interval: How often to log status messages (default 10 seconds)
        max_age_seconds: Maximum age of files to consider (default None, all files)
        
    Returns:
        str: Full path of the downloaded file
    """
    # Get initial files in directory
    initial_files = set(os.listdir(download_dir))
    logging.info(f"Waiting for {file_pattern}.")
    
    start_time = time.time()
    last_log_time = start_time
    download_started = False
    
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
                
                # Filter by creation time if max_age_seconds is set
                if max_age_seconds is not None:
                    now = time.time()
                    complete_files = [
                        f for f in complete_files
                        if now - os.path.getctime(os.path.join(download_dir, f)) <= max_age_seconds
                    ]
                
                # Case 1: Download is in progress (temporary file exists)
                if temp_files:
                    if download_started == False:
                        download_started = True
                        temp_file = max(temp_files, key=lambda f: os.path.getctime(os.path.join(download_dir, f)))
                        logging.info(f"Download started: {temp_file}")
                    
                    # Log download in progress message (but not too often)
                    current_time = time.time()
                    if current_time - last_log_time >= log_interval:
                        logging.info(f"Waiting for download completion... ({current_time - start_time:.1f}s elapsed)")
                        last_log_time = current_time
                
                # Case 2: Download is complete
                if complete_files:
                    newest_file = max(complete_files, key=lambda f: os.path.getctime(os.path.join(download_dir, f)))
                    full_path = os.path.join(download_dir, newest_file)
                    logging.info(f"Download complete: {newest_file} after {time.time() - start_time:.1f} seconds")
                    return full_path, newest_file
        
        # Log "waiting for initiation" if no download has started yet
        current_time = time.time()
        if not download_started and current_time - last_log_time >= log_interval:
            logging.info(f"Still waiting for download initiation... ({current_time - start_time:.1f}s elapsed)")
            last_log_time = current_time
    
    logging.warning(f"Download timeout after {timeout} seconds")
    return None
    
# Function to take debug screenshot
def debug_screenshot(driver, debug_dir, county_name, description):
    base_filename = f"{county_name}_{description.replace(' ', '_')}"
    screenshot_path = os.path.join(debug_dir, f"{base_filename}.png")
    
    # Check if file already exists, add number if needed
    counter = 1
    while os.path.exists(screenshot_path):
        screenshot_path = os.path.join(debug_dir, f"{base_filename}_{counter}.png")
        counter += 1
    
    try:
        driver.save_screenshot(screenshot_path)
        logging.info(f"Screenshot saved to {screenshot_path}")
    except Exception as sce:
        logging.error(f"Failed to save screenshot for {description}: {sce}")

# Function to initialize driver
def initialize_driver():
     # Update Chrome options for better popup handling
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-popup-blocking")  # Explicitly disable popup blocking
    options.add_argument("--allow-popups-during-page-unload")
    options.add_argument("--allow-popups")
    # Ensure browser is NOT headless, so Cloudflare sees a real user browser
    options.headless = False
    # Set default download directory and file handling prefs
    download_dir = os.path.expanduser("~/Downloads")
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,       # Auto-download without prompt
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
        "profile.default_content_settings.popups": 1,  # Allow popups (1 allows popups)
        "profile.content_settings.exceptions.automatic_downloads.*.setting": 1  # Allow automatic downloads
    }
    options.add_experimental_option("prefs", prefs)

    # Launch the undetected Chrome driver
    driver = uc.Chrome(options=options)
    driver.maximize_window()  # open browser in max window for visibility

    return driver, download_dir

# Function to safely click an element (5 main arguments, 1 config argument)
def safe_click(driver, element, debug_dir, county_name, description, return_to_default=False):
    """
    Safely click an element using multiple methods with fallbacks.
    
    Args:
        driver: Selenium WebDriver instance
        element: The WebElement to click
        debug_dir: Directory to save debug screenshots
        county_name: Name of the county being processed
        description: Descriptive name of the element for logging
        return_to_default: Whether to return to default content after clicking (for frame handling)
        critical: If True, exit on failure
        
    Returns:
        bool: True if click was successful, False otherwise
    """
    critical = True
    if element is None:
        logging.error(f"Cannot click {description}: Element is None")
        if critical:
            driver.quit()
            exit(1)
        return False
    
    try:
        # Try JavaScript click first, as this seems most reliable
        driver.execute_script("arguments[0].click();", element)
        logging.info(f"Clicked {description} using JavaScript.")
        success = True
    except Exception as js_e:
        try:
            # Fallback to regular click
            element.click()
            logging.info(f"Clicked {description} using regular click.")
            success = True
        except Exception as e:
            try:
                # Try scrolling first then JS click
                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                time.sleep(1)  # Short pause after scrolling
                driver.execute_script("arguments[0].click();", element)
                logging.info(f"Clicked {description} after scrolling it into view with JavaScript.")
                success = True
            except Exception as scroll_e:
                try:
                    # ActionChains as last resort
                    from selenium.webdriver.common.action_chains import ActionChains
                    ActionChains(driver).move_to_element(element).click().perform()
                    logging.info(f"Clicked {description} using ActionChains.")
                    success = True
                except Exception as action_e:
                    logging.error(f"All attempts to click {description} failed.")
                    debug_screenshot(driver, debug_dir, county_name, f"{description}_click_fail")
                    success = False
                    if critical:
                        driver.quit()
                        exit(1)
    
    # Make sure we're back in the main document after clicking if requested
    if return_to_default:
        try:
            driver.switch_to.default_content()
            logging.info(f"Switched back to main document after {description} interaction")
        except Exception as switch_e:
            logging.error(f"Error switching to default content after clicking {description}: {switch_e}")
    
    return None

# Function to extract data date
def find_data_date(driver, debug_dir, county_name, selectors):
    try:
        # Get data date text
        try:
            WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selectors["data_date"])))
            data_date_text = driver.find_element("css selector", selectors["data_date"]).text
            logging.info(f"Data date text: \"{data_date_text}\"")
        except TimeoutException:
            logging.error("Error: Timeout occurred while waiting for element to be visible.")
            logging.error(f"Attempting to open info tab...")
            try:
                info_tab = driver.find_element(By.CSS_SELECTOR, selectors["info_tab"])
                info_tab.click()
                info_tab.click()
                logging.info(f"Clicked info tab")
            except Exception as e_1:
                logging.error(f"Failed to click info tab: {e_1}")
                debug_screenshot(driver, debug_dir, county_name, "info_tab_not_found")
                exit(1)
            try:
                WebDriverWait(driver, 3).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selectors["data_date"])))
                data_date_text = driver.find_element("css selector", selectors["data_date"]).text
                logging.info(f"Data date text: \"{data_date_text}\"")
            except Exception as e_2:
                logging.error(f"Failed to extract data date text: {e_2}")
                debug_screenshot(driver, debug_dir, county_name, "data_date_text_not_found")
                exit(1)
        except Exception as e_3:
            logging.error(f"Failed to extract data date text: {e_3}")
            debug_screenshot(driver, debug_dir, county_name, "data_date_text_not_found")
            exit(1)

        # Extract date and time
        # Extract date from text (format like "March 31, 2025")
        date_pattern = r'([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})'
        match = re.search(date_pattern, data_date_text)

        if not match:
            logging.warning(f"Could not extract date from text: \"{data_date_text}\"")
            logging.warning(f"Attempting alternate date source...")
            try:
                WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selectors["data_date_alt"])))
                data_date_text = driver.find_element("css selector", selectors["data_date_alt"]).text
                logging.info(f"Data date text: \"{data_date_text}\"")
                date_pattern = r'([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})'
                match = re.search(date_pattern, data_date_text)
            except Exception as e:
                logging.error(f"Failed to extract data date text: {e}")
                debug_screenshot(driver, debug_dir, county_name, "date_extraction_failed")
                exit(1)
            
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
            logging.info(f"Extracted date: {data_date}")
            
            return data_date
        else:
            logging.warning(f"Failed to extract date from text: \"{data_date_text}\"")
            debug_screenshot(driver, debug_dir, county_name, "date_extraction_failed")
            exit(1)

    except NoSuchElementException:
        logging.error("Error: Element not found.")
        debug_screenshot(driver, debug_dir, county_name, "date_extraction_failed")
        exit(1)
    
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        debug_screenshot(driver, debug_dir, county_name, "date_extraction_failed")
        exit(1)

# Function to click download button
def find_download_button(driver, debug_dir, county_name, selectors, critical=True):
    """Use JavaScript to traverse shadow DOM and find the download button."""
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
            logging.info("Found download button using JavaScript traversal")
            return download_button
        else:
            raise Exception("Could not find download button with JavaScript traversal")
            
    except Exception as e:
        logging.error(f"Error finding download button in shadow DOM: {e}")
        debug_screenshot(driver, debug_dir, county_name, "download_button_not_found")
        if critical:
            driver.quit()
            exit(1)
        return None

# Function to add an asterisk to the filename
def add_asterisk(filename):
    """Add asterisk before .zip extension if present."""
    if ".zip" in filename:
        # Split at .zip and add wildcard
        parts = filename.split(".zip")
        og_filename = parts[0]
        return f"{parts[0]}*.zip{parts[1] if len(parts) > 1 else ''}", og_filename
    return filename, og_filename

# Function to unzip a file
def unzip_file(download_dir, filename, zip_dir=None, extract_dir=None):
    """Unzip a file to a specified directory or current directory.
    
    Args:
        download_dir: Directory to save downloads
        filename: Filename to unzip
        zip_dir: Directory to unzip
        extract_dir: Directory to extract to

    Returns:
        str: Path to extracted directory
    """
    if zip_dir is None:
        zip_dir = os.path.join(download_dir, filename)
    if extract_dir is None:
        extract_dir = download_dir
    
    with zipfile.ZipFile(zip_dir, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    
    logging.info(f"Extracted {filename} to {extract_dir}")
    return extract_dir

# Function to rename a file
def rename_file(download_dir, input_filename, target_filename):
    """
    Renames a file in the downloads directory to the target filename.
    
    Args:
        download_dir: Path to the downloads directory
        input_filename: Name of the file to rename (without path)
        target_filename: New filename to use (without path)
        
    Returns:
        str: Path to the renamed file, or None on failure
    """
    
    # Create full paths for both input and output files
    input_file_path = os.path.join(download_dir, input_filename)
    new_file_path = os.path.join(download_dir, target_filename)
    
    try:
        # Check if the input file exists
        if not os.path.exists(input_file_path):
            logging.error(f"Error: File {input_filename} not found in downloads folder")
            return None
            
        # Check if target file already exists
        if os.path.exists(new_file_path):
            logging.warning(f"Target file {target_filename} already exists, removing it first")
            return None
            
        # Rename the file using os.rename
        os.rename(input_file_path, new_file_path)
        logging.info(f"File {input_filename} renamed to: {target_filename}")
        
        # Return the path to the renamed file
        return new_file_path, target_filename
        
    except PermissionError:
        logging.error(f"Permission denied when trying to rename {input_filename}")
        return None
    except OSError as e:
        logging.error(f"OS error when renaming {input_filename}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error when renaming {input_filename}: {e}")
        return None


def main():
    try:

        # Get usecase
        usecase = input("Enter 'parcels' or 'URL filename zip? rename?': ")
        if usecase == "parcels":
            print("Downloading Palm Beach parcels...")
            county_name = "Palm Beach"
            layer = "parcels"
            county_config = load_county_config(county_name)
            url = county_config[f"{layer}"]["url"]
            selectors = county_config[f"{layer}"]["selectors"]
            filename = county_config[f"{layer}"]["filename"]
            if not county_config:
                print(f"County '{county_name}' not found in configuration.")
                exit(1)
        else:
            # Split the input by space to get parameters
            parts = usecase.strip().split(" ")
            county_name = "opendata"
            layer = "opendata"
            
            if len(parts) >= 2:
                # Both URL and filename provided
                url = parts[0]
                filename = parts[1]

                # Check for unzip
                if "unzip" in parts:
                    unzip = True
                else:
                    unzip = False
                
                # Check for rename
                if "rename" in parts:
                    rename = True
                else:
                    rename = False
            else:
                # Only URL provided, ask for filename separately
                url = parts[0]
                filename = input("Enter the filename: ")
                unzip = input("Unzip? (y/n): ")
                if unzip == "y":
                    unzip = True
                else:
                    unzip = False
            if unzip == True:
                print(f"Downloading and extracting {filename}...")
            else:
                print(f"Downloading {filename}...")
            filename, og_filename = add_asterisk(filename)
            county_config = load_county_config(county_name)
            selectors = county_config["selectors"]

        # Initialize debug and driver
        debug_dir = initialize_debug(county_name)
        driver, download_dir = initialize_driver()

        # Navigate to the URL
        driver.get(url)

        # Wait for page to load
        #WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selectors["wait_for_selector"])))

        # Get data date
        data_date = find_data_date(driver, debug_dir, county_name, selectors)

        # Click download tab
        try:
            download_tab = driver.find_element(By.CSS_SELECTOR, selectors["download_tab"])
            driver.execute_script("arguments[0].click();", download_tab)
            logging.info(f"Clicked download tab")
        except Exception as e_3:
            logging.error(f"Error clicking download tab, attempting alternate selector...")
            try:
                WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selectors["wait_for_selector"])))
                download_tab = driver.find_element(By.CSS_SELECTOR, selectors["DOWNLOAD_TAB_OLD"])
                driver.execute_script("arguments[0].click();", download_tab)
                logging.info(f"Clicked alternatedownload tab")
            except Exception as e_4:
                logging.error(f"Failed to click download tab: {e_4}")
                debug_screenshot(driver, debug_dir, county_name, "download_tab_not_found")
                exit(1)

        # Wait for tab to load
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selectors["outer_shadow_selector"])))
        
        # Click download button
        download_button = find_download_button(driver, debug_dir, county_name, selectors, critical=True)
        safe_click(driver, download_button, debug_dir, county_name, "download button")
            
        # Wait for download
        download_path, download_file = wait_for_download(download_dir, f"{filename}", timeout=90, log_interval=10)

        if download_path and county_name != "opendata":
            logging.info(f"Download complete for {county_name} {layer}. Data date: {data_date}")
            driver.quit()
            exit(0)
        elif download_path and county_name == "opendata":
            if rename == True:
                # Convert data date to YYYY-MM-DD format
                data_date = datetime.strptime(data_date, "%m/%d/%Y").strftime("%Y-%m-%d")
                download_path, download_file = rename_file(download_dir, download_file, f"{og_filename}_{data_date}.zip")

            if unzip == True:
                unzip_file(download_dir, download_file, zip_dir=download_path)

            logging.info(f"{download_file} downloaded. Data date: {data_date}")
            driver.quit()
            exit(0)
        else:
            logging.error(f"Download failed for {county_name} {layer}")
            debug_screenshot(driver, debug_dir, county_name, "download_failed")
            exit(1)

    except TimeoutException as te:
        logging.error(f"Timeout: {te}")
        driver.quit()
        exit(1)
    except Exception as e:
        logging.error(f"Error in main: {e}")
        driver.quit()
        exit(1)
    
if __name__ == "__main__":
    main()