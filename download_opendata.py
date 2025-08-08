#!/usr/bin/env python3

import argparse
import os
import sys
import time
import shutil
import logging
import subprocess
from datetime import datetime
from typing import List, Optional, Tuple, Dict

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Local utilities
from layers_helpers import parse_entity_pattern, resolve_layer_directory


# ---------------------------------------------------------------------------
# Constants and Selectors
# ---------------------------------------------------------------------------

BATCH_DOWNLOAD_DIR = "/srv/datascrub/_batch_downloads"

# ArcGIS Hub/OpenData selectors for zoning/flu style pages
OPEN_DATA_SELECTORS = {
    "info_tab": ".hub-toolbar-inner.hide-overflow > button:nth-child(1)",
    "data_date": "li.metadata-item[data-test=\"modified\"] > div:nth-child(2)",
    "data_date_alt": "li.metadata-item[data-test=\"modified\"] > div:nth-child(3)",
    "download_tab": "button.btn.btn-default.btn-block",
    "download_tab_old": ".hub-toolbar-inner.hide-overflow > button:nth-child(3)",
    # Shadow DOM traversal to shapefile download button
    "outer_shadow_selector": "arcgis-hub-download-list",
    "nested_shadow_selector": "arcgis-hub-download-list-item:nth-of-type(2)",  # shapefile item
    "nested_shadow_selector_2": "calcite-button",
}


# ---------------------------------------------------------------------------
# Selenium session management
# ---------------------------------------------------------------------------

def init_selenium(
    download_dir: str = BATCH_DOWNLOAD_DIR,
    headless: bool = True,
    chromium: bool = True,
    debug: bool = False,
):
    """Initialize a Chromium-based undetected Selenium driver with download prefs."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    os.makedirs(download_dir, exist_ok=True)

    options = uc.ChromeOptions()

    # Choose browser binary
    browser_executable_path = None
    if chromium:
        # Prefer Linux path for server usage
        if os.path.exists("/usr/bin/chromium-browser"):
            browser_executable_path = "/usr/bin/chromium-browser"
        elif os.path.exists("/Applications/Chromium.app/Contents/MacOS/Chromium"):
            browser_executable_path = "/Applications/Chromium.app/Contents/MacOS/Chromium"
        else:
            raise RuntimeError("Chromium binary not found. Please install Chromium.")
    else:
        if os.path.exists("/usr/bin/google-chrome"):
            browser_executable_path = "/usr/bin/google-chrome"
        elif os.path.exists("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"):
            browser_executable_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        else:
            raise RuntimeError("Google Chrome binary not found. Please install Chrome.")

    options.binary_location = browser_executable_path

    # Resolve browser major version for UA coherence
    try:
        result = subprocess.run([browser_executable_path, "--version"], capture_output=True, text=True)
        browser_version = result.stdout.strip()
        major = None
        if "Chromium" in browser_version:
            major = browser_version.split()[1].split(".")[0]
        elif "Google Chrome" in browser_version:
            major = browser_version.split()[2].split(".")[0]
        else:
            # Fallback regex
            import re as _re
            m = _re.search(r"(\d+)\.", browser_version)
            if m:
                major = m.group(1)
        if not major:
            raise RuntimeError(f"Unable to parse browser version: {browser_version}")
    except Exception as e:
        raise RuntimeError(f"Failed to get browser version: {e}")

    # Anti-detection and headless
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-first-run")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--allow-popups-during-page-unload")
    options.add_argument("--allow-popups")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    if headless:
        options.add_argument("--headless")
    options.add_argument(
        f"--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        f"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36"
    )

    # Downloads
    prefs = {
        "profile.default_content_settings.popups": 0,
        "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.images": 1,
        "download.default_directory": download_dir,
        "plugins.always_open_pdf_externally": True,
        "safebrowsing.disable_download_protection": True,
    }
    options.add_experimental_option("prefs", prefs)
    options.set_capability("pageLoadStrategy", "none")

    driver = uc.Chrome(
        browser_executable_path=browser_executable_path,
        options=options,
        version_main=int(major),
    )
    # Hide webdriver
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def shutdown_selenium(driver) -> None:
    """Gracefully close and quit the driver. Caller controls lifetime."""
    try:
        driver.quit()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date_any(fmt_str: str) -> Optional[datetime.date]:
    """Parse a date string in either MM/DD/YYYY or YYYY-MM-DD."""
    from datetime import datetime as _dt
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return _dt.strptime(fmt_str.strip(), fmt).date()
        except Exception:
            continue
    return None


def _extract_data_date(driver, debug: bool = False) -> str:
    sel = OPEN_DATA_SELECTORS
    # Try primary location
    try:
        WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, sel["data_date"])) )
        text = driver.find_element(By.CSS_SELECTOR, sel["data_date"]).text
    except TimeoutException:
        # Try clicking Info tab then retry
        try:
            info_tab = driver.find_element(By.CSS_SELECTOR, sel["info_tab"])  
            info_tab.click()
            info_tab.click()
        except Exception:
            pass
        try:
            WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, sel["data_date"])) )
            text = driver.find_element(By.CSS_SELECTOR, sel["data_date"]).text
        except Exception:
            # Fallback alt selector
            WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, sel["data_date_alt"])) )
            text = driver.find_element(By.CSS_SELECTOR, sel["data_date_alt"]).text

    # Text format like "March 31, 2025" -> MM/DD/YYYY
    import re as _re
    m = _re.search(r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})", text)
    if not m:
        raise RuntimeError(f"Could not extract date from: '{text}'")

    month_name, day, year = m.groups()
    month_map = {
        'January': '01', 'February': '02', 'March': '03', 'April': '04',
        'May': '05', 'June': '06', 'July': '07', 'August': '08',
        'September': '09', 'October': '10', 'November': '11', 'December': '12'
    }
    month = month_map.get(month_name, '01')
    day = day.zfill(2)
    return f"{month}/{day}/{year}"


def _open_download_panel(driver) -> None:
    sel = OPEN_DATA_SELECTORS
    try:
        # Try new download tab
        btn = WebDriverWait(driver, 6).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, sel["download_tab"]))
        )
        driver.execute_script("arguments[0].click();", btn)
        return
    except Exception:
        pass
    try:
        # Fallback older tab
        btn = WebDriverWait(driver, 4).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, sel["download_tab_old"]))
        )
        driver.execute_script("arguments[0].click();", btn)
        return
    except Exception:
        # In some pages, the download list may already be visible; proceed
        pass


def _click_shapefile_download(driver) -> None:
    sel = OPEN_DATA_SELECTORS
    # Ensure the list is rendered before JS traversal
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, sel["outer_shadow_selector"]))
    )

    js = """
        const outer = document.querySelector(arguments[0]);
        if (!outer || !outer.shadowRoot) return null;
        const inner1 = outer.shadowRoot.querySelector(arguments[1]);
        if (!inner1 || !inner1.shadowRoot) return null;
        const btnContainer = inner1.shadowRoot.querySelector(arguments[2]);
        if (!btnContainer) return null;
        const button = btnContainer.shadowRoot ? btnContainer.shadowRoot.querySelector('a,button,calcite-button') : btnContainer.querySelector('a,button,calcite-button');
        return button;
    """
    button = driver.execute_script(
        js,
        sel["outer_shadow_selector"],
        sel["nested_shadow_selector"],
        sel["nested_shadow_selector_2"],
    )
    if not button:
        raise RuntimeError("Could not locate Shapefile download button in Shadow DOM")
    driver.execute_script("arguments[0].click();", button)


def _list_current_files(directory: str) -> Dict[str, int]:
    """Return mapping of filename -> size for files currently in directory."""
    try:
        return {f: os.path.getsize(os.path.join(directory, f)) for f in os.listdir(directory)}
    except FileNotFoundError:
        return {}


def _is_temp_file(name: str) -> bool:
    return name.endswith(".crdownload") or name.endswith(".part")


def _wait_for_new_files(
    directory: str,
    baseline: Dict[str, int],
    timeout: int = 180,
    stabilize_secs: int = 2,
    debug: bool = False,
) -> List[str]:
    """Wait until new, non-temp files appear and stabilize in size; return new file names."""
    start = time.time()
    last_sizes: Dict[str, int] = {}
    while time.time() - start < timeout:
        time.sleep(0.5)
        current = _list_current_files(directory)
        # New files (could include temp)
        new_names = [n for n in current.keys() if n not in baseline]
        # Completed files only
        completed = [n for n in new_names if not _is_temp_file(n)]
        if completed:
            # Size stabilization: ensure sizes unchanged for stabilize_secs
            if last_sizes and all(current.get(n, 0) == last_sizes.get(n, -1) for n in completed):
                if debug:
                    logging.debug(f"Download stabilized for: {completed}")
                return completed
            last_sizes = {n: current.get(n, 0) for n in completed}
            # Wait a bit more for stabilization
            stable_wait_start = time.time()
            while time.time() - stable_wait_start < stabilize_secs:
                time.sleep(0.5)
                current2 = _list_current_files(directory)
                if any(current2.get(n, 0) != last_sizes[n] for n in completed):
                    # sizes changed, restart stabilization
                    last_sizes = {n: current2.get(n, 0) for n in completed}
                    break
            else:
                # Completed stabilization window
                return completed
        # Periodic debug
        if debug and int(time.time() - start) % 10 == 0:
            logging.debug("Waiting for download completion...")
    raise TimeoutException("Timed out waiting for new files to complete download")


def _basic_validate_files(directory: str, names: List[str]) -> List[str]:
    """Ensure files exist, are non-empty, and not temp. Return full paths."""
    valid_paths: List[str] = []
    for n in names:
        if _is_temp_file(n):
            continue
        p = os.path.join(directory, n)
        if os.path.isfile(p) and os.path.getsize(p) > 0:
            valid_paths.append(p)
    return valid_paths


def _resolve_target_dir_from_entity(entity: str) -> str:
    layer, state, county, city = parse_entity_pattern(entity)
    return resolve_layer_directory(layer or "", state or "", county or "", city or "")


def _transfer_files(paths: List[str], target_dir: str) -> List[str]:
    os.makedirs(target_dir, exist_ok=True)
    moved: List[str] = []
    for src in paths:
        dst = os.path.join(target_dir, os.path.basename(src))
        shutil.move(src, dst)
        moved.append(dst)
    return moved


# ---------------------------------------------------------------------------
# Core operation
# ---------------------------------------------------------------------------

def download_opendata(
    driver,
    entity: str,
    url: str,
    target_dir: Optional[str] = None,
    catalog_data_date: Optional[str] = None,
    transfer: bool = True,
    debug: bool = False,
) -> dict:
    """
    Navigate to OpenData URL, extract data_date, perform NND check, click Shapefile download,
    wait for completed file(s), optionally transfer them, and return result.
    """
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Ensure batch dir exists
    os.makedirs(BATCH_DOWNLOAD_DIR, exist_ok=True)

    logging.info(f"[SELENIUM] {entity}: Opening {url}")
    driver.get(url)

    # Extract data date (MM/DD/YYYY)
    data_date = _extract_data_date(driver, debug=debug)
    logging.info(f"[SELENIUM] {entity}: Data date on page: {data_date}")

    # NND check (if provided)
    if catalog_data_date:
        existing = _parse_date_any(catalog_data_date)
        new_date = _parse_date_any(data_date)
        if existing and new_date and existing == new_date:
            logging.info(f"[SELENIUM] {entity}: No new data (catalog {catalog_data_date} == page {data_date}). Skipping download.")
            return {
                "status": "SKIPPED_NND",
                "data_date": data_date,
                "validated_files": [],
                "transferred_files": [],
                "target_dir": target_dir or _resolve_target_dir_from_entity(entity),
                "message": "No new data detected",
            }

    # Open download list and click Shapefile
    _open_download_panel(driver)

    # Snapshot baseline before clicking download
    baseline = _list_current_files(BATCH_DOWNLOAD_DIR)

    _click_shapefile_download(driver)

    # Wait for completed files (generic, not ZIP-only)
    new_names = _wait_for_new_files(BATCH_DOWNLOAD_DIR, baseline, timeout=180, stabilize_secs=2, debug=debug)
    valid_paths = _basic_validate_files(BATCH_DOWNLOAD_DIR, new_names)
    if not valid_paths:
        return {
            "status": "FAILED",
            "data_date": data_date,
            "validated_files": [],
            "transferred_files": [],
            "target_dir": target_dir or _resolve_target_dir_from_entity(entity),
            "message": "No completed files detected",
        }

    moved: List[str] = []
    resolved_target = target_dir or _resolve_target_dir_from_entity(entity)
    if transfer:
        moved = _transfer_files(valid_paths, resolved_target)
        logging.info(f"[SELENIUM] {entity}: Transferred {len(moved)} file(s) to {resolved_target}")

    return {
        "status": "SUCCESS",
        "data_date": data_date,
        "validated_files": [os.path.basename(p) for p in valid_paths],
        "transferred_files": [os.path.basename(p) for p in moved],
        "target_dir": resolved_target,
        "message": f"Downloaded {len(valid_paths)} file(s)",
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Download OpenData Shapefile via Selenium (modular)")
    p.add_argument("--entity", required=True, help="Entity string, e.g., zoning_fl_palm_beach")
    p.add_argument("--url", required=True, help="OpenData page URL")
    p.add_argument("--target-dir", default=None, help="Target data directory (optional; will derive if omitted)")
    p.add_argument("--catalog-date", dest="catalog_date", default=None, help="Catalog data date (NND check)")
    p.add_argument("--debug", action="store_true", help="Increase verbosity")
    p.add_argument("--headful", action="store_true", help="Run browser with a visible UI")
    return p


def main_cli():
    args = _build_arg_parser().parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    os.makedirs(BATCH_DOWNLOAD_DIR, exist_ok=True)

    headless = not args.headful
    driver = None
    try:
        driver = init_selenium(download_dir=BATCH_DOWNLOAD_DIR, headless=headless, chromium=True, debug=args.debug)
        result = download_opendata(
            driver,
            entity=args.entity,
            url=args.url,
            target_dir=args.target_dir,
            catalog_data_date=args.catalog_date,
            transfer=True,
            debug=args.debug,
        )
        status = result.get("status", "FAILED")
        if status != "SUCCESS" and status != "SKIPPED_NND":
            logging.error(result.get("message", "Download failed"))
            sys.exit(1)
        # Basic standalone summary
        logging.info(f"Status: {status}; Date: {result.get('data_date')}; Files: {result.get('transferred_files') or result.get('validated_files')}")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Error: {e}")
        sys.exit(1)
    finally:
        if driver:
            shutdown_selenium(driver)


if __name__ == "__main__":
    main_cli()