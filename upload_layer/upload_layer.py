#!/usr/bin/env python3
"""
Automation script for pushing layer data to production.

Features:
1. Rsync retrieval of new data (json plans + backups + bat files).
2. Supports --test-retrieve (dry-run of rsync, skip further processing).
3. Supports --test-execute (print commands instead of executing; runs even with no new data).
4. Supports --debug for verbose rsync and detailed step logging.
5. Parses each new upload-plan JSON file and executes its command list per entity, ensuring
   corresponding .backup and .bat files exist.
6. Sensitive login details (REMOTE_USER, REMOTE_HOST, optional REMOTE_PORT) are loaded from a .env file.
7. Logs are written to /srv/data/layers/logs, with console output mirrored.
8. Optional --local flag uses '~/Downloads/test' as local base directory for testing.
   It also sets REMOTE_BASE_DIR to '/srv/tools/python/layers_scraping/upload_layer/test' so rsync pulls from the test directory.

Environment variables expected in .env:
    REMOTE_USER   – SSH username for rsync.
    REMOTE_HOST   – SSH hostname/IP for rsync.
    REMOTE_PORT   – (optional) SSH port.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Any

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Constants & Paths
# ---------------------------------------------------------------------------
LOCAL_BASE_DIR = Path("/srv/data/layers")
REMOTE_BASE_DIR = "/srv/data/layers"  # absolute path on remote host – keep trailing path same as local
OUTPUT_DIR = LOCAL_BASE_DIR / "output"
LOG_DIR = LOCAL_BASE_DIR / "logs"

RSYNC_COMMON_FLAGS = ["-ah", "--no-motd"]  # -a (archive), -h (human-readable sizes), --no-motd (avoids protocol issues)
RSYNC_ITEMIZE_FLAG = "-i"  # useful for detecting new/updated files
RSYNC_BIN = os.getenv("RSYNC_BIN", "/opt/homebrew/bin/rsync")  # path to modern rsync binary

# ---------------------------------------------------------------------------
# Argument Parsing
# ---------------------------------------------------------------------------


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Automate upload of layer data to production")
    parser.add_argument("--test-retrieve", action="store_true", help="Dry-run rsync only; skip further processing.")
    parser.add_argument("--test-execute", action="store_true", help="Print would-be executed commands instead of running them.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging and verbose rsync (-v).")
    parser.add_argument("--local", action="store_true", help="Use ~/Downloads/test as LOCAL_BASE_DIR for local testing.")
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------


def setup_logging(debug: bool) -> None:
    """Configure root logger to log to console and file under LOG_DIR."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    logfile_path = LOG_DIR / f"upload_layers_{timestamp}.log"
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[
            logging.FileHandler(logfile_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.debug("Logging initialized. File: %s", logfile_path)


# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------


def run_subprocess(cmd: List[str], capture: bool = True) -> subprocess.CompletedProcess[str]:
    """Run a subprocess command and return the CompletedProcess.

    Raises subprocess.CalledProcessError on non-zero exit codes.
    """
    logging.debug("Running command: %s", shlex.join(cmd))
    result = subprocess.run(cmd, text=True, capture_output=capture, check=False)
    # Always print rsync output immediately for visibility, even if not captured
    if capture:
        if result.stdout:
            logging.info(result.stdout.strip())
        if result.stderr:
            logging.error(result.stderr.strip())
    if result.returncode != 0:
        logging.error("Command failed with exit code %s", result.returncode)
        raise subprocess.CalledProcessError(result.returncode, cmd, output=result.stdout, stderr=result.stderr)
    return result


# ---------------------------------------------------------------------------
# Rsync Handling
# ---------------------------------------------------------------------------


def build_rsync_command(
    remote_user: str,
    remote_host: str,
    remote_port: str | None,
    dry_run: bool,
    verbose: bool,
) -> List[str]:
    """Construct rsync command for retrieving /srv/data/layers recursively."""
    remote_target = f"{remote_user}@{remote_host}:{REMOTE_BASE_DIR.rstrip('/')}/"
    cmd = [RSYNC_BIN, *RSYNC_COMMON_FLAGS]
    if verbose:
        cmd.append("-v")
    cmd.append(RSYNC_ITEMIZE_FLAG)
    if dry_run:
        cmd.append("--dry-run")
    # Fix for macOS rsync protocol compatibility
    # cmd.append("--rsync-path=/usr/bin/rsync")  # Removed as it didn't help
    if remote_port:
        cmd.extend(["-e", f"ssh -p {remote_port}"])
    cmd.append(remote_target)
    cmd.append(str(LOCAL_BASE_DIR))
    return cmd



def perform_rsync(
    remote_user: str,
    remote_host: str,
    remote_port: str | None,
    dry_run: bool,
    verbose: bool,
) -> List[str]:
    """Execute rsync and return list of files that changed (added/updated)."""
    cmd = build_rsync_command(remote_user, remote_host, remote_port, dry_run, verbose)
    proc = run_subprocess(cmd)
    changed_files: List[str] = []
    # Parse itemized change output: lines starting with a change indicator (e.g., ">f.st...... json_file")
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        # rsync itemized lines start with single char of change >, c, etc.
        parts = line.split(maxsplit=1)
        if len(parts) == 2:
            filepath = parts[1]
            changed_files.append(filepath)
    logging.debug("Detected %d changed files via rsync", len(changed_files))
    return changed_files


# ---------------------------------------------------------------------------
# BAT Processing (new)
# ---------------------------------------------------------------------------


def files_with_extension(changed_files: List[str], ext: str) -> List[Path]:
    """Helper to convert changed file strings to Path objects filtered by extension."""
    return [LOCAL_BASE_DIR / f for f in changed_files if f.endswith(ext)]


def gather_bat_paths(changed_files: List[str], force_all: bool) -> List[Path]:
    """Return list of .bat files to process."""
    if force_all or not changed_files:
        return list(OUTPUT_DIR.glob("*.bat"))
    return files_with_extension(changed_files, ".bat")


def parse_bat_commands(bat_path: Path) -> List[str]:
    """Parse a .bat file and return a list of commands (ignores empty/comment lines)."""
    commands: List[str] = []
    for line in bat_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.lower().startswith("rem") or stripped.startswith("::"):
            continue  # skip comments and empty lines
        commands.append(stripped)
    return commands


def process_bat_file(bat_path: Path, test_execute: bool) -> None:
    """Validate matching .backup exists and execute commands from .bat."""
    backup_path = bat_path.with_suffix(".backup")
    if not backup_path.exists():
        logging.error("Missing .backup file for %s", bat_path.name)
        return

    logging.info("Processing batch file: %s", bat_path)
    commands = parse_bat_commands(bat_path)
    if not commands:
        logging.warning("No commands parsed from %s; skipping", bat_path)
        return

    context = {
        "backup": str(backup_path),
        "bat": str(bat_path),
    }

    for cmd in commands:
        formatted_cmd = cmd.format(**context)
        if test_execute:
            logging.info("[TEST-EXECUTE] %s", formatted_cmd)
        else:
            run_subprocess(formatted_cmd, capture=True)


# ---------------------------------------------------------------------------
# Main Routine
# ---------------------------------------------------------------------------


def set_local_base_dir(new_dir: Path) -> None:
    """Update module-level directory constants when --local flag is used."""
    global LOCAL_BASE_DIR, OUTPUT_DIR, LOG_DIR
    LOCAL_BASE_DIR = new_dir.expanduser()
    OUTPUT_DIR = LOCAL_BASE_DIR / "output"
    LOG_DIR = LOCAL_BASE_DIR / "logs"


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)

    # If --local flag is provided, redirect all local paths to user's Downloads/test
    if args.local:
        # When --local is specified, use the repository-local test directory
        set_local_base_dir(Path(__file__).resolve().parent / "test")
        # Point rsync at the test directory on the same host
        global REMOTE_BASE_DIR
        REMOTE_BASE_DIR = "/srv/tools/python/layers_scraping/upload_layer/test"
        args.test_execute = True

    # Load .env for remote credentials
    load_dotenv()
    remote_user = os.getenv("REMOTE_USER")
    remote_host = os.getenv("REMOTE_HOST")
    remote_port = os.getenv("REMOTE_PORT")

    if not remote_user or not remote_host:
        print("ERROR: REMOTE_USER and REMOTE_HOST must be set in .env", file=sys.stderr)
        sys.exit(1)

    setup_logging(args.debug)

    logging.debug("Arguments: %s", args)
    logging.info("Starting layer data upload process…")

    # Ensure output directory exists locally
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        changed_files = perform_rsync(
            remote_user,
            remote_host,
            remote_port,
            dry_run=args.test_retrieve,
            verbose=args.debug,
        )
    except subprocess.CalledProcessError:
        logging.error("Rsync failed; aborting.")
        sys.exit(1)

    if args.test_retrieve:
        logging.info("--test-retrieve specified; skipping further processing.")
        sys.exit(0)

    # Determine which .bat files to process
    bat_paths = gather_bat_paths(changed_files, force_all=args.test_execute)

    if not bat_paths and not args.test_execute:
        logging.info("No new .bat files detected; nothing to process.")
        return

    for bat_path in bat_paths:
        process_bat_file(bat_path, test_execute=args.test_execute)

    logging.info("Upload process completed.")


if __name__ == "__main__":
    main()
