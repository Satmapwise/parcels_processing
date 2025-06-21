#!/usr/bin/env python3
"""
Parcels Download Merge Clean Script
===================================

Clean up and merge files downloaded from county websites.
Processes real estate sales data files and removes headers, empty lines, and HTML content.

Usage: python parcels_download_merge_clean.py <county_name>
"""

import sys
import os
import subprocess
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# County configurations
QPUBLIC_COUNTIES = {
    'BAY': {'search_field': 'Parcel ID', 'file_pattern': 'Bay*.csv'},
    'CALHOUN': {'search_field': 'Parcel ID', 'file_pattern': 'Calhoun*.csv'},
    'DIXIE': {'search_field': 'Parcel ID', 'file_pattern': 'Dixie*.csv'},
    'FLAGLER': {'search_field': 'Parcel  Number', 'file_pattern': 'Flagler*.csv'},
    'FRANKLIN': {'search_field': 'Property ID', 'file_pattern': 'Franklin*.csv'},
    'GADSDEN': {'search_field': 'Parcel ID', 'file_pattern': 'Gadsden*.csv'},
    'GILCHRIST': {'search_field': 'Parcel ID', 'file_pattern': 'Gilchrist*.csv'},
    'GLADES': {'search_field': 'Parcel ID', 'file_pattern': 'Glades*.csv'},
    'GULF': {'search_field': 'Parcel ID', 'file_pattern': 'Gulf*.csv'},
    'HAMILTON': {'search_field': 'Parcel ID', 'file_pattern': 'Hamilton*.csv'},
    'HARDEE': {'search_field': 'Parcel ID', 'file_pattern': 'Hardee*.csv'},
    'HENDRY': {'search_field': 'Parcel ID', 'file_pattern': 'Hendry*.csv'},
    'HOLMES': {'search_field': 'Parcel ID', 'file_pattern': 'Holmes*.csv'},
    'JACKSON': {'search_field': 'Parcel ID', 'file_pattern': 'Jackson*.csv'},
    'JEFFERSON': {'search_field': 'Parcel ID', 'file_pattern': 'Jefferson*.csv'},
    'LEVY': {'search_field': 'Parcel ID', 'file_pattern': 'Levy*.csv'},
    'LIBERTY': {'search_field': 'Parcel ID', 'file_pattern': 'Liberty*.csv'},
    'MADISON': {'search_field': 'Parcel ID', 'file_pattern': 'Madison*.csv'},
    'OKALOOSA': {'search_field': 'Parcel ID', 'file_pattern': 'Okaloosa*.csv'},
    'SANTA_ROSA': {'search_field': 'Parcel', 'file_pattern': 'SantaRosa*.csv'},
    'SUMTER': {'search_field': 'Parcel ID', 'file_pattern': 'Sumter*.csv'},
    'TAYLOR': {'search_field': 'Parcel ID', 'file_pattern': 'Taylor*.csv'},
    'WAKULLA': {'search_field': 'Parcel ID', 'file_pattern': 'Wakulla*.csv'},
    'WALTON': {'search_field': 'Parcel ID', 'file_pattern': 'Walton*.csv'},
    'WASHINGTON': {'search_field': 'Parcel ID', 'file_pattern': 'Washington*.csv'},
}

GRIZZLY_COUNTIES = {
    'BAKER': {'sales_field': 'Sale1_Date', 'mailing_field': 'Address1', 'sales_pattern': 'baker_sales_2*.txt', 'mailing_pattern': 'baker_mailing_2*.txt'},
    'BRADFORD': {'sales_field': 'Sale1_Date', 'mailing_field': 'Address1', 'sales_pattern': 'bradford_sales_2*.txt', 'mailing_pattern': 'bradford_mailing_2*.txt'},
    'COLUMBIA': {'sales_field': 'Sale1_Date', 'mailing_field': 'Address1', 'sales_pattern': 'columbia_sales_2*.txt', 'mailing_pattern': 'columbia_mailing_2*.txt'},
    'DESOTO': {'sales_field': 'Sale1_Date', 'mailing_field': 'Address1', 'sales_pattern': 'desoto_sales_2*.txt', 'mailing_pattern': 'desoto_mailing_2*.txt'},
    'HENDRY': {'sales_field': 'Sale1_Date', 'mailing_field': 'Address1', 'sales_pattern': 'hendry_sales_2*.txt', 'mailing_pattern': 'hendry_mailing_2*.txt'},
    'LAFAYETTE': {'sales_field': 'Sale_Date', 'mailing_field': 'Address1', 'sales_pattern': 'lafayette_briefsales_2*.txt', 'mailing_pattern': 'lafayette_mailing_2*.txt'},
    'OKEECHOBEE': {'sales_field': 'Sale1_Date', 'mailing_field': 'Address1', 'sales_pattern': 'okeechobee_sales_2*.txt', 'mailing_pattern': 'okeechobee_mailing_2*.txt'},
    'PUTNAM': {'sales_field': 'Sale Price', 'mailing_field': 'Address 1', 'sales_pattern': 'putnam_sales_2*.csv', 'mailing_pattern': 'putnam_mailing_2*.csv'},
    'SUWANNEE': {'sales_field': 'Sale1_Price', 'mailing_field': 'Address1', 'sales_pattern': 'suwannee_sales_2*.txt', 'mailing_pattern': 'suwannee_mailing_2*.txt'},
    'UNION': {'sales_field': 'Sale1_Price', 'mailing_field': 'Address1', 'sales_pattern': 'union_sales_2*.txt', 'mailing_pattern': 'union_mailing_2*.txt'},
    'SUMTER': {'sales_field': 'SaleDate', 'mailing_field': 'Address1', 'sales_pattern': 'sumter_briefsales_2*.txt', 'mailing_pattern': 'sumter_mailing_2*.txt'},
}


class ParcelsProcessor:
    """Main class for processing parcels data files."""
    
    def __init__(self, county: str, base_path: str = '/srv/mapwise_dev/county'):
        self.county = county.upper()
        self.county_lower = county.lower()
        self.base_path = Path(base_path)
        self.processing_path = self.base_path / self.county_lower / 'processing' / 'database' / 'current' / 'source_data'
        
        # Validate county name to prevent path traversal
        if not re.match(r'^[A-Za-z_]+$', county):
            raise ValueError(f"Invalid county name: {county}")
    
    def validate_paths(self) -> None:
        """Validate that required directories exist."""
        if not self.processing_path.exists():
            raise FileNotFoundError(f"Processing path does not exist: {self.processing_path}")
        
        if not self.processing_path.is_dir():
            raise NotADirectoryError(f"Processing path is not a directory: {self.processing_path}")
    
    def run_command(self, command: List[str], description: str) -> None:
        """Safely run a shell command using subprocess."""
        logger.info(f"Executing: {description}")
        logger.debug(f"Command: {' '.join(command)}")
        
        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                cwd=self.processing_path
            )
            logger.info(f"Successfully executed: {description}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {description}")
            logger.error(f"Error: {e.stderr}")
            raise
    
    def process_qpublic_county(self) -> None:
        """Process QPublic county files."""
        if self.county not in QPUBLIC_COUNTIES:
            raise ValueError(f"County {self.county} not found in QPublic counties")
        
        config = QPUBLIC_COUNTIES[self.county]
        search_field = config['search_field']
        file_pattern = config['file_pattern']
        
        # Create temporary file
        temp_file = self.processing_path / 'sales_current_temp.csv'
        output_file = self.processing_path / 'sales_current.csv'
        
        # Use find and sed to process files
        find_cmd = ['find', '.', '-name', file_pattern, '-type', 'f']
        sed_cmd = ['sed', f'/{re.escape(search_field)}/Id']
        
        # Combine commands using pipes
        full_command = [
            'bash', '-c',
            f"find . -name '{file_pattern}' -type f -exec cat {{}} + | sed '/{re.escape(search_field)}/Id' > {temp_file}"
        ]
        
        self.run_command(full_command, f"Processing QPublic files for {self.county}")
        
        # Convert line endings
        dos2unix_cmd = ['dos2unix', '-n', str(temp_file), str(output_file)]
        self.run_command(dos2unix_cmd, f"Converting line endings for {self.county}")
        
        # Clean up temp file
        if temp_file.exists():
            temp_file.unlink()
    
    def process_grizzly_county(self) -> None:
        """Process Grizzly county files."""
        if self.county not in GRIZZLY_COUNTIES:
            raise ValueError(f"County {self.county} not found in Grizzly counties")
        
        config = GRIZZLY_COUNTIES[self.county]
        
        # Process sales files
        sales_temp = self.processing_path / 'sales_dnld_2014-01-01_current_temp.txt'
        sales_output = self.processing_path / 'sales_dnld_2014-01-01_current.txt'
        
        sales_cmd = [
            'bash', '-c',
            f"find . -name '{config['sales_pattern']}' -type f -exec cat {{}} + | sed '/{re.escape(config['sales_field'])}/Id' > {sales_temp}"
        ]
        self.run_command(sales_cmd, f"Processing sales files for {self.county}")
        
        # Process mailing files
        mailing_temp = self.processing_path / 'sales_owner_mailing_dnld_2014-01-01_current_temp.txt'
        mailing_output = self.processing_path / 'sales_owner_mailing_dnld_2014-01-01_current.txt'
        
        mailing_cmd = [
            'bash', '-c',
            f"find . -name '{config['mailing_pattern']}' -type f -exec cat {{}} + | sed '/{re.escape(config['mailing_field'])}/Id' > {mailing_temp}"
        ]
        self.run_command(mailing_cmd, f"Processing mailing files for {self.county}")
        
        # Convert line endings
        dos2unix_sales = ['dos2unix', '-n', str(sales_temp), str(sales_output)]
        dos2unix_mailing = ['dos2unix', '-n', str(mailing_temp), str(mailing_output)]
        
        self.run_command(dos2unix_sales, f"Converting sales file line endings for {self.county}")
        self.run_command(dos2unix_mailing, f"Converting mailing file line endings for {self.county}")
        
        # Clean up temp files
        for temp_file in [sales_temp, mailing_temp]:
            if temp_file.exists():
                temp_file.unlink()
    
    def process(self) -> None:
        """Main processing method."""
        logger.info(f"Starting processing for county: {self.county}")
        
        self.validate_paths()
        
        if self.county in QPUBLIC_COUNTIES:
            logger.info(f"Processing {self.county} as QPublic county")
            self.process_qpublic_county()
        elif self.county in GRIZZLY_COUNTIES:
            logger.info(f"Processing {self.county} as Grizzly county")
            self.process_grizzly_county()
        else:
            logger.warning(f"County {self.county} not found in either QPublic or Grizzly counties")
            logger.info("Available counties:")
            logger.info(f"QPublic: {', '.join(QPUBLIC_COUNTIES.keys())}")
            logger.info(f"Grizzly: {', '.join(GRIZZLY_COUNTIES.keys())}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Process and merge parcels data files from county websites",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python parcels_download_merge_clean.py bay
  python parcels_download_merge_clean.py columbia
        """
    )
    
    parser.add_argument(
        'county',
        help='County name to process (case insensitive)'
    )
    
    parser.add_argument(
        '--base-path',
        default='/srv/mapwise_dev/county',
        help='Base path for county data (default: /srv/mapwise_dev/county)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        processor = ParcelsProcessor(args.county, args.base_path)
        processor.process()
        logger.info("Processing completed successfully")
        
    except (ValueError, FileNotFoundError, NotADirectoryError) as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        logger.error(f"Command execution failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main() 