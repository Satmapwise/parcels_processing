#!/bin/bash
# parcels_merge_shell.sh
# Alternative implementation using pure shell commands
# Usage: ./parcels_merge_shell.sh <county>

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Validate input
if [ $# -ne 1 ]; then
    print_error "Usage: $0 <county>"
    exit 1
fi

COUNTY="${1,,}"  # Convert to lowercase
COUNTY_UPPER="${1^^}"  # Convert to uppercase
BASE_PATH="/srv/mapwise_dev/county"
PROCESSING_PATH="$BASE_PATH/$COUNTY/processing/database/current/source_data"

# Validate county name (prevent path traversal)
if [[ ! $COUNTY =~ ^[a-z_]+$ ]]; then
    print_error "Invalid county name: $COUNTY"
    exit 1
fi

# Check if processing path exists
if [ ! -d "$PROCESSING_PATH" ]; then
    print_error "Processing path does not exist: $PROCESSING_PATH"
    exit 1
fi

print_status "Processing county: $COUNTY_UPPER"
print_status "Processing path: $PROCESSING_PATH"

# QPublic counties
QPUBLIC_COUNTIES=("BAY" "CALHOUN" "DIXIE" "FLAGLER" "FRANKLIN" "GADSDEN" "GILCHRIST" "GLADES" "GULF" "HAMILTON" "HARDEE" "HENDRY" "HOLMES" "JACKSON" "JEFFERSON" "LEVY" "LIBERTY" "MADISON" "OKALOOSA" "SANTA_ROSA" "SUMTER" "TAYLOR" "WAKULLA" "WALTON" "WASHINGTON")

# Grizzly counties
GRIZZLY_COUNTIES=("BAKER" "BRADFORD" "COLUMBIA" "DESOTO" "HENDRY" "LAFAYETTE" "OKEECHOBEE" "PUTNAM" "SUWANNEE" "UNION")

# Function to check if county is in array
is_county_in_array() {
    local county="$1"
    shift
    local counties=("$@")
    
    for c in "${counties[@]}"; do
        if [[ "$county" == "$c" ]]; then
            return 0
        fi
    done
    return 1
}

# Function to process QPublic county
process_qpublic() {
    local county="$1"
    local search_field="Parcel ID"
    local file_pattern="${county}*.csv"
    
    # Handle special cases
    case "$county" in
        "FRANKLIN")
            search_field="Property ID"
            ;;
        "FLAGLER")
            search_field="Parcel  Number"
            ;;
        "SANTA_ROSA")
            search_field="Parcel"
            file_pattern="SantaRosa*.csv"
            ;;
    esac
    
    print_status "Processing QPublic county: $county"
    print_status "Search field: $search_field"
    print_status "File pattern: $file_pattern"
    
    cd "$PROCESSING_PATH"
    
    # Merge files and remove headers
    if find . -name "$file_pattern" -type f | head -1 | grep -q .; then
        find . -name "$file_pattern" -type f -exec cat {} + | sed "/$search_field/Id" > sales_current_temp.csv
        
        # Convert line endings
        dos2unix -n sales_current_temp.csv sales_current.csv
        
        # Clean up
        rm -f sales_current_temp.csv
        
        print_status "Successfully created sales_current.csv"
    else
        print_warning "No files found matching pattern: $file_pattern"
    fi
}

# Function to process Grizzly county
process_grizzly() {
    local county="$1"
    local sales_field="Sale1_Date"
    local mailing_field="Address1"
    local sales_pattern="${county,,}_sales_2*.txt"
    local mailing_pattern="${county,,}_mailing_2*.txt"
    
    # Handle special cases
    case "$county" in
        "SUMTER")
            sales_field="SaleDate"
            sales_pattern="${county,,}_briefsales_2*.txt"
            ;;
        "LAFAYETTE")
            sales_field="Sale_Date"
            sales_pattern="${county,,}_briefsales_2*.txt"
            ;;
        "PUTNAM")
            sales_field="Sale Price"
            mailing_field="Address 1"
            sales_pattern="${county,,}_sales_2*.csv"
            mailing_pattern="${county,,}_mailing_2*.csv"
            ;;
        "SUWANNEE"|"UNION")
            sales_field="Sale1_Price"
            ;;
    esac
    
    print_status "Processing Grizzly county: $county"
    print_status "Sales field: $sales_field"
    print_status "Mailing field: $mailing_field"
    
    cd "$PROCESSING_PATH"
    
    # Process sales files
    if find . -name "$sales_pattern" -type f | head -1 | grep -q .; then
        find . -name "$sales_pattern" -type f -exec cat {} + | sed "/$sales_field/Id" > sales_dnld_2014-01-01_current_temp.txt
        dos2unix -n sales_dnld_2014-01-01_current_temp.txt sales_dnld_2014-01-01_current.txt
        rm -f sales_dnld_2014-01-01_current_temp.txt
        print_status "Successfully created sales_dnld_2014-01-01_current.txt"
    else
        print_warning "No sales files found matching pattern: $sales_pattern"
    fi
    
    # Process mailing files
    if find . -name "$mailing_pattern" -type f | head -1 | grep -q .; then
        find . -name "$mailing_pattern" -type f -exec cat {} + | sed "/$mailing_field/Id" > sales_owner_mailing_dnld_2014-01-01_current_temp.txt
        dos2unix -n sales_owner_mailing_dnld_2014-01-01_current_temp.txt sales_owner_mailing_dnld_2014-01-01_current.txt
        rm -f sales_owner_mailing_dnld_2014-01-01_current_temp.txt
        print_status "Successfully created sales_owner_mailing_dnld_2014-01-01_current.txt"
    else
        print_warning "No mailing files found matching pattern: $mailing_pattern"
    fi
}

# Main processing logic
if is_county_in_array "$COUNTY_UPPER" "${QPUBLIC_COUNTIES[@]}"; then
    process_qpublic "$COUNTY_UPPER"
elif is_county_in_array "$COUNTY_UPPER" "${GRIZZLY_COUNTIES[@]}"; then
    process_grizzly "$COUNTY_UPPER"
else
    print_warning "County $COUNTY_UPPER not found in either QPublic or Grizzly counties"
    print_status "Available QPublic counties: ${QPUBLIC_COUNTIES[*]}"
    print_status "Available Grizzly counties: ${GRIZZLY_COUNTIES[*]}"
    exit 1
fi

print_status "Processing completed successfully for $COUNTY_UPPER" 