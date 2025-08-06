#!/bin/bash
# /srv/tools/bash/zip_rename_date.sh
# 2023-03-27 -- bmay
# ChatGPT inspired!
# Took a few runs, tweaking awk and the exact commnad, but we got it in less than an hour.
# Open a zip file, list the contents and dates, get the most recent file date, 
# 	rename the zip file to append the latest file date to the zip file name.
# Sample output from unzip -l:
# Archive:  Road_Centerlines.zip
#   Length      Date    Time    Name
# ---------  ---------- -----   ----
#   2183024  2023-03-27 15:46   ACFR_DBO_RoadCenterline.shp
#    144988  2023-03-27 15:46   ACFR_DBO_RoadCenterline.shx
#   7281904  2023-03-27 15:46   ACFR_DBO_RoadCenterline.dbf
#         5  2023-03-27 15:46   ACFR_DBO_RoadCenterline.cpg
#       536  2023-03-27 15:46   ACFR_DBO_RoadCenterline.prj
# ---------                     -------
#   9610457                     5 files


# Check that the user provided a filename as an argument
if [ $# -eq 0 ]; then
  echo "Please provide the name of the zip file as an argument."
  exit 1
fi

# Extract the filename from the first argument
ZIPFILE="$1"

# Extract the most recent file date from the zip file
# unzip -l "Road_Centerlines.zip" | awk 'NR>3 {print $2}' | sort -r | head -1

# NR (special variable) = number of records
#NEWDATE=$(unzip -l "$ZIPFILE" | awk 'NR>3 {print $2}' | sort -r | head -1)

# Use grep to find lines that start with a date

#dates=$(unzip -l "Road_Centerlines.zip" |awk '{print $2}' |grep "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}")
dates=$(unzip -l "$1" |awk '{print $2}' |grep "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}")

# Use awk to extract the date column and sort the dates in descending order
sorted_dates=$(echo "$dates" | awk '{print $1}' | sort -r)

# Use head to get the most recent date
most_recent_date=$(echo "$sorted_dates" | head -n 1)

echo $most_recent_date

# Rename the zip file with the new date
mv "$ZIPFILE" "${ZIPFILE%.zip}_$most_recent_date.zip"