#!/usr/bin/python
# parcels_download_merge_clean.py
# clean up and merge files downloaded from websites
#
# arguments: <county>
# For a specified county, read all files with naming convention:
#   L:\mapwise_dev\county\bay\processing\database\current\source_data\sales_2*
#   - write the lines of each file to a file called sales_current.txt
#   - skip lines with the following characteristics:
#       - empty line
#       - header line, contains Sale Date
#       - html line, contains <tr>
# 
# 3/22/2022 - THIS CAN BE REDUCED TO A FEW LINES OF CODE! No python required.
# Just delete the header, blank lines and lines containing some html code
# the whole script is just building a couple unix commands!
# cat - merge files together
# delete header, blank lines and lines containing some html code
# dos2unix
# 
# that is all folks!
#
# ----------------------------------------------------------------------
#
# Import needed modules
import sys,os,fileinput

try:
    county = sys.argv[1].lower()
    
    county_upper = county.upper()
    county_lower = county.lower()
    county_capital = county_upper.capitalize() 

except:
    print 'missing county'
    sys.exit(0)

# /srv/mapwise_dev/county/walton/processing/database/current/source_data/
# set main processing path
pathProcessing = '/srv/mapwise_dev/county/' + county + '/processing/database/current/source_data/'


qpublic_counties = ['BAY','CALHOUN','DIXIE','FLAGLER','FRANKLIN',
    'GADSDEN','GILCHRIST','GLADES','GULF','HAMILTON','HARDEE','HENDRY','HOLMES',
     'JACKSON','JEFFERSON','LEVY','LIBERTY','MADISON','OKALOOSA','SANTA_ROSA','SUMTER',
     'TAYLOR','WAKULLA','WALTON','WASHINGTON']

# PUTNAM not a Grizzly county, but acts like one
grizzly_counties = ['BAKER','BRADFORD','COLUMBIA','DESOTO',
    'HENDRY','LAFAYETTE','OKEECHOBEE','PUTNAM','SUWANNEE','UNION']

# cat /srv/mapwise_dev/county/bay/processing/database/current/source_data/Bay*.csv | sed '/Parcel ID/d' > /srv/mapwise_dev/county/bay/processing/database/current/source_data/sales_current.csv

if (county_upper in qpublic_counties) :
    
    search_field = "Parcel ID"
    
    if (county_upper =='FRANKLIN') :
        search_field = "Property ID"
        
    if (county_upper == 'FLAGLER') :
        search_field = "Parcel  Number"
    
    if (county_upper == 'SANTA_ROSA') :
        search_field = "Parcel"
        # hack - change naming convention at some point
        county_capital = 'SantaRosa'
    
    mycmd = ''.join(["cat ",pathProcessing,county_capital,"*.csv | sed '/",search_field,"/Id' > ",pathProcessing,"sales_current_temp.csv"])
    print 'Executing: ', mycmd
    os.system(mycmd)
    
    # since work is being done on text files in MacOS, must convert files to unix for proper line endings
    # mix and match of line endings - postgres loader no likey
    mycmd = 'dos2unix -n ' + pathProcessing + 'sales_current_temp.csv ' + pathProcessing + 'sales_current.csv'
    print 'Executing: ', mycmd
    os.system(mycmd)
    
    
# cat /srv/mapwise_dev/county/columbia/processing/database/current/source_data/columbia_sales_2*.txt | sed '/Sale1_Date/d' > /srv/mapwise_dev/county/columbia/processing/database/current/source_data/sales_dnld_2014-01-01_current.txt
# cat /srv/mapwise_dev/county/columbia/processing/database/current/source_data/columbia_mailing_2*.txt | sed '/Address1/d' > /srv/mapwise_dev/county/columbia/processing/database/current/source_data/sales_owner_mailing_dnld_2014-01-01_current.txt

# TODO: make code more robust - inspect header and see what the sale date naming convention is
if (county_upper in grizzly_counties) :

    # Sales files fisrt
    search_field = "Sale1_Date"
    
    if (county_upper in ['SUMTER']) :
        search_field = "SaleDate"

    if (county_upper in ['LAFAYETTE']) :
        search_field = "Sale_Date" 
        
    if (county_upper in ['PUTNAM']) :
        search_field = "Sale Price"
        
    if (county_upper in ['SUWANNEE','UNION']) :
        search_field = "Sale1_Price"
        
    if (county_upper in ['LAFAYETTE','SUMTER']) :
        mycmd = ''.join(["cat ",pathProcessing,county_lower,"_briefsales_2*.txt | sed '/",search_field,"/Id' > ",pathProcessing,"sales_dnld_2014-01-01_current_temp.txt"])
    
    elif (county_upper in ['PUTNAM']) :
        mycmd = ''.join(["cat ",pathProcessing,county_lower,"_sales_2*.csv | sed '/",search_field,"/Id' > ",pathProcessing,"sales_dnld_2014-01-01_current_temp.txt"])
    
    else:
        mycmd = ''.join(["cat ",pathProcessing,county_lower,"_sales_2*.txt | sed '/",search_field,"/Id' > ",pathProcessing,"sales_dnld_2014-01-01_current_temp.txt"])
                
    print 'Executing: ', mycmd
    os.system(mycmd)

    # Now mailing files
    search_field = "Address1"
    
    if (county_upper in ['PUTNAM']) :
        search_field = "Address 1"
        mycmd = ''.join(["cat ",pathProcessing,county_lower,"_mailing_2*.csv | sed '/",search_field,"/Id' > ",pathProcessing,"sales_owner_mailing_dnld_2014-01-01_current_temp.txt"])
    else:
        mycmd = ''.join(["cat ",pathProcessing,county_lower,"_mailing_2*.txt | sed '/",search_field,"/Id' > ",pathProcessing,"sales_owner_mailing_dnld_2014-01-01_current_temp.txt"])
    
    
    print 'Executing: ', mycmd
    os.system(mycmd)

    # since work is being done on text files in MacOS, must convert files to unix for proper line endings
    # mix and match of line endings - postgres loader no likey
    mycmd = 'dos2unix -n ' + pathProcessing + 'sales_dnld_2014-01-01_current_temp.txt ' + pathProcessing + 'sales_dnld_2014-01-01_current.txt'
    print 'Executing: ', mycmd
    os.system(mycmd)
    
    mycmd = 'dos2unix -n ' + pathProcessing + 'sales_owner_mailing_dnld_2014-01-01_current_temp.txt ' + pathProcessing + 'sales_owner_mailing_dnld_2014-01-01_current.txt'
    print 'Executing: ', mycmd
    os.system(mycmd)
    
else:
    pass

