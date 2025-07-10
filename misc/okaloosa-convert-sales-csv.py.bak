#!/usr/bin/python
# miami-dade-convert-tabdelimited.py
# 20050905 - bmay
# See word docs for more explanation. Basically, takes the final joined GDB data
# that has been exported to TAB delimited and creates a clean TAB delimited for
# for import into Access parcel_template table, which is then joined to a
# PIN-only version of the parcels GDB. This is customized for each county.
# ----------------------------------------------------------------------
#
# Import needed modules
import os,fileinput,csv

#----------------------------------------------------
# CONVERT DATE
# - convert from mm/dd/yyyy
# - account for single digit day and/or month
#----------------------------------------------------
def convert_date(date) :

    #print '-- SALE_DATE: ', date
    date = date.replace('/','-')       
    #print '-- SALE_DATE: ', date
    
    if date.find('-') > 1 :
        #print 'SALE_DATE: ', date
        z2 = date.split('-')
        #print z2
        SALE_MO = z2[0]
        SALE_DAY = z2[1]
        SALE_YEAR = z2[2]
        
        if SALE_YEAR == '0000' :
            SALE_DATE = ''
            return ''
        
        if len(SALE_MO) == 1:
            SALE_MO = '0' + SALE_MO
            
        if len(SALE_DAY) == 1:
            SALE_DAY = '0' + SALE_DAY
            
        date = SALE_YEAR + '-' + SALE_MO + '-' + SALE_DAY
        #print 'SALE_DATE_NEW: ', date
    else :
        date = ''

    return date

# close the imput and output file - needed for troubleshooting and errors
fileinput.close()
#f1.close()

# convert to arguments at some point
f1 = open('/srv/mapwise_dev/county/okaloosa/processing/database/current/parcels_sales.txt','w')


# used counting interations when troubleshooting, e.g. only process 200 records, then inspect and fix
cnt = 1

# iterate through all records in the input file
file_name = '/srv/mapwise_dev/county/okaloosa/processing/database/current/source_data/sales_current.csv'
reader = csv.reader(open(file_name, 'rb'))
reader.next()  # skip the headers
for a in reader:

    #print cnt
    # assign all standard columns a nodata value in case the county does not have a corresponding column

    PIN = ''
    PIN_CLEAN = ''
    PIN2 = ''
    PIN2_CLEAN = ''
    STR = ''

    NAME_MISC = ''
    O_NAME1 = ''
    O_NAME2 = ''
    O_NAME3 = ''
    O_ADDRESS1 = ''
    O_ADDRESS2 = ''
    O_ADDRESS3 = ''
    O_CITY = ''
    O_STATE = ''
    O_COUNTRY = ''
    O_ZIPCODE = ''
    O_ZIPCODE4 = ''

    SALE_AMT = ''
    SALE_YEAR = ''
    SALE_DATE = ''
    SALE_DATE_DATE = ''
    SALE_VAC = ''
    SALE_TYP = ''
    SALE_QUAL = ''
    SALE_MULTI = ''
    SALE_BK = ''
    SALE_PG = ''
    SALE_DOCNUM = ''
    SALE_GRANTOR = ''
    SALE_GRANTEE = ''


    
    # Begin assignments of data from county to their proper column
    # Test to see if PIN is null, if it is not, assign it
    #print a[1]


    PIN = a[0].strip()
    #PIN = PIN.replace('-','')
    
    # S_ADDRESS
    SALE_DATE = a[2].strip()
    #print SALE_DATE
    
    SALE_AMT = a[3].strip()
    x = SALE_AMT.split('.')
    SALE_AMT = x[0]
    
    SALE_QUAL = a[4].strip()
    SALE_BK = a[5].strip()
    SALE_PG = a[6].strip()
    SALE_TYP = a[12].strip()
    # acres
    #SALE_VAC = a[9].strip()
    STR = a[11]
    

    
    # date is mm-yyyy
    try:
        #print SALE_DATE
        x = SALE_DATE.split('/')
        SALE_YEAR = x[2]
        SALE_MO = x[0]
        SALE_DAY = x[1]
        if len(SALE_MO) == 1 :
            SALE_MO = '0' + SALE_MO
        if len(SALE_DAY) == 1 :
            SALE_DAY = '0' + SALE_DAY
        SALE_DATE = SALE_YEAR + '-' + SALE_MO + '-' + SALE_DAY
    except:
        print 'Date problem'
    
    SALE_AMT = SALE_AMT.replace('$','')
    SALE_AMT = SALE_AMT.replace(',','')
    SALE_AMT = SALE_AMT.replace('"','')
    SALE_AMT = SALE_AMT.replace(' ','')


    STR = STR.replace('-','')

    PIN_CLEAN = PIN.replace('-','')
    #PIN_CLEAN = STR + PIN_CLEAN
    #PIN2_CLEAN = PIN_CLEAN
    PIN2 = PIN

    # Create a list of all the columns in the proper order

    a2 = [
            PIN,
            O_NAME1, O_NAME2, O_ADDRESS1, O_ADDRESS2, O_ADDRESS3, O_CITY, O_STATE, O_ZIPCODE, O_ZIPCODE4,
            SALE_AMT, SALE_YEAR, SALE_DATE, SALE_VAC, SALE_TYP, SALE_QUAL, SALE_BK, SALE_PG
            ]
    # check the string contents - troubleshooting
    #print a2

    # Put the string back together as TAB delimited    
    string = '\t'.join(a2)
    # check the string contents - troubleshooting
    # print string
    # write the string to the file
    if (cnt > 1) :
        f1.write(string + '\n')

    # Next three lines used counting interations when troubleshooting, e.g. only process 200 records, then inspect and fix    
    cnt = 1 + cnt
    #if cnt > 10:
    #   break

# close the imput and output file
fileinput.close()
f1.close()
   