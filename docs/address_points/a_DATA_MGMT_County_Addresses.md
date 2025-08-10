# Address Points - Data Management


Document History:

- 10/22/2021

- 3/31/2023 - update, cleanup

- 8/26/24 - more cleanup, etc.

- 4/19/25 - whatup
  


Data Management Tasks

- Research data
- Acquire data
- Document data
- Process data
- Edit data
- Publish data



## TODO

TODO - First, update counties that are not in PROD yet AND are in top 25.

```bash
escambia
marion
```


TODO - Update counties already in PROD

```bash
"ALACHUA"
"BAY"
"BREVARD"
"BROWARD"
"CHARLOTTE"
"CLAY"
"COLLIER"
"DUVAL"
"HERNANDO"
"HILLSBOROUGH"
"LAKE"
"LEON"
"MANATEE"
"MIAMI-DADE"
"ORANGE"
"OSCEOLA"
"PALM_BEACH"
"PASCO"
"PINELLAS"
"POLK"
"SANTA_ROSA"
"SARASOTA"
"SEMINOLE"
"ST_JOHNS"
"ST_LUCIE"
"VOLUSIA"
```



TODO - Update counties that are not in PROD yet AND are in bottom 42.

TODO - Add in all remaining counties by doing centroids from parcels.
- Can replace with county address points as we go.
- OR leave the known county address points counties blank and fill them in?






## Research Data


What do we currently have loaded as of 3/31/2023? Largely unchanged - need an update.

```sql
select count(*),d_county from address_points  group by d_county order by d_county
# 27 counties
147350	"ALACHUA"
129172	"BAY"
277411	"BREVARD"
477759	"BROWARD"
289088	"CHARLOTTE"
81768	"CLAY"
279522	"COLLIER"
602145	"DUVAL"
99262	"HERNANDO"
658910	"HILLSBOROUGH"
189103	"LAKE"
657845	"LEE"
146590	"LEON"
239447	"MANATEE"
1151072	"MIAMI-DADE"
561063	"ORANGE"
155003	"OSCEOLA"
755901	"PALM_BEACH"
289979	"PASCO"
549454	"PINELLAS"
347455	"POLK"
86396	"SANTA_ROSA"
284031	"SARASOTA"
215397	"SEMINOLE"
132935	"ST_JOHNS"
177602	"ST_LUCIE"
248277	"VOLUSIA"
```



What do we currently have loaded as of 10/2021?

```sql
select count(*),d_county from address_points  group by d_county order by d_county
# 22 counties
146977;"ALACHUA"
129172;"BAY"
277411;"BREVARD"
477759;"BROWARD"
289088;"CHARLOTTE"
81768;"CLAY"
602145;"DUVAL"
99262;"HERNANDO"
658910;"HILLSBOROUGH"
189103;"LAKE"
1151072;"MIAMI-DADE"
561063;"ORANGE"
155003;"OSCEOLA"
755901;"PALM_BEACH"
289979;"PASCO"
549454;"PINELLAS"
347455;"POLK"
86396;"SANTA_ROSA"
215397;"SEMINOLE"
132935;"ST_JOHNS"
177602;"ST_LUCIE"
248277;"VOLUSIA"
```



### County Notes

Manatee - opened an OS issue stating address points are available. Double check they are rooftop.

Pinellas - no city names?

Sarasota - no city names in OA, because source uses abbreviated city names



QUESTION: What to do in counties that do not have county site address data? 

ANSWER: Create it from parcel centroids. Counties to do it this way - see tracking spreadsheet.



QUESTION: Could / should we add to county addresses?
Example - XX is 3 years old because we can't get a fresh copy.

ANSWER:

How old is OA version?
- could fill in missing point addresses from OSM and parcels
	- use OSM first?
	- check if parcels say structure(s) are on the property
		- if so, then grab centroids
		- this would break the - "just county data" mold and move more into blended data.




## Acquire Data



### SHP Downloads

```bash
# Download data / zip files

```


### AGS Downloads

```bash
# Extract data from ArcGIS Server (AGS)

```



### OpenAddresses

This project acquires site address points from around the globe. They use JSON files to define data sources and store them in github. If having trouble finding data, this is a good source for where it may be hiding.

```bash
https://github.com/openaddresses/openaddresses/blob/master/sources/us/fl/
```





### Issues:

- Hillsborough won't download from portal
	- grab from AGS?
		- have shapefile, then ready to go
	- grab from OA?
		- a CSV with lat/longs
		/mnt/ntfs_red/m_drive/datascrub/05_Parcels/address_points/openaddresses/us/fl
		LON,LAT,NUMBER,STREET,UNIT,CITY,DISTRICT,REGION,POSTCODE,ID
		- does the schema change between counties?
		NO?, lakeland example: LON,LAT,NUMBER,STREET,UNIT,CITY,DISTRICT,REGION,POSTCODE,ID
		

see open addresses doc for downloading, etc.




Use the SQL create table code as a field reference for available fields to use for field mapping, demonstrated below after the code.
```sql
CREATE TABLE address_points
(
  ogc_fid serial NOT NULL,
  wkb_geometry geometry,
  address_id text,
  fullname text,
  aliasname text,
  buildingname text,
  landmarkname text,
  s_number text,
  s_pdir text,
  s_name text,
  s_type text,
  s_sdir text,
  s_unit text,
  s_address text,
  s_city text,
  s_city_orig text,
  s_state text,
  s_zipcode text,
  s_zipcode4 text,
  pin text,
  altkey text,
  type_orig text,
  type2_orig text,
  date_created text,
  date_edited text,
  addr_status text,
  luse text,
  lusedor text,
  d_county text,
  CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(wkb_geometry) = 2),
  CONSTRAINT enforce_srid_the_geom CHECK (st_srid(wkb_geometry) = 32767)
)
```

### Standardized fields for transformation code:

    buildingname
    landmarkname
    s_number
    s_pdir
    s_name
    s_type
    s_sdir
    s_unit
    s_address
    s_city
    s_city_orig
    s_state
    s_zipcode
    s_zipcode4
    pin
    altkey
    address_id
    type_orig
    type2_orig
    luse
    lusedor


Field mapping example. This goes into the Fields Object Transform: field in the Data Catalog for the address data set record.

    s_number:full_addre,s_pdir:pre_direct,s_name:basename,s_type:post_type,s_sdir:post_direc,s_address: complete_a,s_city_orig:municipal_,s_zipcode:zipcode,landmarkname:landmark_n,luse:dor_use_co,pin:official_p,type_orig:address_ty,type2_orig:address_st

**Field Mapping Shortcut**

Field mapping can take up to 15 minutes per data set. To expidite getting the data into the system, an option for the first load is to simply document the s_number and s_addrss fields and not worry about the rest and come back around the second time to add more fields, if needed. 


### Data Issues

Below are problems seen with various data sets.

**Geometry**

Point placement
- doesn't match appraiser (further elaborate?)
- parcel centroids only

**Attributes**

Address
- no housenumber
- mixed case
- extra words
- extra punctuation

City
- no city
- city names abbreviated
- name = 'Unincorporated'
- is city the postal city?
- mixed case
- punctuation variation
- ST vs SAINT
	

Zip Code
- no zip code
- 9 digit zip code?




## Process Data


```bash
cd /srv/projects/tools/python/lib


```




### Alachua

```bash
cd /srv/datascrub/05_Parcels/address_points/county/alachua

Alachua - AGS

update_address_points.py alachua

```

### Bay

```bash
cd /srv/datascrub/05_Parcels/address_points/county/bay

update_address_points.py bay
```

### Brevard

```bash
cd /srv/datascrub/05_Parcels/address_points/county/brevard

# Manual download from Open Data portal - fast to get shapefile
unzip -o Address_Points.zip

zip_rename_date.sh Address_Points.zip

ogrinfo Address_Points.shp Address_Points|less

# Check in Data Catalog: Fields Object Transform:
# This defines mapping raw data columns to standard MapWise columns
# All available columns are above
s_number:SITE_HOUSE,s_pdir:STREET_DIR,s_name:STREET_NAM,s_city_orig:CITY,s_address:ADDRESS,s_zipcode:ZIPCODE,sunit:UNIT,s_type:STREET_TYP,s_sdir:SUFFIX,s_address:FULLSTREET,address_id:ADDRESS_ID,folio:RENUM,s_zipcode:SITE_ZIP

update_address_points.py brevard


# Metadata
OBJECTID: Integer (6.0)
SITE_HOUSE: String (5.0)
STREET_DIR: String (2.0)
STREET_NAM: String (27.0)
STREET_TYP: String (4.0)
SUFFIX: String (5.0)
FULLADDRES: String (35.0)
FULLSTREET: String (30.0)
TIB_STREET: String (27.0)
TIB_STTYPE: String (2.0)
TIB_COMM: String (2.0)
SITE_APT_N: String (6.0)
SITE_CITY: String (24.0)
SITE_ZIP: String (5.0)
NAME: String (45.0)
LONGITUDE: Real (24.15)
LATITUDE: Real (24.15)
EDITED: Date (10.0)
EDITOR: String (2.0)
SUBDIV: String (30.0)
ORIGINATED: Date (10.0)
ORIGINATOR: String (2.0)
ID_911: String (8.0)
ESN_: String (3.0)
COMMENT_: String (50.0)
ADDRKEY: Integer (6.0)
RENUM: Integer (7.0)
ID_EXTRACT: Integer (9.0)
IDEX2: String (6.0)
SUBDIV_LIN: String (132.0)
DiscrpAgID: String (13.0)
DateUpdate: Date (10.0)
Effective: Date (10.0)
Country: String (2.0)
Site_NGUID: String (50.0)
Expire: Date (10.0)
State: String (2.0)
County: String (7.0)
AddCode: String (1.0)
AddDataURI: String (1.0)
Inc_Muni: String (22.0)
Uninc_Comm: String (1.0)
Nbrhd_Comm: String (1.0)
AddNum_Pre: String (1.0)
Add_Number: Integer (5.0)
AddNum_Suf: String (1.0)
St_PreMod: String (1.0)
St_PreDir: String (9.0)
St_PreTyp: String (1.0)
St_PreSep: String (1.0)
St_Name: String (27.0)
St_PosTyp: String (9.0)
St_PosDir: String (9.0)
St_PosMod: String (1.0)
LSt_PreDir: String (2.0)
LSt_Name: String (27.0)
LSt_Type: String (4.0)
LSt_PosDir: String (2.0)
ESN: String (3.0)
MSAGComm: String (24.0)
Post_Comm: String (24.0)
Post_Code: String (5.0)
Post_Code4: String (1.0)
Building: String (1.0)
Floor: String (3.0)
Unit: String (1.0)
Room: String (1.0)
Seat: String (1.0)
Addtl_Loc: String (1.0)
LandmkName: String (1.0)
Mile_Post: String (1.0)
Place_Type: String (1.0)
Placement: String (1.0)
Long: Real (24.15)
Lat: Real (24.15)
Elev: Integer (1.0)
TAX_ACCOUN: Integer (7.0)
UNIT_VALUE: String (10.0)
ADDRESS_TY: String (10.0)





```

### Broward

```bash
cd /srv/datascrub/05_Parcels/address_points/county/broward

Broward - point address labels ags - I think its using centroids 
    - metadata in AGS says its from 2013 - need to check
        - how are condos handled?

download_data.py /data/address-points/broward

update_address_points.py broward

```

### Charlotte

```bash
cd /srv/datascrub/05_Parcels/address_points/county/charlotte

update_address_points.py charlotte

```

### Citrus

```bash
cd /srv/datascrub/05_Parcels/address_points/county/citrus

update_address_points.py citrus
```

### Clay

```bash
cd /srv/datascrub/05_Parcels/address_points/county/clay

ll

ags_extract_data2.py address_points_clay delete 15


#
s_number:House,s_pdir:PreDir,s_name:StreetName,s_type:StreetType,s_sdir:SuffixDir,s_unit:Unit,s_address:WholeAddre,s_city:Community,s_state:State,s_zipcode:Zip,type_orig:DwellingTy

update_address_points.py clay

# Metadata
  Zip (Integer) = 32065
  OBJECTID (Integer) = 10
  StreetType (String) = DR
  DwellingTy (String) = RES
  House (Integer) = 1019
  Confidenti (String) = No
  VacancyCod (String) = B
  StreetName (String) = OTTER CREEK
  Community (String) = ORANGE PARK
  WholeAddre (String) = 1019 OTTER CREEK DR
  Source (String) = VER
  SuffixDir (String) = (null)
  UnitType (String) = (null)
  Unit (String) = (null)
  Half (String) = (null)
  State (String) = FL
  PreDir (String) = (null)
  POINT (-81.8395962 30.1624307)

```

### Collier

```bash
cd /srv/datascrub/05_Parcels/address_points/county/collier

# Manual download
# Takes a while to export

ll

unzip -o Site_Address_Points.zip

ogrinfo Site_Address_Points.shp Site_Address_Points|less

# Check in Data Catalog: Fields Object Transform:
# This defines mapping raw data columns to standard MapWise columns
# All available columns are above
s_number:ADDRNUM,s_city:MUNICIPALI,type_orig:POINTTYPE,type2_orig:STATUS,s_address:STUBADDRES,s_unit:lot_unit

update_address_points.py collier

# Metadata
  OBJECTID (Integer) = 1
  ADDPTKEY (String) = (null)
  PREADDRNUM (String) = (null)
  ADDRNUMSUF (String) = (null)
  ADDRNUM (String) = 7762
  ADDRRANGE (String) = (null)
  FULLADDR (String) = 7762 Ashton RD, Naples
  UNITTYPE (String) = (null)
  UNITID (String) = (null)
  ALTUNITTYP (String) = (null)
  ALTUNITID (String) = (null)
  FULLNAME (String) = Ashton RD
  PLACENAME (String) = (null)
  MUNICIPALI (String) = Naples
  ADDRCLASS (Integer) = (null)
  POINTTYPE (String) = Single Family
  CAPTUREMET (String) = (null)
  STATUS (String) = Current
  STUBADDRES (String) = 7762 Ashton RD
  SITEADDID (Integer) = 199350
  block_buil (String) = (null)
  lot_unit (String) = 1
  subdivisio (Integer) = 168670
  FLN (String) = 22455700124
  is_primary (Integer) = 1
  RECKEY (Integer64) = 22455700124
  GlobalID (String) = d41727fa-18fb-4be0-b88b-1330ab5aa7b0
  POINT (426035.12693667 641240.060080923)


```

### Duval

```bash
cd /srv/datascrub/05_Parcels/address_points/county/duval

ags_extract_data2.py address_points_duval delete 15

ogrinfo  address_points_duval.shp address_points_duval|less

# Check in Data Catalog: Fields Object Transform:
# This defines mapping raw data columns to standard MapWise columns
# All available columns are above
s_number:HOUSE_NUMB,s_name:STNAME,s_city_orig:CITY,s_address:ADDRESS,s_zipcode:ZIPCODE,sunit:UNIT,s_type:STTYPE,s_sdir:STDIR,address_id:ADDRESS_ID

# Update address data in DEV
update_address_points.py duval



# Metadata
SUBDIVISIO: String (80.0)
HOUSE_NUMB: Integer (9.0)
PROPERTYKE: String (80.0)
X_COORD: Real (24.15)
APID: Integer (9.0)
CREATE_USE: String (80.0)
ADDRESS_LO: String (80.0)
STTYPE: String (80.0)
STDIR: String (80.0)
AICUZ: String (80.0)
CREATE_DAT: Integer64 (18.0)
Y_COORD: Real (24.15)
LOT_NUMBER: String (80.0)
ADDRESS_ID: Integer (9.0)
NATIONAL_E: String (80.0)
STCODE: Integer (9.0)
EDIT_DATE: Integer64 (18.0)
FLOODZONE: String (80.0)
CNSTRACT: String (80.0)
LOCAL_LAND: String (80.0)
TAZ: Real (24.15)
DEV_NUMBER: Real (24.15)
UNIT_TYPE: String (80.0)
RE: String (80.0)
JSOSUBSEC: String (80.0)
LOCAL_DIST: String (80.0)
NEIGHBOR: String (80.0)
EXPIRE_DAT: Integer64 (18.0)
SWHAULER: String (80.0)
NATIONAL_D: String (80.0)
CITY_CODE: Integer (9.0)
FULL_HOUSE: String (80.0)
APZ: String (80.0)
OBJECTID: Integer (9.0)
POSTAL_TOW: String (80.0)
STNAME: String (80.0)
ZONING: String (80.0)
LONGITUDE: Real (24.15)
ZIPCODE: String (80.0)
BLOCK_NUMB: String (80.0)
STATE: String (80.0)
EDIT_USER: String (80.0)
FULL_NAME: String (80.0)
CPAC: Integer (9.0)
LANDUSE: String (80.0)
COUNTDIST: String (80.0)
UNIT: String (80.0)
TRUE_LOCAT: Integer (9.0)
ANOMALY: String (82.0)
CITY: String (80.0)
GGMID: Integer (9.0)
OLDADDRESS: String (80.0)
TYPEUSPS: String (80.0)
ASHSITE: String (80.0)
ADDRESS: String (80.0)
LATITUDE: Real (24.15)
WHOLE_ADDR: String (80.0)
MATCHTYPE: String (80.0)
SIDE: String (80.0)
STRUCTURE: String (80.0)

```

### Escambia

```bash
cd /srv/datascrub/05_Parcels/address_points/county/escambia

ags_extract_data.py address_points_escambia delete 15

```

### Hernando

```bash
cd /srv/datascrub/05_Parcels/address_points/county/hernando



```

### Highlands

```bash
cd /srv/datascrub/05_Parcels/address_points/county/highlands

update_address_points.py highlands
```


### Hillsborough

```bash
cd /srv/datascrub/05_Parcels/address_points/county/hillsborough

# Manual download from portal
# Comes with funky naming
unzip Site_Address_Point_-2304909744571171218.zip

# File Geodatabase
205c3877-63ca-4c72-8533-2d3e26f6716f.gdb

ogrinfo 205c3877-63ca-4c72-8533-2d3e26f6716f.gdb SiteAddressPoint|less

# Check in Data Catalog: Fields Object Transform:
# This defines mapping raw data columns to standard MapWise columns
s_number:ADDRNUM,s_pdir:DIRPRE,s_name:STREETNAME,s_type:type,s_city_orig:POSTALCOMM,s_zipcode:zip,pin:STRAP,address_id:SITEADDID

update_address_points.py hillsborough

# Metadata
OGRFeature(SiteAddressPoint):1
  SITEADDID (String) = 000032
  SVCLOC_NBR (String) = 0063057
  STRAP (String) = 1829114P2000007000010A
  FOLIO (String) = 177116.0000
  FULLADDR (String) = 2104 W Abdella St
  FULLADDRUNIT (String) = 2104 W Abdella St
  ADDRNUM (String) = 2104
  FULLNAME (String) = W Abdella St
  DIRPRE (String) = W
  STREETNAME (String) = Abdella
  TYPE (String) = St
  ZIP (String) = 33607
  PLACENAME (String) = (null)
  MUNICIPALITY (String) = T
  ATLASID (String) = G-11
  STR (String) = 112918
  ESN (String) = 251
  PSAP (String) = 1
  MSAG (String) = Tampa
  USNGCOORD (String) = 17R LL 542 947
  POINTTYPE (String) = Location
  STATUS (String) = Current
  MEMO (String) = (null)
  LASTUPDATE (DateTime) = 2025/01/17 12:45:44
  LASTEDITOR (String) = GIS
  CREATIONDATE (DateTime) = 1985/05/24 00:00:00
  AUTHOR (String) = BTCH
  SOURCE (String) = S
  GlobalID (String) = {0896CB26-8E22-41F1-84C3-712A5F71715B}
  MSAGFULLNAME (String) = Abdella St W
  ADDRCLASS (String) = 001110
  POSTALCOMM (String) = Tampa
  POINT (500738.937502496 1322144.94334716)


# Old notes
# Hillsborough - is city of tampa addresses
#     - source shapefile has ?? records
#     - downloadable data from OA has 648k records, huh?
#     - no city, but zip code included

```


### Indian River

```bash
cd /srv/datascrub/05_Parcels/address_points/county/lake/indian_river

ags_extract_data2.py address_points_indian_river delete 15

ogrinfo Addresspoints.shp Addresspoints|less

# Check 

update_address_points.py indian_river
```

### Lake

```bash
cd /srv/datascrub/05_Parcels/address_points/county/lake

# Manual download from ftp site (could use wget)

ll

unzip -o Addresspoints.zip

zip_rename_date.sh Addresspoints.zip

ogrinfo Addresspoints.shp Addresspoints|less

# Check 

update_address_points.py lake

# Metadata
  AddressID (Integer64) = 100001
  AddressNum (Integer64) = 18730
  Building (String) = (null)
  UnitType (String) = U 214
  UnitNumber (String) = (null)
  PostalCity (String) = MOUNT DORA
  ZipCode (String) = 32757
  PrefixDire (String) = (null)
  PrefixType (String) = US HWY
  BaseStreet (String) = 441
  SuffixType (String) = (null)
  AliasName (String) = (null)
  FullAddres (String) = 18730 US HWY 441 UNIT 214
  State (String) = (null)
  Jurisdicti (String) = MOUNT DORA
  Floor (Integer64) = 1
  POINT (449019.088842664 1632525.56995999)

```

### Lee

```bash
cd /srv/datascrub/05_Parcels/address_points/county/lee

unzip -o LeeCountyNG911AddressPointsFGDB.zip

zip_rename_date.sh LeeCountyNG911AddressPointsFGDB.zip

ogrinfo LeeCountyNG911AddressPoints.gdb NG911AddressPoints|less

# Check in Data Catalog: Fields Object Transform:
# This defines mapping raw data columns to standard MapWise columns
# All available columns are above
buildingname:Building,landmarkname:LandmkName,s_number:Add_Number,s_pdir:St_PreDir,s_name:St_Name,s_type:St_PosTyp,s_sdir:St_PosDir,s_address:Full_Address,s_city:Post_Comm,s_zipcode:Post_Code,s_zipcode4:PostCodeEx,pin:STRAP,type_orig:e911type

update_address_points.py lee
```


### Leon

```bash

update_address_points.py leon
```


### Manatee

```bash
cd /srv/datascrub/05_Parcels/address_points/county/manatee

ll

unzip -o Address_Points.zip

zip_rename_date.sh Address_Points.zip

ogrinfo Address_Points.shp Address_Points|less

s_city:POSTAL_COM,
s_state:STATE,
type_orig:FEATURE_TY,
landmarkname:DEVELOPMEN,
type2_orig:STATUS,
s_unit:LOTNUMBER,
s_number:ST_NUM,
s_name:SFEANME,
s_type:SFEATYP,
s_pdir:SDIRPRE,
s_sdir:SDIRSUF,
s_zipcode:ZIP,
s_address:FULLRDNAME

update_address_points.py manatee

# Metadata
  OBJECTID (Integer) = 1
  LV_AREA (String) = (null)
  UPDATED (Date) = 2022/12/01
  LOC_FLD1 (String) = (null)
  LOC_FLD2 (String) = (null)
  LOC_FLD3 (String) = (null)
  ADDRESS_ID (String) = (null)
  STREET_ID (String) = (null)
  EMERGENCY_ (String) = (null)
  POSTAL_COM (String) = LONGBOAT KEY
  STATE (String) = FL
  FEATURE_TY (String) = BUILDING
  PERMIT_TYP (String) = (null)
  ADDRESS_AN (String) = (null)
  ADDRESS_ST (Date) = (null)
  ADDRESS_EN (Date) = (null)
  DEVELOPMEN (String) = (null)
  DEVELOPM_1 (String) = (null)
  PLAT_NAME (String) = (null)
  DTS_NUMBER (String) = (null)
  BUZZSAW_NU (String) = (null)
  SEGMENT_LI (String) = (null)
  FULL_ADDRE (String) = 510 Shinbone Alley Longboat Key 34228
  FULL_ADD_1 (String) = (null)
  NOTES1 (String) = (null)
  NOTES2 (String) = (null)
  CREATION_D (Real) = 20210106.014235001057386
  CREATION_U (String) = kyra.lamb@mymanatee.org
  MODIFY_DAT (Real) = 20221201.115451999008656
  MODIFY_USE (String) = Lyn.Dellinger@manateepao.com
  STATUS (Integer) = 4
  COUNTY_NAM (String) = MANATEE
  LOTNUMBER (String) = (null)
  ST_NUM (String) = 510
  SFEANME (String) = SHINBONE
  SFEATYP (String) = ALY
  SDIRPRE (String) = (null)
  SDIRSUF (String) = (null)
  MUN (String) = LK
  ZIP (String) = 34228
  COM_NME (String) = (null)
  LV_APT (String) = (null)
  MSG (String) = SLEEPY LAGOON: 2123
  CONFIDENCE (String) = (null)
  ESN (Integer) = 707
  MSAGCOMM (String) = Longboat Key
  MBI_NOTES (String) = (null)
  PSAP_ID (String) = Sarasota County Emergency Communications Center
  EXTERNAL_N (String) = 310139
  FULLRDNAME (String) = SHINBONE ALLEY
  POINT (-9203683.15731552 3177161.16684622)


```
### Martin

```bash



update_address_points.py martin
```

### Miami-Dade

**Acquire Data**

```bash
# Manual download from portal - see Data Catalog.

```

**Document Data**

Update Data Catalog

Always Update:

- Data Date
- Publish Date
- Date Checked



Optional / if needed Update:

- Distribution Comments
- Source URL
- Source File URL
- Format
- SRS EPSG
- Download Data?
- Raw Data Folder
- Raw Data Zip File
- Raw Data File
- Fields Object Transform

```bash

```



Process Data

```bash
cd /srv/datascrub/05_Parcels/address_points/county/miami_dade

ll

update_address_points.py miami-dade

```


```bash
# Metadata

ogrinfo Address_With_Condo.shp Address_With_Condo | less

INFO: Open of `Address_With_Condo.shp'
      using driver `ESRI Shapefile' successful.

Layer name: Address_With_Condo
Metadata:
  DBF_DATE_LAST_UPDATE=2024-12-13
Geometry: Point
Feature Count: 1132654
Extent: (-8977480.694800, 2908609.407000) - (-8918779.928100, 2995985.800800)
Layer SRS WKT:
PROJCS["WGS 84 / Pseudo-Mercator",
    GEOGCS["WGS 84",
        DATUM["WGS_1984",
            SPHEROID["WGS 84",6378137,298.257223563,
                AUTHORITY["EPSG","7030"]],
            AUTHORITY["EPSG","6326"]],
        PRIMEM["Greenwich",0,
            AUTHORITY["EPSG","8901"]],
        UNIT["degree",0.0174532925199433,
            AUTHORITY["EPSG","9122"]],
        AUTHORITY["EPSG","4326"]],
    PROJECTION["Mercator_1SP"],
    PARAMETER["central_meridian",0],
    PARAMETER["scale_factor",1],
    PARAMETER["false_easting",0],
    PARAMETER["false_northing",0],
    UNIT["metre",1,
        AUTHORITY["EPSG","9001"]],
    AXIS["X",EAST],
    AXIS["Y",NORTH],
    EXTENSION["PROJ4","+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs"],
    AUTHORITY["EPSG","3857"]]
OBJECTID: Integer (7.0)
ADDRESSID: Integer (6.0)
FOLIO: String (13.0)
point_x: Real (24.15)
point_y: Real (24.15)
HSE_NUM: Integer (5.0)
PRE_DIR: String (2.0)
ST_NAME: String (30.0)
ST_TYPE: String (4.0)
SUF_DIR: String (1.0)
ZIP: Integer (5.0)
PLUS4: Integer (4.0)
MUNIC: Integer (2.0)
MAILING_MU: String (20.0)
SNAME: String (39.0)
subaddress: Integer (6.0)
PARENT_FOL: String (13.0)
BLDG: String (16.0)
UNIT: String (13.0)
EnerGov: String (1.0)
State: String (1.0)
GlobalID: String (36.0)
```



### Orange

```bash
cd /srv/datascrub/05_Parcels/address_points/county/orange

ll

unzip -o Address_Point.zip

zip_rename_date.sh Address_Point.zip

ogrinfo Address_Point.shp Address_Point|less

update_address_points.py orange

```

### Osceola

```bash
cd /srv/datascrub/05_Parcels/address_points/county/osceola

ll

unzip -o Situs_Addresses.zip

# Situs_Points is newer and has more addresses, not sure why there are two versions of the data
ogrinfo 95ec09e2-98c6-499d-bb64-0ddbd1c216d1.gdb Situs_Points|less

ogrinfo NGCS_Address_Points.shp NGCS_Address_Points|less

# for Situs_Points
buildingname:BUILDING,s_number:NUMB,s_pdir:PREFIX,s_name:STREETNAME,s_type:TYPE,s_unit:UNIT,s_city:City,s_zipcode:ZIP

update_address_points.py osceola

# Metadata "regular"
Feature Count: 185718

 STREET (String) = TIJUANA DR
  NUMB (String) = 123
  PREFIX (String) = (null)
  STREETNAME (String) = TIJUANA
  TYPE (String) = DR
  DIRECTIONAL (String) = (null)
  CITYCD (String) = KIS
  ZIP (String) = 34743
  LZ (String) = 64
  LA (String) = 6411
  FZ (String) = ST62
  FA (String) = ST62
  EZ (String) = AC3
  EA (String) = AC3
  ESN (String) = 305
  COMMUNITYNAME (String) = KISSIMMEE
  CDISTRICT (String) = 2
  ZoneID (String) = 64
  LGID (Integer) = 1380
  MZ (String) = R62
  MA (String) = R62
  DelSuffix (String) = (null)
  DelLocat (String) = (null)
  SUBDIVISION (String) = (null)
  BUILDING (String) = (null)
  UNIT (String) = (null)
  LOCATN (String) = (null)
  CreateUser (String) = (null)
  CreateDate (DateTime) = (null)
  EditUser (String) = (null)
  EditDate (DateTime) = (null)
  created_user (String) = DATA
  created_date (DateTime) = 2022/05/09 15:16:35
  last_edited_user (String) = E911
  last_edited_date (DateTime) = 2023/10/20 14:47:59
  City (String) = KISSIMMEE
  State (String) = FL
  Neighborhood (String) = (null)
  Latitude (Real) = 28.32046355
  Longitude (Real) = -81.3662295
  Address (String) = (null)
  GlobalID (String) = {457E9E99-2545-4714-B739-C5D2798F775F}
  POINT (-81.366232595 28.320468841)


# Metadata NG911
Feature Count: 184102

 OBJECTID (Integer) = 1
  DiscrpAgID (String) = osceolasheriff.org
  DateUpdate (Date) = 2023/03/13
  Expire (String) = (null)
  Site_NGUID (String) = urn:emergency:uid:gis:SSAP:80662:osceolasheriff.org
  Country (String) = US
  State (String) = FL
  County (String) = OSCEOLA COUNTY
  AddCode (String) = (null)
  AddDataURI (String) = (null)
  Inc_Muni (String) = KISSIMMEE
  Uninc_Comm (String) = (null)
  Nbrhd_Comm (String) = (null)
  AddNum_Pre (String) = (null)
  Add_Number (Integer) = 1251
  AddNum_Suf (String) = (null)
  St_PreMod (String) = (null)
  St_PreDir (String) = (null)
  St_PreTyp (String) = (null)
  St_PreSep (String) = (null)
  St_Name (String) = DYER
  St_PosTyp (String) = BOULEVARD
  St_PosDir (String) = (null)
  St_PosMod (String) = (null)
  LSt_PreDir (String) = (null)
  LSt_Name (String) = DYER
  LSt_Type (String) = BLVD
  LSt_PosDir (String) = (null)
  ESN (String) = 302
  MSAGComm (String) = KISSIMMEE
  Post_Comm (String) = (null)
  Post_Code (String) = 34741
  Post_Code4 (String) = (null)
  Building (String) = (null)
  Floor (String) = (null)
  Unit (String) = (null)
  Room (String) = (null)
  Seat (String) = (null)
  Addtl_Loc (String) = (null)
  LandmkName (String) = (null)
  Mile_Post (String) = (null)
  Place_Type (String) = (null)
  Placement (String) = (null)
  Long (Real) = -81.436401540000006
  Lat (Real) = 28.303399729999999
  Elev (String) = (null)
  DateTest (String) = (null)
  created_us (String) = DATA
  created_da (Date) = 2024/09/29
  last_edite (String) = DATA
  last_edi_1 (Date) = 2024/09/29
  GlobalID (String) = {58B73476-C9BB-4991-BD9F-560DB62A5BB5}
  UniqueID (String) = 80662
  POINT (515740.929152689 1443261.69514192)


```

### Palm Beach

```bash
cd /srv/datascrub/05_Parcels/address_points/county/palm_beach

ll

unzip -o Situs_Addresses.zip

zip_rename_date.sh Situs_Addresses.zip

# what is true option?
update_address_points.py palm_beach true

update_address_points.py palm_beach


```

### Pasco

```bash
cd /srv/datascrub/05_Parcels/address_points/county/pasco

ags_extract_data2.py address_points_pasco delete 15

ogrinfo address_points_pasco.shp address_points_pasco|less

# Check in Data Catalog: Fields Object Transform:
# This defines mapping raw data columns to standard MapWise columns
# All available columns are above
buildingname:address_su,s_number:address_nu,s_name:base_name,s_type:suffix,s_unit:unit_ident,s_address:full_addre,s_city:mailing_ci,s_zipcode:zip_code5,pin:parcel_num,address_id:situs_id,type_orig:address_ty

update_address_points.py pasco


# Metadata
  GRID_NO (String) = Z30
  ENTERPRISE (Integer) = 200766
  Y_COORD (Real) = 1395507.118800990050659
  UNIT_IDENT (String) = (null)
  X_COORD (Real) = 553830.038669159985147
  CONDOMINIU (String) = (null)
  SHELL_SITU (Integer) = (null)
  RANGE_ID (Integer) = 37620
  ADDRESS_TY (String) = STRUCTURE
  BUILDING_E (String) = (null)
  SUFFIX (String) = DRIVE
  LAT_DMS (String) = 28 10 20.54
  ADDRESS_ID (Integer) = 212420
  BASE_NAME (String) = MOSSBANK
  DATE_MODIF (Integer64) = 1461166548000
  SITUS_ID (Integer) = 212420
  BUILDING_I (String) = (null)
  Z_COORD (Real) = (null)
  UNIT_TYPE (String) = (null)
  LONG_DMS (String) = -82 19 3.511
  PARCEL_NUM (String) = 2026330170010000040
  MAP_NO (String) = 541
  STATUS (String) = ACTIVE
  OBJECTID (Integer) = 1
  JURISDICTI (String) = UNINCORPORATED PASCO
  LONGITUDE (Real) = -82.317642109999994
  STATE (String) = FL
  GlobalID (String) = e6be69ea-96d5-4fdf-85da-e3a8bdff6dc3
  ADDRESS_SU (String) = RESIDENTIAL
  DATE_CREAT (Integer64) = 631152000000
  STREET_NAM (Integer) = 9525
  SUBADDRESS (String) = N
  ADDRESS_NU (Integer) = 30132
  PARCEL_ID (String) = 33 26 20 0170 01000 0040
  FULL_ADDRE (String) = 30132 MOSSBANK DRIVE
  SUFFIX_ABB (String) = DR
  BUILDING_T (String) = (null)
  MAILING_CI (String) = WESLEY CHAPEL
  FULL_STREE (String) = MOSSBANK DRIVE
  LATITUDE (Real) = 28.172374940000001
  ZIP_CODE5 (String) = 33543
  UNIT_ELEME (String) = (null)
  POINT (-82.3176452 28.1723801)


```

### Pinellas

```bash
cd /srv/datascrub/05_Parcels/address_points/county/pinellas

unzip -o "Site_Address_Points_(911).zip"

zip_rename_date.sh "Site_Address_Points_(911).zip"

shpmv 'Site_Address_Points_(911).shp' Site_Address_Points.shp

ogrinfo Site_Address_Points.shp Site_Address_Points|less

# project it first
ogr2ogr -t_srs "EPSG:2882" -nlt GEOMETRY -f "ESRI Shapefile" Site_Address_Points_2882.shp Site_Address_Points.shp

s_number:ADDRNUM,s_pdir:PREFIX,s_name:STRNAME,s_type:STRTYPE,s_sdir:SUFFIX,s_address:FULLADDR,s_city:MAILINGCIT,s_zipcode:POSTCODE,pin:PIN_NUM,address_id:SITEADDID,s_unit:UNITID,s_city_orig:MUNICIPALI,buildingname:PLACENAME,type_orig:COMMENTS_1,type2_orig:POINTTYPE

update_address_points.py pinellas


```

### Polk

```bash
cd /srv/datascrub/05_Parcels/address_points/county/polk

ll

ags_extract_data2.py address_points_polk delete 15

ogrinfo address_points_polk.shp address_points_polk|less

#
s_number:House_Numb,s_name:Street_Nam,s_type:Street_Suf,s_sdir:Street_Dir,s_unit:Unit_Numbe,s_city:City,s_zipcode:zip,pin:Parcel_ID


update_address_points.py polk


# Metadata
  City (String) = LAKELAND
  Owner_Name (String) = MOORE TERRY ALAN
  Zip (String) = 33810
  Street_Nam (String) = OLD DADE CITY
  Parcel_ID (String) = 232633000000032020
  Taxing_Dis (String) = 90000
  Join_Count (Integer) = 1
  Mailing_Zi (String) = 33810
  ORIGIN_FID (String) = (null)
  State (String) = FL
  TARGET_FID (Integer) = 8
  Descriptio (String) = UNINCORP/SWFWMD
  House_Numb (String) = 9915
  Subdivisio (String) = (null)
  Street_Suf (String) = RD
  Acreage (Real) = 4.786900000000000
  Mailing_Ci (String) = LAKELAND
  POINT_X (Real) = 650268.767379250028171
  POINT_Y (Real) = 1398616.507595319999382
  Add_Number (String) = 9915
  OBJECTID (Integer) = 2
  FLU_Code (String) = A/RR
  Subdivis_1 (String) = 000000
  Mailing_St (String) = FL
  Mailing_Ad (String) = 9915 OLD DADE CITY RD
  Mailing__1 (String) = (null)
  Unit_Numbe (String) = (null)
  Greenbelt_ (String) = N
  Inspection (Integer) = 1
  Street_Dir (String) = (null)
  POINT (-82.0183113 28.1812998)

```

### Santa Rosa

```bash


update_address_points.py santa_rosa


```

### Sarasota

```bash
cd /srv/datascrub/05_Parcels/address_points/county/sarasota

ll

# Manual download 
# https://data-sarco.opendata.arcgis.com/datasets/addresspoint/explore?showTable=true

unzip -o AddressPoint.zip

zip_rename_date.sh AddressPoint.zip

ogrinfo AddressPoint.shp AddressPoint | less

#
address_id:addpointid,s_number:addnumber,buildingname:bldgname,s_unit:unitnumber,s_pdir:streetpred,s_name:streetname,s_type:streetsuff,s_sdir:streetpost,s_state:state,s_zipcode:zip,s_zipcode4:plus4,s_city:postalcomm,landmarkname:landmarkna,type2_orig:addresssta,type_orig:dwellingty,s_address:address


update_address_points.py sarasota

# Metadata
 objectid (Integer) = 2
  addpointid (String) = AP_12182012_000017
  addnumber (String) = 4141
  addsuffix (String) = (null)
  bldgname (String) = (null)
  floornum (String) = 1
  unittype (String) = UNIT
  unitnumber (String) = 33
  streetpred (String) = (null)
  streetname (String) = GULF OF MEXICO
  streetsuff (String) = DR
  streetpost (String) = (null)
  muni (String) = TLK
  state (String) = FL
  zip (String) = 34228
  plus4 (String) = 2605
  county (String) = SC
  postalcomm (String) = Longboat Key
  landmarkna (String) = (null)
  gtgdesc (String) = A new address point has been identified and added during field inspection
  gtgnotes (String) = (null)
  addresssta (String) = AC
  dwellingty (String) = (null)
  scnotes (String) = (null)
  address (String) = 4141 GULF OF MEXICO DR, UNIT 33
  accela (String) = YES
  creator (String) = tlkae
  createdate (Date) = (null)
  lasteditor (String) = MSTRANOV2
  lastupdate (Date) = 2023/08/22
  globalid (String) = {DFEB3AFE-785C-4449-976C-57EF1A91F780}
  POINT (447961.269435994 1111332.25408099)

```

### Seminole

```bash
cd /srv/datascrub/05_Parcels/address_points/county/seminole

# Manual download

# remove filegdb
rm -r addresses.gdb

unzip -o addresses.gdb.zip

zip_rename_date.sh addresses.gdb.zip

ll

update_address_points.py seminole

```

### St Johns

```bash
cd /srv/datascrub/05_Parcels/address_points/county/st_johns

# Manual download

ll

ogrinfo c3d6f99a-b15e-4f67-b0c2-fd22c8003862.gdb Address_Sites | less


s_number:hsnum,s_pdir:predir,s_name:roadname,s_type:roadtype,s_sdir:sufdir,s_unit:UNIT,s_city:postal,s_zipcode:zipcode,address_id:siteid,type_orig:sitetype


update_address_points.py st_johns

# Metadata
INFO: Open of `c3d6f99a-b15e-4f67-b0c2-fd22c8003862.gdb'
      using driver `OpenFileGDB' successful.

Layer name: Address_Sites
Geometry: Point
Feature Count: 172876
Extent: (-9091342.066400, 3455212.126600) - (-9040746.988700, 3536449.771900)
Layer SRS WKT:
PROJCS["WGS 84 / Pseudo-Mercator",
    GEOGCS["WGS 84",
        DATUM["WGS_1984",
            SPHEROID["WGS 84",6378137,298.257223563,
                AUTHORITY["EPSG","7030"]],
            AUTHORITY["EPSG","6326"]],
        PRIMEM["Greenwich",0,
            AUTHORITY["EPSG","8901"]],
        UNIT["degree",0.0174532925199433,
            AUTHORITY["EPSG","9122"]],
        AUTHORITY["EPSG","4326"]],
    PROJECTION["Mercator_1SP"],
    PARAMETER["central_meridian",0],
    PARAMETER["scale_factor",1],
    PARAMETER["false_easting",0],
    PARAMETER["false_northing",0],
    UNIT["metre",1,
        AUTHORITY["EPSG","9001"]],
    AXIS["X",EAST],
    AXIS["Y",NORTH],
    EXTENSION["PROJ4","+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs"],
    AUTHORITY["EPSG","3857"]]
FID Column = OBJECTID
Geometry Column = SHAPE
SITEID: Integer (0.0)
ROADID: Integer (0.0)
HSNUM: Integer (0.0)
HSNUMSFX: String (10.0)
PREMOD: String (15.0)
PRETYPE: String (20.0)
PREDIR: String (2.0)
ROADNAME: String (50.0)
ROADTYPE: String (8.0)
SUFDIR: String (2.0)
NAME: String (4000.0)
UNIT_TYPE: String (6.0)
UNIT: String (10.0)
ADDRESS: String (50.0)
FULLADDRESS: String (200.0)
POSTAL: String (25.0)
STATE: String (2.0)
ZIPCODE: Integer (0.0)
SITETYPE: String (2.0)
POINT_X: Real (0.0)
POINT_Y: Real (0.0)
SIDE: String (5.0)
FULLHSNUM: String (35.0)
COUNTY: String (25.0)
CITY: String (40.0)
OGRFeature(Address_Sites):2
  SITEID (Integer) = 1218834
  ROADID (Integer) = 11258
  HSNUM (Integer) = 65
  HSNUMSFX (String) =
  PREMOD (String) =
  PRETYPE (String) =
  PREDIR (String) =
  ROADNAME (String) = WILBUR
  ROADTYPE (String) = DR
  SUFDIR (String) =
  NAME (String) = Wilbur Dr
  UNIT_TYPE (String) =
  UNIT (String) =
  ADDRESS (String) = 65 Wilbur Dr
  FULLADDRESS (String) = 65 Wilbur Dr, Saint Augustine, FL 32086
  POSTAL (String) = Saint Augustine
  STATE (String) = FL
  ZIPCODE (Integer) = 32086
  SITETYPE (String) = VP
  POINT_X (Real) = 555979.68874776
  POINT_Y (Real) = 1980075.84503488
  SIDE (String) = LEFT
  FULLHSNUM (String) = 65
  COUNTY (String) = St Johns
  CITY (String) = Unincorporated
  POINT (-9052035.0962 3475307.4738)


```

### St Lucie

```bash
cd /srv/datascrub/05_Parcels/address_points/county/st_lucie

ll

# Manual download
unzip -o AddressMaster.zip

zip_rename_date.sh AddressMaster.zip

ogrinfo AddressMaster.shp AddressMaster | less

#
landmarkname:placename,s_number:addrnum,s_pdir:roadpredir,s_name:fullname,s_type:roadtype,s_sdir:roadpostdi,s_unit:unitid,s_address:fulladdr,s_city:postcomm,s_zipcode:postal,address_id:siteaddid,type_orig:SiteType,type2_orig:structure

update_address_points.py st_lucie

# Metadata
  OBJECTID (Integer) = 410334
  siteaddid (String) = SID-183747
  addresspti (String) = (null)
  rclnguid (String) = SID-183747.stlucieco.org
  discrpagid (String) = SLC911.stlucieco.org
  preaddrnum (String) = (null)
  addrnumsuf (String) = (null)
  addrnum (String) = 5167
  addrrange (String) = (null)
  unittype (String) = UNIT
  unitid (String) = 402
  altunittyp (String) = (null)
  altunitid (String) = (null)
  secondaltu (String) = (null)
  secondal_1 (String) = (null)
  thirdaltun (String) = (null)
  thirdalt_1 (String) = (null)
  fourthaltu (String) = (null)
  fourthal_1 (String) = (null)
  fullname (String) = N Highway A1A
  fulladdr (String) = 5167 N Highway A1A UNIT 402
  placename (String) = Ocean Harbour Condo Bldg E
  country (String) = US
  stateabbre (String) = FL
  municipali (String) = St. Lucie County
  esn (String) = 00311
  msag (String) = FORT PIERCE
  usngcoord (String) = 17R NL 67448 45734
  addrclass (Integer) = (null)
  pointtype (String) = (null)
  capturemet (String) = Plan or Drawing
  status (String) = Current
  created_us (String) = SA
  created_da (Date) = 2024/03/26
  last_edite (String) = SA
  last_edi_1 (Date) = 2024/03/26
  bldngid (String) = (null)
  nbrhdid (String) = NID-1201
  GlobalID (String) = {08E0A3D9-68FD-411E-86C9-B070E93844F5}
  roadpremod (String) = (null)
  roadpredir (String) = N
  roadpretyp (String) = (null)
  roadpret_1 (String) = (null)
  roadname (String) = Highway A1A
  roadtype (String) = (null)
  roadpostdi (String) = (null)
  roadpostmo (String) = (null)
  postal (String) = 34949
  parcelnum (String) = 141170900240005
  dateadded (Date) = (null)
  structure (String) = Multi Family
  comments (String) = (null)
  postcomm (String) = Fort Pierce
  ChangeType (String) = (null)
  ChangeNote (String) = (null)
  County (String) = St. Lucie
  POINT Z (877529.527423747 1163787.89851332 0)

```

### Volusia

```bash
cd /srv/datascrub/05_Parcels/address_points/county/volusia

ll

unzip Address_Situs.zip

#
s_number:HOUSE_NUM,s_pdir:PRE_DIR,s_name:STREET_NAM,s_type:STREET_TYP,s_sdir:POST_DIR,s_unit:UNIT,s_address:ADDRESS,s_city:CITYNAME,s_zipcode:ZIP,type_orig:TYPE

ogrinfo Address_Situs.shp Address_Situs | less

update_address_points.py volusia

# Metadata
 OBJECTID (Integer) = 2
  PID (String) = 030300000135
  UNIT (String) = (null)
  HOUSE_NUM (Integer) = 1165
  PRE_DIR (String) = (null)
  STREET_NAM (String) = UNDERHILL BRANCH
  STREET_TYP (String) = RD
  POST_DIR (String) = (null)
  CITY_ABBRV (String) = OST
  CITYNAME (String) = OSTEEN
  ZIP (Integer) = 32764
  TYPE (Integer) = 10
  ADDRESS (String) = 1165 UNDERHILL BRANCH RD
  Date_ (Date) = 2006/07/24
  TARG_P (String) = (null)
  TARG_F (String) = (null)
  TARG_E (String) = (null)
  MOD_P (String) = (null)
  MOD_F (String) = (null)
  MOD_E (String) = (null)
  INIT (String) = (null)
  last_edite (Date) = 2024/02/27
  Address_ID (Integer) = 3
  POINT (653454.874852162 1618155.99994591)


```




## Edit Data


Shouldn't this be with parcel update code????

```sql
-- update parcels ORANGE
SELECT p.pin,p.altkey,p.s_number, p.s_pdir, p.s_name, p.s_type, p.s_sdir, p.s_unit, p.s_address, p.s_city, p.s_state, p.s_zipcode, p.s_zipcode4,
 s.pin,s.altkey,s.s_number, s.s_pdir, s.s_name, s.s_type, s.s_sdir, s.s_unit, s.s_address, s.s_city, s.s_state, s.s_zipcode, s.s_zipcode4 
FROM parcels_std_project as p LEFT JOIN address_points as s ON p.pin_clean = s.pin 
WHERE p.d_county = 'ORANGE' and p.lusedor = '04'  limit 100

UPDATE parcels_std_project as p 
    SET s_unit = s.s_unit
FROM address_points as s
WHERE p.d_county = 'ORANGE' and p.pin_clean = s.pin;
```




## Publish Data



