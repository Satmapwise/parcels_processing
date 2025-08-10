# Streets Data Management

Data Management Tasks

- Research data
- Acquire data
- Document data
- Process data
- Edit data
- Publish data


## Research Data


## Acquire Data


Automate streets downloads where possible - any current ones????

cd /srv/datascrub/03_Transportation/streets/streets_county/florida/county/


### SHP Downloads


BUG download_data.py - enclose URL in double quotes to handle spaces in file names

```bash
download_data.py /data/streets/charlotte
download_data.py /data/streets/clay
download_data.py /data/streets/escambia
download_data.py /data/streets/lake
download_data.py /data/streets/okaloosa
# download_data.py /data/streets/st_johns
download_data.py /data/streets/st_lucie
# download_data.py /data/streets/volusia
```


### AGS Downloads

```bash
ags_extract_data2.py streets_baker delete 15
ags_extract_data2.py streets_bradford delete 15
ags_extract_data2.py streets_bradford_clay delete 15
ags_extract_data2.py streets_desoto delete 15
ags_extract_data2.py streets_duval delete 15
ags_extract_data2.py streets_hendry delete 15
ags_extract_data2.py streets_hernando delete 15
ags_extract_data2.py streets_highlands delete 15
ags_extract_data2.py streets_leon delete 15
ags_extract_data2.py streets_nassau delete 15
ags_extract_data2.py streets_osceola delete 15
?? ags_extract_data.py streets_pasco delete 15
ags_extract_data2.py streets_putnam delete 15
ags_extract_data2.py streets_santa_rosa delete 15
ags_extract_data2.py streets_st_johns delete 15
ags_extract_data2.py streets_st_lucie delete 15
ags_extract_data2.py streets_sumter delete 15

```


### Extract from (multi-layer) GeoDatabase


Walton
```bash
cd /srv/mapwise_dev/county/walton/processing/vector/propapp/current/source_data
ogrinfo -so PublicData_20230227.gdb
ogr2ogr streets.shp PublicData_20230227.gdb 


cd /srv/datascrub/03_Transportation/streets/streets_county/florida/county/flagler

zip_rename_date.sh 

```

## Document Data


Processing depends on Data Catalog being filled out with mininum info.


Map source fields to standard fields in "Fields Object Transform:" field in Data Catalog

NOTE: You can have both AGS and field mapping info in there! If applicable.
example: ags_version:10.31,query:,out_format:json,fullname:longname,l_zipcode:lzip,r_zipcode:rzip

Options:
	fullname:<street_name> 
	l_zipcode:<left zipcode> 
	r_zipcode:<right zipcode>
	l_munic:<left municipality> (flagler puts county here and we delete extra counties using it)
	r_munic:<right municipality> (flagler puts county here and we delete extra counties using it)
	
	TODO: expand options to include: LEE COUNTY NEEDS NAME COMPONENTS TO MAKE FULL NAME
		lzip, rzip, 
		l_add_from, l_add_to, r_add_from, r_add_to, pre_dir, st_name, st_type, suf_dir, class

Example:
	fullname:sname,l_zipcode:lzip,r_zipcode:rzip

```bash
cd /srv/datascrub/03_Transportation/streets/streets_county/florida/county/hernando

ogrinfo Roads_HC1.shp Roads_HC1|less
ogrinfo streets_hernando.shp streets_hernando|less
ogrinfo streets_desoto.shp streets_desoto|less


ogr2ogr -f "CSV" streets_hernando.csv streets_hernando.shp streets_hernando

```


Miami-Dade

```bash

INFO: Open of `Street.shp'
      using driver `ESRI Shapefile' successful.

Layer name: Street
Geometry: Line String
Feature Count: 111741
Extent: (-80.874342, 25.211143) - (-80.119324, 25.981646)
Layer SRS WKT:
GEOGCS["GCS_WGS_1984",
    DATUM["WGS_1984",
        SPHEROID["WGS_84",6378137,298.257223563]],
    PRIMEM["Greenwich",0],
    UNIT["Degree",0.017453292519943295]]
OBJECTID: Integer (10.0)
STREETID: Integer (10.0)
STREET_ID_: Integer (10.0)
L_ADD_FROM: Integer (10.0)
L_ADD_TO: Integer (10.0)
R_ADD_FROM: Integer (10.0)
R_ADD_TO: Integer (10.0)
PRE_DIR: String (80.0)
ST_NAME: String (80.0)
ST_TYPE: String (80.0)
SUF_DIR: String (80.0)
LZIP: Integer (10.0)
RZIP: Integer (10.0)
LMUNIC: Integer (10.0)
RMUNIC: Integer (10.0)
CLASS: String (80.0)
FROM_TO_IM: Integer (10.0)
TO_FROM_IM: Integer (10.0)
SNAME: String (80.0)
Shape__Len: Real (24.15)

```



## Process Data

reproject anything in 4326 to avoid any issues




STREETS COUNTY -- update plato

```bash
update_streets_county.py alachua
update_streets_county.py baker
update_streets_county.py bay
update_streets_county.py bradford_clay
update_streets_county.py brevard
update_streets_county.py broward
update_streets_county.py charlotte
update_streets_county.py citrus
update_streets_county.py clay
update_streets_county.py collier
update_streets_county.py columbia
update_streets_county.py desoto
update_streets_county.py dixie
update_streets_county.py duval
update_streets_county.py escambia
update_streets_county.py flagler
update_streets_county.py hendry
update_streets_county.py hernando
update_streets_county.py highlands
update_streets_county.py hillsborough
update_streets_county.py indian_river
update_streets_county.py lake
update_streets_county.py lee
update_streets_county.py leon
update_streets_county.py manatee
update_streets_county.py marion
update_streets_county.py martin
update_streets_county.py miami-dade
update_streets_county.py monroe
update_streets_county.py nassau
update_streets_county.py okaloosa
update_streets_county.py orange
update_streets_county.py osceola
update_streets_county.py palm_beach
update_streets_county.py pasco
update_streets_county.py pinellas
update_streets_county.py polk
update_streets_county.py putnam
update_streets_county.py santa_rosa
update_streets_county.py sarasota
update_streets_county.py seminole
update_streets_county.py st_johns
update_streets_county.py st_lucie
update_streets_county.py sumter
update_streets_county.py volusia
update_streets_county.py walton

```

##  Edit Data


## Publish Data

