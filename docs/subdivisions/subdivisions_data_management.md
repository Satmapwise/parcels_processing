# Subdivisions Data Management

DATA MANAGEMENT OUTLINE

- Standard template for documenting data mangement per data set.


Data Management Tasks

- Research data
- Acquire data
- Document data
- Process data
- Edit data
- Publish data


## Research Data



There appear to be two levels of detail or even three.

Basic
- subdivision polygon boundary
- subdivision name

More
+ plat book and page
+ county link to OR plat document 

More Better
+ units
+ developer name
+ builder names
+ lot sizes
+ sales info
+ more 



"MIAMI-DADE"
"HILLSBOROUGH"
"LAKE"
"ORANGE"
"POLK"



```sql
-- create Orange county subdivisions from parcel data
CREATE TABLE raw_subdivisions_orange as SELECT ST_UNION(wkb_geometry) as wkb_geometry, subdiv_nm,subdiv_id from parcels_std_project where subdiv_nm is not null and d_county = 'ORANGE' GROUP BY subdiv_nm,subdiv_id 



-- Import a community name + legal name subdivision list so we can join on legal name and get community name into the database
- note that this will have to be done each time the parcel data is updated, so it has to be part of the county update routine
```


## Notes

Getting Plat Index Info for Building Plat Map Links to PDFs Online



Hillsborough Plat Maps

- Link example
https://pubrec6.hillsclerk.com/ORIPublicAccess/customSearch.html?instrument=2017495713

Where do we get the instrument number?
Its in the official records index files. With the book and Page and document type - so we can look it up there
- need a complete index

Where else?
- should be in subdivision list but its not

Other subdivision lists 
- what other lists are there that may contain this info?

https://pubrec6.hillsclerk.com/ORIPublicAccess/customSearch.html
- you can search by doc type = plat and book/page. Search by book only and you get a list of links for each book.
You can then export those.
BUT - developer tools reveal we have an API baby!
- results contain key index info like the Instrument!

https://pubrec6.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search
POST
{"BookType":"P","BookNum":"110"}

```bash
curl -H "Accept: application/json" \
	-H "Content-Type: application/json" \
	-d '{"BookType":"P","BookNum":"110"}' \
	-X POST https://pubrec6.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search \
	> test.json
```
SO, that works!

Loop through book numbers 1 through 138 (max book number as of 9/2020)

Save as JSON and concat them all together.

Load JSON directly to PG table?

```bash
in2csv -k "ResultList" test.json > test.csv

# doesn't work - inrecognized format
ogrinfo -so test.json

ogr2ogr -append -nlt GEOMETRY -f "PostgreSQL" PG:"user=postgres dbname=gisdev host=localhost port=5432 password=galactic529" -nln temp.plat_index_hillsborough test.csv
```

SQL
```sql
-- select * from plat_index_hillsborough limit 10 -- instrument,booknum,pagenum,legal,"partiesone/0","partiestwo/0",recorddate
-- select * from subdivisions limit 10
select * FROM subdivisions as s LEFT OUTER JOIN plat_index_hillsborough as p ON s.plat_bk = p.booknum AND s.plat_pg = p.pagenum WHERE  p.booknum = '110' and s.d_county = 'HILLSBOROUGH' limit 10

-- update subdivisions with plat info
UPDATE subdivisions as s
	SET
		plat_instrument = p.instrument,
		party_one = p."partiesone/0",
		party_two = p."partiestwo/0",
		legal = p.legal,
		county_link = 'https://pubrec6.hillsclerk.com/ORIPublicAccess/customSearch.html?instrument=' || p.instrument

	FROM plat_index_hillsborough as p 
	-- LEFT OUTER JOIN plat_index_hillsborough as p ON s.plat_bk = p.booknum AND s.plat_pg = p.pagenum 
	WHERE  
		s.plat_bk = p.booknum AND s.plat_pg = p.pagenum AND
		p.booknum = '110' AND s.d_county = 'HILLSBOROUGH'
```



8/27/24

@Jake Mieczkowski does my post address about subdivision search issue help resolve anything? There are more things we can do to provide more searching capability regarding subdivisions: 1) Dig deeper into available data to see if we can grab it from existing raw data. 2) Acquire more raw parcel data where don't have it - there's also gaps in things like beds and baths for some counties. 3) Purchase ATTOM data for FL which will come with more attributes, just not sure about other possible holes in that data, but maybe we blend it with what we have. 4) Where a separate subdivision polygon layer exists, but the subdivision name info is not in the parcel data - do a GIS overlay to transfer subdivision names to the parcels. 5) Create a subdivisions "custom tab" - basically turn the existing subdivision layer into a custom tab that can be independently searched, sorted etc. - Maybe do this anyway irrespective of other options. 6) In some cases, there's subdivision info in the parcels, but not a separate subdivision layer - create it using the parcels - easier sad than done - tried it several years ago and had problems, but things change - another crack at it may prove fruitful.

## Acquire Data



Automate Subdivisions downloads where possible - any current ones????


```bash
# make sure dir is clean
# older zips renamed with dates
# no extra directories, except for x_old or yearly dirs, e.g. 2015
# If some there, make sure dated zip files are present and nothing else inside them.
# and move the dated zip files up to main level.
```



### SHP Downloads


```bash
download_data.py /data/subdivisions/fl/brevard
download_data.py /data/subdivisions/fl/clay

# Escambia

cd /srv/datascrub/05_Parcels/subdivisions/county/escambia/
# make sure dir is clean
ll

download_data.py /data/subdivisions/fl/escambia
unzip -oj SUBDIV.zip
zip_rename_date.sh SUBDIV.zip


download_data.py /data/subdivisions/fl/hernando
download_data.py /data/subdivisions/fl/lake
download_data.py /data/subdivisions/fl/manatee
download_data.py /data/subdivisions/fl/palm_beach
download_data.py /data/subdivisions/fl/pasco
download_data.py /data/subdivisions/fl/seminole
download_data.py /data/subdivisions/fl/volusia
```



### AGS Downloads


```bash
ags_extract_data.py subdivisions_alachua delete 15
ags_extract_data.py subdivisions_broward delete 15
ags_extract_data.py subdivisions_leon delete 15
ags_extract_data.py subdivisions_marion delete 15
ags_extract_data.py subdivisions_orange delete 15
ags_extract_data.py subdivisions_osceola delete 15
ags_extract_data.py subdivisions_sarasota delete 15
ags_extract_data.py subdivisions_st_lucie delete 15

cd /srv/datascrub/05_Parcels/subdivisions/county/miami_dade

```



## Document Data


```bash
cd /srv/datascrub/05_Parcels/subdivisions/
```

Processing depends on Data Catalog being filled out with mininum info.


Map source fields to standard fields in "Fields Object Transform:" field in Data Catalog

NOTE: You can have both AGS and field mapping info in there! If applicable.
    example: ags_version:10.31,query:,out_format:json,fullname:longname,l_zipcode:lzip,r_zipcode:rzip
HOWEVER - no need for any AGS info, so with or without AGS source: 
    example: fullname:longname,l_zipcode:lzip,r_zipcode:rzip

```bash   
fullname:name,abbrname:name,subdiv_id:sub_id,plat_bk:book,plat_pg:page,unit:unit

fullname:
abbrname:
subdiv_id:
subdiv_id2:
plat_bkpg:
plat_bk:
plat_pg:
county_link:
units:


Options:
	fullname:<subdivision name>,
	abbrname:<abbreviated name>,
	subdiv_id:<subdivision ID>,
	subdiv_id2:<subdivision ID2>,
	plat_bkpg:<Plat book and page>,
	plat_bk:<Plat book>,
	plat_pg:<Plat page>,
	county_link:<County Web Link>,
	units:<Units>

sub8:subdiv_id,legal_line:subdiv_nm,legal_li_1:subdiv_nm2

fullname:legal_line,subdiv_id:sub8,subdiv_id2:legal_li_1


# Example:
	fullname:name,abbrname:name,subdiv_id:sub_id,subdiv_id2:myid2,plat_bkpg:pb_pg,plat_bk:book,plat_pg:page,county_link:hyperlink,unit:unit
```

NOTE: 6/2022 - ransomware breach affected subdivision data
Was stored under:
/mnt/ntfs_red/m_drive/datascrub/05_Parcels/subdivisions

11/2022 - data restored from 2020 copy??


### Alachua


subdiv_id:sub_id,fullname:name,plat_bk:bookno,plat_pg:pageno,county_link:link_plat

sub_id
name
booktype
bookno
pageno
link_plat




### Bay

```bash
cd /srv/datascrub/05_Parcels/subdivisions/county/bay

# rename older stuff before downkloading into folder
zip_rename_date.sh Subdivsions.zip

unzip -oj Subdivsions.zip

zip_rename_date.sh Subdivsions.zip


ogrinfo Subdivsions.shp Subdivsions|less


fullname:subdivid,plat_bk:plattbook,plat_pg:bookpage
```


### Brevard


fullname:name,subdiv_id:idnumber,plat_bkpg:orb_pg 




### Citrus


fullname:subdivis_1,subdiv_id:sub_number,plat_bk:plat_book,plat_pg:page




### Clay


fullname:sub_div,abbrname:marketingn,unit:unit,block:block,subdiv_id:subid



### Collier


fullname:subname,subdiv_id:subnum,plat_bkpg:pbpg



### Escambia


subdiv_id:refno,fullname:sdname,plat_bkpg:bookpage

cd /srv/datascrub/05_Parcels/subdivisions/county/escambia/


### Hernando



fullname:name,subdiv_id:subdiv_cod,subdiv_id2:parcel_num,plat_bk:book,plat_pg:from_page,units:lots




### Indian River


fullname:name,abbrname:sb_group_n,subdiv_id:sb_subhdr1,subdiv_id2:sb_subhdr2,plat_bkpg:sb_doc_num,county_link:link_to_pl



### Lake

```bash
cd /srv/datascrub/05_Parcels/subdivisions/county/lake

subdiv_id:gissubnumb,fullname:name,subdiv_id2:subnumber,year_added:yearadded,plat_bk:booknumber,plat_pg:platpage1,county_link:imagepath,units:units

fullname:
abbrname:
subdiv_id:
subdiv_id2:
plat_bkpg:
plat_bk:
plat_pg:
county_link:
units:

```

### Martin


fullname:name,abbrname:aka_namesubdiv_id:subdivisio,plat_bkpg:pb_pg



### Marion

fullname:sub_name,plat_bkpg:plat_bk_pg



### Okaloosa

```bash
cd /srv/datascrub/05_Parcels/subdivisions/county/okaloosa

# rename older stuff before downkloading into folder
zip_rename_date.sh subdivisions.zip

unzip -oj subdivisions.zip

zip_rename_date.sh subdivisions.zip


ogrinfo sub.shp sub|less

# data catalog Fields Object Transform:
fullname:sub_name,subdiv_id:sub_id,subdiv_id2:sub_pin
```



### Orange


fullname:subdivis_1,subdiv_id:subdivisio,plat_bkpg:instrument_no,plat_bk:subdivis_2,plat_pg:subdivis_3






### Osceola


no associated data!

Ask Nan about a subdivision layer with attributes.



### Pasco

```bash
cd /srv/datascrub/05_Parcels/subdivisions/county/pasco

unzip -o subdivisions.zip

zip_rename_date.sh subdivisions.zip

```




### Santa Rosa


```bash
cd /srv/datascrub/05_Parcels/subdivisions/county/santa_rosa

ogrinfo Subdivisions.shp Subdivisions|less

fullname:Subdivisio,abbrname:MapLabel,subdiv_id:Subdivnumb,plat_bk:PlatBook,plat_pg:PlatBookPa,units:NumberOfLo


FID: Integer (4.0)
Developmen: String (10.0)
Subdivnumb: Integer (4.0)
Subdivisio: String (50.0)
NumberOfLo: Integer (4.0)
PlatBook: String (8.0)
PlatBookPa: String (14.0)
Preliminar: Date (10.0)
Constructi: Date (10.0)
FinalPlatA: Date (10.0)
PRMInitial: Date (10.0)
PRMWarrant: Date (10.0)
MapLabel: String (50.0)
GlobalID: String (38.0)
FinalPlat_: String (86.0)
AsBuilt_Im: String (80.0)
TypeOfSubd: String (11.0)
created_us: String (10.0)
created_da: Date (10.0)
last_edite: String (10.0)
last_edi_1: Date (10.0)
SHAPE_STAr: Real (24.15)
SHAPE_STLe: Real (24.15)
Shape__Are: Real (24.15)
Shape__Len: Real (24.15)
OGRFeature(Subdivisions):0

```


### Seminole


fullname:PlatName,subdiv_id:SubdivisionNumber,plat_bk:PlatBook,plat_pg:PlatPage



### St Lucie


subdiv_id:subdivisio,fullname:subdivis_1,plat_bk:plat_book,plat_pg:page



### Volusia


fullname:subname,subdiv_id:subnum,county_link:platlink,plat_bk:mb,plat_pg:pg




### Sarasota

```bash

cd /srv/datascrub/05_Parcels/subdivisions/county/sarasota

ogrinfo PlatBoundary.shp PlatBoundary|less
ogrinfo Subdivision_Boundary.shp Subdivision_Boundary|less



Layer name: PlatBoundary
Geometry: Polygon
Feature Count: 5023
Extent: (447664.716551, 949597.189428) - (637462.391089, 1111368.752040)
Layer SRS WKT:
PROJCS["NAD83_HARN_Florida_West_ftUS",
    GEOGCS["GCS_NAD83(HARN)",
        DATUM["NAD83_High_Accuracy_Reference_Network",
            SPHEROID["GRS_1980",6378137,298.257222101]],
        PRIMEM["Greenwich",0],
        UNIT["Degree",0.017453292519943295]],
    PROJECTION["Transverse_Mercator"],
    PARAMETER["latitude_of_origin",24.33333333333333],
    PARAMETER["central_meridian",-82],
    PARAMETER["scale_factor",0.999941177],
    PARAMETER["false_easting",656166.667],
    PARAMETER["false_northing",0],
    UNIT["Foot_US",0.30480060960121924]]
objectid: Integer (4.0)
sub_name: String (67.0)
max_sub_id: String (4.0)
pb_pg: String (15.0)
comments: String (5.0)
acre: Real (24.15)
hyperlink: String (70.0)
lasteditor: String (9.0)
lastupdate: Date (10.0)
creator: String (9.0)
createdate: Date (10.0)
globalid: String (38.0)
cluster: String (3.0)
SHAPE__Len: Real (24.15)
SHAPE__Are: Real (24.15)

fullname:sub_name,subdiv_id:max_sub_id,plat_bkpg:pb_pg,county_link:hyperlink



-- update sys_raw_folder
--UPDATE m_gis_data_catalog_main set sys_raw_folder = concat('/mnt/ntfs/l_drive/mapwise_dev/county/a_GA/',county,'/vector/current/source_data') where layer_group = 'parcels' and title like 'Parcel Pol%' and state = 'GA'

/srv/datascrub/05_Parcels/subdivisions/county/hillsborough
```



## Process Data



SUBDIVISIONS COUNTY -- update on plato

NOTE: Useful to have a list of running them all at once. Similar to download all at once.

```bash
update_subdivisions_county.py alachua
update_subdivisions_county.py bay
update_subdivisions_county.py brevard
update_subdivisions_county.py broward
update_subdivisions_county.py charlotte
update_subdivisions_county.py citrus
update_subdivisions_county.py clay
update_subdivisions_county.py collier
update_subdivisions_county.py escambia
update_subdivisions_county.py hernando
update_subdivisions_county.py hillsborough
update_subdivisions_county.py indian_river
update_subdivisions_county.py lake
update_subdivisions_county.py lee
update_subdivisions_county.py leon
update_subdivisions_county.py manatee
update_subdivisions_county.py marion
update_subdivisions_county.py martin
update_subdivisions_county.py miami-dade
update_subdivisions_county.py monroe
update_subdivisions_county.py okaloosa
update_subdivisions_county.py orange
update_subdivisions_county.py palm_beach
update_subdivisions_county.py pasco
update_subdivisions_county.py polk
update_subdivisions_county.py santa_rosa
update_subdivisions_county.py sarasota
update_subdivisions_county.py seminole
update_subdivisions_county.py st_johns
update_subdivisions_county.py st_lucie

update_subdivisions_county.py volusia
```



## Edit Data




## Publish Data


```bash
cd /srv/temp/

cd /var/www/apps/mapwise/htdocs/x342/


ogr2ogr -f "ESRI Shapefile"  -sql "select * from subdivisions WHERE d_county IN ('ESCAMBIA','SANTA_ROSA','OKALOOSA','BAY')" subdivisions.shp PG:"user=postgres dbname=gisdev host=localhost port=5432 password=galactic529" gisdata.subdivisions

wouldn;t do the transform for some reason
 -t_srs "EPSG:2238"


zip up files

zip subdivisions.zip subdivisions.*


mv subdivisions.zip qusllc_subdivisions.zip

```