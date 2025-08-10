# Statewide FEMA Data Processing

- Run the state all at once.

  

TODO - convert this to a bash or python script



## STEP: Load Data into Postgres

- This is the main process for updating FEMA DFIRM data.
- Loads multiple layers at same time.


```bash
# Assumed at this point data has been acquired and placed in the proper folder
# One data set per state.
# Example: NFHL_12_20250423.zip contains NFHL_12_20250423.gdb

cd /srv/datascrub/12_Hazards/fema_flood/fema_dfirm/NFHL_12_20250423

# ogr2ogr complaining about not being able to use the command line 
#  config, although docs says its ok
#  not seeing a major difference with or without it - 
#  more testing to make sure its being used properly
#  its supposed to speed up loading and dumping by a lot

# set environment variable: PG_USE_COPY=YES
# this should probably be set in the .bashrc file as well / instead
PG_USE_COPY=YES

# question - does this replace using -lco PG_USE_COPY=yes, which says not supported?

# S_Fld_Haz_Ar
ogr2ogr -overwrite -nlt GEOMETRY -t_srs "EPSG:43000" -skipfailures  -f "PostgreSQL" PG:"dbname=gisdev host=localhost port=5432 user=postgres password=galactic529" -nln fema_dfirm_20250423  NFHL_12_20250423.gdb S_Fld_Haz_Ar

# S_FIRM_Pan firm panels 
ogr2ogr -overwrite -nlt GEOMETRY -t_srs "EPSG:43000" -f "PostgreSQL" PG:"dbname=gisdev host=localhost port=5432 user=postgres password=galactic529" -nln fema_firm_pan_20250423  NFHL_12_20250423.gdb S_FIRM_Pan

# S_Fld_Haz_Ln
ogr2ogr -overwrite -nlt GEOMETRY -t_srs "EPSG:32767" -f "PostgreSQL" PG:"user=postgres dbname=gisdev host=localhost port=5432 password=galactic529" -nln fema_fld_haz_ln_20250423  NFHL_12_20250423.gdb S_Fld_Haz_Ln

# S_BFE
ogr2ogr -overwrite -nlt GEOMETRY -t_srs "EPSG:32767" -f "PostgreSQL" PG:"user=postgres dbname=gisdev host=localhost port=5432 password=galactic529" -nln fema_bfe_lines_20250423  NFHL_12_20250423.gdb S_BFE

# S_LOMR 
# effective LOMRs that have been incorporated into the NFHL since the last publication of the FIRM panel for the area
ogr2ogr -overwrite -nlt GEOMETRY -t_srs "EPSG:32767" -f "PostgreSQL" PG:"user=postgres dbname=gisdev host=localhost port=5432 password=galactic529" -nln fema_lomr_20250423  NFHL_12_20250423.gdb S_LOMR

# S_Pol_Ar
ogr2ogr -overwrite -nlt GEOMETRY -t_srs "EPSG:32767" -f "PostgreSQL" PG:"user=postgres dbname=gisdev host=localhost port=5432 password=galactic529" -nln fema_pol_ar_20250423  NFHL_12_20250423.gdb S_Pol_Ar

# L_COMM_INFO
ogr2ogr -overwrite -nlt GEOMETRY -t_srs "EPSG:32767" -f "PostgreSQL" PG:"user=postgres dbname=gisdev host=localhost port=5432 password=galactic529" -nln fema_comm_info_20250423  NFHL_12_20250423.gdb L_COMM_INFO

```



## STEP: Process / Transform Data

Basically a bunch of SQL commands "doing stuff" (further explain!).

```sql
-- !!! BREVARD HAS DUPLICATE POLYGONS IN STATEWIDE VERSION - WTF!!
-- If you see another county like that, refer to dfirm_brevard.sql for duplicate elimination code

-- not sure why, but when loading using zeus, the srid is not properly set
SELECT st_srid(wkb_geometry) from fema_dfirm_20250423 WHERE dfirm_id = '12009C'   limit 10

-- NOTE - if data is already in production, need to specify schema for this to work, otherwise its just
-- searching in default schema and says its not found!
-- SELECT UpdateGeometrySRID('gisdata','fema_pol_ar','wkb_geometry',32767);
SELECT UpdateGeometrySRID('fema_dfirm_20250423','wkb_geometry',32767);
SELECT UpdateGeometrySRID('fema_firm_pan_20250423','wkb_geometry',32767);
SELECT UpdateGeometrySRID('fema_fld_haz_ln_20250423','wkb_geometry',32767);
SELECT UpdateGeometrySRID('fema_bfe_lines_20250423','wkb_geometry',32767);
SELECT UpdateGeometrySRID('fema_lomr_20250423','wkb_geometry',32767);
SELECT UpdateGeometrySRID('fema_pol_ar_20250423','wkb_geometry',32767);

ALTER TABLE fema_dfirm_20250423
  DROP COLUMN shape_area,
  DROP COLUMN shape_length,
  DROP COLUMN gfid;

-- check to make sure there are no invalid polygons
-- there are probably always invalid polygons
SELECT st_isvalidreason(wkb_geometry),* from fema_dfirm_20250423 WHERE st_isvalid(wkb_geometry) is false  limit 10;

-- Update invalid geometry
-- do this once
-- approx 24 minutes on plato
UPDATE temp.fema_dfirm_20250423
    SET wkb_geometry = ST_MakeValid(wkb_geometry)
    WHERE ST_IsValid(wkb_geometry) is false;

-- source citations
-- select count(*),source_cit from  temp.fema_dfirm_20250423 group by source_cit;

-- add new fgdl columns

ALTER TABLE fema_dfirm_20250423 ADD COLUMN floodway text;
ALTER TABLE fema_dfirm_20250423 ADD COLUMN acres numeric(19,3);
ALTER TABLE fema_dfirm_20250423 ADD COLUMN county text;
ALTER TABLE fema_dfirm_20250423 ADD COLUMN floodplain text;
ALTER TABLE fema_dfirm_20250423 ADD COLUMN risk_level text;
ALTER TABLE fema_dfirm_20250423 ADD COLUMN descript text;
ALTER TABLE fema_dfirm_20250423 ADD COLUMN source_date date;
ALTER TABLE fema_dfirm_20250423 ADD COLUMN effective_date date;

/*
76591;"0.2 PCT ANNUAL CHANCE FLOOD HAZARD"
617;"0.2 PCT ANNUAL CHANCE FLOOD HAZARD CONTAINED IN CHANNEL"
4;"0.2 PCT ANNUAL CHANCE FLOOD HAZARD CONTAINED IN STRUCTURE"
10814;"0.2 PCT ANNUAL CHANCE FLOOD HAZARD IN COASTAL ZONE"
1061;"0.2 PCT ANNUAL CHANCE FLOOD HAZARD IN COMBINED RIVERINE AND COASTAL ZONE"
2539;"1 PCT DEPTH LESS THAN 1 FOOT"
107019;"AREA OF MINIMAL FLOOD HAZARD"
144;"AREA WITH REDUCED FLOOD RISK DUE TO LEVEE"
2673;"COASTAL FLOODPLAIN"
419;"COMBINED RIVERINE AND COASTAL FLOODPLAIN"
1253;"FLOODWAY"
74;"RIVERINE FLOODWAY IN COMBINED RIVERINE AND COASTAL ZONE"
3;"RIVERINE FLOODWAY SHOWN IN COASTAL ZONE"
224720;""
*/

-- floodway
UPDATE fema_dfirm_20250423 
	SET floodway = 'FLOODWAY' WHERE zone_subty in ('FLOODWAY','RIVERINE FLOODWAY SHOWN IN COASTAL ZONE','RIVERINE FLOODWAY IN COMBINED RIVERINE AND COASTAL ZONE');

-- fld_zone X500
UPDATE fema_dfirm_20250423 
	SET fld_zone = 'X500' WHERE zone_subty like '%0.2 PCT ANNUAL CHANCE FLOOD HAZARD%';

-- acres
UPDATE fema_dfirm_20250423 
	SET acres = ST_Area(wkb_geometry) / 4046.86;

-- select count(*) as cnt, fld_zone from fema_dfirm_20250423 group by fld_zone
/*
145347;"A"
72898;"AE"
6287;"AH"
360;"AO"
18;"AREA NOT INCLUDED"
100;"D"
111;"OPEN WATER"
1;"V"
4020;"VE"
198789;"X"
*/

-- floodplain
UPDATE fema_dfirm_20250423 
	SET floodplain = '100-YEAR FLOODPLAIN' WHERE fld_zone in ('A','AE','AH','AO','V','VE');
UPDATE fema_dfirm_20250423 
	SET floodplain = '500-YEAR FLOODPLAIN' WHERE fld_zone in ('X500');
UPDATE fema_dfirm_20250423 
	SET floodplain = 'OPEN WATER' WHERE fld_zone in ('OPEN WATER');
UPDATE fema_dfirm_20250423 
	SET floodplain = 'UNDETERMINED' WHERE fld_zone in ('AREA NOT INCLUDED','D');
UPDATE fema_dfirm_20250423 
	SET floodplain = 'OUTSIDE FLOODPLAIN' WHERE fld_zone in ('X');


-- risk_level
UPDATE fema_dfirm_20250423 
	SET risk_level = 'HIGH RISK AREAS' WHERE fld_zone in ('A','AE','AH','AO');
UPDATE fema_dfirm_20250423 
	SET risk_level = 'HIGH RISK - COASTAL AREAS' WHERE fld_zone in ('V','VE');
UPDATE fema_dfirm_20250423 
	SET risk_level = 'MODERATE RISK AREAS' WHERE fld_zone in ('X500');
UPDATE fema_dfirm_20250423 
	SET risk_level = 'MODERATE TO LOW RISK AREAS' WHERE fld_zone in ('X');
UPDATE fema_dfirm_20250423 
	SET risk_level = 'OPEN WATER' WHERE fld_zone in ('OPEN WATER');
UPDATE fema_dfirm_20250423 
	SET risk_level = 'UNDETERMINED' WHERE fld_zone in ('AREA NOT INCLUDED','D');
	
-- descript
UPDATE fema_dfirm_20250423 
	SET descript = 'INSIDE SPECIAL FLOOD HAZARD AREA' WHERE sfha_tf = 'T';
UPDATE fema_dfirm_20250423 
	SET descript = 'OUTSIDE SPECIAL FLOOD HAZARD AREA' WHERE sfha_tf = 'F';	
	
	
-- Update county names
-- SELECT * FROM fema_dfirm as f, fdor_code_county as c WHERE replace(f.dfirm_id,'C','') = c.fips limit 10
UPDATE fema_dfirm_20250423 as f SET county = c.d_county
    FROM fdor_code_county as c
    WHERE replace(f.dfirm_id,'C','') = c.fips;


-- Update source_date
UPDATE fema_dfirm_20250423 
	SET source_date = '2025-04-23';	

-- Update effective_date
-- NO - because panel dates and numbers / ids are not directly tied to the fema data.
-- could do it via overlay, but get effective date by panel instead of flood zone info

-- SELECT pcomm, panel, firm_pan, eff_date FROM gisdata.fema_firm_pan order by eff_date desc limit 200;

-- Dataset specific unique ids are as follows:
-- BFE = BFE_FN_ID
-- CBRS = CBRS_ID
-- FLDHAZ = FLD_AR_ID
-- PANEL = FIRM_ID 

-- select * from fema_dfirm limit 20

-- select count(*),county,sourcedate,effective_date,fgdlaqdate  from fema_dfirm group by county,sourcedate,effective_date,fgdlaqdate order by sourcedate desc

```



## STEP: Issues with Primary Keys 

If you find that fema_dfirm has a primary key named with a date in it, like this: `fema_dfirm_20240802` then these need to be dropped and added back. Not safe to try and rename them.

```sql
# fema_bfe_lines
ALTER TABLE IF EXISTS gisdata.fema_bfe_lines 
    DROP CONSTRAINT IF EXISTS fema_bfe_lines_20240802_pkey;

ALTER TABLE IF EXISTS gisdata.fema_bfe_lines
    ADD CONSTRAINT fema_bfe_lines_pkey PRIMARY KEY (objectid);
    
DROP INDEX IF EXISTS gisdata.fema_bfe_lines_20240802_wkb_geometry_geom_idx;

CREATE INDEX IF NOT EXISTS fema_bfe_lines_wkb_geometry_geom_idx
    ON gisdata.fema_bfe_lines USING gist (wkb_geometry)
    TABLESPACE pg_default;

# fema_dfirm
ALTER TABLE IF EXISTS gisdata.fema_dfirm 
    DROP CONSTRAINT IF EXISTS fema_dfirm_20240802_pkey;

ALTER TABLE IF EXISTS gisdata.fema_dfirm
    ADD CONSTRAINT fema_dfirm_pkey PRIMARY KEY (objectid);
    
DROP INDEX IF EXISTS gisdata.fema_dfirm_20240802_wkb_geometry_geom_idx;

CREATE INDEX IF NOT EXISTS fema_dfirm_wkb_geometry_geom_idx
    ON gisdata.fema_dfirm USING gist (wkb_geometry)
    TABLESPACE pg_default;
    
    
# fema_firm_pan
ALTER TABLE IF EXISTS gisdata.fema_firm_pan 
    DROP CONSTRAINT IF EXISTS fema_firm_pan_20240802_pkey;

ALTER TABLE IF EXISTS gisdata.fema_firm_pan
    ADD CONSTRAINT fema_firm_pan_pkey PRIMARY KEY (objectid);
    
DROP INDEX IF EXISTS gisdata.fema_firm_pan_20240802_wkb_geometry_geom_idx;

CREATE INDEX IF NOT EXISTS fema_firm_pan_wkb_geometry_geom_idx
    ON gisdata.fema_firm_pan USING gist (wkb_geometry)
    TABLESPACE pg_default;


# fema_fld_haz_ln
ALTER TABLE IF EXISTS gisdata.fema_fld_haz_ln 
    DROP CONSTRAINT IF EXISTS fema_fld_haz_ln_20240802_pkey;

ALTER TABLE IF EXISTS gisdata.fema_fld_haz_ln
    ADD CONSTRAINT fema_fld_haz_ln_pkey PRIMARY KEY (objectid);
    
DROP INDEX IF EXISTS gisdata.fema_fld_haz_ln_20240802_wkb_geometry_geom_idx;

CREATE INDEX IF NOT EXISTS fema_fld_haz_ln_wkb_geometry_geom_idx
    ON gisdata.fema_fld_haz_ln USING gist (wkb_geometry)
    TABLESPACE pg_default;
    
    
# fema_lomr
ALTER TABLE IF EXISTS gisdata.fema_lomr 
    DROP CONSTRAINT IF EXISTS fema_lomr_20240802_pkey;

ALTER TABLE IF EXISTS gisdata.fema_lomr
    ADD CONSTRAINT fema_lomr_pkey PRIMARY KEY (objectid);
    
DROP INDEX IF EXISTS gisdata.fema_lomr_20240802_wkb_geometry_geom_idx;

CREATE INDEX IF NOT EXISTS fema_lomr_wkb_geometry_geom_idx
    ON gisdata.fema_lomr USING gist (wkb_geometry)
    TABLESPACE pg_default;


# fema_pol_ar
ALTER TABLE IF EXISTS gisdata.fema_pol_ar 
    DROP CONSTRAINT IF EXISTS fema_pol_ar_20240802_pkey;

ALTER TABLE IF EXISTS gisdata.fema_pol_ar
    ADD CONSTRAINT fema_pol_ar_pkey PRIMARY KEY (objectid);
    
DROP INDEX IF EXISTS gisdata.fema_pol_ar_20240802_wkb_geometry_geom_idx;

CREATE INDEX IF NOT EXISTS fema_pol_ar_wkb_geometry_geom_idx
    ON gisdata.fema_pol_ar USING gist (wkb_geometry)
    TABLESPACE pg_default;
```



## STEP: Make backups of the tables

This is how we transfer the info to PROD.

```bash
# On DEV
#
# DO THIS BEFORE REPLACING DEV SO WE HAVE THE TEMP VERSION TO UPLOAD
# backup fema tables
pg_dump -U postgres -F custom -v -f "/var/www/apps/mapwise/htdocs/x342/fema_dfirm_20250423.backup" -t "temp.fema_dfirm_20250423" gisdev

pg_dump -U postgres -F custom -v -f "/var/www/apps/mapwise/htdocs/x342/fema_firm_pan_20250423.backup" -t "temp.fema_firm_pan_20250423" gisdev

pg_dump -U postgres -F custom -v -f "/var/www/apps/mapwise/htdocs/x342/fema_fld_haz_ln_20250423.backup" -t "temp.fema_fld_haz_ln_20250423" gisdev

pg_dump -U postgres -F custom -v -f "/var/www/apps/mapwise/htdocs/x342/fema_bfe_lines_20250423.backup" -t "temp.fema_bfe_lines_20250423" gisdev

pg_dump -U postgres -F custom -v -f "/var/www/apps/mapwise/htdocs/x342/fema_lomr_20250423.backup" -t "temp.fema_lomr_20250423" gisdev

pg_dump -U postgres -F custom -v -f "/var/www/apps/mapwise/htdocs/x342/fema_pol_ar_20250423.backup" -t "temp.fema_pol_ar_20250423" gisdev

pg_dump -U postgres -F custom -v -f "/var/www/apps/mapwise/htdocs/x342/fema_comm_info_20250423.backup" -t "temp.fema_comm_info_20250423" gisdev
```



## STEP: Update DEV FEMA tables

```bash
# replace gisdata. tables in DEV

# fema_dfirm
psql -d gisdev -U postgres -c "DROP TABLE IF EXISTS gisdata.fema_dfirm"
psql -d gisdev -U postgres -c "ALTER TABLE fema_dfirm_20250423 SET SCHEMA gisdata"
psql -d gisdev -U postgres -c "ALTER TABLE fema_dfirm_20250423 RENAME TO fema_dfirm"

# fema_firm_pan
psql -d gisdev -U postgres -c "DROP TABLE IF EXISTS gisdata.fema_firm_pan"
psql -d gisdev -U postgres -c "ALTER TABLE fema_firm_pan_20250423 SET SCHEMA gisdata"
psql -d gisdev -U postgres -c "ALTER TABLE fema_firm_pan_20250423 RENAME TO fema_firm_pan"

# fema_fld_haz_ln
psql -d gisdev -U postgres -c "DROP TABLE IF EXISTS gisdata.fema_fld_haz_ln"
psql -d gisdev -U postgres -c "ALTER TABLE fema_fld_haz_ln_20250423 SET SCHEMA gisdata"
psql -d gisdev -U postgres -c "ALTER TABLE fema_fld_haz_ln_20250423 RENAME TO fema_fld_haz_ln"

# fema_bfe_lines
psql -d gisdev -U postgres -c "DROP TABLE IF EXISTS gisdata.fema_bfe_lines"
psql -d gisdev -U postgres -c "ALTER TABLE fema_bfe_lines_20250423 SET SCHEMA gisdata"
psql -d gisdev -U postgres -c "ALTER TABLE fema_bfe_lines_20250423 RENAME TO fema_bfe_lines"

# fema_lomr
psql -d gisdev -U postgres -c "DROP TABLE IF EXISTS gisdata.fema_lomr"
psql -d gisdev -U postgres -c "ALTER TABLE fema_lomr_20250423 SET SCHEMA gisdata"
psql -d gisdev -U postgres -c "ALTER TABLE fema_lomr_20250423 RENAME TO fema_lomr"

# fema_pol_ar
psql -d gisdev -U postgres -c "DROP TABLE IF EXISTS gisdata.fema_pol_ar"
psql -d gisdev -U postgres -c "ALTER TABLE fema_pol_ar_20250423 SET SCHEMA gisdata"
psql -d gisdev -U postgres -c "ALTER TABLE fema_pol_ar_20250423 RENAME TO fema_pol_ar"

# fema_comm_info
psql -d gisdev -U postgres -c "DROP TABLE IF EXISTS gisdata.fema_comm_info"
psql -d gisdev -U postgres -c "ALTER TABLE fema_comm_info_20250423 SET SCHEMA gisdata"
psql -d gisdev -U postgres -c "ALTER TABLE fema_comm_info_20250423 RENAME TO fema_comm_info"

```



## STEP: QA Results

Fire up DEV Map Viewer and review the FEMA layers, make sure everything imported OK.



## STEP: Transfer to PROD

```bash
# manual transfer via FileZilla

# TODO: scp code here - or rsync
```



## STEP: Update Data on PROD 


```bash
# on mapserver-test
# rsync data from mapserver-test incoming to m1 incoming
rsync -a /home/bmay/incoming/*.backup  bmay@104.248.122.118:/home/bmay/incoming

# DO THIS ON mapserv-test and mapserv-m1 !!!!

cd /home/bmay/incoming

# fema_dfirm
psql -d gislib -U postgres -c "DROP TABLE IF EXISTS fema_dfirm_20250423"

pg_restore -p 5432 -U postgres -d gislib -v "/home/bmay/incoming/fema_dfirm_20250423.backup"

psql -d gislib -U postgres -c "DROP TABLE IF EXISTS fema_dfirm_old"
psql -d gislib -U postgres -c "ALTER TABLE fema_dfirm RENAME TO fema_dfirm_old"
psql -d gislib -U postgres -c "DROP TABLE IF EXISTS fema_dfirm"
psql -d gislib -U postgres -c "ALTER TABLE fema_dfirm_20250423 SET SCHEMA gisdata"
psql -d gislib -U postgres -c "ALTER TABLE fema_dfirm_20250423 RENAME TO fema_dfirm"

# ROLLBACK CHANGE
# psql -d gislib -U postgres -c "ALTER TABLE fema_dfirm RENAME TO fema_dfirm_20250423_bad"
# psql -d gislib -U postgres -c "ALTER TABLE fema_dfirm_old RENAME TO fema_dfirm"
# load backup from mapserver-test on mapserver-m1
# rsync -a /mnt/volume_nyc1_01/backups/postgres/gislib/weekly/gisdata.fema_dfirm.backup  bmay@104.248.122.118:/home/bmay/incoming
# pg_restore -p 5432 -U postgres -d gislib -v "/home/bmay/incoming/gisdata.fema_dfirm.backup"

# fema_firm_pan
psql -d gislib -U postgres -c "DROP TABLE IF EXISTS fema_firm_pan_20250423"

pg_restore -p 5432 -U postgres -d gislib -v "/home/bmay/incoming/fema_firm_pan_20250423.backup"

psql -d gislib -U postgres -c "DROP TABLE IF EXISTS fema_firm_pan"
psql -d gislib -U postgres -c "ALTER TABLE fema_firm_pan_20250423 SET SCHEMA gisdata"
psql -d gislib -U postgres -c "ALTER TABLE fema_firm_pan_20250423 RENAME TO fema_firm_pan"


# fema_fld_haz_ln
psql -d gislib -U postgres -c "DROP TABLE IF EXISTS fema_fld_haz_ln_20250423"

pg_restore -p 5432 -U postgres -d gislib -v "/home/bmay/incoming/fema_fld_haz_ln_20250423.backup"

psql -d gislib -U postgres -c "DROP TABLE IF EXISTS fema_fld_haz_ln"
psql -d gislib -U postgres -c "ALTER TABLE fema_fld_haz_ln_20250423 SET SCHEMA gisdata"
psql -d gislib -U postgres -c "ALTER TABLE fema_fld_haz_ln_20250423 RENAME TO fema_fld_haz_ln"


# fema_bfe_lines
psql -d gislib -U postgres -c "DROP TABLE IF EXISTS fema_bfe_lines_20250423"

pg_restore -p 5432 -U postgres -d gislib -v "/home/bmay/incoming/fema_bfe_lines_20250423.backup"

psql -d gislib -U postgres -c "DROP TABLE IF EXISTS fema_bfe_lines"
psql -d gislib -U postgres -c "ALTER TABLE fema_bfe_lines_20250423 SET SCHEMA gisdata"
psql -d gislib -U postgres -c "ALTER TABLE fema_bfe_lines_20250423 RENAME TO fema_bfe_lines"


# fema_lomr
psql -d gislib -U postgres -c "DROP TABLE IF EXISTS fema_lomr_20250423"

pg_restore -p 5432 -U postgres -d gislib -v "/home/bmay/incoming/fema_lomr_20250423.backup"

psql -d gislib -U postgres -c "DROP TABLE IF EXISTS fema_lomr"
psql -d gislib -U postgres -c "ALTER TABLE fema_lomr_20250423 SET SCHEMA gisdata"
psql -d gislib -U postgres -c "ALTER TABLE fema_lomr_20250423 RENAME TO fema_lomr"


# fema_pol_ar
psql -d gislib -U postgres -c "DROP TABLE IF EXISTS fema_pol_ar_20250423"

pg_restore -p 5432 -U postgres -d gislib -v "/home/bmay/incoming/fema_pol_ar_20250423.backup"

psql -d gislib -U postgres -c "DROP TABLE IF EXISTS fema_pol_ar"
psql -d gislib -U postgres -c "ALTER TABLE fema_pol_ar_20250423 SET SCHEMA gisdata"
psql -d gislib -U postgres -c "ALTER TABLE fema_pol_ar_20250423 RENAME TO fema_pol_ar"


# fema_comm_info
psql -d gislib -U postgres -c "DROP TABLE IF EXISTS fema_comm_info_20250423"

pg_restore -p 5432 -U postgres -d gislib -v "/home/bmay/incoming/fema_comm_info_20250423.backup"

psql -d gislib -U postgres -c "DROP TABLE fema_comm_info"
psql -d gislib -U postgres -c "ALTER TABLE fema_comm_info_20250423 SET SCHEMA gisdata"
psql -d gislib -U postgres -c "ALTER TABLE fema_comm_info_20250423 RENAME TO fema_comm_info"

```

