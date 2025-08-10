
# Update sunbiz_processed Postgres Table

## Overview

Sunbiz raw data is processed on DEV once per quarter. The version of data we use is released once per quarter. The data gets processed into a table called 1sunbiz_processed1, which gets regularly copied to the server via the parcel update scripts.

TODO: I think `sunbiz_processed` should be setup as its own Custom Tab that can be searched, is tied to parcels and you can save search results and selections, etc.



Process:


- download raw data
- run through python script
- drop sunbiz_processed table (or truncate it)
- load processed text files
- Done.
- Whole process should take an hour or less.



## STEP: Download Data

Download options:

1. Browse to the data via logging into their website.
2. Go direct via SFTP in FileZilla.

```bash
sftp.floridados.gov
Username = Public
Password = PubAccess1845!
/Public/doc/Quarterly/Cor
```

Download raw data from sunbiz.org
- raw data is split into multiple files - same format
- fixed width format
- contains funky characters
	
```bash
# Download via FileZilla and copy to:
# cd /srv/datascrub/21_Misc/Sunbiz/current

#  Unzip
unzip -o cordata.zip
 
# Rename Zip
zip_rename_date.sh cordata.zip

```


​	
​	
## STEP: Process Raw Data


- strip bad characters out

```bash
cd /srv/datascrub/21_Misc/Sunbiz/current/Cor

tr -c '\11\12\15\40-\176' 'Z' < cordata0.txt > cordata0_strip.txt
tr -c '\11\12\15\40-\176' 'Z' < cordata1.txt > cordata1_strip.txt
tr -c '\11\12\15\40-\176' 'Z' < cordata2.txt > cordata2_strip.txt
tr -c '\11\12\15\40-\176' 'Z' < cordata3.txt > cordata3_strip.txt
tr -c '\11\12\15\40-\176' 'Z' < cordata4.txt > cordata4_strip.txt
tr -c '\11\12\15\40-\176' 'Z' < cordata5.txt > cordata5_strip.txt
tr -c '\11\12\15\40-\176' 'Z' < cordata6.txt > cordata6_strip.txt
tr -c '\11\12\15\40-\176' 'Z' < cordata7.txt > cordata7_strip.txt
tr -c '\11\12\15\40-\176' 'Z' < cordata8.txt > cordata8_strip.txt
tr -c '\11\12\15\40-\176' 'Z' < cordata9.txt > cordata9_strip.txt
```



Run python script, which:

- Parses the fields
- Concatenate the separate files
- Output tab-delimited format ready to import into postgres

If 9 files, run this 9 times.

```bash
/srv/tools/python/lib/sunbiz_corpdata_processing.py 0
/srv/tools/python/lib/sunbiz_corpdata_processing.py 1
/srv/tools/python/lib/sunbiz_corpdata_processing.py 2
/srv/tools/python/lib/sunbiz_corpdata_processing.py 3
/srv/tools/python/lib/sunbiz_corpdata_processing.py 4
/srv/tools/python/lib/sunbiz_corpdata_processing.py 5
/srv/tools/python/lib/sunbiz_corpdata_processing.py 6
/srv/tools/python/lib/sunbiz_corpdata_processing.py 7
/srv/tools/python/lib/sunbiz_corpdata_processing.py 8
/srv/tools/python/lib/sunbiz_corpdata_processing.py 9

```

Drop and recreate Table

```sql
/* drop the table and recreate it */

DROP TABLE sunbiz_processed;

CREATE TABLE sunbiz_processed
(
  corporate_id text,
  corporate_name text,
  corporate_name2 text,
  corp_status text,
  corp_file_type text,
  corp_add1 text,
  corp_add2 text,
  corp_city text,
  corp_state text,
  corp_zip text,
  mail_add1 text,
  mail_add2 text,
  mail_city text,
  mail_state text,
  mail_zip text,
  ra_name text,
  ra_add1 text,
  ra_city text,
  ra_state text,
  ra_zip text,
  c1_title text,
  c1_name text,
  c1_add1 text,
  c1_city text,
  c1_state text,
  c1_zip text,
  c2_title text,
  c2_name text,
  c2_add1 text,
  c2_city text,
  c2_state text,
  c2_zip text,
  c3_title text,
  c3_name text,
  c3_add1 text,
  c3_city text,
  c3_state text,
  c3_zip text,
  c4_title text,
  c4_name text,
  c4_add1 text,
  c4_city text,
  c4_state text,
  c4_zip text,
  c5_title text,
  c5_name text,
  c5_add1 text,
  c5_city text,
  c5_state text,
  c5_zip text,
  c6_title text,
  c6_name text,
  c6_add1 text,
  c6_city text,
  c6_state text,
  c6_zip text
)
WITH (
  OIDS=FALSE
);
ALTER TABLE sunbiz_processed OWNER TO postgres;
GRANT ALL ON TABLE sunbiz_processed TO postgres;
GRANT SELECT ON TABLE sunbiz_processed TO public;

```

Load the processed text files

```bash
cd /srv/datascrub/21_Misc/Sunbiz/current/Cor

psql -d gisdev -U postgres -c "\copy sunbiz_processed from 'sunbiz_processed_0.txt' with delimiter as E'\t' null as ''"

psql -d gisdev -U postgres -c "\copy sunbiz_processed from 'sunbiz_processed_1.txt' with delimiter as E'\t' null as ''"

psql -d gisdev -U postgres -c "\copy sunbiz_processed from 'sunbiz_processed_2.txt' with delimiter as E'\t' null as ''"

psql -d gisdev -U postgres -c "\copy sunbiz_processed from 'sunbiz_processed_3.txt' with delimiter as E'\t' null as ''"

psql -d gisdev -U postgres -c "\copy sunbiz_processed from 'sunbiz_processed_4.txt' with delimiter as E'\t' null as ''"

psql -d gisdev -U postgres -c "\copy sunbiz_processed from 'sunbiz_processed_5.txt' with delimiter as E'\t' null as ''"

psql -d gisdev -U postgres -c "\copy sunbiz_processed from 'sunbiz_processed_6.txt' with delimiter as E'\t' null as ''"

psql -d gisdev -U postgres -c "\copy sunbiz_processed from 'sunbiz_processed_7.txt' with delimiter as E'\t' null as ''"

psql -d gisdev -U postgres -c "\copy sunbiz_processed from 'sunbiz_processed_8.txt' with delimiter as E'\t' null as ''"

psql -d gisdev -U postgres -c "\copy sunbiz_processed from 'sunbiz_processed_9.txt' with delimiter as E'\t' null as ''"

```

Create Indexes

```sql
-- Index: idx_sunbiz_corp_name

-- DROP INDEX idx_sunbiz_corp_name;

CREATE INDEX idx_sunbiz_corp_name
  ON sunbiz_processed
  USING btree
  (corporate_name);

-- Index: idx_sunbiz_corp_name2

-- DROP INDEX idx_sunbiz_corp_name2;

CREATE INDEX idx_sunbiz_corp_name2
  ON sunbiz_processed
  USING btree
  (corporate_name2);

-- Index: idx_sunbiz_proc_corp_id

-- DROP INDEX idx_sunbiz_proc_corp_id;

CREATE INDEX idx_sunbiz_proc_corp_id
  ON sunbiz_processed
  USING btree
  (corporate_id);

```



OPTIONAL STEPS / Do this IF / WHEN we host this table on PROD

## STEP: Create Backup File

```bash
# Table is only used in DEV, for now.

# pg_dump --port 5432 --username postgres --format custom --verbose --file "/var/www/apps/mapwise/htdocs/x342/sunbiz_processed.backup" --table "sunbiz_processed" gisdev
```



## STEP: Copy Backup Files to PROD

```bash
# Table is only used in DEV, for now.
```

## STEP: Update PROD Server

```bash
# Table is only used in DEV, for now.
```

