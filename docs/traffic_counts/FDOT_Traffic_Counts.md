# FDOT Traffic Counts

## Download / Update Data Catalog

```bash
# Manual download
# Login first at https://ftp.fdot.gov/login
# Username: Guest, no password
# Once logged in, this URL works
# https://guest:@ftp.fdot.gov/file/d/FTP/FDOT/co/planning/transtat/gis/shapefiles/aadt.zip

# Place the downloaded file in this folder
cd /mnt/sdb/datascrub/03_Transportation/traffic_counts/fdot

ll

unzip -o aadt.zip

zip_rename_date.sh aadt.zip

# Show metadata / records
ogrinfo aadt.shp aadt | less
```

## Process Data

```bash
# Create a spatial index for MapServer
shptree aadt.shp

# Review
ll aadt.*
```


## Publish Data

```bash
# Publish to wms1
cd /srv/mapwise/state/vector

cp /mnt/sdb/datascrub/03_Transportation/traffic_counts/fdot/aadt.* .

# Review file dates and manually view in FMO
ll aadt.*

# Publish to mapserver-prod
scp /srv/mapwise/state/vector/aadt.* bmay@157.230.218.18:/srv/mapwise/state/vector

# Publish to mapserver-m1
# Is this necessary? Yes, for failover purposes.
# publickey error
scp /srv/mapwise/state/vector/aadt.* bmay@104.248.122.118:/srv/mapwise/state/vector

# Review file dates and manually view in FMO
```

## Main Server for Layer

```bash
# mapserver-prod is the main source for showing the layer
# mapserver-m1 should be a backup and not the main source for showing the layer
# Verify in browser developer tools Network tab
# 2025-04-16: OK
```

