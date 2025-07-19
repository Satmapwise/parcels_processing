pg_restore -h -U postgres -d gislib -v "/home/bmay/incoming/raw_zon_hillsborough_plant_city.backup"
psql -p 5432 -U postgres -d gislib -c "DELETE FROM zoning WHERE city_name = 'PLANT_CITY' and county_name = 'HILLSBOROUGH';"
psql -d gislib -U postgres -p 5432 -c "INSERT INTO zoning (zon_code,zon_code2,zon_desc,zon_gen,ord_num,city_name,county_name,notes,the_geom) SELECT pczoning,Null,Null,Null,Null,'PLANT_CITY','HILLSBOROUGH',Null, wkb_geometry FROM raw_zon_hillsborough_plant_city_2;"
psql -d gislib -U postgres -p 5432 -c "DROP TABLE raw_zon_hillsborough_plant_city_2;"
