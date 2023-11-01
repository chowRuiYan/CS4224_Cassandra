USE cs4224_keyspace;
COPY warehouse("w_id", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip", "w_tax", "w_ytd") FROM '/home/stuproj/cs4224p/project_files/data_files/warehouse.csv' WITH DELIMITER=',' AND NUMPROCESS=5;
