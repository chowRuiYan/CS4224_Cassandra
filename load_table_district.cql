USE cs4224_keyspace;
COPY district("w_id", "d_id", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip", "d_tax", "d_ytd") FROM '/home/stuproj/cs4224p/project_files/data_files/district1.csv' WITH DELIMITER=',' AND NUMPROCESS=5;
