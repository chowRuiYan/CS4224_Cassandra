USE cs4224_keyspace;
COPY order_by_wd("w_id", "d_id", "c_id", "o_id", "o_entry_d", "o_carrier_id", "o_ol_cnt", "o_amount", "orderlines") FROM '/home/stuproj/cs4224p/project_files/data_files/order_by_wd.csv' WITH DELIMITER=',' AND NULL = 'null' AND CHUNKSIZE=500 AND NUMPROCESS=5;
