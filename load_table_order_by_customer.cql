USE cs4224_keyspace;
COPY order_by_customer("w_id", "d_id", "c_id", "o_id", "o_entry_d", "o_carrier_id", "o_delivery_d", "orderlines", "o_i_id_list") FROM '/home/stuproj/cs4224p/project_files/data_files/order_by_customer.csv' WITH DELIMITER=',' AND NULL = 'null' AND CHUNKSIZE=500 AND NUMPROCESSES=5;
