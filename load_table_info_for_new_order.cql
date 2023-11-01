USE cs4224_keyspace;
COPY info_for_new_order("w_id", "d_id", "c_id", "c_last", "c_discount", "c_credit", "next_o_id", "w_tax", "d_tax") FROM '/home/stuproj/cs4224p/project_files/data_files/info_for_new_order.csv' WITH DELIMITER=',' AND NUMPROCESSES=5;
