USE cs4224_keyspace;
COPY stock_by_item("i_id", "w_id", "s_quantity", "s_ytd", "s_order_cnt", "s_remote_cnt", "i_name", "i_price") FROM '/home/stuproj/cs4224p/project_files/data_files/stock_by_item.csv' WITH DELIMITER=',' AND NUMPROCESSES=5;
