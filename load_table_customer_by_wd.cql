USE cs4224_keyspace;
COPY customer_by_wd("w_id", "d_id", "c_id", "c_first", "c_middle", "c_last", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip", "c_phone", "c_since", "c_credit", "c_credit_lim", "c_discount", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt") FROM '/home/stuproj/cs4224p/project_files/data_files/customer_by_wd.csv' WITH DELIMITER=',' AND NUMPROCESS=5; 
