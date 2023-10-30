-- load database from the respective csv files

-- Access cs4224 keyspace
CREATE  KEYSPACE IF NOT EXISTS cs4224_keyspace 
    WITH REPLICATION = { 
        'class' : 'SimpleStrategy',
        'replication_factor' : 1 
    };
USE cs4224_keyspace;

-- import data into tables
COPY order_by_wd("w_id", "d_id", "o_id", "c_id", "o_carrier_id", "o_ol_cnt", "o_amount", "orderlines") FROM '/home/CS4224_Cassandra/project_files/data_files/order_by_wd.csv' WITH DELIMITER=',' AND NULL = 'null' AND CHUNKSIZE=500;
COPY order_by_customer("w_id", "d_id", "c_id", "o_id", "o_entry_d", "o_carrier_id", "o_delivery_d", "orderlines", "o_i_id_list") FROM '/home/CS4224_Cassandra/project_files/data_files/order_by_customer.csv' WITH DELIMITER=',' AND NULL = 'null' AND CHUNKSIZE=500;
COPY warehouse("w_id", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip", "w_tax", "w_ytd") FROM '/home/CS4224_Cassandra/project_files/data_files/warehouse.csv' WITH DELIMITER=',';
COPY district("w_id", "d_id", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip", "d_tax", "d_ytd") FROM '/home/CS4224_Cassandra/project_files/data_files/district1.csv' WITH DELIMITER=',';
COPY info_for_new_order("w_id", "d_id", "c_id", "c_last", "c_discount", "c_credit", "next_o_id", "w_tax", "d_tax") FROM '/home/CS4224_Cassandra/project_files/data_files/info_for_new_order.csv' WITH DELIMITER=',';
COPY stock_by_item("i_id", "w_id", "s_quantity", "s_ytd", "s_order_cnt", "s_remote_cnt", "i_name", "i_price") FROM '/home/CS4224_Cassandra/project_files/data_files/stock_by_item.csv' WITH DELIMITER=',';
COPY customer_by_wd("w_id", "d_id", "c_id", "c_first", "c_middle", "c_last", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip", "c_phone", "c_since", "c_credit", "c_credit_lim", "c_discount", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt") FROM '/home/CS4224_Cassandra/project_files/data_files/customer_by_wd.csv' WITH DELIMITER=','; 
