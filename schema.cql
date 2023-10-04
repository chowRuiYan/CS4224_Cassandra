-- create your own tables here

DROP KEYSPACE IF EXISTS wholesale;
CREATE KEYSPACE wholesale WITH replication =
  {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wholesale;

-- warehouse
-- district
-- customer_by_wd (shared by 2 & 3)
-- order_by_wd
-- orderline_by_order

-- potential optimisation: can we do away with orderlines table? t3 only needs aggregation of ol_amount, can sum of ol_amount be stored in orders table instead
-- potential optimisation: do we need to store both d_ytd in district table and w_ytd in warehouse table? need to check other transactions

DROP TABLE IF EXISTS warehouse;
CREATE TABLE IF NOT EXISTS warehouse (
  w_id int,
--  w_name text,
  w_street_1 text,
  w_street_2 text,
  w_city text,
  w_state text,
  w_zip text,
--  w_tax decimal,
  w_ytd decimal,
  PRIMARY KEY (W_ID)
);

DROP TABLE IF EXISTS district;
CREATE TABLE IF NOT EXISTS district (
  d_w_id int,
  d_id int,
--  d_name text,
  d_street_1 text,
  d_street_2 text,
  d_city text,
  d_state text,
  d_zip text,
--  d_tax decimal,
  d_ytd decimal,
--  d_next_o_id int,
  PRIMARY KEY (d_w_id, d_id)
--  FOREIGN KEY (d_w_id) REFERENCES warehouse(w_id)
);

DROP TABLE IF EXISTS customer_by_wd;
CREATE TABLE IF NOT EXISTS customer_by_wd (
  c_w_id int,
  c_d_id int,
  c_id int,
  c_first text,
  c_middle text,
  c_last text,
  c_street_1 text,
  c_street_2 text,
  c_city text,
  c_state text,
  c_zip text,
  c_phone text,
  c_since timestamp,
  c_credit text,
  c_credit_lim decimal,
  c_discount decimal,
  c_balance decimal,
  c_ytd_payment float,
  c_payment_cnt int,
  c_delivery_cnt int,
--  c_data text,
  PRIMARY KEY (c_w_id, c_d_id, c_id)
--  FOREIGN KEY (c_w_id, c_d_id) REFERENCES district(d_w_id, d_id)
);

DROP TABLE IF EXISTS order_by_wd;
CREATE TABLE IF NOT EXISTS order_by_wd (
  o_w_id int,
  o_d_id int,
  o_id int,
  o_c_id int,
  o_carrier_id int,
  ol_delivery_d timestamp, -- from original orderlines table
--  o_ol_cnt decimal,
--  o_all_local decimal,
--  o_entry_d timestamp,
  PRIMARY KEY (o_w_id, o_d_id, o_id)
--  FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES customer_by_wd(c_w_id, c_d_id, c_id),
--  CHECK (o_carrier_id BETWEEN 1 AND 10)
);

DROP TABLE IF EXISTS orderline_by_order;
CREATE TABLE IF NOT EXISTS orderline_by_order (
  ol_w_id int,
  ol_d_id int,
  ol_o_id int,
--  ol_number int,
--  ol_i_id int,
  ol_amount decimal,
--  ol_supply_w_id int,
--  ol_quantity decimal,
--  ol_dist_info text,
  PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
--  FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) REFERENCES order_by_wd(o_w_id, o_d_id, o_id)
--  FOREIGN KEY (ol_i_id) REFERENCES item(i_id)
-- Note: An order-line is classified as a home order-line if OL SUPPLY W ID = OL W ID; otherwise, it it classified as a remote order-line. An order’s O ALL LOCAL is set to true if and only if all its order-lines are home order-lines.
);



-- DROP TABLE IF EXISTS warehouses;
-- CREATE TABLE IF NOT EXISTS warehouses (
--   W_ID int,
--   W_NAME text,
--   W_STREET_1 text,
--   W_STREET_2 text,
--   W_CITY text,
--   W_STATE text,
--   W_ZIP text,
--   W_TAX decimal,
--   W_YTD decimal,
--   PRIMARY KEY (W_ID)
-- );

-- DROP TABLE IF EXISTS districts;
-- CREATE TABLE IF NOT EXISTS districts (
--   D_W_ID int,
--   D_ID int,
--   D_NAME text,
--   D_STREET_1 text,
--   D_STREET_2 text,
--   D_CITY text,
--   D_STATE text,
--   D_ZIP text,
--   D_TAX decimal,
--   D_YTD decimal,
--   D_NEXT_O_ID int,
--   PRIMARY KEY ((D_W_ID, D_ID))
-- );

-- DROP TABLE IF EXISTS customers;
-- CREATE TABLE IF NOT EXISTS customers (
--   C_W_ID int,
--   C_D_ID int,
--   C_ID int,
--   C_FIRST text,
--   C_MIDDLE text,
--   C_LAST text,
--   C_STREET_1 text,
--   C_STREET_2 text,
--   C_CITY text,
--   C_STATE text,
--   C_ZIP text,
--   C_PHONE text,
--   C_SINCE timestamp,
--   C_CREDIT text,
--   C_CREDIT_LIMIT decimal,
--   C_DISCOUNT decimal,
--   C_BALANCE decimal,
--   C_YTD_PAYMENT float,
--   C_PAYMENT_CNT int,
--   C_DELIVERY_CNT int,
--   C_DATA text,
--   PRIMARY KEY ((C_W_ID, C_D_ID, C_ID))
-- );

-- DROP TABLE IF EXISTS orders;
-- CREATE TABLE IF NOT EXISTS orders (
--   O_W_ID int,
--   O_D_ID int,
--   O_ID int,
--   O_C_ID int,
--   O_CARRIER_ID int,
--   O_OL_CNT decimal,
--   O_ALL_LOCAL decimal,
--   O_ENTRY_D timestamp,
--   PRIMARY KEY ((O_W_ID, O_D_ID, O_ID))
-- );

-- DROP TABLE IF EXISTS items;
-- CREATE TABLE IF NOT EXISTS items (
--   I_ID int,
--   I_NAME text,
--   I_PRICE decimal,
--   I_IM_ID int,
--   I_DATA text,
--   PRIMARY KEY (I_ID)
-- );

-- DROP TABLE IF EXISTS order_lines;
-- CREATE TABLE IF NOT EXISTS order_lines (
--   OL_W_ID int,
--   OL_D_ID int,
--   OL_O_ID int,
--   OL_NUMBER int,
--   OL_I_ID int,
--   OL_DELIVERY_D timestamp,
--   OL_AMOUNT decimal,
--   OL_SUPPLY_W_ID int,
--   OL_QUANTITY decimal,
--   OL_DIST_INFO text,
--   PRIMARY KEY ((OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER))
-- );

-- DROP TABLE IF EXISTS stocks;
-- CREATE TABLE IF NOT EXISTS stocks (
--   S_W_ID int,
--   S_I_ID int,
--   S_QUANTITY decimal,
--   S_YTD decimal,
--   S_ORDER_CNT int,
--   S_REMOTE_CNT int,
--   S_DIST_01 text,
--   S_DIST_02 text,
--   S_DIST_03 text,
--   S_DIST_04 text,
--   S_DIST_05 text,
--   S_DIST_06 text,
--   S_DIST_07 text,
--   S_DIST_08 text,
--   S_DIST_09 text,
--   S_DIST_10 text,
--   S_DATA text,
--   PRIMARY KEY ((S_W_ID, S_I_ID))
-- );
	
