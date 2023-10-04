-- create your own tables here

DROP KEYSPACE IF EXISTS wholesale;
CREATE KEYSPACE wholesale WITH replication =
  {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wholesale;

DROP TABLE IF EXISTS new_order_by_wd;
CREATE TABLE IF NOT EXISTS new_order_by_wd (
  w_id int,
  d_id int,
  c_id int,
  c_last text,
  c_discount decimal,
  c_credit text,
  next_o_id int static,
  w_tax decimal static,
  d_tax decimal static,
  PRIMARY KEY ((w_id, d_id), c_id)
)

DROP TABLE IF EXISTS stock_by_items;
CREATE TABLE IF NOT EXISTS stock_by_items (
  i_id int,
  w_id int,
  s_quantity decimal,
  s_ytd decimal,
  s_order_cnt int,
  s_remote_cnt int,
  i_name text static,
  i_price decimal static,
  PRIMARY KEY (i_id, w_id)
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
	
-- create xact 6-8 tables
DROP TABLE IF EXISTS xact_six_districts;
CREATE TABLE IF NOT EXISTS xact_six_districts (
   D_W_ID int,
   D_ID int,
   D_NAME text,
   D_STREET_1 text,
   D_STREET_2 text,
   D_CITY text,
   D_STATE text,
   D_ZIP text,
   D_TAX decimal,
   D_YTD decimal,
   D_NEXT_O_ID int,
   PRIMARY KEY ((D_W_ID), D_ID)
);

CREATE TYPE order.item_qty (
  I_NAME text,
  OL_QUANTITY decimal
);

DROP TABLE IF EXISTS xact_six_order_by_warehouse;
CREATE TABLE IF NOT EXISTS xact_six_order_by_warehouse (
   O_W_ID int,
   O_D_ID int,
   O_ID int,
   O_C_ID int,
   O_CARRIER_ID int,
   O_OL_CNT decimal,
   O_ALL_LOCAL decimal,
   O_ENTRY_D timestamp,
   O_ITEM_QTY list<frozen<item_qty>>,
   C_FIRST text,
   C_MIDDLE text,
   C_LAST text,
   PRIMARY KEY ((O_W_ID, O_D_ID), O_ID, O_C_ID)
)
WITH CLUSTERING ORDER BY (O_ID DESC, O_C_ID ASC);

DROP TABLE IF EXISTS xact_seven;
CREATE TABLE IF NOT EXISTS xact_seven (
  C_W_ID int,
  C_D_ID int,
  C_BALANCE int,
  C_ID int,
  C_FIRST VARCHAR(16),
  C_MIDDLE CHAR(2),
  C_LAST VARCHAR(16),
  D_NAME VARCHAR(10),
  W_NAME VARCHAR(10),
  PRIMARY KEY ((C_W_ID, C_D_ID), C_BALANCE, C_ID)
)
WITH CLUSTERING ORDER BY (C_BALANCE DESC, C_ID ASC);

DROP TABLE IF EXISTS order_by_customer_t8;
CREATE TABLE IF NOT EXISTS order_by_customer_t8 (
  O_W_ID int,
  O_D_ID int,
  O_C_ID int,
  O_ID int,
  O_ITEM_QTY list<frozen<item_qty>>,
  PRIMARY KEY ((O_W_ID, O_D_ID, O_C_ID), O_ID)
);