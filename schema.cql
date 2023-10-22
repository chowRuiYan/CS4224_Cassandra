-- create your own tables here

-- xact2&3
-- potential optimisation: can we do away with orderlines table? t3 only needs aggregation of ol_amount, can sum of ol_amount be stored in orders table instead
-- potential optimisation: do we need to store both d_ytd in district table and w_ytd in warehouse table? need to check other transactions

-- Access cs4224_keyspace
CREATE KEYSPACE IF NOT EXISTS cs4224_keyspace 
    WITH REPLICATION = { 
        'class' : 'SimpleStrategy',
        'replication_factor' : 1 
    };
USE cs4224_keyspace;

-- Drop all
DROP TABLE IF EXISTS warehouses;
DROP TABLE IF EXISTS districts;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS items;
DROP TABLE IF EXISTS order_lines;
DROP TABLE IF EXISTS stocks;

DROP TABLE IF EXISTS new_order_by_wd;
DROP TABLE IF EXISTS orders_by_customers;
DROP TABLE IF EXISTS orders_by_warehouse;
DROP TABLE IF EXISTS customer_by_wd;
DROP TABLE IF EXISTS order_by_wd;
DROP TABLE IF EXISTS orderline_by_order;
DROP TABLE IF EXISTS stock_by_items;
DROP TABLE IF EXISTS xact_six_order_by_warehouse;
DROP TABLE IF EXISTS xact_seven;
DROP TABLE IF EXISTS order_by_customer_t8;
DROP TYPE IF EXISTS item_qty;
DROP TYPE IF EXISTS order_items;

-- Create Types
CREATE TYPE item_qty (
  I_NAME text,
  OL_QUANTITY decimal
);

CREATE TYPE order_items (
  OL_NUMBER int,
  OL_I_ID int,
  OL_AMOUNT decimal,
  OL_SUPPLY_W_ID int,
  OL_QUANTITY decimal,
);

-- Create Baseline Tables
CREATE TABLE IF NOT EXISTS warehouses (
  W_ID int,
  W_NAME text,
  W_STREET_1 text,
  W_STREET_2 text,
  W_CITY text,
  W_STATE text,
  W_ZIP text,
  W_TAX decimal,
  W_YTD decimal,
  PRIMARY KEY (W_ID)
);

CREATE TABLE IF NOT EXISTS districts (
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
  PRIMARY KEY ((D_W_ID, D_ID))
);

CREATE TABLE IF NOT EXISTS customers (
  C_W_ID int,
  C_D_ID int,
  C_ID int,
  C_FIRST text,
  C_MIDDLE text,
  C_LAST text,
  C_STREET_1 text,
  C_STREET_2 text,
  C_CITY text,
  C_STATE text,
  C_ZIP text,
  C_PHONE text,
  C_SINCE timestamp,
  C_CREDIT text,
  C_CREDIT_LIM decimal,
  C_DISCOUNT decimal,
  C_BALANCE decimal,
  C_YTD_PAYMENT float,
  C_PAYMENT_CNT int,
  C_DELIVERY_CNT int,
  C_DATA text,
  PRIMARY KEY ((C_W_ID, C_D_ID, C_ID))
);

CREATE TABLE IF NOT EXISTS orders (
  O_W_ID int,
  O_D_ID int,
  O_ID int,
  O_C_ID int,
  O_CARRIER_ID int,
  O_OL_CNT decimal,
  O_ALL_LOCAL decimal,
  O_ENTRY_D timestamp,
  PRIMARY KEY ((O_W_ID, O_D_ID, O_ID))
);

CREATE TABLE IF NOT EXISTS items (
  I_ID int,
  I_NAME text,
  I_PRICE decimal,
  I_IM_ID int,
  I_DATA text,
  PRIMARY KEY (I_ID)
);

CREATE TABLE IF NOT EXISTS order_lines (
  OL_W_ID int,
  OL_D_ID int,
  OL_O_ID int,
  OL_NUMBER int,
  OL_I_ID int,
  OL_DELIVERY_D timestamp,
  OL_AMOUNT decimal,
  OL_SUPPLY_W_ID int,
  OL_QUANTITY decimal,
  OL_DIST_INFO text,
  PRIMARY KEY ((OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER))
);

CREATE TABLE IF NOT EXISTS stocks (
  S_W_ID int,
  S_I_ID int,
  S_QUANTITY decimal,
  S_YTD decimal,
  S_ORDER_CNT int,
  S_REMOTE_CNT int,
  S_DIST_01 text,
  S_DIST_02 text,
  S_DIST_03 text,
  S_DIST_04 text,
  S_DIST_05 text,
  S_DIST_06 text,
  S_DIST_07 text,
  S_DIST_08 text,
  S_DIST_09 text,
  S_DIST_10 text,
  S_DATA text,
  PRIMARY KEY ((S_W_ID, S_I_ID))
);

-- Create Custom Tables
CREATE TABLE IF NOT EXISTS new_order_by_wd (
  W_ID int,
  D_ID int,
  C_ID int,
  C_LAST text,
  C_DISCOUNT decimal,
  C_CREDIT text,
  NEXT_O_ID int static,
  W_TAX decimal static,
  D_TAX decimal static,
  PRIMARY KEY ((W_ID, D_ID), C_ID)
);

CREATE TABLE IF NOT EXISTS orders_by_customers (
  O_W_ID int,
  O_D_ID int,
  O_ID int,
  O_C_ID int,
  O_C_FIRST text,
  O_C_MIDDLE text,
  O_C_LAST text,
  O_C_BALANCE decimal,
  O_CARRIER_ID int,
  O_ENTRY_D timestamp,
  OL_DELIVERY_D timestamp,
  ORDER_ITEMS list<frozen<order_items>>,
  PRIMARY KEY ((O_W_ID, O_D_ID), O_C_ID, O_ID)
)
WITH CLUSTERING ORDER BY (O_C_ID ASC, O_ID DESC);

CREATE TABLE IF NOT EXISTS orders_by_warehouse (
  O_W_ID int,
  O_D_ID int,
  O_ID int,
  ORDER_ITEMS list<frozen<order_items>>,
  PRIMARY KEY (O_D_ID, O_W_ID, O_ID)
)
WITH CLUSTERING ORDER BY (O_W_ID DESC, O_ID DESC);

CREATE TABLE IF NOT EXISTS customer_by_wd (
  C_W_ID int,
  C_D_ID int,
  C_ID int,
  C_FIRST text,
  C_MIDDLE text,
  C_LAST text,
  C_STREET_1 text,
  C_STREET_2 text,
  C_CITY text,
  C_STATE text,
  C_ZIP text,
  C_PHONE text,
  C_SINCE timestamp,
  C_CREDIT text,
  C_CREDIT_LIMIT decimal,
  C_DISCOUNT decimal,
  C_BALANCE decimal,
  C_YTD_PAYMENT float,
  C_PAYMENT_CNT int,
  C_DELIVERY_CNT int,
  C_DATA text,
  PRIMARY KEY (C_W_ID, C_D_ID, C_ID)
);

--  FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES customer_by_wd(c_w_id, c_d_id, c_id),
--  CHECK (o_carrier_id BETWEEN 1 AND 10)
CREATE TABLE IF NOT EXISTS order_by_wd (
  O_W_ID int,
  O_D_ID int,
  O_ID int,
  O_C_ID int,
  O_CARRIER_ID int,
  OL_DELIVERY_D timestamp, -- from original orderlines table
  O_OL_CNT decimal,
  O_ALL_LOCAL decimal,
  O_ENTRY_D timestamp,
  PRIMARY KEY (O_W_ID, O_D_ID, O_ID)
);

-- Note: An order-line is classified as a home order-line if OL SUPPLY W ID = OL W ID; otherwise, it it classified as a remote order-line. 
-- An order’s O ALL LOCAL is set to true if and only if all its order-lines are home order-lines.
-- FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) REFERENCES order_by_wd(o_w_id, o_d_id, o_id)
-- FOREIGN KEY (ol_i_id) REFERENCES item(i_id)
CREATE TABLE IF NOT EXISTS orderline_by_order (
  OL_W_ID int,
  OL_D_ID int,
  OL_O_ID int,
  OL_NUMBER int,
  OL_I_ID int,
  OL_AMOUNT decimal,
  OL_SUPPLY_W_ID int,
  OL_QUANTITY decimal,
  OL_DIST_INFO text,
  PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
);

CREATE TABLE IF NOT EXISTS stock_by_items (
  I_ID int,
  W_ID int,
  S_QUANTITY decimal,
  S_YTD decimal,
  S_ORDER_CNT int,
  S_REMOTE_CNT int,
  I_NAME text static,
  I_PRICE decimal static,
  PRIMARY KEY (I_ID, W_ID)
);
	
-- create xact 6-8 tables
-- xact six needs to use districts
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

CREATE TABLE IF NOT EXISTS xact_seven (
  C_W_ID int,
  C_D_ID int,
  C_BALANCE int,
  C_ID int,
  C_FIRST text,
  C_MIDDLE text,
  C_LAST text,
  D_NAME text,
  W_NAME text,
  PRIMARY KEY ((C_W_ID, C_D_ID), C_BALANCE, C_ID)
)
WITH CLUSTERING ORDER BY (C_BALANCE DESC, C_ID ASC);

CREATE TABLE IF NOT EXISTS order_by_customer_t8 (
  O_W_ID int,
  O_D_ID int,
  O_C_ID int,
  O_ID int,
  O_ITEM_QTY list<frozen<item_qty>>,
  PRIMARY KEY ((O_W_ID, O_D_ID, O_C_ID), O_ID)
);