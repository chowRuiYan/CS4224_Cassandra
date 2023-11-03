-- Access cs4224_keyspace
CREATE  KEYSPACE IF NOT EXISTS cs4224_keyspace
WITH REPLICATION = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 3
};

USE cs4224_keyspace;

-- Drop all
DROP TYPE IF EXISTS orderline;

DROP TABLE IF EXISTS warehouse;
DROP TABLE IF EXISTS district;
DROP TABLE IF EXISTS info_for_new_order;
DROP TABLE IF EXISTS stock_by_item;
DROP TABLE IF EXISTS customer_by_wd;
DROP TABLE IF EXISTS order_by_wd;
DROP TABLE IF EXISTS order_by_customer;

DROP MATERIALIZED VIEW IF EXISTS order_by_oid;
DROP MATERIALIZED VIEW IF EXISTS customer_by_balance;

-- Create Types
CREATE TYPE orderline (
  OL_NUMBER int,
  I_ID int,
  I_NAME text,
  OL_AMOUNT decimal,
  OL_SUPPLY_W_ID int,
  OL_QUANTITY decimal,
);

-- Create Baseline Tables
CREATE TABLE IF NOT EXISTS warehouse (
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

CREATE TABLE IF NOT EXISTS district (
  W_ID int,
  D_ID int,
  D_NAME text,
  D_STREET_1 text,
  D_STREET_2 text,
  D_CITY text,
  D_STATE text,
  D_ZIP text,
  D_TAX decimal,
  D_YTD decimal,
  PRIMARY KEY ((W_ID, D_ID))
);

-- Create Custom Tables
CREATE TABLE IF NOT EXISTS info_for_new_order (
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

CREATE TABLE IF NOT EXISTS stock_by_item (
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

CREATE TABLE IF NOT EXISTS customer_by_wd (
  W_ID int,
  D_ID int,
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
  C_YTD_PAYMENT decimal,
  C_PAYMENT_CNT int,
  C_DELIVERY_CNT int,
  PRIMARY KEY ((W_ID, D_ID), C_ID)
);

CREATE TABLE IF NOT EXISTS order_by_wd (
  W_ID int,
  D_ID int,
  C_ID int,
  O_ID int,
  O_ENTRY_D timestamp,
  O_CARRIER_ID int,
  O_AMOUNT decimal,
  O_OL_CNT decimal,
  ORDERLINES list<frozen<orderline>>,
  PRIMARY KEY ((W_ID, D_ID), O_ID)
)
WITH CLUSTERING ORDER BY (O_ID ASC);

CREATE TABLE IF NOT EXISTS order_by_customer (
  W_ID int,
  D_ID int,
  C_ID int,
  O_ID int,
  O_ENTRY_D timestamp,
  O_CARRIER_ID int,
  O_DELIVERY_D timestamp,
  ORDERLINES list<frozen<orderline>>,
  O_I_ID_LIST list<int>,
  PRIMARY KEY ((W_ID, D_ID, C_ID), O_ID)
)
WITH CLUSTERING ORDER BY (O_ID DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS order_by_oid AS
SELECT * FROM order_by_wd
WHERE W_ID IS NOT NULL AND D_ID IS NOT NULL AND O_ID IS NOT NULL
PRIMARY KEY ((W_ID, D_ID), O_ID)
WITH CLUSTERING ORDER BY (O_ID DESC);
	

CREATE MATERIALIZED VIEW IF NOT EXISTS customer_by_balance AS
SELECT C_BALANCE, W_ID, D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST
FROM customer_by_wd
WHERE C_BALANCE IS NOT NULL AND W_ID IS NOT NULL AND D_ID IS NOT NULL AND C_ID IS NOT NULL
PRIMARY KEY ((W_ID, D_ID), C_BALANCE, C_ID)
WITH CLUSTERING ORDER BY (C_BALANCE DESC, C_ID ASC);