import psycopg2

# Replace these with your Citus database connection details
db_host = "localhost"
db_name = "testDB"
db_user = "name"
db_password = "password"

# Create a connection to the Citus database
connection = psycopg2.connect(
    host=db_host,
    database=db_name,
    user=db_user,
    password=db_password
)

# Create a cursor object to interact with the database
cursor = connection.cursor()

# Create transaction procedures

# Xact 1
print("Create procedure for Transaction 1: New Order")
cursor.execute("""
DROP TYPE IF EXISTS new_order_init_type CASCADE;
CREATE TYPE new_order_init_type AS (
    N INT,
    C_LAST VARCHAR(16),
    C_CREDIT VARCHAR(2),
    C_DISCOUNT DECIMAL(5,4),
    W_TAX DECIMAL(4,4),
    D_TAX DECIMAL(4,4)
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION new_order_init(
    IN_W_ID INT,
    IN_D_ID INT,
    IN_C_ID INT,
    IN_NUM_ITEMS INT
)
RETURNS new_order_init_type
AS $$
DECLARE 
N INT;
GET_D_TAX DECIMAL(4,4);
GET_W_TAX DECIMAL(4,4);
GET_C_DISCOUNT DECIMAL(5,4);
GET_C_LAST VARCHAR(16);
GET_C_CREDIT VARCHAR(2);
BEGIN
SELECT d.d_next_o_id INTO N
FROM district d
WHERE d.d_w_id = IN_W_ID
    AND d.d_id = IN_D_ID;
UPDATE district
SET d_next_o_id = d_next_o_id + 1
WHERE d_w_id = IN_W_ID
    AND d_id = IN_D_ID;
SELECT d_tax INTO GET_D_TAX
FROM district
WHERE d_w_id = IN_W_ID
    AND d_id = IN_D_ID;
SELECT w_tax INTO GET_W_TAX
FROM warehouse 
WHERE w_id = IN_W_ID;
SELECT c_last INTO GET_C_LAST
FROM customer
WHERE c_id = IN_C_ID
AND c_w_id = IN_W_ID
AND c_d_id = IN_D_ID;
SELECT c_discount INTO GET_C_DISCOUNT
FROM customer
WHERE c_id = IN_C_ID
AND c_w_id = IN_W_ID
AND c_d_id = IN_D_ID;
SELECT c_credit INTO GET_C_CREDIT
FROM customer
WHERE c_id = IN_C_ID
AND c_w_id = IN_W_ID
AND c_d_id = IN_D_ID;
INSERT INTO orders(
    o_id,
    o_d_id,
    o_w_id,
    o_c_id,
    o_entry_d,
    o_carrier_id,
    o_ol_cnt,
    o_all_local
) VALUES(
    N,
    IN_D_ID,
    IN_W_ID,
    IN_C_ID,
    CURRENT_TIMESTAMP,
    NULL,
    IN_NUM_ITEMS,
    1);
RETURN (N, GET_C_LAST, GET_C_CREDIT, GET_C_DISCOUNT, GET_W_TAX, GET_D_TAX);
END;
$$ LANGUAGE plpgsql;
""")

cursor.execute("""
DROP TYPE IF EXISTS new_order_add_orderline_type CASCADE;
CREATE TYPE new_order_add_orderline_type AS (
    I_NAME VARCHAR(24),
    OL_AMOUNT DECIMAL,
    OL_SUPPLY_W_ID INT,
    OL_QUANTITY INT,
    S_QUANTITY INT
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION new_order_add_orderline(
    IN_W_ID INT,
    IN_D_ID INT,
    IN_C_ID INT,
    N INT, 
    IN_OL_NUMBER INT,
    IN_OL_I_ID INT,
    IN_OL_SUPPLY_W_ID INT,
    IN_OL_QUANTITY INT
)
RETURNS new_order_add_orderline_type
AS $$
DECLARE 
ADJUSTED_S_QUANTITY INT;
RETRIEVED_S_QUANTITY INT;
RETRIEVED_I_PRICE DECIMAL(5, 2);
ITEM_AMOUNT DECIMAL;
ITEM_NAME VARCHAR(24);
FORMATTED_DIST_INFO VARCHAR;
BEGIN
IF IN_OL_SUPPLY_W_ID <> IN_W_ID THEN
UPDATE orders
SET o_all_local = 0
WHERE o_w_id = IN_W_ID
AND o_d_id = IN_D_ID
AND o_c_id = IN_C_ID;
END IF;
FORMATTED_DIST_INFO := 'S_DIST_' || IN_D_ID;
SELECT s.s_quantity INTO RETRIEVED_S_QUANTITY
FROM stock s
WHERE s.s_w_id = IN_OL_SUPPLY_W_ID
    AND s.s_i_id = IN_OL_I_ID;
ADJUSTED_S_QUANTITY := RETRIEVED_S_QUANTITY - IN_OL_QUANTITY;
IF ADJUSTED_S_QUANTITY < 10 THEN 
ADJUSTED_S_QUANTITY := ADJUSTED_S_QUANTITY + 100;
END IF;
UPDATE stock 
SET s_quantity = ADJUSTED_S_QUANTITY,
    s_ytd = s_ytd + IN_OL_QUANTITY,
    s_order_cnt = s_order_cnt + 1
WHERE s_w_id = IN_OL_SUPPLY_W_ID
    AND s_i_id = IN_OL_I_ID;
IF IN_OL_SUPPLY_W_ID <> IN_W_ID THEN
UPDATE stock
SET s_remote_cnt = s_remote_cnt + 1
WHERE s_w_id = IN_OL_SUPPLY_W_ID
    AND s_i_id = IN_OL_I_ID;
END IF;
SELECT i_price INTO RETRIEVED_I_PRICE
FROM item i
WHERE i.i_id = IN_OL_I_ID;
SELECT i_name INTO ITEM_NAME
FROM item i
WHERE i.i_id = IN_OL_I_ID;
ITEM_AMOUNT := IN_OL_QUANTITY * RETRIEVED_I_PRICE;
INSERT INTO order_lines(
        ol_o_id,
        ol_d_id,
        ol_w_id,
        ol_number,
        ol_i_id,
        ol_supply_w_id,
        ol_quantity,
        ol_amount,
        ol_delivery_d,
        ol_dist_info
    )
VALUES(
        N,
        IN_D_ID,
        IN_W_ID,
        IN_OL_NUMBER,
        IN_OL_I_ID,
        IN_OL_SUPPLY_W_ID,
        IN_OL_QUANTITY,
        ITEM_AMOUNT,
        NULL,
        FORMATTED_DIST_INFO
);
RETURN (ITEM_NAME, ITEM_AMOUNT, IN_OL_SUPPLY_W_ID, IN_OL_QUANTITY, ADJUSTED_S_QUANTITY);
END;
$$ LANGUAGE plpgsql;
""")
print("Completed procedure for Transaction 1")

# Xact 2
print("Create procedure for Transaction 2: Payment")
cursor.execute("""
DROP TYPE IF EXISTS payment_customer CASCADE;
CREATE TYPE payment_customer AS (
    C_FIRST VARCHAR,
    C_MIDDLE CHAR,
    C_LAST VARCHAR,
    C_STREET_1 VARCHAR,
    C_STREET_2 VARCHAR,
    C_CITY VARCHAR,
    C_STATE CHAR,
    C_ZIP CHAR,
    C_PHONE CHAR,
    C_CREDIT CHAR,
    C_CREDIT_LIMIT DECIMAL,
    C_DISCOUNT DECIMAL
);
""")
cursor.execute("""
DROP TYPE IF EXISTS payment_warehouse CASCADE;
CREATE TYPE payment_warehouse AS (
    W_STREET_1 VARCHAR,
    W_STREET_2 VARCHAR,
    W_CITY VARCHAR,
    W_STATE CHAR,
    W_ZIP CHAR
);
""")
cursor.execute("""
DROP TYPE IF EXISTS payment_district CASCADE;
CREATE TYPE payment_district AS (
    D_STREET_1 VARCHAR,
    D_STREET_2 VARCHAR,
    D_CITY VARCHAR,
    D_STATE CHAR,
    D_ZIP CHAR
);
""")
cursor.execute("""
DROP TYPE IF EXISTS payment_type CASCADE;
CREATE TYPE payment_type AS (
    CUSTOMER_DETAIL payment_customer,
    WAREHOUSE_DETAIL payment_warehouse,
    DISTRICT_DETAIL payment_district
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION payment(IN_C_W_ID INT, IN_C_D_ID INT, IN_C_ID INT, PAYMENT NUMERIC) 
RETURNS payment_type
AS $$ 
DECLARE
CUSTOMER_DETAIL payment_customer;
WAREHOUSE_DETAIL payment_warehouse;
DISTRICT_DETAIL payment_district;
BEGIN
UPDATE warehouse
SET w_ytd = w_ytd + PAYMENT
WHERE w_id = IN_C_W_ID;
UPDATE district 
SET d_ytd = d_ytd + PAYMENT
WHERE d_w_id = IN_C_W_ID
    AND d_id = IN_C_D_ID;
UPDATE customer
SET c_balance = c_balance - PAYMENT,
    c_ytd_payment = c_ytd_payment - PAYMENT,
    c_payment_cnt = c_payment_cnt + 1
WHERE c_w_id = IN_C_W_ID
    AND c_d_id = IN_C_D_ID
    AND c_id = IN_C_ID;
SELECT (c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_limit, c_discount) INTO CUSTOMER_DETAIL
FROM customer
WHERE c_w_id = IN_C_W_ID
    AND c_d_id = IN_C_D_ID
    AND c_id = IN_C_ID;
SELECT (d_street_1, d_street_2, d_city, d_state, d_zip) INTO DISTRICT_DETAIL
FROM district
WHERE d_w_id = IN_C_W_ID
AND d_id = IN_C_D_ID;
SELECT (w_street_1, w_street_2, w_city, w_state, w_zip) INTO WAREHOUSE_DETAIL
FROM warehouse
WHERE w_id = IN_C_W_ID;
RETURN (CUSTOMER_DETAIL, WAREHOUSE_DETAIL, DISTRICT_DETAIL);
END;
$$ LANGUAGE plpgsql;              
""")
print("Completed procedure for Transaction 2")

# Xact 3
print("Create procedure for Transaction 3: Delivery")
cursor.execute("""
CREATE OR REPLACE PROCEDURE delivery(W_ID INT, CARRIER_ID INT) AS $$
DECLARE DISTRICT_NO INT;
N INT;
C INT;
B INT;
BEGIN FOR DISTRICT_NO IN 1..10 LOOP
SELECT MIN(o_id) INTO N
FROM orders o
WHERE o_w_id = W_ID
	AND o_d_id = DISTRICT_NO
    AND o_carrier_id = -1;
SELECT o_c_id INTO C
FROM orders
WHERE o_w_id = W_ID
	AND o_d_id = DISTRICT_NO
	AND o_id = N;
UPDATE orders
SET o_carrier_id = CARRIER_ID
WHERE o_w_id = W_ID
	AND o_d_id = DISTRICT_NO
	AND o_id = N;
SELECT SUM(ol_amount) INTO B
FROM order_lines
WHERE ol_w_id = W_ID
	AND ol_d_id = DISTRICT_NO
	AND ol_o_id = N;
UPDATE order_lines
SET ol_delivery_d = CURRENT_TIMESTAMP
WHERE ol_w_id = W_ID
	AND ol_d_id = DISTRICT_NO
	AND ol_o_id = N;
UPDATE customer
SET c_balance = c_balance - B,
    c_delivery_cnt = c_delivery_cnt + 1
WHERE c_id = C;
END LOOP;
END;
$$ LANGUAGE plpgsql;
""")
print("Completed procedure for Transaction 3")

# Xact 4
print("Create functions for Transaction 4: Order Status")

cursor.execute("""
DROP TYPE IF EXISTS order_status_1_type CASCADE;
CREATE TYPE order_status_1_type AS (
    c_first VARCHAR(16),
    c_middle CHAR(2),
    c_last VARCHAR(16),
    c_balance DECIMAL(12, 2)
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION order_status_1(W_ID INT, D_ID INT, CUST_ID INT) RETURNS SETOF order_status_1_type AS $$
BEGIN
RETURN QUERY
SELECT c_first,
    c_middle,
    c_last,
    c_balance
FROM customer
WHERE c_w_id = W_ID
    AND c_d_id = D_ID
    AND c_id = CUST_ID;
END;
$$ LANGUAGE plpgsql;
""")

cursor.execute("""
DROP TYPE IF EXISTS order_status_2_type CASCADE;
CREATE TYPE order_status_2_type AS (
    o_id INT,
    o_entry_d TIMESTAMP,
    o_carrier_id INT,
    ol_i_id INT,
    ol_supply_w_id INT,
    ol_quantity DECIMAL(2, 0),
    ol_amount DECIMAL(7, 2),
    ol_delivery_d TIMESTAMP
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION order_status_2(C_W_ID INT, C_D_ID INT, C_ID INT) RETURNS SETOF order_status_2_type AS $$
DECLARE LAST_ORDER_ID INT;
LAST_ORDER_ENTRY_D TIMESTAMP;
LAST_ORDER_CARRIER_ID INT;
BEGIN
SELECT MAX(o_id) INTO LAST_ORDER_ID
FROM orders
WHERE o_w_id = C_W_ID
    AND o_d_id = C_D_ID
    AND o_c_id = C_ID;
SELECT o_entry_d, o_carrier_id
INTO LAST_ORDER_ENTRY_D, LAST_ORDER_CARRIER_ID
FROM orders
WHERE o_w_id = C_W_ID
    AND o_d_id = C_D_ID
    AND o_id = LAST_ORDER_ID;
RETURN QUERY
SELECT LAST_ORDER_ID,
    LAST_ORDER_ENTRY_D,
    LAST_ORDER_CARRIER_ID,
    ol_i_id,
    ol_supply_w_id,
    ol_quantity,
    ol_amount,
    ol_delivery_d
FROM order_lines
WHERE ol_w_id = C_W_ID
    AND ol_d_id = C_D_ID
    AND ol_o_id = LAST_ORDER_ID;
END;
$$ LANGUAGE plpgsql;
""")
print("Completed procedure for Transaction 4")

# Xact 5
print("Create function for Transaction 5: Stock Level")
cursor.execute("""
CREATE OR REPLACE FUNCTION stock_level(W_ID INT, D_ID INT, T INT, L INT) RETURNS INT AS $$
DECLARE N INT;
RESULT INT;
BEGIN
SELECT 1+MAX(o_id) INTO N
FROM orders
WHERE o_w_id = W_ID
    AND o_d_id = D_ID;
WITH S AS (
    SELECT DISTINCT OL_I_ID
    FROM order_lines
    WHERE ol_w_id = W_ID
        AND ol_d_id = D_ID
        AND ol_o_id BETWEEN N-L AND N-1
)
SELECT COUNT(*) INTO RESULT
FROM stock JOIN S ON stock.s_i_id = S.ol_i_id
WHERE stock.s_w_id = W_ID
    AND s_quantity < T;
RETURN RESULT;
END;
$$ LANGUAGE plpgsql;
""")
print("Completed procedure for Transaction 5")

# Xact 6
print("Create functions for Transaction 6: Popular")
cursor.execute("""
DROP TYPE IF EXISTS last_L_orders_type CASCADE;
CREATE TYPE last_L_orders_type AS (
    o_id INT,
    o_c_id INT,
    o_entry_d TIMESTAMP,
    c_first VARCHAR(16),
    c_middle CHAR(2),
    c_last VARCHAR(16)
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION last_L_orders(W_ID INT, D_ID INT, L INT) RETURNS last_L_orders_type AS $$
DECLARE N INT;
BEGIN
SELECT d_next_o_id INTO N
    FROM district
    WHERE d_id = D_ID AND d_w_id = W_ID;
RETURN (SELECT 
    o_id, 
    o_c_id, 
    o_entry_d,
    c_first,
    c_middle,
    c_last
    FROM orders
        INNER JOIN customer ON orderItems.c_id = customer.c_id
    WHERE o_w_id = W_ID AND o_d_id = D_ID AND o_id >= N-L AND o_id <= N
);
END;
$$ LANGUAGE plpgsql;
""")

cursor.execute("""
DROP TYPE IF EXISTS order_item_type CASCADE;
CREATE TYPE order_item_type AS (
    i_name VARCHAR(24),
    ol_quantity DECIMAL(2, 0)
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION order_item(O_ID INT) RETURNS order_item_type AS $$ BEGIN
RETURN (SELECT
    item.i_name,          
    orderItems.ol_quantity
    FROM orderItems 
        INNER JOIN item ON orderItems.ol_i_id = item.i_id
    WHERE order_lines.ol_o_id = O_ID
);
END;
$$ LANGUAGE plpgsql;
""")
print("Completed procedure for Transaction 6")

# Xact 7
print("Create functions for Transaction 7: top_balance")
cursor.execute("""
DROP TYPE IF EXISTS top_balance_type CASCADE;
CREATE TYPE top_balance_type AS (
    c_first VARCHAR(16),
    c_middle CHAR(2),
    c_last VARCHAR(16),
    c_balance DECIMAL(12, 2),
    w_name VARCHAR(10),
    d_name VARCHAR(10)
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION top_balance() RETURNS top_balance_type AS $$ BEGIN
RETURN(SELECT 
    c_first, 
    c_middle, 
    c_last, 
    c_balance,
    'w_name',
    'd_name'
    FROM customer 
        INNER JOIN district ON d_id = C_D_ID 
        INNER JOIN warehouse ON w_id = C_W_ID
    ORDER BY customer.c_balance DESC
    LIMIT 10
);
END;
$$ LANGUAGE plpgsql;
""")
print("Completed procedure for Transaction 7")
        

# Xact 8
print("Create functions for Transaction 8: related")
cursor.execute("""
DROP TYPE IF EXISTS get_customer_order_items_type CASCADE;
CREATE TYPE get_customer_order_items_type AS (
    o_w_id INT,
    o_d_id INT,
    o_c_id INT,
    o_id INT,
    ol_i_id INT
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION get_customer_order_items(C_W_ID INT, C_D_ID INT, C_ID INT) RETURNS get_customer_order_items_type AS $$ BEGIN
RETURN(SELECT
    orders.o_w_id,
    orders.o_d_id, 
    orders.o_c_id, 
    orders.o_id, 
    order_lines.ol_i_id
    FROM orders
        INNER JOIN order_lines ON orders.o_id = order_lines.ol_o_id
    WHERE orders.o_c_id = C_ID AND orders.o_w_id = C_W_ID AND orders.o_d_id = C_D_ID
);
END;
$$ LANGUAGE plpgsql;
""")

cursor.execute("""
DROP TYPE IF EXISTS get_other_customer_type CASCADE;
CREATE TYPE get_other_customer_type AS (
    w_id INT,
    d_id INT,
    c_id INT
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION get_other_customer(C_W_ID INT, C_D_ID INT, C_ID INT) 
    RETURNS get_other_customer_type AS $$ BEGIN
RETURN(SELECT 
    c_w_id, 
    c_d_id, 
    c_id
    FROM customer
    WHERE c_w_id <> C_W_ID
);
END;
$$ LANGUAGE plpgsql;
""")
print("Completed procedure for Transaction 8")

print("Commiting")
connection.commit()
# Call Procedures with
# cursor.callproc(<procedure_name>[, <parameters>, ...])

# Close the cursor and connection
cursor.close()
connection.close()
