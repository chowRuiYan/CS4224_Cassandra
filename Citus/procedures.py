import psycopg2

# Replace these with your Citus database connection details
db_host = "localhost"
db_name = "your_database_name"
db_user = "your_database_user"
db_password = "your_database_password"

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
cursor.execute("""
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

# Xact 2
cursor.execute("""
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
    C_SINCE TIMESTAMP,
    C_CREDIT CHAR,
    C_CREDIT_LIMIT DECIMAL,
    C_DISCOUNT DECIMAL,
    C_BALANCE DECIMAL
);
""")
cursor.execute("""
CREATE TYPE payment_warehouse AS (
    W_STREET_1 VARCHAR,
    W_STREET_2 VARCHAR,
    W_CITY VARCHAR,
    W_STATE CHAR,
    W_ZIP CHAR
);
""")
cursor.execute("""
CREATE TYPE payment_district AS (
    D_STREET_1 VARCHAR,
    D_STREET_2 VARCHAR,
    D_CITY VARCHAR,
    D_STATE CHAR,
    D_ZIP CHAR
);
""")
cursor.execute("""
CREATE TYPE payment_type AS (
    CUSTOMER_DETAIL payment_customer,
    WAREHOUSE_DETAIL payment_warehouse,
    DISTRICT_DETAIL payment_district
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION payment(IN_C_W_ID INT, IN_C_D_ID INT, IN_C_ID INT, PAYMENT INT) 
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
SELECT (c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_limit, c_discount, c_balance) INTO CUSTOMER_DETAIL
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

# Xact 3
print("Create procedure for Transaction 3: Delivery")
cursor.execute("""
CREATE OR REPLACE PROCEDURE delivery(W_ID INT, CARRIER_ID INT) AS $$
DECLARE DISTRICT_NO INT;
N INT;
B INT;
C INT;
BEGIN FOR DISTRICT_NO IN 1..10 LOOP
SELECT MIN(o.o_id) INTO N
FROM orders o
    JOIN district d ON d.d_id = o.o_d_id
    AND d.d_w_id = o.o_w_id
WHERE d.d_id = DISTRICT_NO
    AND d.d_w_id = W_ID
    AND o.o_carrier_id = NULL;
SELECT o_c_id INTO C
FROM orders
WHERE o_id = N;
SELECT SUM(ol_amount) INTO B
FROM order_lines
WHERE ol_o_id = N;
UPDATE orders
SET o_carrier_id = CARRIER_ID
WHERE o_id = N;
UPDATE order_lines
SET ol_delivery_d = CURRENT_TIMESTAMP
WHERE ol_o_id = N;
UPDATE customer
SET c_balance = c_balance - B,
    c_delivery_cnt = c_delivery_cnt + 1
WHERE c_id = C;
END LOOP;
END;
$$ LANGUAGE plpgsql;
""")

# Xact 4
print("Create functions for Transaction 4: Order Status")

cursor.execute("""
CREATE TYPE order_status_1_type AS (
    c_first VARCHAR(16),
    c_middle CHAR(2),
    c_last VARCHAR(16),
    c_balance DECIMAL(12, 2)
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION order_status_1(C_W_ID INT, C_D_ID INT, C_ID INT) RETURNS SETOF order_status_1_type AS $$
BEGIN
RETURN (SELECT c_first,
    c_middle,
    c_last,
    c_balance
FROM customer
WHERE c_w_id = C_W_ID
    AND c_d_id = C_D_ID
    AND c_id = C_ID);
END;
$$ LANGUAGE plpgsql;
""")

cursor.execute("""
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
DECLARE LAST_ORDER INT;
O_ID INT;
O_ENTRY_D TIMESTAMP;
O_CARRIER_ID INT;
BEGIN
SELECT MAX(o_id) INTO LAST_ORDER
FROM orders
WHERE o_w_id = C_W_ID
    AND o_d_id = C_D_ID
    AND o_c_id = C_ID;
SELECT o_id INTO O_ID,
    o_entry_d INTO O_ENTRY_D,
    o_carrier_id INTO O_CARRIER_ID
FROM orders
WHERE o_id = LAST_ORDER;
RETURN (SELECT
    O_ID,
    O_ENTRY_D,
    O_CARRIER_ID,
    ol_i_id,
    ol_supply_w_id,
    ol_quantity,
    ol_amount,
    ol_delivery_d
FROM order_lines
WHERE ol_o_id = LAST_ORDER);
END;
$$ LANGUAGE plpgsql;
""")

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
    SELECT DISTINCT OL_I_ID INTO S
    FROM order_lines
    WHERE ol_w_id = W_ID
        AND ol_d_id = D_ID
        AND ol_o_id BETWEEN N-L AND N-1
)
SELECT COUNT(*) INTO RESULT
FROM stock JOIN S ON stock.s_i_id = S.ol_i_id
WHERE s_w_id = W_ID
    AND s_quantity < T;
RETURN RESULT;
END;
$$ LANGUAGE plpgsql;
""")

# Xact 6
cursor.execute("""
CREATE PROCEDURE popular_item(W_ID INT, D_ID INT, L INT) AS $$ BEGIN
DECLARE N INT;
SELECT d_next_o_id INTO N
    FROM district
    WHERE d_id = D_ID AND d_w_id = W_ID;
WITH orderSet(o_id) as
    (SELECT o_id
        FROM order
        WHERE o_w_id = W_ID AND o_d_id = D_ID AND o_id >= N-L AND o_id <= N
    ),
    maxOrderQuant(ol_o_id, ol_quantity) as
    (SELECT ol_o_id, max(ol_quantity)
        FROM order_lines
        WHERE ol_o_id in orderSet
        GROUPBY ol_o_id
    )
SELECT 
    FROM order_lines
    WHERE ol_o_id in orderSet AND ol_quantity = 
END;
$$ LANGUAGE plpgsql;
""")

# Xact 7
cursor.execute("""
CREATE PROCEDURE top_balance() AS $$ BEGIN
WITH slimWarehouse(w_name, w_id) as
    (SELECT w_name, w_id
        FROM warehouse
    ),
    slimDistrict(d_name, d_id) as
    (SELECT d_name, d_id
        FROM district
    )
SELECT c_first, c_middle, c_last, c_balance, slimWarehouse.w_name, slimDistrict.d_name
    FROM customer 
    INNER JOIN slimDistrict ON slimDistrict.d_id = customer.C_D_ID 
    INNER JOIN slimWarehouse ON slimWarehouse.w_id = customer.C_W_ID
    ORDER BY customer.c_balance DESC
    LIMIT 10;
END;
$$ LANGUAGE plpgsql;
""")

# Xact 8 
cursor.execute("""
CREATE PROCEDURE getCustomerOrderItems(C_W_ID, C_D_ID, C_ID) AS $$ BEGIN
SELECT order.o_c_id, order.o_id, order_lines.ol_i_id
    FROM order 
        INNER JOIN order_lines ON order.o_id = order_lines.ol_o_id
    WHERE order.o_c_id = C_ID
END;
$$ LANGUAGE plpgsql;
""")

cursor.execute("""
CREATE PROCEDURE getOtherCustomerOrderItems(C_W_ID, C_D_ID, C_ID) AS $$ BEGIN
WITH diffWarehouse(c_w_id, c_d_id, c_id) as
    (SELECT c_w_id, c_d_id, c_id
        FROM customer
        WHERE c_w_id <> C_W_ID AND c_id <> C_ID
    ),
SELECT order.o_w_id, order.o_d_id, order.o_c_id, order.o_id, order_lines.ol_i_id
        FROM order 
            INNER JOIN order_lines ON order.o_id = order_lines.ol_o_id
        WHERE order.o_c_id IN (SELECT c_id FROM diffWarehouse)
END;
$$ LANGUAGE plpgsql;
""")

# Call Procedures with
# cursor.callproc(<procedure_name>[, <parameters>, ...])

# Close the cursor and connection
cursor.close()
connection.close()
