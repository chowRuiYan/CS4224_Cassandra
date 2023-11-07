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
CREATE PROCEDURE new_order(
    W_ID INT,
    D_ID INT,
    C_ID INT,
    NUM_ITEMS INT,
    ITEM_NUMBER INT [],
    SUPPLIER_WAREHOUSE INT [],
    QUANTITY INT []
) AS $$
DECLARE N INT;
TOTAL_AMOUNT INT;
DEFINED_O_ALL_LOCAL_COUNT INT;
IDX INT;
ADJUSTED_QUANTITY INT;
RETRIEVED_S_QUANTITY INT;
RETRIEVED_I_PRICE DECIMAL(5, 2);
ITEM_AMOUNT DECIMAL;
FORMATTED_DIST_INFO VARCHAR;
RETRIEVED_D_TAX DECIMAL;
RETRIEVED_W_TAX DECIMAL;
RETRIEVED_C_DISCOUNT DECIMAL;
BEGIN FORMATTED_DIST_INFO := 'S_DIST_' || D_ID;
DEFINED_O_ALL_LOCAL_COUNT := 1;
SELECT d_next_o_id INTO N
FROM district
WHERE d_w_id = W_ID
    AND d_id = D_ID;
UPDATE district
SET d_next_o_id = d_next_o_id + 1
WHERE d_w_id = W_ID
    AND d_id = D_ID;
FOR IDX IN 1..NUMS_ITEMS LOOP IF SUPPLIER_WAREHOUSE [IDX] <> W_ID THEN DEFINED_O_ALL_LOCAL_COUNT := 0;
END IF;
END LOOP;
INSERT INTO orders(
        o_id,
        o_d_id,
        o_w_id,
        o_c_id,
        o_entry_d,
        o_carrier_id,
        o_ol_cnt,
        o_all_local
    )
VALUES(
        N,
        D_ID,
        W_ID,
        C_ID,
        CURRENT_TIMESTAMP,
        NULL,
        NUM_ITEMS,
        DEFINED_O_ALL_LOCAL_COUNT
    );
TOTAL_AMOUNT := 0;
FOR IDX IN 1..NUMS_ITEMS LOOP
SELECT s_quantity INTO RETRIEVED_S_QUANTITY
FROM stock
WHERE s_w_id = SUPPLIER_WAREHOUSE [IDX]
    AND s_i_id = ITEM_NUMBER [IDX];
ADJUSTED_QUANTITY := RETRIEVED_S_QUANTITY - QUANTITY [IDX];
IF ADJUSTED_QUANTITY < 10 THEN ADJUSTED_QUANTITY := ADJUSTED_QUANTITY + 100;
END IF;
UPDATE stock
SET s_quantity = ADJUSTED_QUANTITY,
    s_ytd = s_ytd + QUANTITY [IDX],
    s_order_cnt = s_order_cnt + 1
WHERE s_w_id = SUPPLIER_WAREHOUSE [IDX]
    AND s_i_id = ITEM_NUMBER [IDX];
IF SUPPLIER_WAREHOUSE [IDX] <> W_ID THEN
UPDATE stock
SET s_remote_cnt = s_remote_cnt + 1
WHERE s_w_id = SUPPLIER_WAREHOUSE [IDX]
    AND s_i_id = ITEM_NUMBER [IDX];
END IF;
SELECT i_price INTO RETRIEVED_I_PRICE
FROM item
WHERE i_id = ITEM_NUMBER [IDX];
ITEM_AMOUNT := QUANTITY [IDX] * RETRIEVED_I_PRICE;
TOTAL_AMOUNT := TOTAL_AMOUNT + ITEM_AMOUNT;
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
        D_ID,
        W_ID,
        IDX,
        ITEM_NUMBER [IDX],
        SUPPLIER_WAREHOUSE [IDX],
        QUANTITY [IDX],
        ITEM_AMOUNT,
        NULL,
        FORMATTED_DIST_INFO
    );
END LOOP;
SELECT d_tax INTO RETRIEVED_D_TAX
FROM district
WHERE d_w_id = W_ID
    AND d_id = D_ID;
SELECT w_tax INTO RETRIEVED_W_TAX
FROM warehouse
WHERE w_id = W_ID;
SELECT c_discount INTO RETRIEVED_C_DISCOUNT
FROM customer
WHERE c_id = C_ID;
TOTAL_AMOUNT := TOTAL_AMOUNT * (1 - RETRIEVED_D_TAX + RETRIEVED_W_TAX) * (1 - RETRIEVED_C_DISCOUNT);
SELECT TOTAL_AMOUNT;
END;
$$ LANGUAGE plpgsql;
""")

# Xact 2
cursor.execute("""
CREATE PROCEDURE payment(C_W_ID INT, C_D_ID INT, C_ID INT, PAYMENT INT) AS $$ BEGIN
UPDATE warehouse
SET w_ytd = w_ytd + PAYMENT
WHERE w_id = C_W_ID;
UPDATE district
SET d_ytd = d_ytd + PAYMENT
WHERE d_w_id = C_W_ID
    AND d_id = C_D_ID;
UPDATE customer
SET c_balance = c_balance - PAYMENT,
    c_ytd_payment = c_ytd_payment - PAYMENT,
    c_payment_cnt = c_payment_cnt + 1
WHERE c_w_id = C_W_ID
    AND c_d_id = C_D_ID
    AND c_id = C_ID;
SELECT *
FROM customer
WHERE c_w_id = C_W_ID
    AND c_d_id = C_D_ID
    AND c_id = C_ID;
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
CREATE OR REPLACE FUNCTION last_L_orders(W_ID INT, D_ID INT, L INT) RETURNS last_L_orders_type AS $$ BEGIN
DECLARE N INT;
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
    FROM order
        INNER JOIN customer ON orderItems.c_id = customer.c_id
    WHERE o_w_id = W_ID AND o_d_id = D_ID AND o_id >= N-L AND o_id <= N
)
""")

cursor.execute("""
CREATE TYPE order_item_type AS (
    i_name VARCHAR(24),
    ol_quantity DECIMAL(2, 0)
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION order_item(O_ID INT, C_ID INT) RETURNS order_item_type AS $$ BEGIN
WITH 
    orderItems(o_id, o_entry_d, c_id, i_id, ol_quantity) as
    (SELECT order_lines.ol_o_id, orderSet.o_entry_d, order_lines.ol_i_id, order_lines.ol_quantity)
        FROM order_lines
        WHERE order_lines.ol_o_id = O_ID
    )
RETURN (SELECT
    i_name,          
    ol_quantity
FROM orderItems 
    INNER JOIN item ON orderItems.i_id = item.i_id
);
END;
$$ LANGUAGE plpgsql;
""")

# Xact 7
cursor.execute("""
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
CREATE OR REPLACE FUNCTION top_balance() RETURNS popular_item_type AS $$ BEGIN
WITH slimWarehouse(w_name, w_id) as
    (SELECT w_name, w_id
        FROM warehouse
    ),
    slimDistrict(d_name, d_id) as
    (SELECT d_name, d_id
        FROM district
    )
RETURN(SELECT 
    c_first, 
    c_middle, 
    c_last, 
    c_balance, 
    w_name, 
    d_name
    FROM customer 
        INNER JOIN slimDistrict ON slimDistrict.d_id = customer.C_D_ID 
        INNER JOIN slimWarehouse ON slimWarehouse.w_id = customer.C_W_ID
    ORDER BY customer.c_balance DESC
    LIMIT 10;
)
END;
$$ LANGUAGE plpgsql;
""")

# Xact 8
cursor.execute("""
CREATE TYPE get_customer_order_items_type AS (
    o_w_id INT,
    o_d_id INT,
    o_c_id INT,
    o_id INT,
    ol_i_id INT
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION get_customer_order_items(C_W_ID, C_D_ID, C_ID) RETURNS get_customer_order_items_type AS $$ BEGIN
RETURN(SELECT
    order.o_w_id, 
    order.o_d_id, 
    order.o_c_id, 
    order.o_id, 
    order_lines.ol_i_id
    FROM order 
        INNER JOIN order_lines ON order.o_id = order_lines.ol_o_id
    WHERE order.o_c_id = C_ID
)
END;
$$ LANGUAGE plpgsql;
""")

cursor.execute("""
CREATE TYPE get_other_customer_type AS (
    w_id INT,
    d_id INT,
    c_id INT
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION get_other_customer(C_W_ID, C_D_ID, C_ID) 
    RETURNS get_other_customer_type AS $$ BEGIN
RETURN(SELECT 
    c_w_id, 
    c_d_id, 
    c_id
    FROM customer
    WHERE c_w_id <> C_W_ID
)
END;
$$ LANGUAGE plpgsql;
""")

cursor.execute("""
CREATE TYPE get_other_customer_order_items_type AS (
    o_w_id INT,
    o_d_id INT,
    o_c_id INT,
    o_id INT,
    ol_i_id INT
);
""")
cursor.execute("""
CREATE OR REPLACE FUNCTION get_other_customer_order_items(C_W_ID, C_D_ID, C_ID) 
    RETURNS get_other_customer_order_items_type AS $$ BEGIN
RETURN(SELECT 
    order.o_w_id, 
    order.o_d_id, 
    order.o_c_id, 
    order.o_id, 
    order_lines.ol_i_id
    FROM order 
        INNER JOIN order_lines ON order.o_id = order_lines.ol_o_id
    WHERE o_w_id = C_W_ID AND o_d_id = C_D_ID AND o_c_id = C_ID
)
END;
$$ LANGUAGE plpgsql;
""")

# Call Procedures with
# cursor.callproc(<procedure_name>[, <parameters>, ...])

# Close the cursor and connection
cursor.close()
connection.close()
