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

print("Cursor Created")

print("Create warehouse")
cursor.execute("""
DROP TABLE IF EXISTS warehouse CASCADE;
CREATE TABLE IF NOT EXISTS warehouse (
    W_ID INT,
    W_NAME VARCHAR(10),
    W_STREET_1 VARCHAR(20),
    W_STREET_2 VARCHAR(20),
    W_CITY VARCHAR(20),
    W_STATE CHAR(2),
    W_ZIP CHAR(9),
    W_TAX DECIMAL(4, 4),
    W_YTD DECIMAL(12, 2),
    PRIMARY KEY (W_ID)
)
""")
print("Finished creating warehouse")

print("Create district")
cursor.execute("""
DROP TABLE IF EXISTS district CASCADE;
CREATE TABLE IF NOT EXISTS district (
    D_W_ID int,
    D_ID int,
    D_NAME VARCHAR(10),
    D_STREET_1 VARCHAR(20),
    D_STREET_2 VARCHAR(20),
    D_CITY VARCHAR(20),
    D_STATE CHAR(2),
    D_ZIP CHAR(9),
    D_TAX DECIMAL(4, 4),
    D_YTD DECIMAL(12, 2),
    D_NEXT_O_ID INT,
    PRIMARY KEY (D_W_ID, D_ID),
    FOREIGN KEY(D_W_ID) REFERENCES warehouse(W_ID)
)
""")
print("Finished creating district")

print("Create customer")
cursor.execute("""
DROP TABLE IF EXISTS customer CASCADE;
CREATE TABLE IF NOT EXISTS customer (
    C_W_ID INT,
    C_D_ID INT,
    C_ID INT,
    C_FIRST VARCHAR(16),
    C_MIDDLE CHAR(2),
    C_LAST VARCHAR(16),
    C_STREET_1 VARCHAR(20),
    C_STREET_2 VARCHAR(20),
    C_CITY VARCHAR(20),
    C_STATE CHAR(2),
    C_ZIP CHAR(9),
    C_PHONE CHAR(16),
    C_SINCE TIMESTAMP,
    C_CREDIT CHAR(2),
    C_CREDIT_LIMIT DECIMAL(12, 2),
    C_DISCOUNT DECIMAL(5, 4),
    C_BALANCE DECIMAL(12, 2),
    C_YTD_PAYMENT FLOAT,
    C_PAYMENT_CNT INT,
    C_DELIVERY_CNT INT,
    C_DATA VARCHAR(500),
    PRIMARY KEY (C_W_ID, C_D_ID, C_ID),
    FOREIGN KEY(C_W_ID, C_D_ID) REFERENCES district(D_W_ID, D_ID)
)
""")
print("Finished creating customer")

print("Create orders")
cursor.execute("""
DROP TABLE IF EXISTS orders CASCADE;
CREATE TABLE IF NOT EXISTS orders (
    O_W_ID INT,
    O_D_ID INT,
    O_ID INT,
    O_C_ID INT,
    O_CARRIER_ID INT,
    O_OL_CNT DECIMAL(2, 0),
    O_ALL_LOCAL DECIMAL(1, 0),
    O_ENTRY_D TIMESTAMP,
    PRIMARY KEY (O_W_ID, O_D_ID, O_ID),
    CHECK (
        O_CARRIER_ID BETWEEN -1 AND 10
    ),
    FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) REFERENCES customer(C_W_ID, C_D_ID, C_ID)
)
""")
print("Finished creating orders")

print("Create item")
cursor.execute("""
DROP TABLE IF EXISTS item CASCADE;
CREATE TABLE IF NOT EXISTS item (
    I_ID INT,
    I_NAME VARCHAR(24),
    I_PRICE DECIMAL(5, 2),
    I_IM_ID INT,
    I_DATA VARCHAR(50),
    PRIMARY KEY (I_ID)
)
""")
print("Finished creating item")

print("Create order_lines")
cursor.execute("""
DROP TABLE IF EXISTS order_lines CASCADE;
CREATE TABLE IF NOT EXISTS order_lines (
    OL_W_ID INT,
    OL_D_ID INT,
    OL_O_ID INT,
    OL_NUMBER INT,
    OL_I_ID INT,
    OL_DELIVERY_D TIMESTAMP,
    OL_AMOUNT DECIMAL(7, 2),
    OL_SUPPLY_W_ID INT,
    OL_QUANTITY DECIMAL(2, 0),
    OL_DIST_INFO CHAR(24),
    PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER),
    FOREIGN KEY (OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES orders(O_W_ID, O_D_ID, O_ID),
    FOREIGN KEY (OL_I_ID) REFERENCES item(I_ID)
)
""")
print("Finished creating order_lines")

print("Create stock")
cursor.execute("""
DROP TABLE IF EXISTS stock CASCADE;
CREATE TABLE IF NOT EXISTS stock (
    S_W_ID INT,
    S_I_ID INT,
    S_QUANTITY DECIMAL(4, 0),
    S_YTD DECIMAL(8, 2),
    S_ORDER_CNT INT,
    S_REMOTE_CNT INT,
    S_DIST_01 CHAR(24),
    S_DIST_02 CHAR(24),
    S_DIST_03 CHAR(24),
    S_DIST_04 CHAR(24),
    S_DIST_05 CHAR(24),
    S_DIST_06 CHAR(24),
    S_DIST_07 CHAR(24),
    S_DIST_08 CHAR(24),
    S_DIST_09 CHAR(24),
    S_DIST_10 CHAR(24),
    S_DATA VARCHAR(50),
    PRIMARY KEY (S_W_ID, S_I_ID),
    FOREIGN KEY (S_I_ID) REFERENCES item(I_ID),
    FOREIGN KEY (S_W_ID) REFERENCES warehouse(W_ID)
)
""")
print("Finished creating stock")

print("Create eight")
cursor.execute("""
DROP TABLE IF EXISTS eight CASCADE;
CREATE TABLE IF NOT EXISTS eight (
    W_ID INT,
    D_ID INT,
    O_ID INT,
    C_ID INT,
    I_ID_LIST TEXT,
    PRIMARY KEY (W_ID, D_ID, C_ID, O_ID)
)
""")
print("Finished creating eight")
print("Commiting")
connection.commit()

print("Make item reference table")
cursor.execute("""
SELECT create_reference_table('item');
""")

print("Commiting")
connection.commit()


print("Setting citusmulti_shard_modify_mode")
cursor.execute("""
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
""")
print("Distribute warehouse")
cursor.execute("""
SELECT create_distributed_table('warehouse', 'w_id');
""")
print("Distribute district")
cursor.execute("""
SELECT create_distributed_table('district', 'd_w_id', colocate_with => 'warehouse');
""")
print("Distribute customer")
cursor.execute("""
SELECT create_distributed_table('customer', 'c_w_id', colocate_with => 'warehouse');
""")
print("Distribute orders")
cursor.execute("""
SELECT create_distributed_table('orders', 'o_w_id', colocate_with => 'warehouse');
""")
print("Distribute order_lines")
cursor.execute("""
SELECT create_distributed_table('order_lines', 'ol_w_id', colocate_with => 'warehouse');
""")
print("Distribute stock")
cursor.execute("""
SELECT create_distributed_table('stock', 's_w_id', colocate_with => 'warehouse');
""")

print("Commiting distributions")
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()