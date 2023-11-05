"""NOTE: should batch add order_lines and stock data. takes rly long"""

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
print("Copy item")
cursor.execute("""
COPY item(I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA)
FROM '/home/CS4224_Cassandra/project_files/data_files/item.csv' 
DELIMITER ','
""")
print("Finished copying item")
print("Commiting")
connection.commit()


print("Copy warehouse")
cursor.execute("""
COPY warehouse(W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD)
FROM '/home/CS4224_Cassandra/project_files/data_files/warehouse.csv' 
DELIMITER ','
""")
print("Finished copying warehouse")
print("Commiting")
connection.commit()

print("Copy district")
cursor.execute("""
COPY district(D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID)
FROM '/home/CS4224_Cassandra/project_files/data_files/district.csv' 
DELIMITER ','
""")
print("Finished copying district")
print("Commiting")
connection.commit()

print("Copy customer")
cursor.execute("""
COPY customer(C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, 
               C_SINCE, C_CREDIT, C_CREDIT_LIMIT, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA)
FROM '/home/CS4224_Cassandra/project_files/data_files/customer.csv' 
DELIMITER ','
""")
print("Finished copying customer")
print("Commiting")
connection.commit()

print("Copy orders")
cursor.execute("""
COPY orders(O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D)
FROM '/home/CS4224_Cassandra/project_files/data_files/order1.csv' 
DELIMITER ','
""")
print("Finished copying orders")
print("Commiting")
connection.commit()

print("Copy order_lines")
cursor.execute("""
COPY order_lines(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, 
                    OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO)
FROM '/home/CS4224_Cassandra/project_files/data_files/order-line1.csv' 
DELIMITER ','
""")
print("Finished copying order_lines")
print("Commiting")
connection.commit()

print("Copy stock")
cursor.execute("""
COPY stock(S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, 
               S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA)
FROM '/home/CS4224_Cassandra/project_files/data_files/stock.csv' 
DELIMITER ','
""")
print("Finished copying stock")

print("Commiting")
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()