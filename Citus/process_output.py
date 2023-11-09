import csv
import psycopg2

# Replace these with your Citus database connection details
# db_host = "localhost"
# db_name = "testDB"
# db_user = "name"
# db_password = "password"
db_host = "xgpf7"
db_name = "project"
db_user = "cs4224p"
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

### STEP 1
throughputValues = []
## Create clients.csv
client = 'clients.csv'
with open(client, 'w', newline='') as client_csv:
    outputWriter = csv.writer(client_csv)
    for i in range(0, 20):
        path = f'{i}.csv'
        with open(path, 'r', newline='') as path_csv:
            inputReader = csv.reader(path_csv)
            for row in inputReader:
                throughputValues.append(float(row[2]))
                outputWriter.writerow(row)

## Create throughput.csv
throughput = 'throughput.csv'

# get max, min and average
throughputMax = max(throughputValues)
throughputMin = min(throughputValues)
throughputAvg = sum(throughputValues) / len(throughputValues)

### STEP 2
with open(throughput, 'w', newline='') as throughput_csv:
    throughputOutput = [throughputMin, throughputMax, throughputAvg]
    outputWriter = csv.writer(throughput_csv)
    outputWriter.writerow(throughputOutput)

### STEP 3
dbstate = 'dbstate.csv'
with open(dbstate, 'w', newline='') as dbstate_csv:
    outputWriter = csv.writer(dbstate_csv)

    # Warehouse
    cursor.execute('SELECT sum(W_YTD) FROM warehouse;')
    w_ytd_warehouse = cursor.fetchall()[0][0]
    outputWriter.writerow([w_ytd_warehouse])

    # Districts
    cursor.execute('SELECT sum(D_YTD) FROM district;')
    d_ytd_districts = cursor.fetchall()[0][0]
    outputWriter.writerow([d_ytd_districts])

    cursor.execute('SELECT sum(D_NEXT_O_ID) FROM district;')
    next_o_id_districts = cursor.fetchall()[0][0]
    outputWriter.writerow([next_o_id_districts])

    # Customer
    cursor.execute('SELECT sum(C_BALANCE) FROM customer;')
    balance_customer = cursor.fetchall()[0][0]
    outputWriter.writerow([balance_customer])

    cursor.execute('SELECT sum(C_YTD_PAYMENT) FROM customer;')
    ytd_payment_customer = cursor.fetchall()[0][0]
    outputWriter.writerow([ytd_payment_customer])

    cursor.execute('SELECT sum(C_PAYMENT_CNT) FROM customer;')
    payment_cnt_customer = cursor.fetchall()[0][0]
    outputWriter.writerow([payment_cnt_customer])

    cursor.execute('SELECT sum(C_DELIVERY_CNT) FROM customer;')
    delivery_cnt_customer = cursor.fetchall()[0][0]
    outputWriter.writerow([delivery_cnt_customer])

    # Orders
    cursor.execute('SELECT max(O_ID) FROM orders;')
    id_orders = cursor.fetchall()[0][0]
    outputWriter.writerow([id_orders])

    cursor.execute('SELECT sum(O_OL_CNT) FROM orders;')
    ol_cnt_orders = cursor.fetchall()[0][0]
    outputWriter.writerow([ol_cnt_orders])

    # Order_lines
    cursor.execute('SELECT sum(OL_AMOUNT) FROM order_lines;')
    amount_ol = cursor.fetchall()[0][0]
    outputWriter.writerow([amount_ol])

    cursor.execute('SELECT sum(OL_QUANTITY) FROM order_lines;')
    quantity_ol = cursor.fetchall()[0][0]
    outputWriter.writerow([quantity_ol])

    # Stock
    cursor.execute('SELECT sum(S_QUANTITY) FROM stock;')
    quantity_stock = cursor.fetchall()[0][0]
    outputWriter.writerow([quantity_stock])

    cursor.execute('SELECT sum(S_YTD) FROM stock;')
    ytd_stock = cursor.fetchall()[0][0]
    outputWriter.writerow([ytd_stock])

    cursor.execute('SELECT sum(S_ORDER_CNT) FROM stock;')
    order_cnt_stock = cursor.fetchall()[0][0]
    outputWriter.writerow([order_cnt_stock])

    cursor.execute('SELECT sum(S_REMOTE_CNT) FROM stock;')
    remonte_cnt_stock = cursor.fetchall()[0][0]
    outputWriter.writerow([remonte_cnt_stock])

# Close the cursor and connection
cursor.close()
connection.close()