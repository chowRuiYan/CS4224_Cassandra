import csv
from cassandra.cluster import Cluster

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

with open(throughput, 'w', newline='') as throughput_csv:
    throughputOutput = [throughputMin, throughputMax, throughputAvg]
    outputWriter = csv.writer(throughput_csv)
    outputWriter.writerow(throughputOutput)

# Database state
# # Use on soc compute clusters
cluster = Cluster(['192.168.51.67', '192.168.51.68', '192.168.51.69', '192.168.51.70', '192.168.51.71'])
# Use locally
# cluster = Cluster()
session = cluster.connect("cs4224_keyspace")
session.default_timeout = 180

dbstate = 'dbstate.csv'
with open(dbstate, 'w', newline='') as dbstate_csv:
    outputWriter = csv.writer(dbstate_csv)
    outputWriter.writerow(session.execute('SELECT sum(W_YTD) FROM warehouse').one())
    outputWriter.writerow(session.execute('SELECT sum(D_YTD) FROM district').one())
    
    count = 0
    for row in session.execute('SELECT next_o_id FROM info_for_new_order per partition limit 1'):
        count += row.next_o_id
    outputWriter.writerow(count)

    outputWriter.writerow(session.execute('SELECT sum(C_BALANCE) FROM customer_by_wd').one())
    outputWriter.writerow(session.execute('SELECT sum(C_YTD_PAYMENT) FROM customer_by_wd').one())

    outputWriter.writerow(session.execute('SELECT sum(C_PAYMENT_CNT) FROM customer_by_wd').one())
    outputWriter.writerow(session.execute('SELECT sum(C_DELIVERY_CNT) FROM customer_by_wd').one())
    outputWriter.writerow(session.execute('SELECT max(O_ID) FROM order_by_wd').one())
    outputWriter.writerow(session.execute('SELECT sum(O_OL_CNT) FROM order_by_wd').one())

    # Orderline items
    orderlines = session.execute('SELECT ORDERLINES FROM order_by_wd')
    
    ol_amount = []
    ol_quantity = []
    for orderline in orderlines:
        for order in orderline:
            for ol in order:
                ol_amount.append(ol.ol_amount)
                ol_quantity.append(ol.ol_quantity)
            

    outputWriter.writerow([sum(ol_amount)])
    outputWriter.writerow([sum(ol_quantity)])

    outputWriter.writerow(session.execute('SELECT sum(S_QUANTITY) FROM stock_by_item').one())
    outputWriter.writerow(session.execute('SELECT sum(S_YTD) FROM stock_by_item').one())
    outputWriter.writerow(session.execute('SELECT sum(S_ORDER_CNT) FROM stock_by_item').one())
    outputWriter.writerow(session.execute('SELECT sum(S_REMOTE_CNT) FROM stock_by_item').one())
