## Driver program to gather transaction statistics
# TODO Update switch case for xact types
# from create_db_tables import KEYSPACES, IP_ADDRESSES
# from cassandra.cqlengine import connection
from transactions import (
    new_order_transaction,
    payment_transaction,
    delivery_transaction,
    order_status_transaction,
    stock_level_transaction,
    popular_item_transaction,
    top_balance_transaction,
    related_customer_transaction
)

import decimal
import csv
import time
import sys

# Constants
xactTimes = []

def execute(path):
    with open(path) as file:
        while True:
            line = file.readline()
            if not line:
                break
            
            splitLine = line.strip().split(',')
            xactType = splitLine[0]
            
            startTime = time.time()

            # Switch Case for Xact Type
            try:
                match xactType:
                    case "N":
                        # New Order Xact
                        c_id, w_id, d_id, num = input[1:]
                        num_items = int(num)
                        item_numbers = []
                        supplier_warehouses = []
                        quantities = []
                        for i in range(num_items):
                            item_number, supplier_warehouse, quantity = f.readline().strip().split(',')
                            item_numbers.append(int(item_number))
                            supplier_warehouses.append(int(supplier_warehouse))
                            quantities.append(decimal.Decimal(quantity))
                        new_order_transaction(int(c_id), int(w_id), int(d_id), num_items, item_numbers, supplier_warehouses, quantities)
                    case "P":
                        w_id, d_id, c_id, payment = splitLine[1:]
                        payment_transaction(int(w_id), int(d_id), int(c_id), decimal.Decimal(payment))
                    case "D":
                        w_id, carrier_id = splitLine[1:]
                        delivery_transaction(int(w_id), int(carrier_id))
                    case "O":
                        w_id, d_id, c_id = splitLine[1:]
                        order_status_transaction(int(w_id), int(d_id), int(c_id))
                    case "S":
                        w_id, d_id, T, L = splitLine[1:]
                        stock_level_transaction(int(w_id), int(d_id), decimal.Decimal(T), int(L))
                    case "I":
                        w_id, d_id, L = splitLine[1:]
                        popular_item_transaction(int(w_id), int(d_id), int(L))
                    case "T":
                        top_balance_transaction()
                    case "R":
                        w_id, d_id, c_id = splitLine[1:]
                        related_customer_transaction(int(w_id), int(d_id), int(c_id))
                    case _:
                        # Invalid
                        print('Invalid Xact Type')
            except Exception as e:
                print(e)

            xactTime = time.time() - startTime
            xactTimes.append(xactTime)
            print()

# main     
if __name__ == "__main__":
    # connection.setup(IP_ADDRESSES, KEYSPACES[0])
    # test = sys.argv[1]
    client = sys.argv[1]
        
    execute(f'project_files/xact_files/{client}.txt')

    # Get Stats
    print('Print Stats')
    numOfXacts = len(xactTimes)
    totalXactExecutionTime = sum(xactTimes)
    xactThroughput = numOfXacts / totalXactExecutionTime
    avgXactLat = 1 / (xactThroughput * 1000)
    sortedXactTimes = sorted(xactTimes)
    medianXactLat = sortedXactTimes[numOfXacts // 2] * 1000
    ninetyFifthPercentileXactLat = sortedXactTimes[int(numOfXacts * 0.95)] * 1000
    ninetyNinthPercentileXactLat = sortedXactTimes[int(numOfXacts * 0.99)] * 1000
    
    with open(f'{client}.csv', 'w') as f:
        file_writer = csv.writer(f)
        file_writer.writerow([client, numOfXacts, totalXactExecutionTime, xactThroughput, avgXactLat, \
                              medianXactLat, ninetyFifthPercentileXactLat, ninetyNinthPercentileXactLat
                            ])
