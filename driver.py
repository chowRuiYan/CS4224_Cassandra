## Driver program to gather transaction statistics
# TODO Update switch case for xact types
from create_db_tables import KEYSPACES, IP_ADDRESSES
from cassandra.cqlengine import connection
from transactions import *

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
            
            splitLine = line.split(',')
            xactType = splitLine[0].strip()
            
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
                        # Payment Xact
                        payment_transaction()
                    case "D":
                        # Delivery Xact
                        delivery_transaction()
                    case "O":
                        # Order-status Xact
                        order_status_transaction()
                    case "S":
                        # Stock-Level Xact
                        stock_level_transaction()
                    case "I":
                        # Popular-Item Xact
                        popular_item_transaction()
                    case "T":
                        # Top-Balance Xact
                        top_balance_transaction()
                    case "R":
                        # Related-Customer Xact
                        related_customer_transaction()
                    case _:
                        # Invalid
                        print('Invalid Xact Type')
            except Exception as e:
                print(e)

            xactTime = time.time() - startTime
            xactType.append(xactTime)

# main     
if __name__ == "__main__":
    connection.setup(IP_ADDRESSES, KEYSPACES[0])
    test = sys.argv[1]
    client = sys.argv[2]
        
    execute(f'xact_files_{test}/{client}.txt')

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
    
    sys.stderr.write(client, numOfXacts, totalXactExecutionTime, xactThroughput, avgXactLat, \
          medianXactLat, ninetyFifthPercentileXactLat, ninetyNinthPercentileXactLat
          )
