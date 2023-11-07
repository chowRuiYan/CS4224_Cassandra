import psycopg2

import decimal
import csv
import time
import sys

# Replace these with your Citus database connection details
db_host = "localhost"
db_name = "testDB"
db_user = "name"
db_password = "password"

# Constants
xactTimes = []

def execute(path, connection):
    # Create a cursor object to interact with the database
    cursor = connection.cursor()
    print("Cursor Created")

    with open(path) as file:
        while True:
            line = file.readline()
            if not line:
                break

            print(line)
            splitLine = line.strip().split(',')
            xactType = splitLine[0]

            startTime = time.time()

            # Switch Case for Xact Type
            try:
                if xactType == "N":
                    w_id, d_id, c_id, nums_item = splitLine[1:]
                    cursor.execute(f"""CALL new_order_init({w_id, d_id, c_id, M});""")
                    N, c_last, c_credit, c_discount, w_tax, d_tax = cursor.fetchAll()
                    TOTAL_AMOUNT = 0
                    for i in nums_item:
                        item = file.readline()
                        item_inputs = item.strip().split(',')
                        i_id = item_inputs[0]
                        i_supplier_w_id = item_inputs[1]
                        i_quantity = item_inputs[2]
                        cursor.execute(f"""CALL new_order_add_orderline({w_id, d_id, c_id, N, i, i_id, i_supplier_w_id, i_quantity});""")
                        i_name, ol_amount, ol_supply_w_id, ol_quantity, stock_quantity_updated = cursor.fetchAll()
                        TOTAL_AMOUNT = TOTAL_AMOUNT + ol_amount
                        print(f"Item {i}: ({i_name} {ol_supply_w_id} {ol_quantity} {ol_amount})\tStock quantity: {stock_quantity_updated}")
                    TOTAL_AMOUNT = TOTAL_AMOUNT * (1 + w_tax + d_tax) * (1 - c_discount)
                    print(f"Customer Identifier:({w_id} {d_id} {c_id})\tlastname: {c_last}\tcredit: {c_credit}\tdiscount: {c_discount})")
                    print(f"Warehouse tax rate: {w_tax}\tDistrict tax rate: {d_tax}")
                    print(f"Number of items: {nums_item}\tTotal Amount: {TOTAL_AMOUNT}")
                elif xactType == "P":
                    w_id, d_id, c_id, payment = splitLine[1:]
                    cursor.execute(f"""CALL payment({w_id, d_id, c_id, payment});""")
                    returnVal = cursor.fetchAll()
                    c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_limit, c_discount = returnVal[0]
                    d_street_1, d_street_2, d_city, d_state, d_zip = returnVal[1]
                    w_street_1, w_street_2, w_city, w_state, w_zip = returnVal[2]
                    print(f"Customer Identifier:({w_id} {d_id} {c_id})\tName:({c_first} {c_middle} {c_last})\tAddress: {c_street_1} {c_street_2} {c_city} {c_state} {c_zip}\t {c_phone} {c_credit} {c_credit_limit} {c_discount}")
                    print(f"Warehouse address: {w_street_1} {w_street_2} {w_city} {w_state} {w_zip}")
                    print(f"District address: {d_street_1} {d_street_2} {d_city} {d_state} {d_zip}")
                    print(f"Payment: {payment}")
                elif xactType == "D":
                    w_id, carrier_id = splitLine[1:]
                    cursor.execute(f"""CALL delivery({w_id}, {carrier_id});""")

                elif xactType == "O":
                    c_w_id, c_d_id, c_id = splitLine[1:]
                    cursor.execute(f"""SELECT order_status_1({c_w_id}, {c_d_id}, {c_id});""")
                    c_first, c_middle, c_last, c_balance = cursor.fetchall()[0]
                    print(f"Customer: ({c_first} {c_middle} {c_last})\tBalance: {c_balance}")
                    cursor.execute(f"""SELECT order_status_2({c_w_id}, {c_d_id}, {c_id});""")
                    orderlines = cursor.fetchall()[0]
                    o_id, o_entry_d, o_carrier_id, _, _, _, _, _ = orderlines[0]
                    print(f"Customer Last Order: {o_id}\tO_ENTRY_D: {o_entry_d}\tO_CARRIER_ID: {o_carrier_id}")
                    for orderline in orderlines:
                        _, _, _, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d = orderline
                        print(f"{ol_i_id} {ol_supply_w_id} {ol_quantity} {ol_amount} {ol_delivery_d}")

                elif xactType == "S":
                    w_id, d_id, T, L = splitLine[1:]
                    cursor.execute(f"""SELECT stock_level({w_id}, {d_id}, {T}, {L});""")
                    count = cursor.fetchall()[0][0]
                    print(f"Number Of Items Below Threshold: {count}")

                elif xactType == "I":
                    print(f"District Identifier: ({w_id}, {d_id})")
                    print(f"Number Of Last Orders To Be Examined: {L}")

                    w_id, d_id, L = splitLine[1:]
                    cursor.execute(f"""SELECT last_L_orders({w_id}, {d_id}, {L});""")
                    lastLOrders = cursor.fetchall()[0]

                    # Stores distinct popular items
                    popularSet = set()
                    
                    # Map to get total order count for each item
                    itemMap = {}

                    for order in lastLOrders:
                        o_id, c_id, o_entry_d, c_first, c_middle, c_last = order
                        print(f"Order Number: {o_id}, O_ENTRY_D: {o_entry_d}")
                        print(f"Customer Name: ({c_first} {c_middle} {c_last})")

                        cursor.execute(f"""SELECT order_item({o_id}, {c_id});""")
                        orderItems = cursor.fetchall()[0]

                        # Find most popular item in O
                        popular_qty = 0
                        # Used to get popular items from an order
                        popularList = []
                        for orderItem in orderItems:
                            i_name, ol_quantity = orderItem.i_name, orderItem.ol_quantity
            
                            if i_name not in itemMap.keys():
                                itemMap[i_name] = 1
                            else:
                                itemMap[i_name] += 1
                            
                            if ol_quantity > popular_qty:
                                # Found new largest amount
                                popular_qty = ol_quantity
                                popularList.clear()
                                popularList.append((i_name, popular_qty))
                            elif ol_quantity == popular_qty:
                                # There can be multiple items with the same quantity
                                popularList.append((i_name, popular_qty))
                            else:
                                pass
                        
                        for i in popularList:
                            # Add item name to set
                            popularSet.add(i[0])
                            print(f"Item Name: {i[0]}\tQuantity Ordered: {i[1]}")
                        
                    # Find percentage that contain each popular item
                    for item in popularSet:
                        percentage = (itemMap[item] / L) * 100
                        print(f"Item: {item}\tPercentage: {percentage}")

                elif xactType == "T":
                    cursor.execute(f"""SELECT top_balance();""")
                    topBalances = cursor.fetchall()[0]

                    top = sorted(topBalances, key=lambda x: x.c_balance, reverse=True)[:10]

                    for i in range(len(top)):
                        t = top[i]
                        c_balance, c_first, c_middle, c_last, w_name, d_name = t.c_balance, t.c_first, t.c_middle, t.c_last, t.w_name, t.d_name
                        
                        print(f"{i+1}. Customer Name: {c_first} {c_middle} {c_last}")
                        print(f"   Balance: {c_balance}\tWarehouse: {w_name}\tDistrict: {d_name}")

                elif xactType == "R":
                    w_id, d_id, c_id = splitLine[1:]
                    # To prevent duplicate results
                    relatedCustomerSet = set()
                    
                    # Get Customer OrderItemLists: o_w_id, o_d_id, o_c_id , o_id , ol_i_id 
                    cursor.execute(f"""SELECT get_customer_order_items({w_id}, {d_id}, {c_id})""")
                    thisCustomerOrderItemLists = cursor.fetchall()[0]
                    
                    # Get all other customers from other warehouse and district
                    cursor.execute(f"""SELECT get_other_customer({w_id}, {d_id}, {c_id})""")
                    otherCustomers = cursor.fetchall()[0]

                    for otherCustomer in otherCustomers:
                        c_w_id, c_d_id, other_c_id = otherCustomer

                        # Get other Customer OrderItemLists: o_w_id, o_d_id, o_c_id , o_id , ol_i_id 
                        cursor.execute(f"""SELECT get_other_customer_order_items({c_w_id}, {c_d_id}, {other_c_id})""")
                        otherCustomerOrderItems = cursor.fetchall()[0]
            
                        found = 0
                        # For each order in customer
                        for custOrderItemList in otherCustomerOrderItems:
                            # For each order in other customer
                            for thisCustomerOrderItemList in thisCustomerOrderItemLists:
                                # If ol_i_id is the same
                                if custOrderItemList[4] == thisCustomerOrderItemList[4]:
                                    found += 1
                                    break
                            if found >= 2:
                                custIdTuple = (c_w_id, c_d_id, other_c_id)
                                relatedCustomerSet.add(custIdTuple)
                                break
                    
                    print(f'Customer: ({w_id}, {d_id}, {c_id})\nRelated Customer(s):')
                    index = 0
                    for custId in relatedCustomerSet:
                        index += 1
                        w, d, c = custId
                        print(f'{index}. ({w}, {d}, {c})')

                else:
                    print('Invalid Xact Type')
            
            except Exception as e:
                    
                    print(e)

            connection.commit()
            xactTime = time.time() - startTime
            xactTimes.append(xactTime)
            print()

        cursor.close()

# main     
if __name__ == "__main__":

    client = sys.argv[1]

    # Create a connection to the Citus database
    connection = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password
    )

    execute(f'project_files/xact_files/{client}.txt', connection)
    connection.close()

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
        file_writer.writerow([
            client, numOfXacts, totalXactExecutionTime, xactThroughput, avgXactLat, \
            medianXactLat, ninetyFifthPercentileXactLat, ninetyNinthPercentileXactLat
        ])
