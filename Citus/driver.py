from collections import Counter
import psycopg2

import csv
import time
import sys

# Replace these with your Citus database connection details
# db_host = "localhost"
# db_name = "testDB"
# db_user = "name"
# db_password = "password"
db_host = "xgpf7"
db_name = "project"
db_user = "cs4224p"
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
                    c_id, w_id, d_id, nums_item = splitLine[1:]
                    cursor.execute(f"""SELECT new_order_init({w_id}, {d_id}, {c_id}, {nums_item});""")
                    output = cursor.fetchall()[0][0][1:-1].split(',')
                    N, c_last, c_credit, c_discount, w_tax, d_tax = output

                    TOTAL_AMOUNT = 0
                    i_id_list = ''
                    for i in range(1, int(nums_item)+1):
                        item = file.readline()
                        item_inputs = item.strip().split(',')
                        i_id = item_inputs[0]
                        i_supplier_w_id = item_inputs[1]
                        i_quantity = item_inputs[2]
                        cursor.execute(f"""SELECT new_order_add_orderline({w_id}, {d_id}, {c_id}, {N}, {i}, {i_id}, {i_supplier_w_id}, {i_quantity});""")
                        item_output = cursor.fetchall()[0][0][1:-1].split(',')
                        i_name, ol_amount, ol_supply_w_id, ol_quantity, stock_quantity_updated = item_output
                        i_id_list += f"{i_name}/"
                        TOTAL_AMOUNT = TOTAL_AMOUNT + float(ol_amount)
                        print(f"Item {i}: ({i_name} {ol_supply_w_id} {ol_quantity} {ol_amount})\tStock quantity: {stock_quantity_updated}")
                    TOTAL_AMOUNT = TOTAL_AMOUNT * (1 + float(w_tax) + float(d_tax)) * (1 - float(c_discount))
                    cursor.execute(f"""CALL new_order_add_eight({w_id}, {d_id}, {o_id}, {c_id}, '{i_id_list}')""")
                    print(f"Customer Identifier:({w_id} {d_id} {c_id})\tlastname: {c_last}\tcredit: {c_credit}\tdiscount: {c_discount})")
                    print(f"Warehouse tax rate: {w_tax}\tDistrict tax rate: {d_tax}")
                    print(f"Number of items: {nums_item}\tTotal Amount: {TOTAL_AMOUNT}")

                elif xactType == "P":
                    w_id, d_id, c_id, payment = splitLine[1:]
                    cursor.execute(f"""SELECT payment({w_id}, {d_id}, {c_id}, {payment});""")
                    returnVal = eval(cursor.fetchall()[0][0][1:-1])
                    cust, district, warehouse = returnVal
                    c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_limit, c_discount = cust[2:-15].split(',')
                    d_street_1, d_street_2, d_city, d_state, d_zip = district[2:-6].split(',')
                    w_street_1, w_street_2, w_city, w_state, w_zip = warehouse[2:-6].split(',')
                    print(f"Customer Identifier:({w_id} {d_id} {c_id})\t\nName:({c_first} {c_middle} {c_last})\t\nAddress: {c_street_1} {c_street_2} {c_city} {c_state} {c_zip}\t {c_phone} {c_credit} {c_credit_limit} {c_discount}")
                    print(f"Warehouse address: {w_street_1} {w_street_2} {w_city} {w_state} {w_zip}")
                    print(f"District address: {d_street_1} {d_street_2} {d_city} {d_state} {d_zip}")
                    print(f"Payment: {payment}")

                elif xactType == "D":
                    w_id, carrier_id = splitLine[1:]
                    cursor.execute(f"""CALL delivery({w_id}, {carrier_id});""")

                elif xactType == "O":
                    c_w_id, c_d_id, c_id = splitLine[1:]
                    cursor.execute(f"""SELECT order_status_1({c_w_id}, {c_d_id}, {c_id});""")
                    c_first, c_middle, c_last, c_balance = cursor.fetchall()[0][0][1:-1].split(',')
                    print(f"Customer: {c_first} {c_middle} {c_last}\nCustomer Balance: {c_balance}")
                    cursor.execute(f"""SELECT order_status_2({c_w_id}, {c_d_id}, {c_id});""")
                    orderlines = cursor.fetchall()
                    o_id, o_entry_d, o_carrier_id, _, _, _, _, _ = orderlines[0][0][1:-1].split(',')
                    print(f"Customer Last Order: {o_id}\nOrder Entry Datetime: {o_entry_d}\nCarrier: {o_carrier_id}\nItems:")
                    for orderline in orderlines:
                        _, _, _, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d = orderline[0][1:-1].split(',')
                        print(f"{ol_i_id} {ol_supply_w_id} {ol_quantity} {ol_amount} {ol_delivery_d}")

                elif xactType == "S":
                    w_id, d_id, T, L = splitLine[1:]
                    cursor.execute(f"""SELECT stock_level({w_id}, {d_id}, {T}, {L});""")
                    count = cursor.fetchall()[0][0]
                    print(f"Number Of Items Below Threshold: {count}")

                elif xactType == "I":
                    w_id, d_id, L = splitLine[1:]
                    print(f"District Identifier: ({w_id}, {d_id})")
                    print(f"Number Of Last Orders To Be Examined: {L}")

                    cursor.execute(f"""SELECT last_L_orders({w_id}, {d_id}, {L});""")
                    lastLOrders = cursor.fetchall()
                    print(lastLOrders)

                    # Stores distinct popular items
                    popularSet = set()
                    
                    # Map to get total order count for each item
                    itemMap = {}

                    for order in lastLOrders:
                        o_id, c_id, o_entry_d, c_first, c_middle, c_last = order[0][1:-1].split(',')
                        print(f"Order Number: {o_id}, O_ENTRY_D: {o_entry_d}")
                        print(f"Customer Name: ({c_first} {c_middle} {c_last})")

                        cursor.execute(f"""SELECT order_item({o_id}, {w_id}, {d_id});""")
                        orderItems = cursor.fetchall()

                        # Find most popular item in O
                        popular_qty = 0
                        # Used to get popular items from an order
                        popularList = []
                        for orderItem in orderItems:
                            i_name, ol_quantity = orderItem[0][1:-1].split(',')
                            ol_quantity_int = int(ol_quantity)
            
                            if i_name not in itemMap.keys():
                                itemMap[i_name] = 1
                            else:
                                itemMap[i_name] += 1
                            
                            if ol_quantity_int > popular_qty:
                                # Found new largest amount
                                popular_qty = ol_quantity_int
                                popularList.clear()
                                popularList.append((i_name, popular_qty))
                            elif ol_quantity_int == popular_qty:
                                # There can be multiple items with the same quantity
                                popularList.append((i_name, popular_qty))
                            else:
                                pass
                        
                        for i in popularList:
                            # Add item name to set
                            popularSet.add(i[0])
                            print(f"Item Name: {i[0]}\tQuantity Ordered: {i[1]}")

                    print("\nPercentages")  
                    # Find percentage that contain each popular item
                    for item in popularSet:
                        percentage = (itemMap[item] / int(L)) * 100
                        print(f"Item: {item}\tPercentage: {percentage}")

                elif xactType == "T":
                    cursor.execute(f"""SELECT top_balance();""")
                    topBalances = cursor.fetchall()

                    for i in range(len(topBalances)):
                        t = topBalances[i][0]

                        c_balance, c_first, c_middle, c_last, w_name, d_name = t[1:-1].split(',')

                        print(f"{i+1}. Customer Name: {c_first} {c_middle} {c_last}")
                        print(f"   Balance: {c_balance}\tWarehouse: {w_name}\tDistrict: {d_name}")

                elif xactType == "R":
                    w_id, d_id, c_id = splitLine[1:]
                    
                    # Get Customer OrderItemLists: w_id, d_id, c_id , o_id , i_id
                    cursor.execute(f"""SELECT get_customer_order_items({w_id}, {d_id}, {c_id})""")
                    customerOrderItems = cursor.fetchall()
                    
                    # Get all other customers from other warehouse and district
                    cursor.execute(f"""SELECT get_other_customer_order_items({w_id}, {d_id}, {c_id})""")
                    otherCustomersOrderItems = cursor.fetchall()

                    commonCounter = Counter()
                    # For each other customer order
                    for orderItem in otherCustomersOrderItems:
                        processedOtherCustomerOrder = orderItem[0][1:-1].split(',')
                        # For each order in this customer
                        for customerOrderItem in customerOrderItems:
                            processedCustomerOrder = customerOrderItem[0][1:-1].split(',')
                            # Process the items in the order
                            otherCustomerOrderItems = processedOtherCustomerOrder[4].split('/')
                            thisCustomerOrderItems = processedCustomerOrder[4].split('/')

                            for otherCustomerOrderItem in otherCustomerOrderItems:
                                if otherCustomerOrderItem in thisCustomerOrderItems:
                                    commonCounter[tuple(processedOtherCustomerOrder[:4])] += 1

                    print(f'Customer: ({w_id}, {d_id}, {c_id})\nRelated Customer(s):')
                    index = 0
                    print(commonCounter)
                    for key in commonCounter.keys():
                        if commonCounter[key] >= 2:
                            w, d, c, _ = key
                            index += 1
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
