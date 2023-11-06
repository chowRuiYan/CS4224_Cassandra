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
                    pass
                elif xactType == "P":
                    pass

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
                    pass

                elif xactType == "T":
                    pass

                elif xactType == "R":
                    pass

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
