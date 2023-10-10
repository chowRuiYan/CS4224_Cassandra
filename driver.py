## Driver program to gather transaction statistics
# TODO Update switch case for xact types
from create_db_tables import KEYSPACES, IP_ADDRESSES
from cassandra.cqlengine import connection
from transactions import *

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
                        # 
                        print('N')
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
    
    with open(f'results/{client}.csv', 'w') as f:
        file_writer = csv.writer(f)
        file_writer.writerow([client, numOfXacts, totalXactExecutionTime, xactThroughput, avgXactLat, \
                              medianXactLat, ninetyFifthPercentileXactLat, ninetyNinthPercentileXactLat
                            ])
