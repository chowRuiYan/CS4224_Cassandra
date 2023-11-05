# load database from the respective csv files
import csv
from datetime import datetime

path = '/home/CS4224_Cassandra/project_files/data_files/' # Path to data files (Edit if necessary and remember to change the path in load_data.cql as well)
order = f'{path}order.csv'
order_line = f'{path}order-line.csv'

order1 = f'{path}order1.csv'
order_line1 = f'{path}order-line1.csv'


with open(order, 'r', newline='') as order_csv, open(order1, 'w', newline='') as order1_csv:
    o_reader = csv.reader(order_csv)
    writer = csv.writer(order1_csv)

    print("Transforming data for Order...")

    for row in o_reader:
        selected = row
        selected[4] = -1 if selected[4] == 'null' else selected[4]
        writer.writerow(selected)

with open(order_line, 'r', newline='') as order_line_csv, open(order_line1, 'w', newline='') as order_line1_csv:
    ol_reader = csv.reader(order_line_csv)
    writer = csv.writer(order_line1_csv)

    print("Transforming data for Orderline...")

    for row in ol_reader:
        selected = row
        selected[5] = datetime.now() if selected[5] == 'null' else selected[5]
        writer.writerow(selected)