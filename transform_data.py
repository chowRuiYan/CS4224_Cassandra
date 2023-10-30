# load database from the respective csv files
import csv
import decimal

path = 'project_files/data_files/' # Path to data files (Edit if necessary and remember to change the path in load_data.cql as well)
customer = f'{path}customer.csv'
district = f'{path}district.csv'
item = f'{path}item.csv'
orderline = f'{path}order-line.csv'
order = f'{path}order.csv'
stock = f'{path}stock.csv'
warehouse = f'{path}warehouse.csv'

district1 = f'{path}district1.csv'
customer_by_wd = f'{path}customer_by_wd.csv'
info_for_new_order = f'{path}info_for_new_order.csv' # t1
stock_by_item = f'{path}stock_by_item.csv'           # t2
order_by_wd = f'{path}order_by_wd.csv'               # t3
order_by_customer = f'{path}order_by_customer.csv'   # t4


t1_required_columns = [0,1,2,5,15,13]
t2_required_columns = [1,0,2,3,4,5]
t3_required_columns = [0,1,2,3,7,4,5]
t4_required_columns = [0,1,3,2,7,4]

with open(district, 'r', newline='') as district_csv, open(district1, 'w', newline='') as district1_csv:
    d_reader = csv.reader(district_csv)
    writer = csv.writer(district1_csv)

    print("Transforming data for District...")

    for row in d_reader:
        selected = row[:-1]
        writer.writerow(selected)

with open(customer, 'r', newline='') as customer_csv, open(customer_by_wd, 'w', newline='') as customer_by_wd_csv:
    c_reader = csv.reader(customer_csv)
    writer = csv.writer(customer_by_wd_csv)

    print("Transforming data for Customer_by_WD...")

    for row in c_reader:
        selected = row[:-1]
        writer.writerow(selected)

with open(warehouse, 'r', newline='') as warehouse_csv, open(district, 'r', newline='') as district_csv, open(customer, 'r', newline='') as customer_csv, open(info_for_new_order, 'w', newline='') as info_for_new_order_csv:
    w_reader = csv.reader(warehouse_csv)
    d_reader = csv.reader(district_csv)
    c_reader = csv.reader(customer_csv)
    writer = csv.writer(info_for_new_order_csv)

    print("Transforming data for Info_for_New_Order...")

    w_tax = {}
    d_tax = {}
    d_next_o_id = {}

    for row in w_reader:
        w_tax[row[0]] = row[7]
    
    for row in d_reader:
        d_tax[(row[0], row[1])] = row[8]
        d_next_o_id[(row[0], row[1])] = row[10]

    for row in c_reader:
        selected = [row[i] for i in t1_required_columns]
        selected.append(d_next_o_id[(row[0], row[1])])
        selected.append(w_tax[row[0]])
        selected.append(d_tax[(row[0], row[1])])

        writer.writerow(selected)

with open(stock, 'r', newline='') as stock_csv, open(item, 'r', newline='') as item_csv, open(stock_by_item, 'w', newline='') as stock_by_item_csv:
    i_reader = csv.reader(item_csv)
    s_reader = csv.reader(stock_csv)
    writer = csv.writer(stock_by_item_csv)

    print("Transforming data for Stock_by_Item...")

    i_name = {}
    i_price = {}

    for row in i_reader:
        i_name[row[0]] = row[1]
        i_price[row[0]] = row[2]

    for row in s_reader:
        selected = [row[i] for i in t2_required_columns]
        selected.append(i_name[row[0]])
        selected.append(i_price[row[0]])

        writer.writerow(selected)

with open(item, 'r', newline='') as item_csv, open(order, 'r', newline='') as order_csv, open(orderline, 'r', newline='') as orderline_csv, open(order_by_wd, 'w', newline='') as order_by_wd_csv, open(order_by_customer, 'w', newline='') as order_by_customer_csv:
    i_reader = csv.reader(item_csv)
    o_reader = csv.reader(order_csv)
    ol_reader = csv.reader(orderline_csv)
    writer1 = csv.writer(order_by_wd_csv)
    writer2 = csv.writer(order_by_customer_csv)

    print("Transforming data for Order_by_WD...")

    name = {}
    for row in i_reader:
        name[row[0]] = row[1]

    order_ol = {}
    order_i_list = {}
    order_amount = {}
    delivery_d = {}

    for row in ol_reader:
        w_id, d_id, o_id = row[:3]
        if (w_id, d_id, o_id) not in order_ol.keys():
            order_ol[(w_id, d_id, o_id)] = []
            order_i_list[(w_id, d_id, o_id)] = []
            order_amount[(w_id, d_id, o_id)] = 0
            delivery_d[(w_id, d_id, o_id)] = row[5]
        
        ol_number, i_id, i_name, ol_amount, ol_supply_w_id, ol_quantity = row[3], row[4], name[row[4]], row[6], row[7], row[8]
        ol = f'{{ol_number: {ol_number}, i_id: {i_id}, i_name: {i_name}, ol_amount: {ol_amount}, ol_supply_w_id: {ol_supply_w_id}, ol_quantity: {ol_quantity}}}'
        order_ol[(w_id, d_id, o_id)].append(ol)
        order_i_list[(w_id, d_id, o_id)].append(int(i_id))
        order_amount[(w_id, d_id, o_id)] += decimal.Decimal(ol_amount)

    print("Transforming data for Order_by_Customer...")

    for row in o_reader:
        w_id, d_id, o_id = row[:3]
        selected1 = [row[i] for i in t3_required_columns]
        selected2 = [row[i] for i in t4_required_columns]

        selected1[4] = -1 if selected1[4] == 'null' else selected1[4]
        selected2[5] = -1 if selected2[5] == 'null' else selected2[5]

        selected1.append(order_amount[(w_id, d_id, o_id)])
        selected1.append(order_ol[(w_id, d_id, o_id)])

        selected2.append(delivery_d[(w_id, d_id, o_id)])
        selected2.append(order_ol[(w_id, d_id, o_id)])
        selected2.append(order_i_list[(w_id, d_id, o_id)])

        writer1.writerow(selected1)
        writer2.writerow(selected2)
