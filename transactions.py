# Write your transactions here

from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from datetime import datetime

from connection import (
    session,
    Orderline,
    new_order_select,
    new_order_update,
    stock_by_item_select,
    stock_by_item_update,
    order_by_wd_insert,
    order_by_customer_insert,
    warehouse_select,
    warehouse_update,
    district_select,
    district_update,
    customer_by_wd_select,
    payment_customer_by_wd_update,
    order_by_wd_select_min,
    delivery_order_by_wd_update,
    delivery_order_by_customer_update,
    delivery_customer_by_wd_update,
    order_by_customer_select,
    last_L_orders_select,
    stock_by_item_select_quantity,
    top_10_customers_in_each_partition_select,
    order_by_customer_select_i_id_list,
    customer_by_wd_select_all
)

def new_order_transaction(c_id, w_id, d_id, num_items, item_numbers, supplier_warehouses, quantities):
    row = session.execute(new_order_select, [w_id, d_id, c_id]).one()
    w_id, d_id, c_id, c_last, c_discount, c_credit, next_o_id, w_tax, d_tax = row.w_id, row.d_id, row.c_id, row.c_last, row.c_discount, row.c_credit, row.next_o_id, row.w_tax, row.d_tax
    session.execute(new_order_update, [next_o_id+1, w_id, d_id])
    print(f"Customer: ({w_id}, {d_id}, {c_id})\tC_LAST: {c_last}\tC_CREDIT: {c_credit}\tC_DISCOUNT: {c_discount}")
    print(f"W_TAX: {w_tax}\tD_TAX: {d_tax}")
    entry_d = datetime.now()
    print(f"O_ID: {next_o_id}\tO_ENTRY_D: {entry_d}")
    total_amt = 0
    ol_output_string = []
    orderlines = []
    o_i_id_list = []
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for i in range(num_items):
        i_id, s_wid, qty = item_numbers[i], supplier_warehouses[i], quantities[i]
        row = session.execute(stock_by_item_select, [i_id, s_wid]).one()
        i_id, s_wid, s_qty, s_ytd, s_order_cnt, s_remote_cnt, i_name, i_price = row.i_id, row.w_id, row.s_quantity, row.s_ytd, row.s_order_cnt, row.s_remote_cnt, row.i_name, row.i_price
        s_qty = s_qty - qty + 100 if s_qty - qty < 10 else s_qty - qty
        s_ytd += qty
        s_order_cnt += 1
        s_remote_cnt = s_remote_cnt if s_wid == w_id else s_remote_cnt + 1
        item_amt = qty * i_price
        total_amt += item_amt
        batch.add(stock_by_item_update, [s_qty, s_ytd, s_order_cnt, s_remote_cnt, i_id, s_wid])

        orderline = Orderline(i+1, i_id, i_name, item_amt, s_wid, qty)
        orderlines.append(orderline)
        o_i_id_list.append(i_id)        
        ol_output_string.append(f"Item Number: {i_id}, I_NAME: {i_name}, Supplier Warehouse: {s_wid}, Quantity: {qty}, OL_AMOUNT: {item_amt}, S_QUANTITY: {s_qty}")
    
    batch.add(order_by_wd_insert, [w_id, d_id, next_o_id, c_id, total_amt, num_items, orderlines])
    batch.add(order_by_customer_insert, [w_id, d_id, c_id, next_o_id, entry_d, orderlines, o_i_id_list])
    session.execute(batch)
    total_amt = total_amt * (1 + w_tax + d_tax) * (1 - c_discount)
    print(f"Number Of Items: {num_items}\tTotal Amount: {total_amt}")
    print("\n".join(ol_output_string))
        

def payment_transaction(w_id, d_id, c_id, payment):
    row = session.execute(warehouse_select, [w_id]).one()
    w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd = row.w_street_1, row.w_street_2, row.w_city, row.w_state, row.w_zip, row.w_ytd
    
    row = session.execute(district_select, [w_id, d_id]).one()
    d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd = row.d_street_1, row.d_street_2, row.d_city, row.d_state, row.d_zip, row.d_ytd
    
    row = session.execute(customer_by_wd_select, [w_id, d_id, c_id]).one()
    w_id, d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_balance, c_ytd_payment, c_payment_cnt = row.w_id, row.d_id, row.c_id, row.c_first, row.c_middle, row.c_last, row.c_street_1, row.c_street_2, row.c_city, row.c_state, row.c_zip, row.c_phone, row.c_since, row.c_credit, row.c_credit_lim, row.c_balance, row.c_ytd_payment, row.c_payment_cnt
    
    session.execute(warehouse_update, [w_ytd+payment, w_id])
    session.execute(district_update, [d_ytd+payment, w_id, d_id])
    session.execute(payment_customer_by_wd_update, [c_balance-payment, c_ytd_payment+payment, c_payment_cnt+1, w_id, d_id, c_id])

    print(f"Customer Identifier: ({w_id}, {d_id}, {c_id})\tName: {c_first} {c_middle} {c_last}")
    print(f"Address: {c_street_1}, {c_street_2}, {c_city}, {c_state}, {c_zip}")
    print(f"Phone: {c_phone}\tSince: {c_since}")
    print(f"Credit: {c_credit}\tCredit Limit: {c_credit_lim}\tBalance: {c_balance-payment}")
    print(f"Warehouse Address: {w_street_1}, {w_street_2}, {w_city}, {w_state}, {w_zip}")
    print(f"District Address: {d_street_1}, {d_street_2}, {d_city}, {d_state}, {d_zip}")
    print(f"Payment Amount: {payment}")


def delivery_transaction(w_id, carrier_id):
    delivery_d = datetime.now()
    for d_id in range(1,11):
        row = session.execute(order_by_wd_select_min, [w_id, d_id]).one()
        if row:
            o_id, c_id, o_amount = row.o_id, row.c_id, row.o_amount

            row = session.execute(customer_by_wd_select, [w_id, d_id, c_id]).one()
            c_balance, c_delivery_cnt = row.c_balance, row.c_delivery_cnt
            new_balance = c_balance + o_amount

            session.execute(delivery_order_by_wd_update, [carrier_id, w_id, d_id, o_id])
            session.execute(delivery_order_by_customer_update, [carrier_id, delivery_d, w_id, d_id, c_id, o_id])
            session.execute(delivery_customer_by_wd_update, [new_balance, c_delivery_cnt+1, w_id, d_id, c_id])
            print(f"Customer To Deliver: ({w_id} {d_id} {c_id})\tBalance: {new_balance}")
        else:
            print(f"No Undelivered Order For District: ({w_id}, {d_id})")


def order_status_transaction(w_id, d_id, c_id):
    row = session.execute(order_by_customer_select, [w_id, d_id, c_id]).one()
    if row:
        o_id, o_carrier_id, o_entry_d, o_delivery_d, orderlines = row.o_id, row.o_carrier_id, row.o_entry_d, row.o_delivery_d, row.orderlines

        row = session.execute(customer_by_wd_select, [w_id, d_id, c_id]).one()
        c_first, c_middle, c_last, c_balance = row.c_first, row.c_middle, row.c_last, row.c_balance

        print(f"Customer: ({c_first} {c_middle} {c_last})\tBalance: {c_balance}")
        print(f"Customer Last Order: {o_id}\tO_ENTRY_D: {o_entry_d}\tO_CARRIER_ID: {o_carrier_id}")

        for orderline in orderlines:
            i_id, ol_supply_w_id, ol_quantity, ol_amount = orderline.i_id, orderline.ol_supply_w_id, orderline.ol_quantity, orderline.ol_amount
            print(f"{i_id} {ol_supply_w_id} {ol_quantity} {ol_amount} {o_delivery_d}")
    else:
        print(f"Customer ({w_id}, {d_id}, {c_id}) Has Not Made An Order")


def stock_level_transaction(w_id, d_id, T, L):
    S = session.execute(last_L_orders_select, [w_id, d_id, L])

    allItems = [o.ORDERLINES for o in S]
    allItemsID = [item.I_ID for item in allItems]
    count = 0

    for itemID in allItemsID:
        quantity = session.execute(stock_by_item_select_quantity, [itemID, w_id])

        if (quantity < T):
            count += 1

    print(f"Number Of Items Below Threshold: {count}")


def popular_item_transaction(w_id, d_id, L):
    print(f"District Identifier: ({w_id}, {d_id})")
    print(f"Number Of Last Orders To Be Examined: {L}")
    # Get last L orders
    L_orders = session.execute(last_L_orders_select, [w_id, d_id, L])
    
    # Stores distinct popular items
    popularSet = set()
    
    # Map to get total order count for each item
    itemMap = {}

    # Find popular items for each order
    for order in L_orders:
        o_id, o_c_id, o_entry_d, orderlines = order.o_id, order.o_c_id, order.o_entry_d, order.orderlines
        row = session.execute(customer_by_wd_select, [w_id, d_id, o_c_id]).one()
        c_first, c_middle, c_last = row.c_first, row.c_middle, row.c_last

        print(f"Order Number: {o_id}, O_ENTRY_D: {o_entry_d}")
        print(f"Customer Name: ({c_first} {c_middle} {c_last})")
        # Find most popular item in O
        popular_qty = 0
        # Used to get popular items from an order
        popularList = []
        for item in orderlines:
            i_name, ol_quantity = item.i_name, item.ol_quantity
            
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


def top_balance_transaction():
    top = list(session.execute(top_10_customers_in_each_partition_select))

    top10 = sorted(top, key=lambda x: x.c_balance, reverse=True)[:10]
    
    for i in range(10):
        t = top10[i]
        w_id, d_id, c_balance, c_id, c_first, c_middle, c_last = t.w_id, t.d_id, t.c_balance, t.c_id, t.c_first, t.c_middle, t.c_last
        row = session.execute(warehouse_select, [w_id]).one()
        w_name = row.w_name
        row = session.execute(district_select, [w_id, d_id]).one()
        d_name = row.d_name

        print(f"{i+1}. Customer Name: {c_first} {c_middle} {c_last}")
        print(f"   Balance: {c_balance}\tWarehouse: {w_name}\tDistrict: {d_name}")


def related_customer_transaction(w_id, d_id, c_id):
    # Get orders from customer in list<list<ITEM_QTY>>. Each element in the outer list represents one order
    # Each element in the inner list represents an item in that order
    customerOrderItemLists = session.execute(order_by_customer_select_i_id_list, [w_id, d_id, c_id])
    
    # To prevent duplicate results
    relatedCustomerSet = set()
    
    # Get all other customers from other warehouse and district 
    otherCustomers = session.execute(customer_by_wd_select_all)
    
    
    for otherCustomer in otherCustomers:
        o_w_id, o_d_id, o_c_id = otherCustomer.w_id, otherCustomer.d_id, otherCustomer.c_id
        if (o_w_id == w_id):
            continue

        otherCustomerOrderItemLists = session.execute(order_by_customer_select_i_id_list, [o_w_id, o_d_id, o_c_id])
        
        found = False
        # For each order in customer
        for custOrderItemList in customerOrderItemLists:
            # For each order in other customer
            for otherCustomerOrderItemList in otherCustomerOrderItemLists:
                if match(custOrderItemList, otherCustomerOrderItemList):
                    custIdTuple = (o_w_id, o_d_id, o_c_id)
                    relatedCustomerSet.add(custIdTuple)
                    found = True
                    break
            if found:
                break
    
    print(f'Customer: ({w_id}, {d_id}, {c_id})\nRelated Customer(s):')
    index = 0
    for custId in relatedCustomerSet:
        index += 1
        w, d, c = custId
        print(f'{index}. ({w}, {d}, {c})')

def match(list1, list2):
    count = 0
    for id1 in list1:
        for id2 in list2:
            if id1 == id2:
                count += 1
                if count >= 2:
                    return True
    return False
