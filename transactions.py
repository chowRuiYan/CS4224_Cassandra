## Write your transactions here

from connection import session
from connection import new_order_prepared
from connection import stock_item_info_prepared
from connection import stock_item_update_prepared
from connection import get_next_available_order_number
from connection import get_last_L_orders
from connection import get_top_10_customers_by_balance
from connection import get_customers_orders_items_t8
from connection import get_all_other_customers_t8
from datetime import datetime

def new_order_transaction(c_id, w_id, d_id, num_items, item_numbers, supplier_warehouses, quantities):
    w_id, d_id, c_id, c_last, c_discount, c_credit, next_o_id, w_tax, d_tax = session.execute(new_order_prepared, [w_id, d_id, c_id])
    print(f"Customer: ({w_id}, {d_id}, {c_id}), C_LAST: {c_last}, C_CREDIT: {c_credit}, C_DISCOUNT: {c_discount}")
    print(f"W_TAX: {w_tax}, D_TAX: {d_tax}")
    entry_d = datetime.now()
    print(f"O_ID: {next_o_id}, O_ENTRY_D: {entry_d}")
    total_amt = 0
    ol_output_string = []
    update_stock_by_item = []
    create_ol_for_o = []
    for i in range(num_items):
        i_id, s_wid, qty = item_numbers[i], supplier_warehouses[i], quantities[i]
        i_id, s_wid, s_qty, s_ytd, s_order_cnt, s_remote_cnt, i_name, i_price = session.execute(stock_item_info_prepared, [i_id, s_wid])
        s_qty = s_qty - qty + 100 if s_qty - qty < 10 else s_qty - qty
        s_ytd += qty
        s_order_cnt += 1
        s_remote_cnt = s_remote_cnt if s_wid == w_id else s_remote_cnt + 1
        item_amt = qty * i_price
        total_amt += item_amt
        update_stock_by_item.append([s_qty, s_ytd, s_order_cnt, s_remote_cnt, i_id, s_wid])
        ol_output_string.append(f"Item Number: {i_id}, I_NAME: {i_name}, Supplier Warehouse: {s_wid}, Quantity: {qty}, OL_AMOUNT: {item_amt}, S_QUANTITY: {s_qty}")
    # update stock_by_item table
    # create new order line
    # create new order
    total_amt = total_amt * (1 + w_tax + d_tax) * (1 - c_discount)
    print(f"NUM_ITEMS: {num_items}, TOTAL_AMOUNT: {total_amt}")
    print("\n".join(ol_output_string))
        

def payment_transaction(c_w_id, c_d_id, c_id, payment):
    return


def delivery_transaction(w_id, carrier_id):
    return


def order_status_transaction(c_w_id, c_d_id, c_id):
    return


def stock_level_transaction(w_id, d_id, T, L):
    return


def popular_item_transaction(w_id, d_id, L):
    print(f"District identifier ({w_id}, {d_id})")
    print(f"Number of last orders to be examined {L}")
    # Get next order id
    d_next_o_id = session.execute(get_next_available_order_number, [w_id, d_id])
    # Get last L orders
    L_orders = session.execute(get_last_L_orders, [w_id, d_id, L])
    
    # Stores distinct popular items
    popularSet = set()
    
    # Map to get total order count for each item
    itemMap = {}

    # Find popular items for each order
    for order in L_orders:
        o_id, o_entry_d, c_first, c_middle, c_last, o_item_qty = order
        print(f"Order number: {o_id}, Entry Data and Time: {o_entry_d}")
        print(f"Customer Name: ({c_first}, {c_middle}, {c_last})")
        # Find most popular item in O
        popular_qty = 0
        # Used to get popular items from an order
        popularList = []
        for item in o_item_qty:
            i_name = item['I_NAME']
            ol_quantity = item['OL_QUANTITY']
            
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
            print(f"Item name {i[0]}, quantity ordered {i[1]}")
        
    # Find percentage that contain each popular item
    for item in popularSet:
        percentage = (itemMap[item] / L) * 100
        print(f"Item: {item}, Percentage: {percentage}")


def top_balance_transaction():
    top10 = session.execute(get_top_10_customers_by_balance)
    
    for cust in top10:
        c_w_id, c_d_id, c_balance, c_id, c_first, c_middle, c_last, d_name, w_name = cust
        print(f"Customer Name: ({c_first}, {c_middle}, {c_last})")
        print(f"Balance: {c_balance}")
        print(f"Warehouse: {w_name}")
        print(f"District: {d_name}")


def related_customer_transaction(c_w_id, c_d_id, c_id):
    # Get orders from customer in list<list<ITEM_QTY>>. Each element in the outer list represents one order
    # Each element in the inner list represents an item in that order
    customerOrderItemLists = session.execute(get_customers_orders_items_t8, [c_w_id, c_d_id, c_id])

    # Convert above into list<list<I_NAME>>
    customerOrderItemNames = [[item['I_NAME'] for item in order] for order in customerOrderItemLists]
    
    # Get all other customers from other warehouse and district 
    otherCustomersOrders = session.execute(get_all_other_customers_t8, [c_w_id, c_d_id])
    
    # To prevent duplicate results
    relatedCustomerSet = set()
    
    for otherOrder in otherCustomersOrders:
        o_w_id, o_d_id, o_c_id, o_id, o_item_qty = otherOrder
        
        custIdTuple = (o_w_id, o_d_id, o_c_id)
        if custIdTuple in relatedCustomerSet:
            continue
        
        found = False
        for custOrder in customerOrderItemNames:
            sameItemCount = 0
            # For each order
            for item in o_item_qty:
                if (item['I_NAME'] in custOrder):
                    sameItemCount += 1
                    if (sameItemCount == 2):
                        print(f"Customer: {o_w_id}, {o_d_id}, {o_c_id}")
                        relatedCustomerSet.add(custIdTuple)
                        found = True
                if found:
                    break
            if found:
                break    
                    
            

