## Write your transactions here

from connection import session
from connection import new_order_prepared
from connection import stock_item_info_prepared
from connection import stock_item_update_prepared
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


def delivery_transaction(w_id, carrier_id):


def order_status_transaction(c_w_id, c_d_id, c_id):


def stock_level_transaction(w_id, d_id, T, L):


def popular_item_transaction(w_id, d_id, L):


def top_balance_transaction():


def related_customer_transaction(c_w_id, c_d_id, c_id):


