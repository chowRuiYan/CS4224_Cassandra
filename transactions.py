## Write your transactions here

from connection import session
from datetime import datetime

from connection import (
    payment_warehouse_select,
    payment_warehouse_update,
    payment_district_select,
    payment_district_update,
    payment_customer_by_wd_select,
    payment_customer_by_wd_update,
    delivery_order_by_wd_select_min,
    delivery_order_by_wd_select_cust,
    delivery_orderline_by_order_select,
    delivery_order_by_wd_update,
    delivery_customer_by_wd_update
)

def new_order_transaction(c_id, w_id, d_id, num_items, item_numbers, supplier_warehouses, quantities):
    pass

def payment_transaction(c_w_id, c_d_id, c_id, payment):
    
    w_id, w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd = session.execute(payment_warehouse_select, [c_w_id])
    d_w_id, d_id, d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd = session.execute(payment_district_select, [c_w_id, c_d_id])
    c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt = session.execute(payment_customer_by_wd_select, [c_w_id, c_d_id, c_id])
    
    session.execute(payment_warehouse_update, [payment, c_w_id])
    session.execute(payment_district_update, [payment, c_w_id, c_d_id])
    session.execute(payment_customer_by_wd_update, [payment, payment, c_w_id, c_d_id, c_id])

    print(f"Customer Identifier: {c_w_id}, {c_d_id}, {c_id}\nName: {c_first}, {c_middle}, {c_last}")
    print(f"Address: {c_street_1}, {c_street_2}, {c_city}, {c_state}, {c_zip}")
    print(f"Phone: {c_phone}\nSince: {c_since}\nCredit: {c_credit}\nCredit Limit: {c_credit_lim}\nBalance: {c_balance}\n")
    print(f"Warehouse Address: {w_street_1}, {w_street_2}, {w_city}, {w_state}, {w_zip}")
    print(f"District Address: {d_street_1}, {d_street_2}, {d_city}, {d_state}, {d_zip}")
    print(f"Payment Amount: {payment}")


def delivery_transaction(w_id, carrier_id):
    for d_id in range(1,11):
        o_id = session.execute(delivery_order_by_wd_select_min, [w_id, d_id])
        c_id = session.execute(delivery_order_by_wd_select_cust, [w_id, d_id, o_id])
        ol_amount_sum = session.execute(delivery_orderline_by_order_select, [w_id, d_id, o_id])

        session.execute(delivery_order_by_wd_update, [carrier_id, datetime.now(), w_id, d_id, o_id])
        session.execute(delivery_customer_by_wd_update, [ol_amount_sum, w_id, d_id, c_id])


def order_status_transaction(c_w_id, c_d_id, c_id):
    pass

def stock_level_transaction(w_id, d_id, T, L):
    pass

def popular_item_transaction(w_id, d_id, L):
    pass

def top_balance_transaction():
    pass

def related_customer_transaction(c_w_id, c_d_id, c_id):
    pass

