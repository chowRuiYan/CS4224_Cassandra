# Specific how to connect to a running Cassandra database
# Prepares Xacts

from cassandra.cluster import Cluster
from datetime import datetime

hosts = [('172.17.0.2', 7000)]
cluster = Cluster(hosts)
session = cluster.connect()
session.execute("USE cs4224_keyspace")

# xact 1
new_order_prepared = session.prepare('SELECT * FROM new_order_by_wd WHERE w_id=? AND d_id=? AND c_id=?')
stock_item_info_prepared = session.prepare('SELECT * FROM stock_by_items WHERE i_id=? and w_id=?')
stock_item_update_prepared = session.prepare('UPDATE stock_by_items SET s_quantity=?, s_ytd=?, s_order_cnt=?, s_remote_cnt=? WHERE i_id=? AND w_id=?')

# xact 2
payment_warehouse_select = session.prepare('SELECT * FROM warehouse WHERE w_id=?')
payment_warehouse_update = session.prepare('UPDATE warehouse SET w_ytd=w_ytd+? WHERE w_id=?')
payment_district_select = session.prepare('SELECT * FROM district WHERE d_w_id=? AND d_id=?')
payment_district_update = session.prepare('UPDATE district SET d_ytd=d_ytd+? WHERE d_w_id=? AND d_id=?')
payment_customer_by_wd_select = session.prepare('SELECT * FROM customer_by_wd WHERE c_w_id=? AND c_d_id=? AND c_id=?')
payment_customer_by_wd_update = session.prepare('UPDATE customer_by_wd SET c_balance=c_balance-?, c_ytd_payment=c_ytd_payment+?, c_payment_cnt=c_payment_cnt+1 WHERE c_w_id=? AND c_d_id=? AND c_id=?')

# xact 3
delivery_order_by_wd_select_min = session.prepare('SELECT MIN(o_id) FROM order_by_wd WHERE o_w_id=? AND o_d_id=? AND o_carrier_id IS NULL')
delivery_order_by_wd_select_cust = session.prepare('SELECT o_c_id FROM order_by_wd WHERE o_w_id=? AND o_d_id=? AND o_id=?')
delivery_orderline_by_order_select = session.prepare('SELECT SUM(ol_amount) FROM orderline_by_order WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id=?')
delivery_order_by_wd_update = session.prepare('UPDATE order_by_wd SET o_carrier_id=? AND ol_delivery_d=? WHERE o_w_id=? AND o_d_id=? AND o_id=?')
delivery_customer_by_wd_update = session.prepare('UPDATE customer_by_wd SET c_balance=c_balance+? AND c_delivery_cnt=c_delivery_cnt+1 WHERE c_w_id=? AND c_d_id=? AND c_id=?')

# xact 4
get_customer_last_order_prepared = session.prepare(
    'SELECT * FROM orders_by_customers WHERE O_W_ID=? AND O_D_ID=? AND O_C_ID=? LIMIT ?')

# xact 5
get_next_available_order_number_prepared = session.prepare(
    'SELECT D_NEXT_O_ID FROM district WHERE D_W_ID=? AND D_ID=?')

get_last_L_orders_prepared = session.prepare(
    'SELECT ORDER_ITEMS FROM orders_by_warehouse WHERE O_W_ID=? AND O_D_ID=? AND O_ID>=? AND O_ID<?')

get_stock_quantity_prepared = session.prepare(
    'SELECT S_QUANTITY FROM stock WHERE S_W_ID=? AND S_I_ID=?')

## xact6
get_next_available_order_number = session.prepare('SELECT D_NEXT_O_ID FROM districts WHERE D_W_ID=? AND D_ID=?')

get_last_L_orders = session.prepare('SELECT O_ID, O_ENTRY_D, C_FIRST, C_MIDDLE, C_LAST, O_ITEM_QTY FROM xact_six_order_by_warehouse WHERE O_W_ID=? AND O_D_ID=? LIMIT ?')

## xact7
# Construct xact_seven table
# WIP: EXTREMELY BRUTE FORCE METHOD TO TEST

# Prepare get d_name and w_name queries
get_d_name = session.prepare('SELECT D_NAME FROM districts WHERE D_ID=?')
get_w_name = session.prepare('SELECT W_NAME FROM warehouse WHERE W_ID=?')

# Get all customer in a resultSet of namedTuple 
custs = session.execute('SELECT C_W_ID, C_D_ID, C_BALANCE, C_ID, C_FIRST, C_MIDDLE, C_LAST FROM customers')
for cust in custs:
    d_name = session.execute(get_d_name, [cust.C_D_ID])
    w_name = session.execute(get_w_name, [cust.C_W_ID])
    session.execute("INSERT INTO xact_seven (C_W_ID, C_D_ID, C_BALANCE, C_ID, C_FIRST, C_MIDDLE, C_LAST, D_NAME, W_NAME,) VALUES (%s)", \
                    [cust.C_W_ID, cust.C_D_ID, cust.C_BALANCE, cust.C_ID, cust.C_FIRST, cust.C_MIDDLE, cust.C_LAST, d_name, w_name])

# Final output
get_top_10_customers_by_balance = session.prepare('SELECT * FROM xact_seven LIMIT 10')

## xact8
get_customers_orders_items_t8 = session.prepare('SELECT O_ITEM_QTY FROM order_by_customer_t8 WHERE O_W_ID=? AND O_D_ID=? AND O_C_ID=?')
get_all_other_customers_t8 = session.prepare('SELECT * FROM order_by_customer_t8 WHERE O_W_ID!=? AND O_D_ID!=?')
