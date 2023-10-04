## Specific how to connect to a running Cassandra database

from cassandra.cluster import Cluster
from datetime import datetime

cluster = Cluster()
session = cluster.connect("wholesale")

new_order_prepared = session.prepare('SELECT * FROM new_order_by_wd WHERE w_id=? AND d_id=? AND c_id=?')
stock_item_info_prepared = session.prepare('SELECT * FROM stock_by_items WHERE i_id=? and w_id=?')
stock_item_update_prepared = session.prepare('UPDATE stock_by_items SET s_quantity=?, s_ytd=?, s_order_cnt=?, s_remote_cnt=? WHERE i_id=? AND w_id=?')

## xact6
get_next_available_order_number = session.prepare('SELECT D_NEXT_O_ID FROM districts WHERE D_W_ID=? AND D_ID=?')

get_last_L_orders = session.prepare('SELECT O_ID, O_ENTRY_D, C_FIRST, C_MIDDLE, C_LAST, O_ITEM_QTY FROM xact_six_order_by_warehouse WHERE O_W_ID=? AND O_D_ID=? LIMIT ?')

## xact7
# Construct xact_seven table

# WIP: EXTREMELY BRUTE FORCE METHOD TO TEST
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