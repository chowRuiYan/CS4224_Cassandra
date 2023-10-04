## Specific how to connect to a running Cassandra database

from cassandra.cluster import Cluster
from datetime import datetime

cluster = Cluster()
session = cluster.connect("wholesale")

new_order_prepared = session.prepare('SELECT * FROM new_order_by_wd WHERE w_id=? AND d_id=? AND c_id=?')
stock_item_info_prepared = session.prepare('SELECT * FROM stock_by_items WHERE i_id=? and w_id=?')
stock_item_update_prepared = session.prepare('UPDATE stock_by_items SET s_quantity=?, s_ytd=?, s_order_cnt=?, s_remote_cnt=? WHERE i_id=? AND w_id=?')

get_next_available_order_number = session.prepare('SELECT D_NEXT_O_ID FROM xact_six_districts WHERE D_W_ID=? AND D_ID=?')
get_last_L_orders = session.prepare('SELECT O_ID, O_ENTRY_D, C_FIRST, C_MIDDLE, C_LAST, O_ITEM_QTY FROM xact_six_order_by_warehouse WHERE O_W_ID=? AND O_D_ID=? LIMIT ?')

get_top_10_customers_by_balance = session.prepare('SELECT * FROM xact_seven LIMIT 10')

get_customers_orders_items_t8 = session.prepare('SELECT O_ITEM_QTY FROM order_by_customer_t8 WHERE O_W_ID=? AND O_D_ID=? AND O_C_ID=?')
get_all_other_customers_t8 = session.prepare('SELECT * FROM order_by_customer_t8 WHERE O_W_ID!=? AND O_D_ID!=?')