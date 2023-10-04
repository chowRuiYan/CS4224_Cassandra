## Specific how to connect to a running Cassandra database

from cassandra.cluster import Cluster
from datetime import datetime

cluster = Cluster()
session = cluster.connect("wholesale")

payment_warehouse_select = session.prepare('SELECT * FROM warehouse WHERE w_id=?')
payment_warehouse_update = session.prepare('UPDATE warehouse SET w_ytd=w_ytd+? WHERE w_id=?')
payment_district_select = session.prepare('SELECT * FROM district WHERE d_w_id=? AND d_id=?')
payment_district_update = session.prepare('UPDATE district SET d_ytd=d_ytd+? WHERE d_w_id=? AND d_id=?')
payment_customer_by_wd_select = session.prepare('SELECT * FROM customer_by_wd WHERE c_w_id=? AND c_d_id=? AND c_id=?')
payment_customer_by_wd_update = session.prepare('UPDATE customer_by_wd SET c_balance=c_balance-?, c_ytd_payment=c_ytd_payment+?, c_payment_cnt=c_payment_cnt+1 WHERE c_w_id=? AND c_d_id=? AND c_id=?')

delivery_order_by_wd_select_min = session.prepare('SELECT MIN(o_id) FROM order_by_wd WHERE o_w_id=? AND o_d_id=? AND o_carrier_id IS NULL')
delivery_order_by_wd_select_cust = session.prepare('SELECT o_c_id FROM order_by_wd WHERE o_w_id=? AND o_d_id=? AND o_id=?')
delivery_orderline_by_order_select = session.prepare('SELECT SUM(ol_amount) FROM orderline_by_order WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id=?')
delivery_order_by_wd_update = session.prepare('UPDATE order_by_wd SET o_carrier_id=? AND ol_delivery_d=? WHERE o_w_id=? AND o_d_id=? AND o_id=?')
delivery_customer_by_wd_update = session.prepare('UPDATE customer_by_wd SET c_balance=c_balance+? AND c_delivery_cnt=c_delivery_cnt+1 WHERE c_w_id=? AND c_d_id=? AND c_id=?')