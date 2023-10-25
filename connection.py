# Specific how to connect to a running Cassandra database

from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect("cs4224_keyspace")

class Orderline(object):
    
    def __init__(self, ol_number, i_id, i_name, ol_amount, ol_supply_w_id, ol_quantity):
        self.ol_number = ol_number
        self.i_id = i_id
        self.i_name = i_name
        self.ol_amount = ol_amount
        self.ol_supply_w_id = ol_supply_w_id
        self.ol_quantity = ol_quantity

cluster.register_user_type('cs4224_keyspace', 'orderline', Orderline)

new_order_select = session.prepare(
    'SELECT * FROM info_for_new_order WHERE w_id=? AND d_id=? AND c_id=?')

new_order_update = session.prepare(
    'UPDATE info_for_new_order SET next_o_id=? WHERE w_id=? AND d_id=?')

stock_by_item_select = session.prepare(
    'SELECT * FROM stock_by_item WHERE i_id=? and w_id=?')

stock_by_item_update = session.prepare(
    'UPDATE stock_by_item SET s_quantity=?, s_ytd=?, s_order_cnt=?, s_remote_cnt=? WHERE i_id=? AND w_id=?')

order_by_wd_insert = session.prepare(
    'INSERT INTO order_by_wd (w_id, d_id, o_id, c_id, o_amount, o_ol_cnt, orderlines, o_carrier_id) VALUES (?, ?, ?, ?, ?, ?, ?, -1)')

order_by_customer_insert = session.prepare(
    'INSERT INTO order_by_customer (w_id, d_id, c_id, o_id, o_entry_d, orderlines, o_i_id_list, o_carrier_id) VALUES (?, ?, ?, ?, ?, ?, ?, -1)')

warehouse_select = session.prepare(
    'SELECT * FROM warehouse WHERE w_id=?')

warehouse_update = session.prepare(
    'UPDATE warehouse SET w_ytd=? WHERE w_id=?')

district_select = session.prepare(
    'SELECT * FROM district WHERE w_id=? AND d_id=?')

district_update = session.prepare(
    'UPDATE district SET d_ytd=? WHERE w_id=? AND d_id=?')

customer_by_wd_select = session.prepare(
    'SELECT * FROM customer_by_wd WHERE w_id=? AND d_id=? AND c_id=?')

payment_customer_by_wd_update = session.prepare(
    'UPDATE customer_by_wd SET c_balance=?, c_ytd_payment=?, c_payment_cnt=? WHERE w_id=? AND d_id=? AND c_id=?')

order_by_wd_select_min = session.prepare(
    'SELECT * FROM order_by_wd WHERE w_id=? AND d_id=? AND o_carrier_id=-1 LIMIT 1 ALLOW FILTERING')

delivery_order_by_wd_update = session.prepare(
    'UPDATE order_by_wd SET o_carrier_id=? WHERE w_id=? AND d_id=? AND o_id=?')

delivery_order_by_customer_update = session.prepare(
    'UPDATE order_by_customer SET o_carrier_id=?, o_delivery_d=? WHERE w_id=? AND d_id=? AND c_id=? AND o_id=?')

delivery_customer_by_wd_update = session.prepare(
    'UPDATE customer_by_wd SET c_balance=?, c_delivery_cnt=? WHERE w_id=? AND d_id=? AND c_id=?')

order_by_customer_select = session.prepare(
    'SELECT * FROM order_by_customer WHERE w_id=? AND d_id=? AND c_id=? LIMIT 1')

last_L_orders_select = session.prepare(
    'SELECT * FROM order_by_oid WHERE w_id=? AND d_id=? LIMIT ?')

stock_by_item_select_quantity = session.prepare(
    'SELECT s_quantity FROM stock_by_item WHERE i_id=? AND w_id=?')

top_10_customers_in_each_partition_select = session.prepare(
    'SELECT * FROM customer_by_balance PER PARTITION LIMIT 10')

order_by_customer_select_i_id_list = session.prepare(
    'SELECT o_i_id_list FROM order_by_customer WHERE w_id=? and d_id=? AND c_id=?')

customer_by_wd_select_all = session.prepare(
    'SELECT * FROM customer_by_wd')
