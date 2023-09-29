## Specific how to connect to a running Cassandra database

from cassandra.cluster import Cluster
from datetime import datetime

cluster = Cluster()
session = cluster.connect("wholesale")

new_order_prepared = session.prepare('SELECT * FROM new_order_by_wd WHERE w_id=? AND d_id=? AND c_id=?')
stock_item_info_prepared = session.prepare('SELECT * FROM stock_by_items WHERE i_id=? and w_id=?')
stock_item_update_prepared = session.prepare('UPDATE stock_by_items SET s_quantity=?, s_ytd=?, s_order_cnt=?, s_remote_cnt=? WHERE i_id=? AND w_id=?')
