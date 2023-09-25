## Specific how to connect to a running Cassandra database

from cassandra.cluster import Cluster
from datetime import datetime

cluster = Cluster()
session = cluster.connect("wholesale")
