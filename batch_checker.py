from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
from cassandra.query import BatchType
from cassandra.query import SimpleStatement
from cassandra.cqlengine.columns import TimeUUID
import sys
import os
import time

def now():
    ts = str(time.time())
    return ts

cluster = Cluster(['10.240.0.11', '10.240.0.12'])
session = cluster.connect()
session.set_keyspace('myks')
session.default_consistency_level=ConsistencyLevel.LOCAL_QUORUM

# set vars
resultsa = []
resultsb = []
loop=int(sys.argv[1])

print '\nchecking values across tables...'

# select rows from tablea
rowsa = session.execute('SELECT col1, col2, col3 FROM tablea')

# check all rows in one table against the other 
for rowa in rowsa:
    rowsb = session.execute('SELECT col1, col2, col3 FROM tableb WHERE col1=\'' + rowa.col1 + '\';')
    for rowb in rowsb:
        # ideally we should only have the one row so we'll
        # do one compare and then break
        print 'checking key values: ' + str(rowa.col1), str(rowb.col1) + '         \r',
        if (rowa != rowb):
            print 'row mismatch! '+ str(rowa) + ' vs ' + str(rowb)
        break


session.shutdown()
