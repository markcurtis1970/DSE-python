from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.query import BatchType
from cassandra.query import SimpleStatement
from cassandra.cqlengine.columns import TimeUUID
import sys
import os
import time

# Argument checks
if len(sys.argv) != 2:
    print "\n***",sys.argv[0], "***\n"
    print 'Incorrect number of arguments, please run script as follows:'
    print '\n'+str(sys.argv[0])+' <number of iterations>'
    sys.exit(0)

def now():
    ts = str(time.time())
    return ts

cluster = Cluster(['10.240.0.11', '10.240.0.12'])
session = cluster.connect()
session.set_keyspace('myks')

# set vars
resultsa = []
resultsb = []
loop=int(sys.argv[1])

print '\nlooping through ' + str(loop) + ' iterations'

# set batch statments
for idx in range(1, loop):
    val = str(idx)
    timest = now()
    batch = BatchStatement(batch_type=BatchType.LOGGED)
    batch.add(SimpleStatement("insert into tablea (col1, col2, col3) VALUES (%s, %s, %s)"), (timest, val, val))
    batch.add(SimpleStatement("insert into tableb (col1, col2, col3) VALUES (%s, %s, %s)"), (timest, val, val))
    session.execute(batch)

print '\ninserts all done...'
session.shutdown()
