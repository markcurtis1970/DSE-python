#!/usr/bin/env python
import sys
import os
import logging
import hashlib
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra.query import dict_factory
from cassandra.metadata import protect_name
from random import shuffle

# Check arguments
# (note the count of arugments includes arg 0 which is this script!)
if len(sys.argv) != 6:
    print "\n***",sys.argv[0], "***\n"
    print 'Incorrect number of arguments, please run script as follows:'
    print '\n'+str(sys.argv[0])+' <ip address> <source_keyspace> <source_table> <target_keyspace> <target_table>'
    sys.exit(0)

# Initialise variables
ip_addr = sys.argv[1]
src_keyspace = sys.argv[2]
src_table =  sys.argv[3]
tgt_keyspace = sys.argv[4]
tgt_table = sys.argv[5]


# Scramble the data
# just a basic example
def scramble(word):
   word = list(word)
   shuffle(word)
   return ''.join(word)

# Scramble the data
# using a hashing function
def scramble_hash(word):
    hash_object = hashlib.md5(word)
    return hash_object.hexdigest()

# Create a prepared statement from any set of results
def createPrepStatement(source_data):
    for field in source_data[0]._fields:
        if 'field_list' in locals():
            field_list = field_list + ',' + protect_name(field) # See note1
            value_list = value_list + ',?'
        else:
            field_list = protect_name(field) # See note1
            value_list = '?'
    prep_statement = 'INSERT INTO ' + tgt_keyspace + '.' + tgt_table + ' (' + field_list + ') VALUES (' + value_list + ');'
    return prep_statement

# Write back a copy of each set of records into
# a copy table, scrambling text only
def write_copy(source_data, prep_statement):
    p_statement = session.prepare(prep_statement)
    for row in source_data:
        sc_field_list = [ ]
        for field in row._fields:
            if isinstance(getattr(row, field), (str, unicode)):
                scrambled_field = scramble_hash(str(getattr(row, field)))
            else:
                scrambled_field = getattr(row, field)
            sc_field_list.append(scrambled_field) # See note2
        # print p_statement, sc_field_list # uncomment to debug
        session.execute(p_statement, sc_field_list)

# Initialise cluster 
cluster = Cluster([ip_addr]) 
session = cluster.connect()

# Read the results from the source table first
# this will form the basis of what we write into
# the target table
# NOTE: you will probably want to remove the limit statement
cql = SimpleStatement('select * from ' + src_keyspace + '.' + src_table + ' LIMIT 100;', fetch_size=1000)
results = session.execute(cql)

# Create the prepared statement
prep_statement = createPrepStatement(results)

# Write out the results
write_copy(results, prep_statement)

