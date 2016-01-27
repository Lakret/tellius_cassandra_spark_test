from cassandra.cluster import *
import csv
from itertools import *
import datetime
from datetime import *

cluster = Cluster(['172.31.57.38', '172.31.57.39'], metrics_enabled=True)
session = cluster.connect()
session.set_keyspace('testairlines')
# use quorum consistency
session.default_consistency_level = 4

def queryAndTime(query):
  import time
  start = time.time()
  inserted = 0
  print "starting query..."

  res = session.execute(query)

  end = time.time()
  print "time to query ", end - start
  print res.current_rows

print("for uniquecarrier in a month, found sum of arrdelays:")
queryAndTime("select sum(arrdelay) from airlines where uniquecarrier = 'WN' and date > '2007-01-01' and date < '2007-01-31';")

print("for all months in the year 2007:")
months = [("2007-01-01", "2007-01-31"), ("2007-02-01", "2007-02-28"), ("2007-03-01", "2007-03-31"), ("2007-04-01", "2007-04-30"), ("2007-05-01", "2007-05-31"), ("2007-06-01", "2007-06-30"), ("2007-07-01", "2007-07-31"), ("2007-08-01", "2007-08-31"), ("2007-09-01", "2007-09-30"), ("2007-10-01", "2007-10-31"), ("2007-11-01", "2007-11-30"), ("2007-12-01", "2007-12-31")]
for (x, y) in months:
	queryAndTime("select sum(arrdelay) from airlines where uniquecarrier = 'WN' and date >= '" + x + "' and date <= '" + y + "';")
