from cassandra.cluster import *
import csv
from itertools import *
import datetime
from datetime import *

def preprocessRow(row):
  def totime(s):
    return time(int(s[:2]), int(s[2:]))
  res = ""
  try:
    res = [ date(int(row[0]), int(row[1]), int(row[2])), int(row[3])] + map(totime, row[4:8]) + row[8:14] + [int(row[14]), int(row[15])] + row[16:]
  except:
    pass
  return res

def getRows(filename):
  with open(filename, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',')
    for row in spamreader:
      yield row

cluster = Cluster(['172.31.57.38', '172.31.57.39'], metrics_enabled=True)
session = cluster.connect()
session.set_keyspace('testairlines')

rows = chain(islice(getRows('2007.csv'), 1, None), islice(getRows('2008.csv'), 1, None))


# crate table 
session.execute("CREATE TABLE airlines3 (year text,month text,day text,dayofweek text, deptime text, crsdeptime text, arrtime text, crsarrtime text, uniquecarrier text, flightnum text, tailnum text, actualelapsedtime text, crselapsedtime text, airtime text, arrdelay text, depdelay text, origin text, dest text, distance text, taxiin text, taxiout text, cancelled text, cancellationcode text, diverted text, carrierdelay text, weatherdelay text, nasdelay text, securitydelay text, lateaircraftdelay text, PRIMARY KEY (uniquecarrier, year, month, day, flightnum));")

insert_stmt = session.prepare("INSERT INTO airlines3 (year, month, day, dayofweek, deptime, crsdeptime, arrtime, crsarrtime, uniquecarrier, flightnum, tailnum, \
actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, origin, dest, distance, taxiin, taxiout, cancelled, cancellationcode, \
diverted, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
insert_stmt.consistency_level = ConsistencyLevel.QUORUM

def insertAndTime():
  import time
  start = time.time()
  inserted = 0
  print "starting insert..."

  from cassandra.concurrent import execute_concurrent_with_args
  results = execute_concurrent_with_args(session, insert_stmt, rows, results_generator=True)
  # for row in rows:
    # session.execute_async(insert_stmt, row)
    # inserted = inserted + 1
    # if inserted % 1000 == 0:
      # print "inserted", inserted

  for (success, result) in results:
    if success:
      inserted = inserted + 1
    if inserted % 1000 == 0:
      print(inserted)

  end = time.time()
  print "time to insert", inserted, "rows: ", end - start

insertAndTime()
