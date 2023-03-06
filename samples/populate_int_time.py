from IOVAPI import IOVDB
import sys, time, random
from datetime import datetime, timedelta

db = IOVDB(connstr=sys.argv[1])

folder = db.createFolder("folder_i", [('x','float'), ('n','int')],
        drop_existing = True, time_type = 'i')

for t in range(1,20,5):
    rows = { c: (1.0+c+1000*t, 1+c+c+1000*t) for c in range(10) } 
    folder.addData(t, rows)


for t in range(2, 20, 5):
    snapshot = folder.getData(t)
    print "request time=%d, validity (from,to): %s" % (t, snapshot.iov())
    for row in snapshot.asList():
        print row

