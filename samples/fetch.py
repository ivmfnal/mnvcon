import time, random, sys
from IOVAPI import IOVDB
from datetime import datetime

db = IOVDB(connstr = "dbname=iovs")
folder = db.openFolder("pedcal")


T0 = time.time()
t_query = datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S")
data = folder.getData(t_query)
iov = data.iov()
T1 = time.time()
data1 = folder.getData(iov[0])
iov1 = data1.iov()
T2 = time.time()
print data, iov
print 'Uncached fetch time: ', (T1-T0), 'seconds'
print data1, iov1
print 'Cached fetch time: ', (T2-T1), 'seconds'

