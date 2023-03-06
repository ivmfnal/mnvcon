from IOVAPI import IOVDB
import sys, time, random
from datetime import datetime, timedelta

db = IOVDB(connstr=sys.argv[1])

folder = db.createFolder("folder_i", [('x','float'), ('n','int')],
        drop_existing = True, time_type = 'i')

for t in range(1,20,5):
    rows = { c: (1.0+c+100*t, 1+c+c*100*t) for c in range(10) } 
    folder.addData(t, rows)


for t in range(2, 20, 5):
    print t, f.getData(t)

