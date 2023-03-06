from IOVAPI import IOVDB
import sys, time, random
from datetime import datetime, timedelta

db = IOVDB(connstr=sys.argv[1])

folders = {}
for typ in ['t', 'tz', 'i', 'f']:
    name = "folder_"+typ
    folders[typ] = db.createFolder(name, [('x','float'), ('n','int')],
        drop_existing = True, time_type = typ)

for t in range(1, 20, 5):
    rows = { c: (1.0+c, 1+c+c) for c in range(10) }
    for typ, f in folders.items():
        f.addData(t, rows)


for t in range(2, 20, 5):
    print "t=", t
    for typ, f in folders.items():
        print typ, f.getTuple(t, 5)

