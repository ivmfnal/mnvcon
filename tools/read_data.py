import time, random, sys, getopt
from IOVAPI import IOVDB
from datetime import datetime

Usage = """
python read_data.py [options] <database name> <folder_name> <column>,...
options:
    -h <host>
    -p <port>
    -U <user>
    -w <password>
    
    -n do not print values, print only number of rows retruned

    -t <time>   default = now
    -t <time0>...<time1>
    -T <tag>    
"""

opts, args = getopt.getopt(sys.argv[1:], 'h:U:w:p:nt:T:')

t0 = time.time()
t1 = t0
tag = None
print_values = True
dbcon = []

if len(args) < 3 or args[0] == 'help':
    print(Usage)
    sys.exit(0)

for opt, val in opts:
    if opt == '-h':         dbcon.append("host=%s" % (val,))
    elif opt == '-p':       dbcon.append("port=%s" % (int(val),))
    elif opt == '-U':       dbcon.append("user=%s" % (val,))
    elif opt == '-w':       dbcon.append("password=%s" % (val,))
    elif opt == '-t':     
        words = val.split('...',1)
        t0 = float(words[0])
        t1 = t0
        if len(words) > 1:
            t1 = float(words[1])
    elif opt == '-T':       tag = val
    elif opt == '-n':       print_values = False
    
dbcon.append("dbname=%s" % (args[0],))
dbcon = ' '.join(dbcon)

tname = args[1]
columns = args[2].split(',')

db = IOVDB(connstr=dbcon)
folder = db.openFolder(tname, columns)

if t1 > t0:
    snapshots = folder.getDataInterval(t0, t1, tag=tag)
else:
    snapshots = [folder.getData(t0, tag=tag)]

if print_values:
    for s in snapshots:
        f, t = s.iov()
        for tup in s.asList():
            print(tup[0], f, tup[1:])
else:
    n = 0;
    for s in snapshots:
        n += len(s.asList())
    print(n, "rows in", len(snapshots),"snapshots")
