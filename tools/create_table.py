from IOVAPI import IOVDB
import sys, getopt, psycopg2

Usage = """
python create_table.py [options] <database name> <folder_name> <column>:<type> [...]
options:
    -h <host>
    -p <port>
    -U <user>
    -w <password>
    -n <namespace>       default "public"
       alternatively, the namespace can be specified as part of folder_name, e.g.: my_namespace.my_folder
    
    -t - time column type. Accepted values are:
         t  - time with the DB implied time zone 
         tz - time with explicit time zone (default)
         f  - floating point
         i  - long integer
    -c - force create, drop existing tables
    -R <db user>,... - DB users to grant read permissions to
    -W <db user>,... - DB users to grant write permissions to
"""

host = None
port = None
user = None
password = None
columns = []
grants_r = []
grants_w = []
drop_existing = False
time_type = "tz"

dbcon = []

opts, args = getopt.getopt(sys.argv[1:], 'h:U:w:p:cR:W:t:n:')

if len(args) < 3 or args[0] == 'help':
    print(Usage)
    sys.exit(0)
    
opts = dict(opts)
dbcon = []
if "-h" in opts:    dbcon.append("host=%s" % (opts["-h"],))
if "-p" in opts:    dbcon.append("port=%s" % (opts["-p"],))
if "-U" in opts:    dbcon.append("user=%s" % (opts["-U"],))
if "-w" in opts:    dbcon.append("password=%s" % (opts["-w"],))

drop_existing = "-c" in opts
if "-R" in opts:    grants_r = opts["-R"].split(',')
if "-W" in opts:    grants_w = opts["-W"].split(',')
time_type = opts.get("-t", "tz")
namespace = opts.get("-n")
                                
dbcon.append("dbname=%s" % (args[0],))

dbcon = ' '.join(dbcon)
tname = args[1]

if namespace is None:
    if "." in tname:
        namespace, tname = tname.split(".", 1)
    else:
        namespace = "public"

ctypes = []
for w in args[2:]:
    n,t = tuple(w.split(':',1))
    ctypes.append((n,t))
    
grants = {}
for u in grants_r:
    grants[u] = 'r'
for u in grants_w:
    grants[u] = grants.get(u, '') + 'w'
    
print("Creating folder %s in namespace %s with columns:" % (tname, namespace))
for n, t in ctypes:
    print("   ", n, ":", t)
if drop_existing:
    print("Will drop existing folder")


db = IOVDB(connstr=dbcon, namespace=namespace)
t = db.createFolder(tname, ctypes, 
    time_type = time_type, 
    grants = grants, 
    drop_existing = drop_existing)
print('Folder created')

