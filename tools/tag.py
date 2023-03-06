from IOVAPI import IOVDB
import sys, getopt, psycopg2

Usage = """
python write_data.py [options] <database name> <table> <tag>
options:
    -h <host>
    -p <port>
    -U <user>
    -w <password>

    -c <comment>
"""


host = None
port = None
user = None
password = None
comment = ''

dbcon = []

opts, args = getopt.getopt(sys.argv[1:], 'h:U:w:p:c:')

if len(args) < 3 or args[0] == 'help':
    print(Usage)
    sys.exit(0)

for opt, val in opts:
    if opt == '-h':         dbcon.append("host=%s" % (val,))
    elif opt == '-p':       dbcon.append("port=%s" % (int(val),))
    elif opt == '-U':       dbcon.append("user=%s" % (val,))
    elif opt == '-w':       dbcon.append("password=%s" % (val,))
    elif opt == '-c':       comment = val
    

dbcon.append("dbname=%s" % (args[0],))
tname = args[1]
tag = args[2]

dbcon = ' '.join(dbcon)

db = IOVDB(connstr=dbcon)
folder = db.openFolder(tname)
folder.createTag(tag)
