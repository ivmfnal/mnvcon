from IOVAPI import IOVDB
import sys, getopt, psycopg2

Usage = """
python write_data.py [options] <csv_file> <time> <database name> <folder_name> <column>[,...]
options:
    -h <host>
    -p <port>
    -U <user>
    -w <password>
"""


host = None
port = None
user = None
password = None
columns = []
t = None

dbcon = []

opts, args = getopt.getopt(sys.argv[1:], 'h:U:w:p:')

if len(args) < 3 or args[0] == 'help':
    print(Usage)
    sys.exit(0)

for opt, val in opts:
    if opt == '-h':         dbcon.append("host=%s" % (val,))
    elif opt == '-p':       dbcon.append("port=%s" % (int(val),))
    elif opt == '-U':       dbcon.append("user=%s" % (val,))
    elif opt == '-w':       dbcon.append("password=%s" % (val,))
    

t = float(args[1])
csv_file = open(args[0], 'r')

dbcon.append("dbname=%s" % (args[2],))

dbcon = ' '.join(dbcon)
tname = args[3]
columns = args[4].split(',')

db = IOVDB(connstr=dbcon)
folder = db.openFolder(tname, columns)
data = {}
line = csv_file.readline()
while line:
    line = line.strip()
    words = line.split(',')
    channel = int(words[0])
    row = []
    for x in words[1:]:
        try:    x = int(x)
        except:
            try:    x = float(x)
            except: pass
        row.append(x)
    data[channel] = tuple(row)
    line = csv_file.readline()
folder.addData(t, data)

