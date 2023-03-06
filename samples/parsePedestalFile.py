import os
import sys, time
from datetime import datetime
import getopt
from IOVAPI import IOVDB

def makeChannelId(crate, croc, chain, board, pixel):
    return (
        ((crate << 25) &    0x1e000000L) | 
        ((croc << 20) &     0x1f00000L) |
        ((chain << 17) &    0xe0000L) |
        ((board << 12) &    0x1f000L) |
        ((pixel << 5) &     0xfe0L)
        )

def parseDataLine(line):
    words = line.split()
    if len(words) != 14:    return None
    return (makeChannelId(int(words[0]), int(words[1]), 
                int(words[2]), int(words[3]), int(words[4])),
            (
                int(words[0]), int(words[1]), int(words[2]), int(words[3]),
                int(words[4]), float(words[5]), float(words[6]), 
                float(words[7]), float(words[8]), float(words[9]), 
                float(words[10]), float(words[11]), float(words[12]),
                float(words[13])
            )
        ) 

def parseHeaderLine(line):
    words = line.split()
    if words[1] == 'begin':
        return 't0', float(words[2])/1000000.0
    elif words[1] == 'end':
        return 't1', float(words[2])/1000000.0
    elif not words[1] in ('elapsed','min'):
        return 'columns', words[1:]
    return None, None

def parseFile(fn):
    t0 = None
    t1 = None
    columns = []
    f = open(fn, 'r')
    line = f.readline()
    data = {}
    while line:
        line = line.strip()
        if line:
            if line[0] == '#':
                what, val = parseHeaderLine(line)
                if what == 't0':    t0 = val
                elif what == 't1':  t1 = val
                elif what == 'columns': columns = val
            else:
                tup = parseDataLine(line)
                chid, tup = tup
                data[chid] = tup
        line = f.readline()
    f.close()
    return t0, data, columns


Usage = """
load_pedcal.py [-c] -f <folder> -d <database connect string> <file> [...]
"""
    
def main():
    dcn = None
    folder_name = None
    create_folder = False
    opts, args = getopt.getopt(sys.argv[1:], 'f:d:c')
    for opt, val in opts:
        if opt == '-d': dcn = val
        if opt == '-f': folder_name = val
        if opt == '-c': create_folder = True

    db = IOVDB(connstr=dcn)
    first_file = True
    folder = None
    #print args
    for fn in args:
        t0, data, columns = parseFile(fn)
        print '%s parsed, %d entries, %d columns' % (fn, len(data), len(columns))
        if first_file:
            if create_folder:
                assert columns != [] and columns != None
                print 'Creating folder %s with columns:\n  %s' % (
                    folder_name, ', '.join(columns))
                db.createFolder(folder_name, 
                    [(x, 'int') for x in columns[:5]] + 
                    [(x, 'float') for x in columns[5:]],
                    drop_existing = False)
            folder = db.openFolder(folder_name, columns)
            first_file = False
        T0 = time.time()
        print 'Storing data for t=%s' % (datetime.fromtimestamp(t0),)
        folder.addData(t0, data)  
        print 'Data stored. Elapsed time = %.1f seconds' % (time.time() - T0,)  

if __name__ == '__main__':
    main()
