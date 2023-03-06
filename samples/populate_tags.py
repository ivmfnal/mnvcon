from IOVAPI import IOVDB
import sys, time, random
from datetime import datetime, timedelta

if __name__ == '__main__':

    db = IOVDB(connstr=sys.argv[1])
    folder = sys.argv[2]
    
    db.createFolder(folder,                       # folder name
        [
            ('x', 'float')
        ],  # columns and their types
        drop_existing=True)
    
    f = db.openFolder(folder,     # folder name 
        ['x']              # columns we want to see
        )
    
    iov1 = f.addData(1, {1:(1,)})  
    f.createTag('v0','Version 0')
    iov2 = f.addData(2, {1:(2,)})  
    iov3 = f.addData(4, {1:(3,)})  
    f.createTag('v1','Version 1')
    iov4 = f.addData(3, {1:(2.5,)})
    f.createTag('v2','Version 2')
    
    print 'current'
    for t in range(1.1, 5.1, 1.0):
        print t, f.getData(t)[1]
    
    print 'v0'
    for t in range(1.1, 5.1, 1.0):
        print t, f.getData(t, tag='v0')[1]

    print 'v1'
    for t in range(1.1, 5.1, 1.0):
        print t, f.getData(t, tag='v1')[1]

    print 'v2'
    for t in range(1.1, 5.1, 1.0):
        print t, f.getData(t, tag='v2')[1]
    
    
    for v in ('v0','v1','v2'):
        print 'IOVs in '+v
        print f.getIOVs(1.1, 5.1, tag=v)

    
    
    
