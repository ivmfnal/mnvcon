from IOVAPI import IOVDB
import sys, time, random
from datetime import datetime, timedelta

if __name__ == '__main__':

    db = IOVDB(connstr=sys.argv[1], role=sys.argv[2])
    folder = sys.argv[3]
    
    db.createFolder(folder,                       # folder name
        [
            ('x1', 'float'),('x2', 'float'),('x3', 'float'),
            ('x4', 'float'),('x5', 'float'),('x6', 'float'),
            ('x7', 'float'),('x8', 'float'),('x9', 'float'),
            ('n', 'int'),('s', 'text')
        ],  # columns and their types
        drop_existing=True)
    
    f = db.openFolder(folder,     # folder name 
        ['x1','x2','x3','x4','x5','x6','x7','x8','x9','n', 's']              # columns we want to see
        )
    
    channels = 32
    
    time0 = datetime.now() - timedelta(days=1)  # 1 day ago
    totalt = 0.0
    snapshots = 1
    for s in range(snapshots):               # record 100 meaurements
        t = time0 + timedelta(minutes=s*60)  # every 10 minutes
        t0 = time.time()
        data = {}
        for i in range(channels):
            ic = i * 10
            tup = (random.random(),random.random(),random.random(), 
                random.random(),random.random(),random.random(),
                random.random(),random.random(),random.random(),
                int(random.random()*100), 'channel=%s' % (ic,))
            data[ic] = tup
        f.addData(t, data)
        totalt += time.time() - t0
        print (s+1), 'snapshots written, average write time per snapshot (sec):', totalt/(s+1)
        
    print 'populated from %s to %s' % (
        time.mktime(time0.timetuple()),time.mktime(t.timetuple()))
        
    print 'Write time per snapshot (sec):', totalt/snapshots
    
    tx = time0 + timedelta(minutes=45)       # t0 + 45 minutes
    t0 = time.time()
    tuple, valid = f.getTuple(tx, 70)
    print "Data for channel=70:", tuple
    print "Valid from %s to %s" % valid
    print "Time:", time.time() - t0
