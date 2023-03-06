import psycopg2
import time
import sys
from datetime import datetime, timedelta, tzinfo
from dbdig import DbDig
from psycopg2 import extensions
import threading

Version = "$Id: IOVAPI.py,v 1.33 2016/02/02 20:11:02 ivm Exp $"

TimeTypes = [
    ("i",        "bigint"),
    ("f",        "double precision"),
    ("t",        "timestamp without time zone"),
    ("tz",       "timestamp with time zone")
]

TimeType_to_DB = dict(TimeTypes)
TimeType_from_DB = dict( [(v, k) for k, v in TimeTypes] )

class IOVDB:
    def __init__(self, db = None, connstr = None, namespace = None,
            role = None,
            cache = None):
        assert not (db == None and connstr == None)
        self.Connstr = connstr
        self.Namespace = namespace or "public"
        self.Role = role
        if db:
            self.DB = db
        else:
            self.reconnect()
        self.DataTypes = {}
        self.Cache = cache
        
    def flushCache(self):
        self.Cache.clear()

    def split_spec(self, table, namespace=None):
        if "." in table:
            return tuple(table.split(".", 1))
        else:
            namespace = namespace or self.Namespace
            return namespace, table

    def reconnect(self):
        assert self.Connstr != None
        self.DB = psycopg2.connect(self.Connstr)
        self.DB.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        if self.Role:
            self.DB.cursor().execute("set role %s" % (self.Role,)) 
            c = self._cursor()
            c.execute("show role")
            #print "Role %s set, %x" % (c.fetchone()[0],id(self.DB))           
        self.DB.cursor().execute("set search_path to %s" % (self.Namespace,))
        
    def disconnect(self):
        if self.DB != None:
            self.DB.close()
        self.DB = None

    def useDB(self, db):
        self.DB = db

    def openFolder(self, name, columns = '*'):
        f = IOVFolder(self, name, columns)
        if not f.exists():
                f = None
        return f
        
    def createFolder(self, name, columns_and_types, drop_existing=False,
                time_type = "timestamp",
                grants={}):
        # grants =  {'username':'r' or 'username':'rw'}
        assert time_type in TimeType_to_DB
        colnames = [cn for cn, ct in columns_and_types]
        f = self.openFolder(name, colnames)
        if f is not None and not drop_existing:
            raise RuntimeError("Folder already exists. Use drop_existing parameter to drop and recreate")
        else:
            f = IOVFolder(self, name)
        f._createTables(columns_and_types, time_type, drop_existing, grants)
        return self.openFolder(name, colnames)      # reopen as existing

    def getFolders(self):

        folders =[]

        dbfolders = DbDig(self.DB).tables(self.Namespace)
        if not dbfolders: return []
        
        #Make sure folder has both tables folder_iovs and folder_data.
        for aval in dbfolders:
            if aval[-5:] == '_iovs':
                fn = aval[:-5]
                if fn + "_data" in dbfolders:
                    folders.append(fn)
                
        return folders


    def probe(self):
        c = self.DB.cursor()
        c.execute("select 1")
        return c.fetchone()[0] == 1
        
    def _cursor(self):
        return self.DB.cursor()


    def _tableExists(self, spec):
        namespace, table = self.split_spec(spec)
        tables = DbDig(self.DB).tables(namespace)
        if not tables:  return False
        return table.lower() in {t.lower() for t in tables}
        
    def _columns(self, spec):
        namespace, table = self.split_spec(spec)
        columns = DbDig(self.DB).columns(namespace, table)
        #print self.Namespace, table
        return [(c[0], c[1]) for c in columns]
        
    def _typeName(self, ctype):
        if ctype not in self.DataTypes:
            c = self._cursor()
            c.execute("select pg_catalog.format_type(%d, null)" % (ctype,))
            self.DataTypes[ctype] = c.fetchone()[0]
        return self.DataTypes[ctype]

class IOVSnapshot:
    def __init__(self, iov, data, columns, interval = None, tag = None):
        # data: list of tuples, 1st element of a tuple is channel id
        # validity interval: tuple (t0, t1) or (t0, None)
        # columns: list of tuple element names and their types. First element
        # of the list is always ("channel","bigint")
        self.Data = data
        self.ValidFrom, self.ValidTo = None, None
        if interval != None:   self.ValidFrom, self.ValidTo = interval
        self.Dict = None
        self.Columns = columns
        self.IOVid = iov
        self.Tag = tag
        #print 'end of constructor'


    # to make it look like a dictionary..
    def __getitem__(self, channel):
        #raise 'call to getitem (%s)' % (channel,)
        self._makeDict()
        return self.Dict[channel]
        
    def items(self):
        self._makeDict()
        return list(self.Dict.items())

    def keys(self):
        self._makeDict()
        return list(self.Dict.keys())

    def get(self, channel, default):
        self._makeDict()
        return self.Dict.get(channel, default)
        
    def asDict(self):
        self._makeDict()
        return self.Dict
        
    def asList(self):
        return self.Data

    def setInterval(self, t0, t1):
        self.ValidFrom = t0
        self.ValidTo = t1

    def iov(self):  
        return (self.ValidFrom, self.ValidTo)

    def columns(self):  return self.Columns

    def _makeDict(self):
        if self.Dict == None:
            data = {}
            for tup in self.Data:
                channel = tup[0]
                data[channel] = tup[1:]
            self.Dict = data
        
    def iovid(self):
        return self.IOVid

 
class UTC(tzinfo):

    ZERO = timedelta(0)

    def utcoffset(self, dt):
        return self.ZERO

    def tzname(self):
        return "UTC"

    def dst(self, dt):
        return self.ZERO


class IOVFolder:
    def __init__(self, db, name, columns = '*', namespace = None):
        self.DB = db
        namespace, name = db.split_spec(name, namespace)
        #print("namespace, name from spec:", namespace, name)
        self.Name = name
        self.Namespace = namespace
        self.TablePrefix = "%s.%s" % (self.Namespace, self.Name)
        self.TableIOVs = '%s_iovs' % (self.TablePrefix,)
        self.TableData = '%s_data' % (self.TablePrefix,)
        self.TableTags = '%s_tags' % (self.TablePrefix,)
        self.TableTagIOVs = '%s_tag_iovs' % (self.TablePrefix,)
        self.Columns = columns
        if self.exists():
            if self.Columns == '*':
                self.Columns = self._getDataColumns()
            self.Colstr = ','.join(self.Columns)
            self.Cache = self.DB.Cache
            self.TimeType = None
            self.WithTimeZone = False
            col_dict = dict(self.DB._columns(self.TableIOVs))
            typ = col_dict["begin_time"]
            assert typ in TimeType_from_DB, "Unknown timestamp column type '%s' in the database" % (typ,)
            self.TimeType = TimeType_from_DB[typ]

    def time_column_cast(self, column):
        if self.TimeType == "t":
            column = f"{column}::timestamptz"
        return column

    def exists(self):
        return self.DB._tableExists(self.TableData) \
            and self.DB._tableExists(self.TableIOVs)

    def _getDataColumns(self):
        columns = self.DB._columns(self.TableData)
        return [n for n, t in columns if not (n == 'channel' or n.startswith('__'))]

    def describe(self):
        # returns list of tuples (name, type) for all data columns
        columns = self.DB._columns(self.TableData)
        return [(n, t) for n, t in columns if not (n == 'channel' or n.startswith('__'))]       

    def numericToDBTime(self, t):
        if t is None:   return None
        assert isinstance(t, (int, float))
        if self.TimeType == "i":
            assert isinstance(t, int)
            return t
        elif self.TimeType == "f":
            return float(t)
        elif self.TimeType == "tz":
            return datetime.utcfromtimestamp(t).replace(tzinfo=UTC())
        elif self.TimeType == "t":
            return datetime.fromtimestamp(t)
        else:
            raise ValueError("Unknown TimeType for folder %s: %s" % (self.Name, self.TimeType))
            
    def getIOVs(self, t, window = None, tag = None):
        assert isinstance(t, (int, float))
        
        db = self.DB
        c = db._cursor()
        t0 = t
        t1 = None if window is None else t0 + window
        
        t0 = self.numericToDBTime(t0)
        t1 = self.numericToDBTime(t1)
        
        begin_time = self.time_column_cast("begin_time")
        
        t1_where = "" if t1 is None else f" and i.{begin_time} <= '%s' " % (t1,)

        if tag:
            c.execute(f"""select i.iov_id, i.{begin_time} 
                            from %s i, %s ti
                            where i.begin_time >= '%s' 
                                %s 
                                and i.iov_id = ti.iov_id 
                                and ti.tag = '%s'
                            order by i.begin_time""" % 
                            (self.TableIOVs, self.TableTagIOVs, t0, t1_where, tag))
        else:
            c.execute(f"""select iov_id, {begin_time} 
                            from %s i
                            where i.{begin_time} >= '%s' 
                                    %s
                                    and active
                            order by i.begin_time""" % 
                            (self.TableIOVs, t0, t1_where))
            
        lst = c.fetchall()
        if tag:
            c.execute(f"""select i.iov_id, i.{begin_time}
                from %s i, %s ti
                where begin_time < '%s'
                    and i.iov_id = ti.iov_id and ti.tag = '%s'
                order by begin_time desc
                limit 1""" % (self.TableIOVs, self.TableTagIOVs, t0, tag))
        else:
            c.execute(f"""select iov_id, {begin_time}
                from %s
                where begin_time < '%s' and active
                order by begin_time desc
                limit 1""" % (self.TableIOVs, t0))
        tup = c.fetchone()
        if tup and (not lst or lst[0][0] != tup[0]):
            lst = [tup] + lst
        return lst

    def getNextIOV(self, t=None, iovid=None, tag=None):
        # t is DB time here
        assert not (t is None and iovid is None)
        db = self.DB
        c = db._cursor()
        begin_time = self.time_column_cast("begin_time")
        if t == None:
            c.execute(f"""select {begin_time} 
                            from %s    
                            where iov_id = %s""" % (self.TableIOVs, iovid))
            tup = c.fetchone()
            if tup == None: return None
            t = tup[0]
        #print ' next iov, t= %s' %t
        if tag:
            c.execute(f"""select i.iov_id, i.{begin_time}
                from %s i, %s ti
                where begin_time > '%s' and
                    ti.tag = '%s' and
                    ti.iov_id = i.iov_id
                order by begin_time limit 1""" % (self.TableIOVs, self.TableTagIOVs, t, tag))
        else:
            c.execute(f"""select iov_id, {begin_time}
                from %s
                where active and begin_time > '%s'
                order by begin_time
                limit 1""" % (self.TableIOVs, t))

        tup = c.fetchone()
        if tup == None: return None, None
        return tup
        
    def getIOV(self, iovid, tag=None):
        c = self.DB._cursor()

        begin_time = self.time_column_cast("begin_time")
        if tag:
            tagsql = f"""select i.{begin_time}
                from %s i, %s ti
                where 
                    ti.tag = '%s' and
                    ti.iov_id = i.iov_id and i.iov_id = %s""" % (self.TableIOVs, self.TableTagIOVs,tag,iovid)
            #print ' getIOV, tag sql = %s \n' %tagsql
            c.execute(tagsql)
        
        else:
            c.execute(f"""select {begin_time}
                from %s
                where iov_id = %s""" % (self.TableIOVs, iovid))

        tup = c.fetchone()
        if not tup:
            #print 'getiov , No data found'   
            return None, None
        t0 = tup[0]
        next_iov, t1 = self.getNextIOV(t0, tag=tag)
        #print ' getIOV, t0= %s, t1=%s' %(t0,t1)
        return t0, t1 

    def getTuple(self, t, channel=0, tag=None):
        "returns the tuple for given time and single channel"
        data = self.getData(t, tag=tag)
        if data == None:    return None, None
        tup = data.get(channel, None)
        return tup, data.iov()

    def getData(self, t = None, iovid = None, chanComps = None, tag=None):
        "returns dictionary {channel -> (data,...)} for given time"
        #print ' in getdata, iovid = %s, chanComps = %s, tag=%s ' %(iovid, str(chanComps), tag)

        assert not (t == None and iovid == None)
        if iovid == None:
            iovid, t0, t1 = self.findIOV(t, tag=tag)
        if iovid == None:
            return None
        
        s = self.getSnapshotByIOV(iovid, chanComps=chanComps, tag=tag)
        return s or None
        
    def getDataInterval(self, t0, t1, tag=None):
        iovs = getIOVs(self, t0, t1-t0, tag = tag)
        return [self.getSnapshotByIOV(iovid, tag=tag) for iovid in iovs]
        

    def addData(self, tt, data, combine = False, override_future = 'no'):
        # override future:
        #   'no' or None or '' - do not override
        #   'hide' - mark them inactive
        #   'delete' - delete IOVs
        t = self.numericToDBTime(tt)
        #print "addData(%s): converted %s -> %s" % (self.TimeType, tt, t)
        
        if combine:
            s = self.getData(t)
            old_data = {}
            if s:   old_data = s.asDict()
            old_data.update(data)
            data = old_data
            
        
        db = self.DB
        c = db._cursor()
        
        #c.execute("begin")
        if override_future == 'hide':
            c.execute("update %s set active='false' where active and begin_time >= '%s'" %
                (self.TableIOVs, t))
        elif override_future == 'delete':
            c.execute("""delete from %s i where active and begin_time >= '%s'
                            and not exists (
                                select * from %s ti
                                    where ti.iov_id = i.iov_id)""" %
                (self.TableIOVs, t, self.TableTagIOVs))
        iov = self._insertSnapshot(t, data, c)
        #c.execute("commit")

    def overrideInterval(self, t0, t1, data, mode = 'hide'):
        # mode: 'hide' - mask existing intervals by making them inactive
        #       'delete' - delete existing intervals 
        assert t1 >= t0
        db = self.DB
        c = db._cursor()
        end_data = self.getData(t1)
        self.addData(t0, data)
        if mode == 'delete':
            c.execute("""delete from %s i
                            where   active and
                                    begin_time >= '%s' and
                                    begin_time < '%s' and
                                    not exists (
                                        select * from %s ti
                                        where ti.iov_id = i.iov_id)""" % 
                    (self.TableIOVs, t0, t1, self.TableTagIOVs))
        else:
            c.execute("""update %s
                            set active = 'false'
                            where   active and 
                                    begin_time >= '%s' and
                                    begin_time < '%s'""" % (self.TableIOVs, t0, t1))
        self.addData(t1, end_data)

    def tagIOV(self, iovid, tag):
        c = db._cursor()
        c.execute("""
            insert into %s(tag, iov_id) values ('%s', %s)""" % 
            (self.TableTagIOVs, tag, iovid))
        
    def untagIOV(self, iovid, tag):
        c = db._cursor()
        c.execute("""
            delete 
                from %s
                where tag='%s' and iovid=%s""" % 
            (self.TableTagIOVs, tag, iovid))
        

    def findIOV(self, t, tag=None):
        tt = self.numericToDBTime(t)
        begin_time = self.time_column_cast("begin_time")
        c = self.DB._cursor()
        if tag:
            c.execute(f"""
                select i.iov_id, i.{begin_time}
                    from %s i, %s ti
                    where i.begin_time <= '%s' and
                        ti.iov_id = i.iov_id and
                        ti.tag = '%s'
                    order by i.begin_time desc, i.iov_id desc
                    limit 1""" % (self.TableIOVs, self.TableTagIOVs, tt, tag))
        else:
            c.execute(f"""select iov_id, {begin_time}
                from %s
                where begin_time <= '%s' and active
                order by begin_time desc, iov_id desc
                limit 1""" % (self.TableIOVs, tt))
        x = c.fetchone()
        if not x:
            # not found
            return None, None, None
        iov, t0 = x
        next_iov, t1 = self.getNextIOV(tt, tag=tag)
        return iov, t0, t1

    def getSnapshotByIOV(self, iov, chanComps=None, tag=None):
        
        #print ' getSnapshotByIOV iov = %s, tag = %s' %(iov,tag)
        t0, t1 = self.getIOV(iov, tag=tag)
        if t0 == None:  return None
        c = self.DB._cursor()
        
        andSql=''
        #if chanComps: print ' chancomps = %s ' %str(chanComps)
        if chanComps:
          for ak in chanComps.keys():
             #print ' snap ,%s, %s' %(ak, chanComps[ak])
             andSql = andSql + " and %s = %s " %(ak,chanComps[ak])

        data, columns = None, None

        if not chanComps and self.Cache:
            tup = self.Cache[(self.Name, iov, self.Colstr)]
            if tup: data, columns = tup
            
        if not data:
            sql = """select channel, %s
                    from %s
                    where __iov_id = %d %s
                    order by channel""" %(self.Colstr, self.TableData, iov, andSql)

            #print 'sql = %s ' %sql
            c.execute(sql)
            desc = c.description
            data = c.fetchall()
            columns = []
            for tup in desc:
                cname, ctype = tup[:2]
                columns.append((cname, self.DB._typeName(ctype)))
                
        if data and not chanComps and self.Cache:
            self.Cache[(self.Name,iov, self.Colstr)] = (data, columns)
        #print columns    
        s = IOVSnapshot(iov, data, columns, interval = (t0, t1))
        return s

    def iovsAndTime(self,tagval):
    
        iovs = self.getIOVs(tag = tagval or None)
        lst = []
        n = len(iovs)
        for i, (iovid, t0) in enumerate(iovs):
            t1 = None if i >= n-1 else iovs[i+1][1]
            lst.append((iovid, t0, t1))
            
        return lst
       
    def createTag(self, tag, comments=''):
        c = self.DB._cursor()
        c.execute("""
            insert into %s(tag, created, comments) values('%s', now(), '%s')""" %
            (self.TableTags, tag, comments))
        c.execute("""
            insert into %s(tag, iov_id) 
                (select '%s', iov_id 
                    from %s
                    where active)""" % (self.TableTagIOVs, tag, self.TableIOVs))
        c.execute('commit')
                    

    def tags(self):
        c = self.DB._cursor()
        
        if self.DB._tablesExist(self.TableTags):

            c.execute("""select tag from %s""" % (self.TableTags,))
            return [x[0] for x in c.fetchall()]
        else:
            #print '\n table %s does not exists..' %self.TableTags
            return None

    def _insertSnapshot(self, t, data, cursor):
        # data is a dictionary {channel -> tuple}
        c = cursor
        c.execute("insert into %s(begin_time) values ('%s')" %
                (self.TableIOVs, t,))

        c.execute("select lastval()")
        iov = c.fetchone()[0]
        for cn, tup in data.items():
            vals = ['%s' % (iov,), '%s' % (cn,)] + ["'%s'" % (x,) for x in tup]
            vals = ','.join(vals)
            c.execute("insert into %s(__iov_id, channel, %s) values(%s)" %
                (self.TableData, self.Colstr, vals))
        c.execute('commit')
        return iov
        
    def _createTables(self, coltypes, time_type, drop_existing, grants={'public':'r'}):
        # grants: {username:'r' or 'rw'}
        db = self.DB
        c = db._cursor()
        columns = ','.join(['%s %s' % (cn, ct) for cn, ct in coltypes])
        #print columns
        if drop_existing:
            try:    
                c.execute("""
                    drop table %(data_table)s;""" %
                    {
                        'tag_iovs_table':self.TableTagIOVs,
                        'tags_table':self.TableTags,
                        'data_table':self.TableData, 
                        'iovs_table':self.TableIOVs,
                    })
            except:
                pass
            try:    
                c.execute("""
                    drop table %(tag_iovs_table)s;""" %
                    {
                        'tag_iovs_table':self.TableTagIOVs,
                        'tags_table':self.TableTags,
                        'data_table':self.TableData, 
                        'iovs_table':self.TableIOVs,
                    })
            except:
                pass
            try:    
                c.execute("""
                    drop table %(tags_table)s;""" % 
                    {
                        'tag_iovs_table':self.TableTagIOVs,
                        'tags_table':self.TableTags,
                        'data_table':self.TableData, 
                        'iovs_table':self.TableIOVs,
                    })
            except:
                pass
            try:    
                c.execute("""
                    drop table %(iovs_table)s;""" % 
                    {
                        'tag_iovs_table':self.TableTagIOVs,
                        'tags_table':self.TableTags,
                        'data_table':self.TableData, 
                        'iovs_table':self.TableIOVs,
                    })
            except:
                pass
        if not self.exists(): 
            #c.execute("show role")
            #print 'Role=%s, %x' % (c.fetchone()[0], id(self.DB.DB))
            time_type = TimeType_to_DB[time_type]
            sql = """
                create table %(iovs_table)s (
                    iov_id      bigserial   primary key,
                    begin_time  %(time_type)s,
                    active      boolean    default 'true');
                create index %(name)s_begin_time_inx on %(iovs_table)s(begin_time);

                create table %(tags_table)s (
                    tag         text    primary key,
                    created     timestamp with time zone,
                    comments    text
                );

                create table %(tag_iovs_table)s (
                    tag     text    references %(tags_table)s(tag) on delete cascade,
                    iov_id  bigint  references %(iovs_table)s(iov_id) on delete cascade,
                    primary key (tag, iov_id)
                );

                create table %(data_table)s (
                    __iov_id  bigint  references %(iovs_table)s(iov_id) on delete cascade,
                    channel bigint default 0, 
                    %(columns)s,
                    primary key(__iov_id, channel)); """ % \
                    {
                        'name':self.Name,
                        'data_table':self.TableData, 'iovs_table':self.TableIOVs,
                        'tags_table':self.TableTags, 'tag_iovs_table':self.TableTagIOVs,
                        'columns':columns, 'time_type':time_type
                    }
            #print sql
            if grants:
                for u, p in grants.items():
                    sql += """
                        grant select on %s, %s, %s, %s to %s; """ % (
                            self.TableData, self.TableIOVs, self.TableTags, self.TableTagIOVs, u)
                    if p == 'rw':
                        sql += """
                            grant insert, update, delete on %s, %s, %s, %s to %s; """ % (
                                self.TableData, self.TableIOVs, self.TableTags, self.TableTagIOVs, u)
            #print sql
                        
            c.execute(sql)       
                 
if __name__ == '__main__':
    import sys, random
    from datetime import datetime, timedelta
    
    db = IOVDB(connstr=sys.argv[1])
    
    db.createFolder("sample",                       # folder name
        [
            ('x1', 'float'),('x2', 'float'),('x3', 'float'),
            ('x4', 'float'),('x5', 'float'),('x6', 'float'),
            ('x7', 'float'),('x8', 'float'),('x9', 'float'),
            ('n', 'int'),('s', 'text')
        ],  # columns and their types
        drop_existing=True)
    
    f = db.openFolder("sample",     # folder name 
        ['x1','x2','x3','x4','x5','x6','x7','x8','x9','n', 's']              # columns we want to see
        )
    
    channels = 3000
    
    time0 = datetime.now() - timedelta(days=1)  # 1 day ago
    totalt = 0.0
    snapshots = 10
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
        
    print('populated from %s to %s' % (
        time.mktime(time0.timetuple()),time.mktime(t.timetuple())))
        
    print('Write time per snapshot (sec):', totalt/snapshots)
    
    tx = time0 + timedelta(minutes=45)       # t0 + 45 minutes
    t0 = time.time()
    tuple, valid = f.getTuple(tx, 70)
    print("Data for channel=70:", tuple)
    print("Valid from %s to %s" % valid)
    print("Time:", time.time() - t0)
    
    tx = time0 + timedelta(minutes=46)         # t0 + 46 minutes - should get data from cache
    t0 = time.time()
    tuple, valid = f.getTuple(tx, 70)
    print("Data for channel=70:", tuple)
    print("Valid from %s to %s" % valid)
    print("Time:", time.time() - t0)
    
    tx = time0 + timedelta(minutes=25)         # t0 + 25 minutes - should go to the db
    t0 = time.time()
    tuple, valid = f.getTuple(tx, 70)
    print("Data for channel=70:", tuple)
    print("Valid from %s to %s" % valid)
    print("Time:", time.time() - t0)
            
        
        
