from WSGIApp import WSGIApp, WSGISessionApp, WSGIHandler, Request, Response, Application
from  ConfigParser import ConfigParser
import os, psycopg2, time, threading
from datetime import datetime
from IOVAPI import IOVDB
from FileAPI import MnvFileDB
from LRUCache import LRUCache
import urllib
from ConnectionPool import ConnectionPool

import ldap, _ldap, sys

IOVCache = LRUCache(50, True)
DBConnectionPool = None
DBConnectionPool_lock = threading.Lock()

def initDBConnectionPool(connstr):
    global DBConnectionPool, DBConnectionPool_lock
    with DBConnectionPool_lock:
        if DBConnectionPool is None:
            DBConnectionPool = ConnectionPool(postgres=connstr, idle_timeout=30)
            

class   ConfigFile(ConfigParser):
    def __init__(self, request, path=None, envVar=None):
        ConfigParser.__init__(self)
        if not path:
            path = request.environ.get(envVar, None) or os.environ[envVar]
        self.read(path)
    
class DBConnection:

    def __init__(self, cfg):
        self.Host = None
        self.DBName = cfg.get('Database','name')
        self.User = cfg.get('Database','user')
        self.Password = cfg.get('Database','password')

        self.Port = None
        try:    
            self.Port = int(cfg.get('Database','port'))
        except:
            pass
            
        self.Host = None
        try:    
            self.Host = cfg.get('Database','host')
        except:
            pass
            
        self.Namespace = 'public'
        try:    
            self.Namespace = cfg.get('Database','namespace')
        except:
            pass
            
        self.DB = None

        connstr = "dbname=%s user=%s password=%s connect_timeout=10" % \
            (self.DBName, self.User, self.Password)
        if self.Port:   connstr += " port=%s" % (self.Port,)
        if self.Host:   connstr += " host=%s" % (self.Host,)

        initDBConnectionPool(connstr)
        
    def connection(self):
        if self.DB == None:
            self.DB = DBConnectionPool.connect()
            self.DB.cursor().execute("set search_path to %s" % (self.Namespace,))
        return self.DB

# --------------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------------
class IOVRequestHandler(WSGIHandler):

    def __init__(self, req, app):
        WSGIHandler.__init__(self, req, app)
        self.DB = app.iovdb()

    def times_(self, req, relpath, **args):
        #print args
        f = self.DB.openFolder(req.GET['f'])
        t0 = float(req.GET['t'])
        tag = req.GET.get('tag', None)  
        window = req.GET.get('d', None)
        tag = req.GET.get('tag', None)
        if window:
            if window[-1] == 'd':
                window = float(window[:-1]) * 24 * 3600
            else:
                window = float(window) 
        else:
            window = float(7*24*3600)
        resp = Response()
        resp.write('iovid,time\n')
        times = f.getIOVs(t0, window, tag=tag)
        #print times
        for iovid, t in times:
            tepoch = time.mktime(t.timetuple()) + float(t.microsecond)/1000000
            resp.write('%d,%.6f\n' % (iovid,tepoch,))
        resp.headers.add('Cache-Control','no-store')
        resp.headers.add('Content-Type','text/plain')
        return resp

    def output_iterator(self, t0, t1, data):
        yield '%.6f\n' % (t0, )
        if t1 == None:
            yield '-\n'
        else:
            yield '%.6f\n' % (t1,)
        colnames = ','.join([cn for cn, ct in data.columns()])
        coltypes = ','.join([ct for cn, ct in data.columns()])
        yield colnames + '\n' + coltypes + '\n'
        for tup in data.asList():
            yield '%s\n' % (','.join(['%s' % (x,) for x in tup]),)
        
    def csv_with_tv_column_iterator(self, snapshot, include_header):
        # return tuple list with "tv" column inserted after the "channel"
        tv = self.timestamp(snapshot.ValidFrom)
        ci = None
        for i, (cn, ct) in enumerate(snapshot.columns()):
            if cn == 'channel':
                ci = i + 1
                break
        assert ci != None
        if include_header:
            colnames = [cn for cn, ct in snapshot.columns()]
            colnames.insert('tv', ci)
            coltypes = [ct for cn, ct in snapshot.columns()]
            coltypes.insert('float',ci)
            yield ','.join(colnames) + '\n'
            yield ','.join(coltypes) + '\n'

        for tup in snapshot.asList():
            tup = tup[:ci] + (tv,) + tup[ci:]
            yield ','.join(['%s' % (x,) for x in tup]) + '\n'
        
    def line_consolidator(self, it, n):
        lines = []
        for line in it:
            lines.append(line)
            if len(lines) > n:
                yield ''.join(lines)
                lines = []
        if lines:
            yield ''.join(lines)     

    def mergeLines(self, iter, maxlen=100000):
        buf = []
        total = 0
        for l in iter:
            n = len(l)
            if n + total > maxlen:
                yield ''.join(buf)
                buf = []
                total = 0
            buf.append(l)
            total += n
        if buf:
            yield ''.join(buf)

    def timestamp(self, dt):
        return time.mktime(dt.timetuple()) + float(dt.microsecond)/1000000
        
    def to_numeric(self, t):
        if t is None:   return None
        if not isinstance(t, (int, float)):
            t = self.timestamp(t)
        return t
            
    def data(self, req, relpath, **args):
        T0 = time.time()
        f = self.DB.openFolder(req.GET['f'])
        #print 'Folder %s open' % (req.GET['f'],)
        tag = req.GET.get('tag', None)
        t = req.GET.get('t', None)
        if t != None:
            t = float(t)
        iovid = req.GET.get('i', None)
        if iovid != None:
            iovid = int(iovid)
        if iovid == None:
            data = f.getData(t, tag=tag)
        else:
            data = f.getData(iovid=iovid)
        #self.DB.disconnect()
        T1 = time.time()
        #print 'got data: %f' % (T1-T0,)
        #raise '%s' % (iov,)
        resp = Response()
        if not data:
            resp.status = 400
            resp.write("Data not found")
            return resp
        t0, t1 = data.iov()
        t0, t1 = self.to_numeric(t0), self.to_numeric(t1)
        resp.app_iter = self.mergeLines(self.output_iterator(t0, t1, data))
        T2 = time.time()
        #print 'Time: %f' % (T2 - T0,)
        resp.headers.add('Cache-Control','max-age=3600')
        resp.headers.add('Content-Type','text/plain')
        resp.headers.add('Connection','close')
        return resp        

    def get(self, req, relpath, f=None, t=None, t0=None, t1=None, tag=None,
                c='*', **args):
        if c != '*':
            c = c.split(',')
        if t != None:
            return self.getAtTime(f, t, c, tag)
        else:
            return self.getInterval(f, t0, t1, c, tag)
        
        
    def csv_with_tv_column_iterator(self, snapshot, include_header):
        # return tuple list with "tv" column inserted after the "channel"
        tv = self.timestamp(snapshot.ValidFrom)
        ci = None
        for i, (cn, ct) in enumerate(snapshot.columns()):
            if cn == 'channel':
                ci = i + 1
                break
        assert ci != None
        if include_header:
            colnames = [cn for cn, ct in snapshot.columns()]
            colnames.insert(ci, 'tv')
            coltypes = [ct for cn, ct in snapshot.columns()]
            coltypes.insert(ci, 'float')
            yield ','.join(colnames) + '\n'
            yield ','.join(coltypes) + '\n'

        for tup in snapshot.asList():
            tup = tup[:ci] + (tv,) + tup[ci:]
            yield ','.join(['%s' % (x,) for x in tup]) + '\n'
        
    def getAtTime(self, f, t, columns, tag):
        t = float(t)
        f = self.DB.openFolder(f, columns)
        snapshot = f.getData(t, tag=tag)
        resp = Response()
        if not snapshot:
            resp.status = 400
            resp.write("Data not found")
            return resp
        resp.app_iter = self.mergeLines(
                self.csv_with_tv_column_iterator(snapshot, True))
        resp.headers.add('Cache-Control','max-age=3600')
        resp.headers.add('Content-Type','text/plain')
        return resp        

    def csv_from_iovs_iter(self, folder, iovs):
        first = True
        for iovid in iovs:
            s = folder.getSnapshotByIOV(iovid)
            for line in self.csv_with_tv_column_iterator(s, first):
                yield line
            first = False
            
    def getInterval(self, f, t0, t1, columns, tag):
        t0 = float(t0)
        t1 = float(t1)
        #print columns
        f = self.DB.openFolder(f, columns)
        iovs = f.getIOVs(t0, window = t1-t0, tag = tag)
        
        resp = Response()

        if not iovs:
            resp.status = 400
            resp.write("Data not found")
            return resp
        
        csv = self.csv_from_iovs_iter(f, (iovid for iovid, t0 in iovs))              
        
        resp.app_iter = self.mergeLines(csv)
        resp.headers.add('Cache-Control','max-age=3600')
        resp.headers.add('Content-Type','text/plain')
        return resp        
                        
    def cache_stats(self, req, relpath, **args):
        html_format = """
        <html>
        <head>
            <style type=text/css>
                p, td, body {
                    font-family: verdana, sans-serif, arial;
                    }
                th {
                    font-weight: normal;
                    text-align: right;
                    color: gray;
                }
            </style>
        </head>
        <body>
            <center>
                <table>
                    <tr><th>id:</th><td>%x@%d/%x</td></tr>
                    <tr><th>long term hit average hit ratio:</th><td>%.4f</td></tr>
                    <tr><th>short term hit average hit ratio:</th><td>%.4f</td></tr>
                    <tr><th>hits:</th><td>%d</td></tr>
                    <tr><th>misses:</th><td>%d</td></tr>
                </table>
            </center>
        </body>
        </html>""" 
    
        json_format = """
            { 
                "id":          "%x@%d/%x",
                "long_term":   %.4f, 
                "short_term":  %.4f, 
                "hits":        %d, 
                "misses":      %d
            }"""
            
        format = json_format if relpath == 'json' else html_format
        content_type = "text/json" if relpath == 'json' else "text/html"
        
        out = format % (id(IOVCache), os.getpid(), id(threading.current_thread()),
            IOVCache.HitRateAvg100, IOVCache.HitRateAvg10,
            IOVCache.Hits, IOVCache.Misses )
        return Response(out, content_type=content_type)
        
    def probe(self, req, relpath, **args):
        return Response("OK")
        if self.App.iovdb().probe():
            return Response("OK")
        else:
            resp = Response("Error")
            resp.status = 400
            return resp

    def flushDbCache(self, req, relpath, **args):
        self.DB.flushCache()
        return Response('Cache flushed')        

        
    def post(self, req, relpath, f=None, t=None, **args):
        #format of the body:
        # channel,[<column name>,...]
        # <channel>,<data>,...
        #
        f = self.DB.openFolder(f)
        t = float(t)
        lines = req.body.split('\n')
        colnames = lines[0].strip().split(',')
        if colnames[0] != 'channel':
            resp = Response("Error: first column must be named 'channel'")
            resp.status = 400
            return resp
        colnames = colnames[1:]
        folder = self.DB.openFolder(f, colnames)
        data = {}
        for i, l in enumerate(lines):
            if i == 0:  continue    # skip the header
            l = l.strip()
            if not l:   continue
            words = l.split(',')
            channel = int(words[0])
            data[channel] = words[1:]
        f.addData(t, data)
        return Response("OK")
        
    def tags(self, req, relpath, f=None, **args):
        f = self.DB.openFolder(f)
        tags = f.tags()
        if tags == None:
            resp = Response("Error: folder not found")
            resp.status = 400
            return resp
            
        return Response("\n".join(tags), content_type="text/plain")

    def describe(self, req, relpath, f=None, **args):
        f = self.DB.openFolder(f)
        columns = f.describe()
        lst = ['name,type'] + ['%s,%s' % (n, t) for n, t in columns]
        return Response("\n".join(lst), content_type="text/plain")
        
    def folders(self, req, relpath, **args):
        folders = self.DB.getFolders()
        return Response("\n".join(folders), content_type="text/plain")
        
    
class IOVServerApp(WSGIApp):
#class IOVServerApp(WSGIApp):

    def __init__(self, request, rootclass):
        WSGIApp.__init__(self, request, rootclass)
        global IOVPersistent
        self.Config = ConfigFile(request, envVar = 'IOV_SERVER_CFG')
        self.DB = DBConnection(self.Config)
        self.IOVDB = None
        self.initJinja2(tempdirs = [os.path.dirname(__file__)])
        
        #self.initJinja2(tempdirs = [os.path.dirname(__file__)])

    def cfgld(self):
        return self.Config
               
    def iovdb(self):
        if self.IOVDB is None:
            self.IOVDB = IOVDB(self.DB.connection(), cache = IOVCache)
        return self.IOVDB

    def destroy(self):
        #if self.IOVDB:  self.IOVDB.disconnect()
        self.IOVDB = None
        
application = Application(IOVServerApp,IOVRequestHandler)
        
    
