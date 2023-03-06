from RequestHandler import TopRequestHandler, RequestHandler
from  ConfigParser import ConfigParser
import os, psycopg2, time
from IOVAPI import IOVDB
from mod_python import apache

class   ConfigFile(ConfigParser):
    def __init__(self, req, path=None, envVar=None):
        ConfigParser.__init__(self)
        if not path:
            req.add_common_vars()
            path = req.subprocess_env.get(envVar, None) or \
                os.environ[envVar]
        self.read(path)
    
class DBConnection:

    def __init__(self, cfg):
        self.Host = cfg.get('Database','host')
        self.DBName = cfg.get('Database','name')
        self.User = cfg.get('Database','user')
        self.Password = cfg.get('Database','password')
        self.Port = 5432
        try:    
            self.Post = int(cfg.get('Database','port'))
        except:
            pass
        self.Namespace = 'public'
        try:    
            self.Namespace = cfg.get('Database','namespace')
        except:
            pass
        
        self.DB = psycopg2.connect("host=%s dbname=%s user=%s password=%s port=%d" %
            (self.Host, self.DBName, self.User, self.Password, self.Port))
        self.DB.cursor().execute("set search_path to %s" % (self.Namespace,))
        self.TableViewMap = {}
        
    def connection(self):
        return self.DB
        
    def close(self):
        self.DB.close()

class IOVServerApp(TopRequestHandler):

    def __init__(self, req):
        TopRequestHandler.__init__(self, req)
        self.Config = ConfigFile(req, envVar = 'IOV_SERVER_CFG')
        self.DB = DBConnection(self.Config)
        self.IOVDB = IOVDB(self.DB.connection())
        
    """ comment out
    def recordspec(self, req, **args):
        f = self.IOVDB.openFolder(args['f'])
        req.content_type = 'text/csv'
        req.write('name,type\n')
        for n, t in f.getDataColumns():
            req.write('%s,%s\n' % (n, t))
    """
    
    def times(self, req, **args):
        f = self.IOVDB.openFolder(args['f'])
        t0 = float(args['t'])
        window = args.get('d', None)
        if window:
            if window[-1] == 'd':
                window = float(window[:-1]) * 24 * 3600
            else:
                window = float(window) 
        else:
            window = float(7*24*3600)
            
        req.write('time\n')
        for iovid, t in f.getIOVs(t0, window):
            tepoch = time.mktime(t.timetuple()) + float(t.microsecond)/1000000
            req.write('%.6f\n' % (tepoch,))
            
    def data(self, req, **args):
        f = self.IOVDB.openFolder(args['f'])
        t = float(args['t'])
        data = f.getData(t)
        #raise '%s' % (iov,)
        if not data:
            req.write("Data not found")
            return apache.HTTP_BAD_REQUEST
        t0, t1 = data.iov()
        t0 = time.mktime(t0.timetuple()) + float(t0.microsecond)/1000000
        if t1 != None:
            t1 = time.mktime(t1.timetuple()) + float(t1.microsecond)/1000000
        req.content_type = 'text/plain'
        req.write('%.6f\n%.6f\n' % (t0, t1))
        colnames = ','.join([cn for cn, ct in data.columns()])
        coltypes = ','.join([ct for cn, ct in data.columns()])
        req.write('%s\n%s\n' % (colnames, coltypes))
        for tup in data.asList():
            req.write('%s\n' % (','.join(['%s' % (x,) for x in tup]),))
        return apache.OK               
        
    def destroy(self):
        self.DB.close()
        
