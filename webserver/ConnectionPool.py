import time
from threading import RLock, Thread

class _WrappedConnection(object):
    #
    # The pool can be used in 2 ways:
    #
    # 1. explicit connection
    #   
    #   conn = pool.connect()
    #   ....
    #   conn.close()
    #
    #   conn = pool.connect()
    #   ...
    #   # connection goes out of scope and closed during garbage collection
    #
    # 2. via context manager
    #
    #   with pool.connect() as conn:
    #       ...
    #

    def __init__(self, pool, connection):
        self.Connection = connection
        self.Pool = pool
        
    def __str__(self):
        return "WrappedConnection(%s)" % (self.Connection,)

    def _done(self):
        if self.Pool is not None:
            self.Pool.returnConnection(self.Connection)
            self.Pool = None
        if self.Connection is not None:
            self.Connection = None
    
    #
    # If used via the context manager, unwrap the connection
    #
    def __enter__(self):
        return self.Connection
        
    def __exit__(self, exc_type, exc_value, traceback):
        self._done()
    
    #
    # If used as is, instead of deleting the connection, give it back to the pool
    #
    def __del__(self):
        self._done()
    
    def close(self):
        if self.Connection is not None:
            self.Connection.close()
            self.Connection = None
            self.Pool = None
    
    #
    # act as a database connection object
    #
    def __getattr__(self, name):
        return getattr(self.Connection, name)

class _IdleConnection(object):
    def __init__(self, conn):
        self.Connection = conn          # db connection
        self.IdleSince = time.time()

class ConnectorBase(object):

    def connect(self):
        raise NotImplementedError
        
    def probe(self, connection):
        return True
        
    def connectionIsClosed(self, c):
        raise NotImplementedError
        

class PsycopgConnector(ConnectorBase):

    def __init__(self, connstr):
        ConnectorBase.__init__(self)
        self.Connstr = connstr
        
    def connect(self):
        import psycopg2
        return psycopg2.connect(self.Connstr)
        
    def connectionIsClosed(self, conn):
        return conn.closed
        
    def probe(self, conn):
        try:
            c = conn.cursor()
            c.execute("rollback; select 1")
            alive = c.fetchone()[0] == 1
            c.execute("rollback")
            return alive
        except:
            return False
            
class MySQLConnector(ConnectorBase):
    def __init__(self, connstr):
        raise NotImplementedError
        
class _ConnectionPool(object):      
    #
    # actual pool implementation, without the reference to the CleanUpThread to avoid circular reference
    # between the pool and the clean-up thread
    #

    def __init__(self, postgres=None, mysql=None, connector=None, idle_timeout = 60):
        self.IdleTimeout = idle_timeout
        if connector is not None:
            self.Connector = connector
        elif postgres is not None:
            self.Connector = PsycopgConnector(postgres)
        elif mysql is not None:
            self.Connector = MySQLConnector(mysql)
        else:
            raise ValueError("Connector must be provided")
        self.IdleConnections = []           # available, [_IdleConnection(c), ...]
        self.Lock = RLock()
        
    def connect(self):
        with self.Lock:
            use_connection = None
            #print "connect(): Connections=", self.IdleConnections
            while self.IdleConnections:
                c = self.IdleConnections.pop().Connection
                if self.Connector.probe(c):
                    use_connection = c
                    break
                else:
                    c.close()       # connection is bad
            else:
                # no connection found
                use_connection = self.Connector.connect()
            return _WrappedConnection(self, use_connection)
        
    def returnConnection(self, c):
        with self.Lock:
            if not self.Connector.connectionIsClosed(c) \
                    and not c in (x.Connection for x in self.IdleConnections):                    
                self.IdleConnections.append(_IdleConnection(c))
            
    def _cleanUp(self):
        with self.Lock:
            now = time.time()
        
            new_connections = []
            for ic in self.IdleConnections:
                t = ic.IdleSince
                c = ic.Connection
                if t < now - self.IdleTimeout:
                    #print "closing idle connection", c
                    c.close()
                else:
                    new_connections.append(ic)
            self.IdleConnections = new_connections
        
class CleanUpThread(Thread):    
    
    def __init__(self, pool, interval):
        Thread.__init__(self)
        self.Interval = interval
        self.Pool = pool
        
    def run(self):
        while True:
              time.sleep(self.Interval)
              self.Pool._cleanUp()
              
class ConnectionPool(object):

    #
    # This class is needed only to break circular dependency between the CleanUpThread and the real Pool
    # The CleanUpThread will be owned by this Pool object, while pointing to the real Pool
    #
    
    def __init__(self, postgres=None, connector=None, idle_timeout = 60):
        self.Pool = _ConnectionPool(postgres=postgres, connector=connector, idle_timeout = idle_timeout)    # the real pool
        self.CleanThread = CleanUpThread(self.Pool, max(1.0, float(idle_timeout)/2.0))
        self.CleanThread.start()

    def __del__(self):
        # make sure to stop the clean up thread
        self.CleanerThread.stop()

    # delegate all functions to the real pool
    def __getattr__(self, name):
        return getattr(self.Pool, name)
        
