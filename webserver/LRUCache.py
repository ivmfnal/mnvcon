from pythreader import Primitive, synchronized

class   LRUCache(Primitive):

    def __init__(self, capacity):
        Primitive.__init__(self)
        self.Capacity = capacity   
        self.init() 

    @synchronized
    def updateHitRates(self, hit):
        x = 1.0 if hit else 0.0
        self.HitRateAvg10 = 0.01*x + 0.99*self.HitRateAvg10
        self.HitRateAvg100 = 0.001*x + 0.999*self.HitRateAvg100
        if hit:
            self.Hits += 1
        else:
            self.Misses += 1
    
    @synchronized
    def __getitem__(self, key):
            if key in self.Cache:
                x = self.Cache[key]
                self.Keys.remove(key)
                self.Keys.insert(0, key)
                self.updateHitRates(True)
                return x
            else:
                self.updateHitRates(False)
                return None
            
    @synchronized
    def __setitem__(self, key, data):
            try:    self.Keys.remove(key)
            except ValueError:  pass
            self.Keys.insert(0, key)
            self.Cache[key] = data
            while len(self.Cache) > self.Capacity:
                k = self.Keys.pop()
                del self.Cache[k]

    @synchronized
    def clear(self):
            self.HitRateAvg10 = 0.0    
            self.HitRateAvg100 = 0.0    
            self.Hits = 0
            self.Misses = 0
            self.Keys = []
            self.Cache = {}

    init = clear

