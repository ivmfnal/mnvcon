SRCFILES = Server.py config.sample \
	LRUCache.py ConnectionPool.py

ALLFILES = $(SRCFILES) Version.py __init__.py

all: $(DESTDIR)
	cp $(SRCFILES) $(DESTDIR)
	cd $(DESTDIR); echo 'Version="$(VERSION)"' > Version.py; touch __init__.py
	cd $(DESTDIR); chmod +rx $(ALLFILES) 


$(DESTDIR):
	mkdir $(DESTDIR)
