SRCFILES = dbdig.py IOVAPI.py FileAPI.py
ALLFILES = $(SRCFILES) Version.py
UPSLIBDIR = $(UPSDIR)/lib

all: $(DESTDIR) $(UPSLIBDIR) $(WEBSRVDIR)
	cp $(SRCFILES) $(DESTDIR)
	cd $(DESTDIR); echo "Version='$(VERSION)'" > Version.py
	cd $(DESTDIR); chmod +rx $(ALLFILES) 
	cp $(SRCFILES) $(UPSLIBDIR)
	cd $(UPSLIBDIR); echo "Version='$(VERSION)'" > Version.py
	cd $(UPSLIBDIR); chmod +rx $(ALLFILES) 
	cp $(SRCFILES) $(WEBSRVDIR)
	cd $(WEBSRVDIR); chmod +rx $(SRCFILES) 


$(DESTDIR):
	mkdir $@

$(UPSLIBDIR):
	mkdir $@

$(WEBSRVDIR):
	mkdir $@
