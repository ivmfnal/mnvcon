SRCFILES = create.py \
    fetch.py \
    parsePedestalFile.py \
    populate.py \
    populate_int_time.py \
    populate_tags.py

ALLFILES = $(SRCFILES) Version.py

UPSTOOLSDIR = $(UPSDIR)/samples

all: $(DESTDIR) $(UPSTOOLSDIR)
	cp $(SRCFILES) $(DESTDIR)
	cd $(DESTDIR); echo $(VERSION) > Version.py
	cd $(DESTDIR); chmod +rx $(ALLFILES) 
	cp $(SRCFILES) $(UPSTOOLSDIR)
	cd $(UPSTOOLSDIR); echo $(VERSION) > Version.py
	cd $(UPSTOOLSDIR); chmod +rx $(ALLFILES) 
    

$(DESTDIR):
	mkdir $@
    
$(UPSTOOLSDIR):
	mkdir $@
