SRCFILES = create_table.py write_data.py tag.py read_data.py get_csv.py
ALLFILES = $(SRCFILES) Version.py
UPSTOOLSDIR = $(UPSDIR)/bin


all: $(DESTDIR) $(UPSTOOLSDIR)
	cp $(SRCFILES) $(DESTDIR)
	cd $(DESTDIR); echo $(VERSION) > Version.py
	cd $(DESTDIR); chmod +rx $(ALLFILES) 
	cp $(SRCFILES) $(UPSTOOLSDIR)
	cd $(UPSTOOLSDIR); echo $(VERSION) > Version.py
	cd $(UPSTOOLSDIR); chmod +rx $(ALLFILES) 
    


$(DESTDIR):
	mkdir -p $@
    
$(UPSTOOLSDIR):
	mkdir -p $@
    
    
