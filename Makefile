VERSION = 6.7

PRODUCT = mnvcon
DESTDIR = $(HOME)/build/$(PRODUCT)
TARDIR = /tmp/$(USER)

UPSDIR = $(DESTDIR)/ups
WEBSRVDIR = $(DESTDIR)/$(PRODUCT)_webserver
TOOLSDIR = $(DESTDIR)/$(PRODUCT)_tools
APIDIR = $(DESTDIR)/$(PRODUCT)_api
SRVTAR = $(TARDIR)/$(PRODUCT)_webserver_$(VERSION).tar
APITAR = $(TARDIR)/$(PRODUCT)_api_$(VERSION).tar
TOOLSTAR = $(TARDIR)/$(PRODUCT)_tools_$(VERSION).tar
UPSTAR = $(TARDIR)/$(PRODUCT)_ups_$(VERSION).tar

all:    build tarfiles #ups
	@echo
	@echo API tarfile ...... $(APITAR)
	@echo Server tarfile ... $(SRVTAR)
	@echo Tools tarfile .... $(TOOLSTAR)
	@echo UPS tarfile ...... $(UPSTAR)
	@echo

clean:
	rm -rf $(DESTDIR) $(APITAR) $(SRVTAR) $(WEBSRVDIR) $(UPSDIR)

build: $(DESTDIR) $(UPSDIR) $(WEBSRVDIR) $(TOOLSDIR) $(APIDIR)
	cd tools; make DESTDIR=$(TOOLSDIR) UPSDIR=$(UPSDIR) VERSION=$(VERSION)
	cd samples; make DESTDIR=$(TOOLSDIR) UPSDIR=$(UPSDIR) VERSION=$(VERSION)
	cd api; make DESTDIR=$(APIDIR) UPSDIR=$(UPSDIR) VERSION=$(VERSION) WEBSRVDIR=$(WEBSRVDIR)
	cd webserver; make DESTDIR=$(WEBSRVDIR) VERSION=$(VERSION)
	#cd fileserver; make DESTDIR=$(WEBSRVDIR) VERSION=$(VERSION)
	cd ups; make UPSDIR=$(UPSDIR) VERSION=$(VERSION)
    
    
tarfiles:  $(TOOLSDIR) $(TARDIR) 
	cd $(TOOLSDIR); tar cf $(TOOLSTAR) *
	cd $(APIDIR); tar cf $(APITAR) *
	cd $(WEBSRVDIR); tar cf $(SRVTAR) *
	cd $(UPSDIR); tar cf $(UPSTAR) *



$(DESTDIR):
	mkdir -p $@

$(WEBSRVDIR):
	mkdir -p $@

$(APIDIR):
	mkdir -p $@
	
$(TOOLSDIR):
	mkdir -p $@
	
$(TARDIR):
	mkdir -p $@

$(UPSDIR):
	mkdir -p $@
