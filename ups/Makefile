SRCFILES = mnvcon.table
DSTDIR = $(UPSDIR)/ups

all: $(DSTDIR)
	cp $(SRCFILES) $(DSTDIR)
	cd $(DSTDIR); chmod +rx $(SRCFILES) 

$(DSTDIR):
	mkdir $@

