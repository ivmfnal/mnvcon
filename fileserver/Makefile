SRCFILES = Server.py FileManagerApp.py config.sample \
	LRUCache.py ConnectionPool.py \
    hello.html BasicTemplate.html page.html filepage.html \
    display_data.html display_chandata.html \
    fileSummary.html setFlags.html \
    error.html errorFile.html errList.html placeHolder.html dtypdet.html \
    fileUpload.html login.html show_files.html show_files_links.html \
    fileinfo.html febpage.html  fileFebUpload.html show_feb_data.html minerva.css \
    tag_list.json tag_nolist.json usersPage.html ReleaseNotes.html

ALLFILES = $(SRCFILES) Version.py __init__.py

all: $(DESTDIR)
	cp $(SRCFILES) $(DESTDIR)
	cd $(DESTDIR); echo $(VERSION) > Version.py; touch __init__.py
	cd $(DESTDIR); chmod +rx $(ALLFILES) 


$(DESTDIR):
	mkdir $(DESTDIR)
