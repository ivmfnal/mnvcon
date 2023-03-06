from WSGIApp import WSGISessionApp, WSGIHandler, Request, Response, Application
from  ConfigParser import ConfigParser
import os, psycopg2, time
from datetime import datetime
from IOVAPI import IOVDB
from FileAPI import MnvFileDB
import urllib

import ldap, _ldap, sys

from LRUCache import LRUCache

IOVCache = LRUCache(1, True)

class   ConfigFile(ConfigParser):
    def __init__(self, request, path=None, envVar=None):
        ConfigParser.__init__(self)
        if not path:
            path = request.environ.get(envVar, None) or os.environ[envVar]
        self.read(path)

    
    
class DBConnection:

    def __init__(self, cfg):
        self.Host = cfg.get('Database','host')
        self.DBName = cfg.get('Database','name')
        self.User = cfg.get('Database','user')
        self.Password = cfg.get('Database','password')
        self.Port = cfg.get('Database','port')
        
        #self.Port = 5432
        
        try:    
            self.Port = int(cfg.get('Database','port'))
        except:
            pass
        self.Namespace = 'public'
        try:    
            self.Namespace = cfg.get('Database','namespace')
        except:
            pass
        self.DB = None
        
        self.TableViewMap = {}
        
    def connection(self):
        if self.DB == None:
            self.DB = psycopg2.connect("host=%s dbname=%s user=%s password=%s port=%d" %
                (self.Host, self.DBName, self.User, self.Password, self.Port))
            self.DB.cursor().execute("set search_path to %s" % (self.Namespace,))
        return self.DB

class GUI(WSGIHandler):

    def __init__(self, req, app):
        WSGIHandler.__init__(self, req, app)            
        self.DB = app.iovdb()
        self.FDB = app.filedb()
        self.CFG = app.cfgld()
        # this is for authentication
        self.Ldap_url     = self.CFG.get('Ldap','ldap_server_url')
        self.Ldap_dn_temp = self.CFG.get('Ldap','ldap_dn_template')
        self.AdminList    = self.CFG.get('Users','adminList')
        self.UsersList    = self.CFG.get('Users','usersList')
        self.UserName = None
        self.UserAdmin = False

        self.DbInstance   = self.CFG.get('Database','instance')
    
    def hello(self, req, relpath, **args):
        return self.render_to_response("hello.html", 
                x="Hello")

    def placeHolder(self, req, relpath, **args):
        
        return self.render_to_response("placeHolder.html", 
                x="Not implemented yet")

    def modifyHolder(self, req, relpath, **args):
        fileName=args.get('filename')
        print ' modifyHolder filename= %s' %fileName

        return self.render_to_response("placeHolder.html", 
                x="Not implemented yet")


    def browser(self, req, relpath, folder=None,dtag=None,**args):

        listFolders = self.DB.getFolders()
        listFolders.sort()
        channel = ''
        iovid = ''
        etime = ''
        chan_range = ''      #nor used yet

        crate=  ''
        croc = ''
        chain = ''
        board = ''
        pixel = ''
        detector = ''
        subdet = ''
        module =''
        plane=''
        curChan=args.get('curchan') or ''
        crate=args.get('crate') or ''
        croc=args.get('croc') or ''
        chain=args.get('chain') or ''
        board=args.get('board') or ''
        pixel=args.get('pixel') or ''
        #print ' curChan=%s' %curChan

        nowtime = time.time()   
        return self.render_to_response("page.html", 
             folder = folder,
             dtag = dtag,
             iovid = iovid,
             nowtime = nowtime,
             etime = etime,
             channel = channel,
             curChan =curChan,
             chan_range = chan_range,
             crate = crate,croc = croc,chain = chain,board = board,pixel=pixel,
             detector = detector, subdet = subdet, module =module,
             plane=plane,
             listFolders = listFolders)


    def test_browser(self, req, relpath, **args):
        resp = Response()
        bHead = self.BrowserHeader()
        bTail = self.BrowserTail()
        #resp.headers.add('Content-Type','text/plain')
        resp.status = 200
        resp.write ('%s' %bHead )
        resp.write ('<hr> <p> Hello Browser <p> <hr>')

        listFolders = self.DB.getFolders()
        for aval in listFolders:
           resp.write ('<p> folder = %s' %str(aval) )
           
        resp.write ('%s' %bTail )
        return resp        
            




    def formAction(self, req,relpath, **args):    
        #resp = Response()
        #resp.write('<p> Got it: %s <p>' % (req.str_params,))      
        
        errMsg  =''
        infoMsg =''
        folder=None
        iovid=None
        etime = None
        ftime = None
        ltime = None
        requestedTime =''
        requestedStartTime =''
        requestedEndTime =''
        stime = None
        chan = None
        t0_st = ''
        t1_st = ''
        tagval = ''
        crate=''
        croc = '' 
        chain=''
        board = ''
        pixel = ''
        
        chanComps={}

        #for name, value in req.str_params.items():
        #   print ' %s: %s' % (name, value)

        fields=[]
        for name, value in req.str_params.items():
           value = value.strip()
           if value: fields.append(name)
        #print ' just testing, fields=%s' %fields
            
        for name, value in req.str_params.items():
           #resp.write( '<br> %s: %r' % (name, value) )
           value = value.strip()
           if name=='folder' and value :
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    folder = value

           if name=='tag' and value :
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    tagval = value


           if name=='iovid' and value:
                    iovid = int(value)

           if name=='etime' and value :
                    requestedTime = value
                    etime,errMsg = self.getTime(value)
                    if errMsg:
                       return self.render_to_response("error.html",
                            errMsg = errMsg) 

           if name=='channel' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    chan = int(value)
           #if name=='chan_range' and value.strip():
           #         #resp.write( '<br> USE %s: %r' % (name, value) )
           if name=='ftime' and value :
                    requestedStartTime = value
                    ftime,errMsg = self.getTime(value)
                    if errMsg:
                       return self.render_to_response("error.html",
                            errMsg = errMsg) 

           if name=='ltime' and value :
                    requestedEndTime = value
                    ltime,errMsg = self.getTime(value)
                    if errMsg:
                       return self.render_to_response("error.html",
                            errMsg = errMsg) 


           if ( iovid and etime):
                     errMsg=" Please select either an ID or a time " 
                     return self.render_to_response("error.html",
                            errMsg = errMsg) 

# Checking now if components were entered
        chanComps={}
        chanCompsGen={}
        chanCompsAtt={}


        for name, value in req.str_params.items():

           #resp.write( '<br> %s: %r' % (name, value) )
           value = value.strip()
# This is for all folders except the attenuation
           if name=='crate' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    crate = int(value)
                    chanCompsGen['crate'] = crate
           if name=='croc' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    croc = int(value)
                    chanCompsGen['croc'] = croc
           if name=='chain' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    chain = int(value)
                    chanCompsGen['chain'] = chain
           if name=='board' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    board = int(value)
                    chanCompsGen['board'] = board
           if name=='pixel' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    pixel = int(value)
                    chanCompsGen['pixel'] = pixel

# This is for  the attenuation folder

           if name=='detector' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    detector = int(value)
                    chanCompsAtt['detector'] = detector
           if name=='subdet' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    subdet = int(value)
                    chanCompsAtt['subdet'] = subdet
           if name=='module' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    module = int(value)
                    chanCompsAtt['module'] = module
           if name=='plane' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    plane = int(value)
                    chanCompsAtt['plane'] = plane

#  --------------------------------------------------------------------------------------------------------------------------------
        
        #print ' done for now'
        #sys.exit()
        
        try:
            f = self.DB.openFolder(folder)
        except:
            print 'no permission for folder %s ' %folder

        #if chanCompsGen : print 'keys = %s' %str(chanCompsGen.keys())
        #if chanCompsAtt : print 'keys = %s' %str(chanCompsAtt.keys())

        if (chanCompsGen or chanCompsAtt):

          colist = f._getDataColumns()
          print ' folder %s, cols=%s' %(folder,colist)
          for aval in colist:
             if aval in chanCompsGen.keys(): 
               chanComps = chanCompsGen
               break
             if aval in chanCompsAtt.keys(): 
               chanComps = chanCompsAtt
               break
          
          if not  chanComps:    
            errMsg=" Wrong selection of channel components for current folder " 
            return self.render_to_response("error.html",
                            errMsg = errMsg) 

          #print ' cols found = %s ' %str(colist)
          #print ' dict= %s' %str(chanComps)
          infoMsg = ' Selection Criteria: '
          for ak in chanComps.keys():
             infoMsg = infoMsg + "  %s = %s " %(ak,chanComps[ak])
          #print ' infomsg = %s' %infoMsg
        else:
          pass   #print 'no comps found'             
        
        #errMsg = " Done for now "
        #return self.render_to_response("error.html",
        #                    errMsg = errMsg)
        

        if chanComps and 'getchan' in fields:           #Just get channel number, don't care about time
            #print ' listing fields=%s' %fields
            
            if 'crate' not in fields or 'croc' not in fields or 'chain' not in fields or 'board' not in fields or 'pixel' not in fields:
                errMsg=""" You must provide all components (crate, croc, chain, board, pixel)
                        to make a channel"""
                return self.render_to_response("error.html",
                            errMsg = errMsg) 
            
            curChan =self.makeChannel(crate, croc, chain, board, pixel)
            #print '  crate %s, croc %s, chain %s, board %s, pixel  %s, channel=%s' %( crate, croc, chain, board, pixel,curChan) 
            redirArgs = "curchan=%s&crate=%s&croc=%s&chain=%s&board=%s&pixel=%s" %(curChan,crate,croc, chain, board, pixel)
            self.redirect("./browser?%s" %redirArgs)
              
            #errMsg=""" Channel = %s for components: crate %s, croc %s, chain %s, 
            #        board %s, pixel  %s """ %(curChan,crate, croc, chain, board, pixel)
            #return self.render_to_response("error.html",
            #                msgTyp='info',
            #                errMsg = errMsg) 

# If selecting a channel, find all the IOVs for it and get data.

        if chan >=0:        
          return self.getChanData(req,relpath,f,folder,chan,tagval,requestedStartTime,requestedEndTime,etime)
          print " don't get here..."

        else:  
          if chanComps and not (etime or iovid):
               errMsg=" You must enter an IOV or a time for this selection " 
               return self.render_to_response("error.html",
                    errMsg = errMsg)

          if folder and (etime or iovid or chanComps):
            #resp.write ('<br> getting data')

            #print 'calling getFolderData with tag = %s' %tagval
            t0,t1,colnames,coltypes, data_templ, error = self.getFolderData(req,folder,iovid, etime,chanComps,tagval)

            if t0: t0_st = datetime.fromtimestamp(t0)
            if t1: t1_st = datetime.fromtimestamp(t1)


            if data_templ:
               rowcnt = len(data_templ)
               #print 'found %d cols ' %len(colnames)
               return self.render_to_response("display_data.html", 
                    folder = folder,
                    rowcnt = rowcnt,
                    tagval = tagval,
                    requestedTime =requestedTime,
                    etime = etime, 
                    t0=t0, t0_st=t0_st,
                    t1=t1, t1_st=t1_st,
                    colnames = colnames,
                    infoMsg = infoMsg,
                    data_templ=data_templ)
            else:
               errMsg=" No data found for folder %s " %folder
               if iovid: errMsg = errMsg + " for iovid = %s " %iovid
               if etime: errMsg = errMsg + " for time = %s " %etime
               if tagval:errMsg = errMsg + " and tag = %s " %tagval

               return self.render_to_response("error.html",
                  folder = folder,
                  errMsg = errMsg) 

          else:
            errMsg=" You must specify at least a time or an iovid " #or a  channel"
            return self.render_to_response("error.html",
               folder = folder,
               errMsg = errMsg) 

# ---------------------------------------------------------------------------------------------------------------------------------------------------        
# M.V.W Jan 8 2014
# This returns records for a particulat channel. Initially there was no time constrained but because it takes about 6 secs per 10 iovs, so for a total of 6265
# it would take OVER AN HOUR FOR THE WHOLE SCAN, time range was introduced and if the user does not specify it we force a max number of records returned .
# This number is arbitrary and for now is set to 30.
# Results are presented in order of time, ascending
# If user specifies only a start time, the max number of IOVs is used.
# If user specifies both start and end time but the time interval would be too long, still max IOVs is used.


    def getChanData(self,req,relpath,f,folder,chan,tagval,requestedStartTime,requestedEndTime,etime):

        #print ' getChanData, getting data for chan = %s, folder=%s,etime=%s' %(chan,folder,etime)
        dt_ftime = None
        dt_ltime = None
        chanMsg=''
        timeInterval=''
        timeMsg=''
        chIovsMsg=''
        errMsg=''
        getNext=False
        getPrev=False
        
        iovData = f.iovsAndTime(tagval)
        if len(iovData) == 0:
           errMsg = " No iovs found for folder %s" %folder
           return self.render_to_response("error.html",
                          errMsg = errMsg)
        else:

           fIov=iovData[0][0]
           fIovTim=iovData[0][1]
           lIov=iovData[len(iovData)-1][0]
           lIovTim=iovData[len(iovData)-1][1]
           #print ' first iov %s ,time %s and last  %s %s ' %(fIov,fIovTim,lIov,lIovTim)
           dt_ftime=fIovTim
           dt_ltime=lIovTim
           formStartTime=False
           formEndTime=False
           
           if requestedStartTime: 
                #print ' found requested start time =%s' %requestedStartTime
                dt_ftime=self.strToDatetime(requestedStartTime)
                formStartTime=True
           if requestedEndTime:   
                #print ' found requested end time =%s' %requestedEndTime
                dt_ltime=self.strToDatetime(requestedEndTime)
                formEndTime=True

           #print ' init start =%s, end=%s' %(dt_ftime,dt_ltime)

           totIovs = len(iovData)
           findIovs=totIovs
           maxIovs= 30 # 30
           # Check on time interval if any
           if requestedStartTime and requestedEndTime:
              #print ' Both time specifyed start=%s end=%s ' %(requestedStartTime,requestedEndTime)
              timeInterval=' for time interval %s - %s ' %(requestedStartTime,requestedEndTime)
           if requestedEndTime and not requestedStartTime:
              errMsg=" Please enter a start time " 
              return self.render_to_response("error.html",
                             errMsg = errMsg) 
           if requestedStartTime and not requestedEndTime:
              findIovs=maxIovs
              dt_ltime=lIovTim      # Just needed for logic
              #print ' Query is long, returning only %s results' %findIovs
              chanMsg=" No end time specified: the query is too long; returning the first %s most recent iovs for chan %s " %(findIovs,chan)

           if not requestedStartTime and not requestedEndTime:
              findIovs=maxIovs #10
              #print ' Query is long, returning only %s results' %findIovs
              chanMsg=" No start or end time specified: the query is too long; searching for the first %s iovs for chan %s " %(findIovs,chan)

           chIovs  = 0
           displayData=[]
           chanFound = False
           #print 'found %s iovs %s (using %s)' %(len(iovData),datetime.now(),maxIovs)

           cntiov=0
           maxTime=''
           getData=True          #Jan 08,2014, just for testing on channel queries which take VERY long
           #print 'start time for %s iovs %s' %(findIovs,datetime.now())
           if getData:
             for iov in iovData :             # Just to limit the returned data
               iovid = int(iov[0])
               tim1  = iov[1]
               tim2  = iov[2]
               #print ' iovid = %s,  tim1=%s, tim2 = %s ,next iov=%s '  %(iovid,tim1,tim2,f.getNextIOV(tim1))
               if tim1< dt_ftime:
                   pass #print ' time =%s > %s end  skip ' %(tim1,requestedEndTime)
               else:
                   #print ' save time =%s < %s end  ' %(tim1,requestedEndTime)
                   if tim1 <= dt_ltime:

                       #print ' loop a, max %s cnt, %s  ch %s ' %(maxIovs,cntiov,chIovs)
                       dd=[]
                       dd.append(iovid)
                       dd.append(tim1)
                       dd.append(tim2)
                       #print ' store this  time =%s , interval %s - %s   ' %(tim1,requestedStartTime, requestedEndTime)

                       if tagval:
                           #print ' finding all values for chan %s, and tag = %s' %(chan,tagval)
                           t0,t1,colnames,coltypes, data_db, error = self.getFolderData(req,folder,iovid, etime,chanComps,tagval)
                       else:
                           #print ' finding all values for chan %s ,iovid=%s ' %(chan,iovid)
                           t0,t1,colnames,coltypes, data_db, error = self.getFolderData(req,folder,iovid, etime)

                       cols = ['ID','Start time','End Time']
                       for ac in colnames: cols.append(ac)
                       if data_db:
                         #print ' found data for iov=%s ' %iovid
                         #print ' loop b, max %s cnt, %s  ch %s ' %(maxIovs,cntiov,chIovs)

                         chanFound=False
                         for atup in data_db:
                            #print 'atup= %s ' %str(atup)
                            if int(atup[0]) == chan:
                               chanFound = True
                               chIovs+=1
                               cntiov+=1
                               for aval in atup: dd.append(aval)
                         #print ' iov=%s dd=%s ' %(iovid,dd)
                         displayData.append(dd)
                         #print ' loop c, max %s cnt, %s  ch %s ' %(maxIovs,cntiov,chIovs)

                         if cntiov >= maxIovs: 
                           #print ' reached max cnt %s cnt, %s  ch %s ' %(maxIovs,cntiov,chIovs)
                           maxTime=tim2
                           getNext=True
                           break
                       #displayData.append(dd)
                       
                       if not chanFound: cntiov+=1   

           #for aval in displayData:  print '\n **  dd = %s ' %str(aval)
           #print 'end time for %s iovs %s' %(findIovs,datetime.now())

           #if chanFound == False:
           #print ' check on chIovs =%s' %chIovs
           if chIovs==0:
              if not formStartTime or not formEndTime: 
                #timeMsg= """ Warning: No start or end time was specified on the form, 
                #             checking %s IOVs within time interval: %s - %s""" %(maxIovs,dt_ftime,maxTime)
                errMsg=" Folder %s :No data found for channel %d within time interval: %s - %s<br><br>" %(folder,chan,dt_ftime,maxTime)
              else:
                ltime=dt_ltime
                if maxTime: ltime=maxTime
                errMsg=" Folder %s :No data found for channel %d for time interval %s - %s" %(folder,chan,dt_ftime,ltime)
              
              return self.render_to_response("error.html",
                errMsg = errMsg) 
           else:
              if chIovs<maxIovs:
                   chIovsMsg= 'Found %s '%chIovs
              else:
                   chIovsMsg= 'Returned first %s '%chIovs
           #print 'displayData=%s ' %str(displayData)
           #print 'chIovs=%s, maxIovs=%s, chIovsMsg =%s ' %(chIovs,maxIovs,chIovsMsg)
           ftimeDisp=displayData[0][1]
           ltimeDisp=displayData[len(displayData)-1][1]
           fIov=displayData[0][0]
           lIov=displayData[len(displayData)-1][0]
           #print ' ftimeDisp = %s,(%s) ltimeDisp=%s (%s)' %(ftimeDisp,fIov,ltimeDisp,lIov)
           return self.render_to_response("display_chandata.html", 
                folder = folder,
                tagval=tagval,
                chan = chan,
                chIovsMsg = chIovsMsg,
                totIovs = totIovs,
                cols = cols,
                chanMsg = chanMsg,
                errMsg = errMsg,
                requestedStartTime=requestedStartTime,
                requestedEndTime=requestedEndTime,
                ftimeDisp=ftimeDisp,
                ltimeDisp=ltimeDisp,
                timeInterval=timeInterval,
                getNext=getNext,
                getPrev=getPrev,
                displayData=displayData)

# ---------------------------------------------------------------------------------------------------------------------------------------------------        
    def getMore (self,req,relpath,**args):
        
        etime=None  #not used but needed for method call
        tagval = ''
        getPrev=False
        getMore=False
        for name, value in req.str_params.items():
            print 'getMore, name =%s, val=%s ' %(name,value)
        msg=" Coming soon"
        for name, value in req.str_params.items():
           value = value.strip()
           if name=='folder' and value :
                    folder = value
           if name=='chan' and value :
                    chan = int(value)
           if name=='tagval' and value :
                    tagval = value
           if name=='requestedStartTime' and value :
                    requestedStartTime = value
           if name=='requestedEndTime' and value :
                    requestedEndTime = value
           if name=='ftimeDisp' and value :
                    ftimeDisp = value.split('.')[0]     #Get rid of microsecs
           if name=='ltimeDisp' and value :
                    ltimeDisp = value.split('.')[0]
           if name=='next' and value:
                    getMore=True
           if name=='prev' and value:
                    getPrev=True

        print """ getMore: folder=%s chan=%s, ftimeDisp = %s, ltimeDisp =%s ,reqSTime =%s 
            reqETime=%s""" %(folder,chan,ftimeDisp,ltimeDisp,requestedStartTime,requestedEndTime)

        try:
            f = self.DB.openFolder(folder)
        except:
            print 'no permission for folder %s ' %folder
        if getMore:
            new_requestedStartTime=ltimeDisp
            new_requestedEndTime=requestedEndTime
        if getPrev:
            new_requestedStartTime=ltimeDisp
            new_requestedEndTime=requestedEndTime
           
        return self.getChanData(req,relpath,f,folder,chan,tagval,new_requestedStartTime,new_requestedEndTime,etime)
        #return self.getChanData(req,relpath,f,folder,chan,tagval,requestedStartTime,requestedEndTime,etime)

        
        return self.render_to_response("error.html",
                errMsg = msg) 
        
        
# ---------------------------------------------------------------------------------------------------------------------------------------------------        
        
    #def getFolderData(self,req,folder,iovid, etime,chan,chanDict=None):
    def getFolderData(self,req,folder,iovid, etime,chanComps=None,tagval=None):
        resp = Response()
    
        errMsg=''
        data = None
        t0 = None
        t1 = None
        colnames = []
        coltypes = []
        retData =[]

        tag=tagval
        #if chanComps: print ' in getfolderdata %s' %str(chanComps)
        T0 = time.time()
        f = self.DB.openFolder(folder)
        #print ' Folder %s open' %folder
        
        if etime != None:
          t = float(etime)
          #print ' time = %f ' %t

        if iovid != None:
            iovid = int(iovid)
            #print 'iovid = %d' %iovid
            
        if iovid == None:
            if chanComps:
              #print 'calling getData with comps'
              data = f.getData(t,chanComps=chanComps,tag=tag)
            else:
              data = f.getData(t,tag=tag)
            #print 'getFolderData: get data by t'
        else:
            #print 'getFolderData : get data by iovid = %d' %iovid
            if chanComps:
              #print 'calling getData with comps'
              data = f.getData(iovid=iovid,chanComps=chanComps,tag=tag)
            else:
              data = f.getData(iovid=iovid,tag=tag)

        if data == None:
           errMsg = " Error, no data found "
           return t0,t1,colnames,coltypes,retData,errMsg       
        #else:
        #   print ' getFolderData : got data'

        T1 = time.time()
        #raise '%s' % (iov,)
        #resp = Response()
        if not data:
            resp.status = 400
            resp.write("Data not found")
            return resp
        
        t0, t1 = data.iov()
        t0 = time.mktime(t0.timetuple()) + float(t0.microsecond)/1000000
        if t1 != None:
            t1 = time.mktime(t1.timetuple()) + float(t1.microsecond)/1000000
         
        cols = data.columns()
        colnames = [cn for cn, ct in data.columns()]
        coltypes = [ct for cn, ct in data.columns()]
        T2 = time.time()
        
        retData = data.asList()
        return t0,t1,colnames,coltypes,retData,errMsg       
       

    def flushCache_action(self, req, relpath, **args):
        resp = Response()
        self.DB.flushCache()
        resp.write('Cache flushed')
        return resp        

    def getTime (self,intime):
              
        etime=0
        errMsg=''
        
        if intime.replace('.','').isdigit():
            etime = float(intime)
            #print ' got float'
        elif '-' in intime:
            time_format = "%Y-%m-%d"
            if ':' in intime:
                time_format = "%Y-%m-%d %H:%M:%S"
            #print """got a string time %s with format =%s """ %(intime,time_format)
            stime = time.strptime(intime,time_format)
            ep = time.mktime(stime)
            etime =  float (ep)
        else:
            errMsg=" Malformed date, try again " 

        return etime,errMsg

    def strToDatetime (self,strtime):

        time_format = "%Y-%m-%d"
        if ':' in strtime:time_format = "%Y-%m-%d %H:%M:%S"
        dtime=datetime.strptime(strtime,time_format)
        return dtime
        
    def BrowserHeader(self):

        bHead = """
               <html>
               <head>
                   <title>Minerva Conditions Database Browser </title>
                   <h2>Minerva Conditions Database Browser </h2>
               </head>
               <body>"""
        return bHead
              
       
    def BrowserTail(self):
        bTail = """  </body> </html>"""
        return bTail            

# --------------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------------------
# This is to handle files to be uploaded.

    def fileBrowser(self, req, relpath,**args):

        listDatatypes=[]
        userName,userAdmin = self.checkPermissions()
        
        datatype = new_datatype =''
        detector = new_detector = ''


        listDtyps = self.FDB.getDataTypes()
        for aval in listDtyps: 
            if 'feb' not in aval:listDatatypes.append(aval) 
        listDetectors = self.FDB.getDetectors()

        return self.render_to_response("filepage.html", 
            user = userName, admin = userAdmin,
            instance = self.DbInstance,
            datatype = datatype, new_datatype = new_datatype,
            detector = detector, new_detector = new_detector,
            listDatatypes = listDatatypes,
            listDetectors = listDetectors)

# -------------------------------------------------------------------------------------------------------------------------------
    def fileformAction(self, req,relpath, **args):    

# check  on permissions

        userName,userAdmin = self.checkPermissions()

        datatype = new_datatype =''
        detector = new_detector = ''
        stat = ''
        filelist = []
        errMsg = infoMsg= ''
        saveFiles =[]
        skipFiles =[]
# 


        for name, value in req.str_params.items():
           value = value.strip()
           if name=='filelist' and value :
                    #print ' name = %s, value = %s' %(name,value)
# Check on single file or multiple
                    if ',' in value:
                      filelist = [af for af in value.rsplit(',')]
                    elif '\r\n' in value:
                      filelist = [af for af in value.rsplit('\r\n')]
                      
                    else:
                      filelist = [value]
                    #print 'filelist = %s '  %(filelist)

           if name=='datatype' and value :
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    datatype = value

           if name=='detector' and value :
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    detector = value
                   
                
# -----------------------------------------------------------------------------------------------------------------------------------------        
        #print ' filelist  = %s, datatype = %s, detector = %s' %(filelist,datatype,detector)
        if filelist == []:
           errMsg = " You must enter at least one file name"
           return self.render_to_response("errorFile.html",
                            errMsg = errMsg) 
        

        for afile in filelist:
          stat = self.FDB.checkFileExist(afile)
          #print ' stat = %s' %stat
          if stat ==1:
             skipFiles.append(afile)  
             #return self.render_to_response("errorFile.html",
             #                 errMsg = errMsg) 
          else:
             stat = 0
             errMsg =''
             stat,errMsg = self.FDB.addFile(afile,datatype,detector)
             if errMsg:
                 return self.render_to_response("errorFile.html", 
                      errMsg=errMsg)
               
             ll = [afile,datatype,detector]
             saveFiles.append(ll)  


             test_flags=0
             if test_flags:
                # just for test, setting flag
                gf = self.FDB.genFile(afile)

                gf.setLoaded()
                gf.setVerified()
                gf.setObsolete()

                val = gf.checkLoaded ()
                print 'with gen, load  val = %s ' %val
                if val== True: 
                     print 'after verify, load is true'
                val = gf.checkVerified()
                print 'very  val = %s ' %val

                valo = gf.checkObsolete ()
                print 'obs  val = %s ' %val
                if valo== True: 
                     print 'after verify, obs is true'
                     gf.resetObsolete()
                     valn = gf.checkObsolete ()
                     print ' new obs  val = %s ' %valn


        return self.render_to_response("fileSummary.html",
                  skipFiles = skipFiles,
                  saveFiles = saveFiles)


# -------------------------------------------------------------------------------------------------------------------------------
    def fileformActionDD(self, req,relpath, **args):    

        # check  on permissions
        userName,userAdmin = self.checkPermissions()
       
        new_datatype =''
        new_detector = ''
        stat = ''
        errMsg = infoMsg= ''
        addTypDet = False
# 

        for name, value in req.str_params.items():
           value = value.strip()
                   
           if name=='new_datatype' and value :
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    new_datatype = value
                    addTypDet = True
                      
           if name=='new_detector' and value :
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    new_detector = value
                    addTypDet = True


        if addTypDet == True:
                    statDtyp = 0
                    statDet  = 0
                    statDtyp,statDet= self.FDB.addTypesDet (new_datatype,new_detector)
                    #statDtyp,statDet= f.addTypesDet (new_datatype,new_detector)
                    #print 'statDtyp = %s,statDet =%s ' %(statDtyp,statDet)
                    return self.render_to_response("dtypdet.html",
                            statDtyp = statDtyp,
                            new_datatype = new_datatype,
                            statDet = statDet,
                            new_detector=new_detector) 
        errMsg = " You must enter at least one item to procede"
        return self.render_to_response("errorFile.html",
                            errMsg = errMsg) 
       
# -----------------------------------------------------------------------------------------------------------------------------
    def showAllFilesLinks(self, req, relpath, **args):

        showCols,fileList = self.FDB.showFiles()
        if fileList: 
            msg = "The following files (%d) are in the db ( %s instance)" %(len(fileList),self.DbInstance)
        else:
            msg = "No files found"
        
        return self.render_to_response("show_files_links.html",
                  msg = msg,
                  showCols = showCols,
                  fileList = fileList ) 
# -----------------------------------------------------------------------------------------------------------------------------
    def showAllFiles(self, req, relpath, **args):

        showCols,fileList = self.FDB.showFiles()
        if fileList: 
            msg = "The following files (%d) are in the db ( %s instance)" %(len(fileList),self.DbInstance)
        else:
            msg = "No files found"
        
        return self.render_to_response("show_files.html",
                  msg = msg,
                  showCols = showCols,
                  fileList = fileList ) 
# -----------------------------------------------------------------------------------------------------------------------------
    def showUploadFiles(self, req, relpath, **args):

        showCols,fileList = self.FDB.showFilesToUpload()
        if fileList: 
            msg = "The following files (%d) are ready to be uploaded ( %s instance)" %(len(fileList),self.DbInstance)
        else:
            msg = "No files found ready to be uploaded"
        return self.render_to_response("show_files.html",
                  msg = msg,
                  showCols = showCols,
                  fileList = fileList ) 

# -----------------------------------------------------------------------------------------------------------------------------
    def showVerifyFiles(self, req, relpath, **args):

        showCols,fileList = self.FDB.showFilesToVerify()
        if fileList: 
            msg = "The following files (%d) are ready to be verified ( %s instance)" %(len(fileList),self.DbInstance)
        else:
            msg = "No files found ready to be verified"
        return self.render_to_response("show_files.html",
                  msg = msg,
                  showCols = showCols,
                  fileList = fileList ) 

# -----------------------------------------------------------------------------------------------------------------------------
    def showErrorFiles(self, req, relpath, **args):

        showCols,fileList = self.FDB.showFilesInError()
        if fileList: 
            msg = "The following files (%d) have NON-0 error code ( %s instance)" %(len(fileList),self.DbInstance)
        else:
            msg = "No files found with NON-0 error code"
        return self.render_to_response("show_files.html",
                  msg = msg,
                  showCols = showCols,
                  fileList = fileList ) 

# -----------------------------------------------------------------------------------------------------------------------------
    def uploadFileList(self, req, relpath, **args):
    
        fn = ''
        return self.render_to_response("fileUpload.html",
                  fn = fn ) 

# -----------------------------------------------------------------------------------------------------------------------------
    def uploadFileListAction(self, req, relpath, **args):


        # check  on permissions
        userName,userAdmin = self.checkPermissions()

        errList =[]
        nfiles=0
        saveFiles = []
        msg=''
         
        form = req.str_POST
        fn = form['fn']
   
        if type(fn) ==  type(""):
          errMsg =" Must enter a filename"
          return self.render_to_response("errorFile.html", 
                  errMsg=errMsg)
           
        filedata = fn.file
        if filedata:

          saveFiles,errList = self.FDB.addFileList(filedata)
          if errList:
            errHead = " Verify upload file failed for the following reasons:"
            errTail = " Upload fail, correct input file and try again "
            return self.render_to_response("errList.html",
                  errHead=errHead, 
                  errTail=errTail, 
                  errList=errList)
          else:
            #print 'saveFiles = %s ' %str(saveFiles)
            errMsg = " Added %d files to DB " %len(saveFiles)
            showfiles=[]
            for afile in saveFiles:
               gf = self.FDB.genFile(afile)
               showCols,fileInfo = gf.getInfo()
               showfiles.append(fileInfo)
               
            msg = " The following files were added to the DB"
            return self.render_to_response("show_files.html",
                  msg = msg,
                  showCols = showCols,
                  fileList = showfiles ) 
            
            
        else:
          errMsg =" Must enter a filename"
          return self.render_to_response("errorFile.html", 
                  errMsg=errMsg)


# -----------------------------------------------------------------------------------------------------------------------------
    def modifyFileAction(self, req, relpath, **args):


        # check  on permissions
        userName,userAdmin = self.checkPermissions()

        filename=args.get('filename')

# get flag info for selected file
        gf = self.FDB.genFile(filename)
        allFlags = gf.getFlags()
        print 'allFlags = %s ' %str(allFlags)
        hiddenFlags = allFlags
        flags = allFlags[:3]
        error = allFlags[3]
        comment = allFlags[4]
        #print 'flag = %s ' %str(flags)
        #print 'error = %s ' %str(error)
        #print 'comment = %s ' %str(comment)
        return self.render_to_response("setFlags.html", 
                filename = filename,
                hiddenFlags = hiddenFlags,
                flags=flags,
                error=error,
                comment = comment)

# -----------------------------------------------------------------------------------------------------------------------------
    def modifyFileformAction(self, req, relpath, **args):


        # check  on permissions
        userName,userAdmin = self.checkPermissions()

        for name, value in req.str_params.items():
            value = value.strip()
            if name=='filename' and value :
                #print ' name = %s, value = %s' %(name,value)
                filename = value
            else:
                errMsg = " You must enter a file name"
                return self.render_to_response("errorFile.html",
                            errMsg = errMsg) 
# get flag info for selected file
        gf = self.FDB.genFile(filename)
        allFlags = gf.getFlags()
        hiddenFlags = allFlags
        flags = allFlags[:3]
        error = allFlags[3]
        comment = allFlags[4]
        #print 'flag = %s ' %str(flags)
        #print 'error = %s ' %str(error)
        #print 'comment = %s ' %str(comment)
        return self.render_to_response("setFlags.html", 
                filename = filename,
                hiddenFlags = hiddenFlags,
                flags=flags,
                error=error,
                comment = comment)

# -------------------------------------------------------------------------------------------------------------------------------
    def setFlagsAction(self, req,relpath, **args):    


        # check  on permissions
        userName,userAdmin = self.checkPermissions()

        errMsg = infoMsg= ''
        curValues = []
        newValues = []
        noValues = []
        updValues=[]
        flags=['loaded','verified','obsolete']

        sess = self.getSessionData()
        if sess.has_key('user'):
            userInfo = sess['user']
            if userInfo: 
                userName,userAdmin = sess['user']
                #print ' modify: from session, user = %s, type = %s' %(userName,userAdmin)
            else:
                self.redirect('./login')
        
        else:
            self.redirect('./login')

# 
        for name, value in req.str_params.items():
           value = value.strip()
           #print ' hidden, name = %s, cur val = %s'  %(name,value)      

        for name, value in req.str_params.items():
           value = value.strip()
           if name =='filename' : filename=value
           if name[:4] == 'cur_': curValues.append( (name[4:],value))
           if name[:4] == 'new_': 
                newValues.append( (name[4:],value))
                if name[4:]=='error':  newErr=int(value)
                if name[4:]=='comment': newComm=value


        #print ' curvals = %s ' %str(curValues)
        #print ' newvals = %s ' %str(newValues)
        if newValues:
           newCols = [nc for nc,nv in newValues]
           
        for cc,cv in curValues:
           if cc in flags:
                if cc in  newCols :
                    #print 'update %s set val=True ' %cc
                    updValues.append( (cc,True)) 
                else:
                    #print 'update %s set val=False ' %cc 
                    updValues.append( (cc,False)) 
           elif cc == 'error':
                    if cc in  newCols :
                        #print 'update %s set val=%s ' %(cc,newErr) 
                        updValues.append( (cc,newErr)) 
                         
           elif cc == 'comment':
                    if cc in  newCols :
                        if newComm:
                            #print 'update %s set val=%s ' %(cc,newComm) 
                            updValues.append( (cc,newComm)) 
                        else:
                          pass #print 'no changes for comment'
# add user 
        #print ' username = %s' %userName
        
        updValues.append( ('modify_user',str(userName)))       

        gf = self.FDB.genFile(filename)
        status = gf.updateFlags(updValues)
        if status:
                msg=" File %s updated " %filename
                return self.render_to_response("fileinfo.html",
                  msg = msg) 
        
        else:
                errMsg=" Error in updating file %s " %filename
                return self.render_to_response("error.html",
                  errMsg = errMsg) 
        
# -----------------------------------------------------------------------------------------------------------------------------
# Dealing with Users



# -----------------------------------------------------------------------------------------------------------------------------
    def crmodUsers(self, req, relpath, **args):


        loginUserName,loginUserAdmin = self.checkPermissions()
        uname = ''
        fname = ''
        lname = ''
        flags = [('is_admin',''),('is_active',True)]
        
        return self.commonAction('create',uname,fname,lname,flags,req, relpath, **args)
    
# -----------------------------------------------------------------------------------------------------------------------------
    def createUserAction(self, req, relpath, **args):
        
        loginUserName,loginUserAdmin = self.checkPermissions()

        uname=''
        fname=''
        lname=''
        cruser=''
        adminFlag=False
        activeFlag=True
        userVals=[]
        ulist=['uname','fname','lname']
        flist=['is_admin','is_active']
        for name, value in req.str_params.items():
            value = value.strip()
            if name=='uname': 
                value=value.lower()
                cruser=value
            #print ' name = %s, cur val = %s'  %(name,value)
            if name in ulist and not value:
                errMsg = " You must supply username,first and last name"
                return self.render_to_response("errorFile.html", 
                    returnUri="./crmodUsers",
                    returnUriLabel = "Return to user form",
                      errMsg=errMsg)
            if name in ulist: userVals.append((name,value))
            if name == 'is_admin'  and value: 
                adminFlag=True
                userVals.append((name,adminFlag))
            if name == 'is_active' and value: 
                activeFlag=True
                userVals.append((name,activeFlag))
                 
        userVals.append(('cr_user',loginUserName))
        
        #print ' login user = %s admin = %s , active = %s ' %(loginUserName,adminFlag,activeFlag)
        ret = self.FDB.addUser(userVals)
        if ret==1:
            errMsg=' User %s already in db' %cruser
            #return self.render_to_response("errorFile.html", 
            #      errMsg=errMsg)
        else:
            errMsg='User added succesfully'
        return self.render_to_response("errorFile.html", 
                  returnUri="./crmodUsers",
                  returnUriLabel = "Return to user form",
                  errMsg=errMsg)
            
# -----------------------------------------------------------------------------------------------------------------------------
    def updateUser(self, req, relpath, **args):


        # check  on permissions

        username=args.get('username')
        userInfo= self.FDB.getUsers(username)
        for aval in userInfo:
            uid,uname,fname,lname,adm_val,usr_val,cr_user,cr_time,mod_user,mod_time = aval
            errMsg=' User info %s %s %s %s %s' %(uname,lname,fname,adm_val,usr_val)

        flags = [('is_admin',adm_val),('is_active',usr_val)]
        return self.commonAction('update',uname,fname,lname,flags,req, relpath, **args)
                
# -----------------------------------------------------------------------------------------------------------------------------
    def updateUserAction(self, req, relpath, **args):

        loginUserName,loginUserAdmin = self.checkPermissions()
        uname=''
        fname=''
        lname=''
        modInfo=[]
        ulist=['uname','fname','lname']
        flist=['is_admin','is_active']
        cmlist=['fname','lname','is_admin','is_active']

        flistForm=[]
        flistNames=[]
        flistUpdate=[]
        username=args.get('hide_uname')
        #print ' args = %s' %str(args)
        #print ' current user to modify= %s' %username
        curInfo=[]
        curFlas=[]
        usrInfo= self.FDB.getUsers(username)
        for aval in usrInfo:
            uid,uname,fname,lname,adm_val,usr_val,cr_user,cr_time,mod_user,mod_time = aval
            if uname==username:
                curUInfo=[('fname',fname),('lname',lname)]
                curFInfo=[('is_admin',adm_val),('is_active',usr_val)]
                
        #print ' db current info = %s  %s' %(str(curUInfo),str(curFInfo))
        #for name, value in req.str_params.items():
        #    print ' from form: Mod:  name = %s, cur val = %s'  %(name,value)
            
        for name, value in req.str_params.items():
            value = value.strip()
            if name in ulist and not value:
                errMsg = " You must supply username,first and last name"
                return self.render_to_response("errorFile.html", 
                    returnUri="./updateUser",
                    returnUriLabel = "Return to update form",
                      errMsg=errMsg)

        for cval in curUInfo:
          col,val=cval
          for name, value in req.str_params.items():
              value = value.strip()
              if col==name:
                  if val==value:
                      pass
                      #print ' skip,found same, cl=%s, cv= %s, entered  cl=%s, cv= %s ' %(col,val,name,value)
                  else:
                      #print ' new value for col %s, val= %s ' %(col,value)
                      modInfo.append((name,value))
#Dealing with flags
        ff=[]
        for name, value in req.str_params.items():
            if name in flist:
                #print ' found flag %s, val= %s ' %(name,value)
                ff.append(name)     #if there, is on
        
        if ff: print ' found flags on form %s ' %(str(ff))
        
        for af in curFInfo:
            col,val= af
            #print ' check on flag %s' %col
            if col in ff:
                if val==True: 
                    pass
                    #print 'skip,found same already true'
                else:
                    #print ' col %s will be set to true' %col
                    modInfo.append((col,True))
            else:
                if val==False: 
                    pass
                    #print 'skip, already false'
                else:
                    #print ' settin col %s to false' %col
                    modInfo.append((col,False))

        if modInfo: 
            pass
            #print ' db info = %s  %s' %(str(curUInfo),str(curFInfo))
            #print ' mod info = %s ' %str(modInfo)
        else:
            return self.render_to_response("errorFile.html", 
                  returnUri="./crmodUsers",
                  returnUriLabel = "Return to user form",
                  errMsg='Nothing to modify')

        mods = self.FDB.updateUser(username,modInfo,loginUserName)
        
        if mods==1:
            return self.render_to_response("errorFile.html", 
                  returnUri="./crmodUsers",
                  returnUriLabel = "Return to user form",
                  errMsg='User modified succesfully')
        else:
            errMsg=' User not modified '
            return self.render_to_response("errorFile.html", 
                  errMsg=errMsg)

# -----------------------------------------------------------------------------------------------------------------------------
    def commonAction(self,action,uname,fname,lname,flags,req, relpath, **args):

        actionUrl=''
        if action=='create':
            actionLabel='Create'
            actionUrl='./createUserAction'
            actionFlag=''
        if action=='update':
	    actionLabel='Update'
            actionUrl='./updateUserAction?username=%s' %uname
            actionFlag='mod'
        #print 'actionUrl = %s ' %actionUrl
        listUsers = self.FDB.getUsers()
                         
        showCols= ['User Name','First Name','Last Name','Admin','Active','Cr User','Cr Time','Mod User','Mod Time']
        if listUsers: 
            msg = "The following users were found in the db (%s), click on name to modify" %(self.DbInstance)

        return self.render_to_response("usersPage.html",
	          actionLabel=actionLabel,
                  actionUrl=actionUrl,
                  actionFlag=actionFlag,
                  uname=uname,
                  fname=fname,
                  lname=lname,
                  flags=flags,
                  listUsers = listUsers,
                  showCols=showCols,
                  msg=msg)
# -----------------------------------------------------------------------------------------------------------------------------
    def deleteUser(self, req, relpath, **args):


        # check  on permissions
        loginUserName,loginUserAdmin = self.checkPermissions()

        deluser=args.get('username')
        if deluser==loginUserName:
            errMsg= " You can't delete yourself from the database, contact application admin"
            return self.render_to_response("errorFile.html", 
                  returnUri="./crmodUsers",
                  returnUriLabel = "Return to user form",
                  errMsg=errMsg)
        else:
            errMsg= " You are about to remove user '%s' from the database, select 'Confirm delete' to proceed" %deluser
            return self.render_to_response("errorFile.html", 
                  returnUri="./delUserAction?deluser=%s" %deluser,
                  returnUriLabel = "Confirm delete",
                  flag='delete',
                  errMsg=errMsg)
            
# -----------------------------------------------------------------------------------------------------------------------------
    def delUserAction(self, req, relpath, **args):

        loginUserName,loginUserAdmin = self.checkPermissions()
        if loginUserAdmin is True:
            deluser=args.get('deluser')
            dele= self.FDB.deleteUser(deluser)
            if dele==1:
                return self.render_to_response("errorFile.html", 
                      returnUri="./crmodUsers",
                      returnUriLabel = "Return to user form",
                      errMsg='User %s deleted succesfully' %deluser)
            else:
                errMsg=' User not deleted '
                return self.render_to_response("errorFile.html", 
                      errMsg=errMsg)

# -----------------------------------------------------------------------------------------------------------------------------
# Dealing with FEB data
# -----------------------------------------------------------------------------------------------------------------------------
    def fileformFeb(self, req, relpath, **args):

        listDetectors = self.FDB.getDetectors()
        listFolders = self.getFebFolders()
        
        return self.render_to_response("febpage.html",
	          listDetectors = listDetectors,
                  listFolders = listFolders ) 
        

# -----------------------------------------------------------------------------------------------------------------------------
    def getFebFolders(self):
        
        febFolders=[]
        listFolders = self.DB.getFolders()
        
        ncolsFeb = 44                           #should this go in config file..
        colFeb = 'hg_adc0'         #pick two cols that are specific to FEB data
        for fld in listFolders:
            ff = self.DB.openFolder(fld)
            cols = ff.Columns #getDataType()
            #print ' \n folder = %s, ncols = %d ' %(fld,len(cols)) #str(cols)) 
            # Populate list of folders only with FEB type
            #if len(cols)== ncolsFeb: febFolders.append(fld)
            if len(cols)== ncolsFeb and colFeb in cols: febFolders.append(fld)
        
        # As requested, see this folder first in the list
        minervaFeb='minerva_febs'
        #minervaFeb='mvi_feb_subset'
        if minervaFeb in febFolders:
            febFolders.remove(minervaFeb)
            febFolders.insert(0,minervaFeb)
        
        return febFolders
        
# -----------------------------------------------------------------------------------------------------------------------------

    def fileformFebInfo(self,form):
    
        crate= '' #None
        croc = '' #None
        chain = '' #None
        board = '' #None
        febtime = ''
        datatype = 'feb'
        detector = '' #None
        folder = '' #None
        errMsg = ''
        febinfo = []
        
        for name, value in form.items():

           #print ' in loop, %s, %s ' %(name,value)
           if name !='fn':  value = value.strip()
           if name=='febtime' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    formtime= value
                    febtime,errMsg = self.getTime(value)
                    if errMsg:
                       return self.render_to_response("error.html",
                            errMsg = errMsg) 
           if name=='crate' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    crate = int(value)
           if name=='croc' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    croc = int(value)
           if name=='chain' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    chain = int(value)
           if name=='board' and value:
                    #resp.write( '<br> USE %s: %r' % (name, value) )
                    board = int(value)
	   if name=='detector' and value :
	            #resp.write( '<br> USE %s: %r' % (name, value) )
                    detector = value
	   if name=='folder' and value :
	            #resp.write( '<br> USE %s: %r' % (name, value) )
                    folder = value

        #if febtime and crate != None and croc != None and chain != None and board != None and detector != None and folder != None:
        if febtime and crate>=0 and croc>=0 and chain>=0 and board>=0 and detector and folder:

            #print 'you selected crt=%d,crc = %d, chn=%d,brd=%d ' %(crate,croc,chain,board)
            febMsg = " time = %s (%s), crate = %s, croc= %s, chain = %s, board = %s " %(febtime,formtime,crate,croc,chain,board)
            febinfo = [(folder,datatype,detector),(febtime,crate,croc,chain,board)]

        else:
            errMsg = " You must enter all the fields to procede"

        return errMsg,febinfo
# ----------------------------------------------------------------------------------------------------------------
    def uploadFileFebAction(self, req, relpath, **args):


        # check  on permissions
        userName,userAdmin = self.checkPermissions()
        returnUri = "./fileformFeb"
        returnUriLabel = "Back to FEB menu page"

        form = req.str_POST

#get info
        errMsg,formFebinfo  = self.fileformFebInfo(form)
        if errMsg:
            return self.render_to_response("errorFile.html",
                errMsg = errMsg,
                returnUriLabel = returnUriLabel,
                returnUri = returnUri) 
        
        errList =[]
        febvals =[]
        febchans=[]
        msg=''
        stat = 0

        if formFebinfo: 
          folderName,datatype,detector = formFebinfo[0]
          febinfo = formFebinfo[1]
          febtime = febinfo[0]
          #print ' febtime, dtyp, det = %s,%s,%s' %(febtime,datatype,detector)
         
        
        fn = form['fn']
        
        if type(fn) ==  type(""):
            errMsg =" Must enter a filename"
            return self.render_to_response("errorFile.html",
                errMsg = errMsg,
                returnUriLabel = returnUriLabel,
                returnUri = returnUri) 

        filename = fn.filename
        #print ' fn = %s ' %fn
        #print ' filename from form = %s ' %filename
        ind=filename.rfind('\\')            #This would be the case using IE on Windows
        if ind!=-1:
           filename= filename[(ind+1):]

        #print ' filename to store = %s ' %filename
        #errMsg = " done for now"
        #return self.render_to_response("errorFile.html",
        #                    errMsg = errMsg)
        # Check if file already exists and loaded
        stat = self.FDB.checkFileExist(filename)
        if stat:
            # check if data was loaded
            gf = self.FDB.genFile(filename)
            ldval = gf.checkLoaded ()
            if ldval== True: 
                errMsg = " File %s already exists in DB and data has been already loaded" %filename
                returnUri = "./fileformFeb"
                returnUriLabel = "Back to FEB menu page"
                return self.render_to_response("errorFile.html",
                    errMsg = errMsg,
                    returnUriLabel = returnUriLabel,
                    returnUri = returnUri) 
           
        filedata = fn.file

        febdata, febcols = self.parseFebFile(filedata,febinfo)
        # This was just for debug..
        #cnt=0
        #for cn, tup in febdata.items():
        #    print '\n mm cn = %s, tup = %s ' %(cn,tup)
        #    if cnt==5: break
        #    cnt+=1


        for aval in febdata:
            febchans.append(aval)
        febchans.sort()    
        for achan in febchans:
           febvals.append((achan,febdata[achan]))

# Upload data here
# This is just for test where the folder is a test folder "

        #folderName="mvi_feb_subset"
        
        add=True
        if add:
            fl = self.DB.openFolder(folderName)
            
            try:
	        fl.addData(febtime, febdata,combine=True)
                # Store ad this point FEB file and set the upload flag
                
                stat,errMsg = self.FDB.addFile(filename,datatype,detector)
                if errMsg:
                 return self.render_to_response("errorFile.html", 
                      errMsg=errMsg)
                print ' file added '
                gf = self.FDB.genFile(filename)
                gf.setLoaded()
                print ' file flag loaded '
                
            except:
                errMsg = "Failing in storing FEB data for file %s" %filename
                returnUri = "./fileformFeb"
                returnUriLabel = "Back to FEB menu page"
                return self.render_to_response("errorFile.html",
                    errMsg = errMsg,
                    returnUriLabel = returnUriLabel,
                    returnUri = returnUri) 

        msgSuccess = " Succesfull upload from file %s for folder %s, showing channels uploaded " %(filename,folderName)
        return self.render_to_response("show_feb_data.html",
                  febcols = febcols, 
                  febvals=febvals,
                  msg = msgSuccess,
                  returnUriLabel = returnUriLabel,
                  returnUri = returnUri) 

        #return self.render_to_response("errorFile.html", 
        #          errMsg='Done for now')


# -------------------------------------------------------------------------------------------------------------------
    #  Parse a single line of data from an feb calibration data file  (got this from Dave and modified accordingly)

    def parseFebFile(self, filedata,febinfo):

        errMsg = ''

        data = {}
        columns = self.getColumnNames()
        
        if filedata:
            lines = filedata.readlines()
            fdata = [ln.strip() for ln in lines]
            #print ' feb files has %d lines' %len(fdata)
            for aline in fdata:
                if aline[0] != 'H':
                    tup,errMsg = self.parseFebLine(aline,febinfo)
                    if errMsg:
                        return self.render_to_response("errorFile.html", 
                        errMsg=errMsg)

                    chid, tup = tup
                    data[chid] = tup
                    #print 'CHID, data: ', chid, ',', data[chid]
                #else:
                #    print '\n === feb header line = %s' %aline
        
        if len(columns) != 44 or len(data) != 64:
            errMsg =  "It seems the file  %s may NOT be an feb file!!" %fn
            return self.render_to_response("errorFile.html", 
                  errMsg=errMsg)

        return data, columns

# -------------------------------------------------------------------------------------------------------------------
    #  Parse a single line of data from an feb calibration data file  (got this from Dave and modified accordingly)

    def parseFebLine(self, line,febinfo):

        errMsg = ''
        words = line.split()
        if len(words) != 58:
            errMsg =  " Malformed line in FEB data file"
            return errMsg
        febtime,crate,croc,chain,board = febinfo
        #print 'for parse, febtime=%s, crt=%d,crc = %d, chn=%d,brd=%d ' %(febtime,crate,croc,chain,board)
        
        pixel = int(line.strip()[2:4])-1
        
        return (self.just(crate, croc, chain, board, pixel),
                (crate, croc, chain, board, pixel,
                 float(words[1]), float(words[3]), float(words[4]), float(words[6]), float(words[7]), 
                 float(words[9]), float(words[10]), float(words[12]), float(words[13]), float(words[15]),
                 float(words[16]), float(words[18]), float(words[19]), float(words[20]), float(words[22]), 
                 float(words[23]), float(words[25]), float(words[26]), float(words[28]), float(words[29]),
                 float(words[31]), float(words[32]), float(words[34]), float(words[35]), float(words[37]),
                 float(words[38]), float(words[39]), float(words[41]), float(words[42]), float(words[44]), 
                 float(words[45]), float(words[47]), float(words[48]), float(words[50]), float(words[51]),
                 float(words[53]), float(words[54]), float(words[56]), float(words[57]))),errMsg 

# got this from iov code..for febs

    #def getChannels(self,chanComps)
        
    
    
    def makeChannel(self,crate, croc, chain, board, pixel):
        chan=   ( ((crate << 25) &    0x1e000000L) | 
            ((croc << 20) &     0x1f00000L) |
            ((chain << 17) &    0xe0000L) |
            ((board << 12) &    0x1f000L) |
            ((pixel << 5) &     0xfe0L)
            )
        #print '\n makeChannel chan = %d --- crt %d  crc %d chin %d brd %d pix %d ' %(chan,crate, croc, chain, board, pixel)
        return chan

    def getColumnNames(self):

        cols = ['crate', 'croc', 'chain', 'board', 'pixel']
        fields=['adc0','slope1','xkink1','slope2','xkink2','slope3']
        gains=['hg','mg','lg']
        for g in gains:
            for f in fields:
                cols.append(g+'_'+f)
                cols.append(g+'_'+f+'_err')
            cols.append(g+'_chi2ndf')
         
        return cols
                    
# -----------------------------------------------------------------------------------------------------------------------------
# Dealing with login and authentication for fileBrowser
# -----------------------------------------------------------------------------------------------------------------------------

    def login(self, req, relpath, message='', **args):
        action_url = "./doLogin"

        #sess = self.getSessionData()
        #if sess.has_key('user'):
        #  sess['user'] = ''
          #print 'login, userInfo = %s ' %str(sess['user'])

        return self.render_to_response('login.html', 
                    message = message, 
                    action = action_url
                )


    def doLogin(self, req, relpath, **args):

        username = req.POST.get('username', None)
        password = req.POST.get('password', None)
        useradmin = False

        uname = self.authenticate(username=username, password=password)
        if uname == None:
            self.redirect('./login?message=%s' %
                (urllib.quote('Authentication error: incorrect username or password '),))
        
        # Get users from db
        listUsers = self.FDB.getUsers()

        if listUsers:
          userNames=[]
          for aval in listUsers:
             userNames.append(aval[1]) 
          #print 'users = %s ' %str(userNames)

          if username not in userNames:
            self.redirect('./login?message=%s' % (' You are not authorized to view this website'))

          for aval in listUsers:
            #print 'aval = %s' %str(aval)
            uid,uname,lname,fname,is_adm,is_usr,cr_user,cr_time,mod_user,mod_time = aval
            if uname == username:
                if is_adm is True: 
                    useradmin = True
                    self.UserAdmin = True
        
          sess = self.getSessionData()
          sess['user'] = (username,useradmin)
          sess.save()

        else:
            errMsg=" No users found in database, please contact the application administrator."
            return self.render_to_response("errorFile.html",
                            errMsg = errMsg,
                            flag='exit')
                    
        self.redirect('./fileBrowser')
        

    def authenticate(self, username=None, password=None):

        
        ldap_url = self.Ldap_url                    #getConfig("LDAP_SERVER_URL")
        dn_temp  = self.Ldap_dn_temp                #getConfig("LDAP_DN_TEMPLATE")
        
        if not ldap_url or not dn_temp:
            return None
        else:
            pass
            #print ' os = %s ' %(os.environ['LD_LIBRARY_PATH'])
            #print ' p-path = %s' %(sys.path)
            #print "ldap_url = '%s', dn_temp = '%s' " %(ldap_url,dn_temp)

        if not password:    return None

        dn = dn_temp % (username)

        ld = ldap.initialize(ldap_url, trace_level=4, trace_file=open('/tmp/trace','w'),trace_stack_limit=20)
        
        #try:
        #    u = User.objects.get(username = username)
        #except User.DoesNotExist:
        #    return None
        
        try:    
            ld.simple_bind_s( dn, password )
            #print 'LDAP OK %s  dn=%s %s' % (type(dn), dn,  ldap.__file__)
        except ldap.INVALID_CREDENTIALS:
            print  ' LDAP error in binding '
            return None
        return username

    def checkPermissions(self):

        sess = self.getSessionData()
        #print ' check on session'
        if sess.has_key('user'):
            userInfo = sess['user']

            if userInfo: 
                userName,userAdmin = sess['user']
                return userName,userAdmin
                #print ' filebrowser: from session, user = %s, type = %s' %(userName,userAdmin)
            else:
                self.redirect('./login')
                self.redirect('./login?message=%s' %
                    (urllib.quote("You don't have permission for current action "),))
        
        else:
            self.redirect('./login')

    def tag_list(self, req, relpath, folder=None, **args):
        tags=[]
        if folder != 'undefined':
            f = self.DB.openFolder(folder)
            tags = f.tags()
            if tags: 
                return self.render_to_response("tag_list.json", tags=tags)
        return self.render_to_response("tag_nolist.json", tags=tags)

# --------------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------------
class FMRequestHandler(WSGIHandler):

    def __init__(self, req, app):
        WSGIHandler.__init__(self, req, app)
        self.GUI = GUI(req, app)

    def probe(self, req, relpath, **args):
        if self.App.iovdb().probe():
            return Response("OK")
        else:
            resp = Response("Error")
            resp.status = 400
            return resp
        

class FileManagerApp(WSGISessionApp):
#class IOVServerApp(WSGIApp):

    def __init__(self, request, rootclass):
        WSGISessionApp.__init__(self, request, rootclass)
        self.Config = ConfigFile(request, envVar = 'IOV_SERVER_CFG')
        self.DB = DBConnection(self.Config)
        self.IOVDB = None
        self.FileDB = None
        self.initJinja2(tempdirs = [os.path.dirname(__file__)])
        
        #self.initJinja2(tempdirs = [os.path.dirname(__file__)])

    def initPersistentIOVDB(self):
        global IOVCache
        iovdb = IOVDB(self.DB.connection(), cache_size=5)
        if IOVCache != None:
            iovdb.useCache(IOVCache)
        else:
            IOVCache = iovdb.getCache()
        return iovdb

    def cfgld(self):
       return self.Config
               
    def iovdb_(self):
        if self.IOVDB == None:  self.IOVDB = self.initPersistentIOVDB()
        return self.IOVDB

    def iovdb(self):
        iovdb = IOVDB(self.DB.connection(), cache = IOVCache)
        return iovdb

    def filedb(self):
        if self.FileDB == None:  self.FileDB = MnvFileDB(self.DB.connection())
        return self.FileDB

    def destroy(self):
        if self.IOVDB:  self.IOVDB.disconnect()
        self.IOVDB = None
        
application = Application(FileManagerApp,FMRequestHandler)
        
    
