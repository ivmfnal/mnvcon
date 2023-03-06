import psycopg2
import time
import sys,string
from datetime import datetime, timedelta
from dbdig import DbDig
from psycopg2 import extensions

Version = "$Id: FileAPI.py,v 1.19 2013/10/02 21:29:32 vittone Exp $"


#class IOVDB:
class MnvFileDB:
    def __init__(self, db = None, connstr = None, namespace = 'public',
            role = None):
        assert not (db == None and connstr == None)
        self.Connstr = connstr
        self.Namespace = namespace
        self.Role = role
        if db:
            self.DB = db
        else:
            self.reconnect()
        self.DataTypes = {}
        mvitest = False #True #
        if mvitest:
            self.FileTable  = 'mvi_upload_files'
            self.DatypTable = 'mvi_upload_files_datatypes'
            self.DetTable   = 'mvi_upload_files_detectors'
        else:
            self.FileTable  = 'upload_files'
            self.DatypTable = 'upload_files_datatypes'
            self.DetTable   = 'upload_files_detectors'
            self.UsersTable = 'upload_users_privs'
        
        self.showCols = ['filepath','datatype','detector','loaded','verified','obsolete','add_time','error','comment','modify_time','modify_user']


    def reconnect(self):
        assert self.Connstr != None
        self.DB = psycopg2.connect(self.Connstr)
        self.DB.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        if self.Role:
            self.DB.cursor().execute("set role %s" % (self.Role,)) 
            c = self._cursor()
            c.execute("show role")
            #print "Role %s set, %x" % (c.fetchone()[0],id(self.DB))           
        self.DB.cursor().execute("set search_path to %s" % (self.Namespace,))
        
    def disconnect(self):
        if self.DB != None:
            self.DB.close()
        self.DB = None

    def useDB(self, db):
        self.DB = db



    def probe(self):
        c = self.DB.cursor()
        c.execute("select 1")
        return c.fetchone()[0] == 1
        
    def _cursor(self):
        return self.DB.cursor()


    def _tablesExist(self, *tlist):
        tables = DbDig(self.DB).tables(self.Namespace)
        if not tables:  return False
        tables = [t.lower() for t in tables]
        for t in tlist:
            if not t.lower() in tables: return False
        return True
        
    def _columns(self, table):
        columns = DbDig(self.DB).columns(self.Namespace, table)
        return [(c[0], c[1]) for c in columns]
        
    def _typeName(self, ctype):
        if ctype not in self.DataTypes:
            c = self._cursor()
            c.execute("select pg_catalog.format_type(%d, null)" % (ctype,))
            self.DataTypes[ctype] = c.fetchone()[0]
        return self.DataTypes[ctype]


    def getDataTypes(self):
      
        sql = """ select * from %s """ %self.DatypTable
        c = self._cursor()
        c.execute(sql)
        data = c.fetchall()
        lst = (aval[0] for aval in data)
        return lst

    def getDetectors(self):
      
        sql = """ select * from %s """ %self.DetTable
        c = self._cursor()
        c.execute(sql)
        data = c.fetchall()
        lst = (aval[0] for aval in data)
        return lst


    def addTypesDet(self,dtyp=None,det=None):
        
        statDt = statDet = ''
        if dtyp: 
            statDt = self.addDataTypes(dtyp)
        if det : 
            statDet = self.addDetectors(det)
        return statDt,statDet


    def addDataTypes(self,dtyp):
      
        sql = """ select * from %s """ %self.DatypTable
        c = self._cursor()
        c.execute(sql)
        data = c.fetchall()
        lst = (aval[0] for aval in data)
        
        if dtyp in lst: return 1
        insql = "insert into %s(datatype) values('%s')" %(self.DatypTable,dtyp)
        c.execute(insql)
        c.execute('commit')
        return 0

    def addDetectors(self,det):
      
        sql = """ select * from %s """ %self.DetTable
        c = self._cursor()
        c.execute(sql)
        data = c.fetchall()
        lst = (aval[0] for aval in data)
        if det in lst: return 1
        insql = "insert into %s(detector) values('%s')" %(self.DetTable,det)
        c.execute(insql)
        c.execute('commit')
        return 0


    def checkDataTypes(self,dtyp):
      
        sql = """ select datatype from %s where datatype = '%s' """ %(self.DatypTable,dtyp)
        c = self._cursor()
        c.execute(sql)
        data = c.fetchone()
        if data==None:
          return None
        return 0
    def checkDetectors(self,det):
      
        sql = """ select detector from %s where detector = '%s' """ %(self.DetTable,det)
        c = self._cursor()
        c.execute(sql)
        data = c.fetchone()
        if data==None:
          return None
        return 0

    def getUsers(self,userOpt=None):
      
        sql = " select * from %s " %self.UsersTable
        if userOpt:
            whereClause= " where uname='%s' " %userOpt
            sql = sql + whereClause
        sql = sql + "  order by uname"
        c = self._cursor()
        c.execute(sql)
        data = c.fetchall()
        return data

    def addUser(self,users):
        data = self.getUsers()
        lst=[]
        cols=[]
        vals=[]
        for aval in data:
            uid,uname,fname,lname,is_adm,is_usr,cr_user,cr_time,mod_user,mod_time = aval
            lst.append(uname)
        for aval in users:
            if aval[0]=='uname' and aval[1] in lst: 
                return 1
            cols.append(aval[0])
            av="'%s'" %aval[1]
            vals.append(av)
        cn=','.join(cols)
        
        cv=','.join(vals)
        insql = "insert into %s(%s,cr_time) values(%s,now())" %(self.UsersTable,cn,cv)
        #print '%s' %insql
        c = self._cursor()
        c.execute(insql)
        c.execute('commit')
        return 0

    def updateUser(self,user,userVals,loginUserName):
        for aval in userVals:
            col,val=aval
            #print 'col = %s, val= %s ' %(col,str(aval))        
        sqlSetList = []
        for aval in userVals:
            if type(aval[1]) == type(""):
              clause = "%s='%s'" % (aval[0],aval[1])
            else:
              clause = "%s=%s" % (aval[0],aval[1])
            sqlSetList.append(clause)    
        
        sqlSetList.append("mod_user='%s',mod_time=now()" %loginUserName)
        sql= "update %s set %s where uname = '%s'" % (self.UsersTable,string.join(sqlSetList,', '),user)
        #print " update sql = %s" %sql
        c = self._cursor()
        c.execute(sql)
        c.execute('commit')
        
        return 1

    def deleteUser(self,user):
    
        sql = "delete from %s where uname='%s' " %(self.UsersTable,user)
        #print " delete sql = %s" %sql
        c = self._cursor()
        c.execute(sql)
        c.execute('commit')
        return 1
        

# ----------------------------- This method is used by the user ----------------------------------------

    def getFiles (self,datatype=None,detector=None,loaded=None,verified=None,obsolete=None,error=None):

        #print 'getfiles'
        fileList =[]
        wclause = []
        whereClause = ''
        if datatype != None:
          wcl = "%s='%s'" %('datatype',datatype)
          wclause.append(wcl)
        if detector != None:
          wcl = "%s='%s'" %('detector',detector)
          wclause.append(wcl)
        if loaded != None:
          wcl = "%s=%s" %('loaded',loaded)
          wclause.append(wcl)
        if verified != None:
          wcl = "%s=%s" %('verified',verified)
          wclause.append(wcl)
        if obsolete != None:
          wcl = "%s=%s" %('obsolete',obsolete)
          wclause.append(wcl)
        if error != None:
          wcl = "%s=%s" %('error',error)
          wclause.append(wcl)
          
        if wclause:
          whereClause = " where %s " %(string.join(wclause,' and '))
        
        #print '  whereClause = %s ' %whereClause 
           
        
        sql = """ select filepath,datatype,detector,loaded,verified,obsolete from %s %s 
                  order by add_time desc""" %(self.FileTable,whereClause)
        #print 'sql =%s ' %sql
        c = self._cursor()
        c.execute(sql)
        data = c.fetchall()
        for aval in data:
           filename,dtyp,det,ld,ver,obs = aval
           #print ' filename =%s, dtyp =%s,det =%s,ld =%s,ver =%s,obs  =%s ' %(filename,dtyp,det,ld,ver,obs)
           fileList.append( (filename,dtyp,det))
        return fileList

    def getFileNames (self):

        lst=[]
        sql = """ select filepath from %s """ %(self.FileTable)
        c = self._cursor()
        c.execute(sql)
        data = c.fetchall()
        if data:
            for aval in data:
                lst.append(aval[0])
        return lst

    def showFiles (self):
    
        fileList =[]
        fileInfo = self.getFilesInfo()
        #showCols = ['filepath','datatype','detector','loaded','verified','obsolete','add_time','error','comment']
        for aval in fileInfo:
           idf,filep,dtyp,det,ld,ver,obs,addtim,err,comm,modtim,modusr = aval
           fileList.append( (filep,dtyp,det,ld,ver,obs,addtim,err,comm,modtim,modusr))   
        
        return self.showCols,fileList


    def showFilesToUpload (self):
    
        fileList =[]
        fileInfo = self.getFilesInfo()
        #showCols = ['filepath','datatype','detector','loaded','verified','obsolete','add_time','error']
        for aval in fileInfo:
           idf,filep,dtyp,det,ld,ver,obs,addtim,err,comm,modtim,modusr = aval
           if ld == False and obs == False and err == 0:
             #print ' filename =%s, dtyp =%s,det =%s,ld =%s,ver =%s,obs  =%s ' %(filep,dtyp,det,ld,ver,obs)

             fileList.append( (filep,dtyp,det,ld,ver,obs,addtim,err,comm,modtim,modusr))   
        
        return self.showCols,fileList

    def showFilesToVerify (self):
    
        fileList =[]
        fileInfo = self.getFilesInfo()
        #showCols = ['filepath','datatype','detector','loaded','verified','obsolete','add_time','error']
        for aval in fileInfo:
           idf,filep,dtyp,det,ld,ver,obs,addtim,err,comm,modtim,modusr = aval
           if ld == True and ver == False and obs == False and err == 0:
             #print ' filename =%s, dtyp =%s,det =%s,ld =%s,ver =%s,obs  =%s ' %(filep,dtyp,det,ld,ver,obs)

             fileList.append( (filep,dtyp,det,ld,ver,obs,addtim,err,comm,modtim,modusr))   
        
        return self.showCols,fileList

    def showFilesInError (self):
    
        fileList =[]
        fileInfo = self.getFilesInfo()
        #showCols = ['filepath','datatype','detector','loaded','verified','obsolete','add_time','error']
        for aval in fileInfo:
           idf,filep,dtyp,det,ld,ver,obs,addtim,err,comm,modtim,modusr = aval
           if err != 0:
             #print ' filename =%s, dtyp =%s,det =%s,ld =%s,ver =%s,obs  =%s ' %(filep,dtyp,det,ld,ver,obs)

             fileList.append( (filep,dtyp,det,ld,ver,obs,addtim,err,comm,modtim,modusr))   
        
        return self.showCols,fileList


# ----------------------------- This method is used by the user ----------------------------------------

    def getFilesToUpload (self,datatype=None,detector=None):
    
        fileList = self.getFiles(datatype,detector,loaded=False,error=0)
        
        return fileList



    def getFilesInfo(self):
      
        
        sql = """ select idfile,filepath,datatype,detector,loaded,verified,obsolete,
                  to_char(add_time,'YYYY-MM-DD HH24:MI:SS'),error,comment,modify_time,modify_user 
                  from %s order by add_time desc""" %self.FileTable

        c = self._cursor()
        c.execute(sql)
        data = c.fetchall()
        lst = (aval for aval in data)
        return lst



    def checkFileExist(self,filename):

        stat = 0
        sql = "select filepath from %s where filepath ='%s' "%(self.FileTable,filename)
        #print ' verify sql = %s' %sql
        c = self._cursor()
        c.execute(sql)
        data = c.fetchone()
        if data :
           stat = 1
           return stat
        return stat
      

    def addFile(self,filename,datatype,detector):

        statAdd = 1
        statCheck = 0
        errMsg = ''

# check if file exists
        statCheck = self.checkFileExist(filename)        
        if statCheck:
          errMsg = " File %s already exists in DB " %filename
          statAdd = 0
          return statAdd,errMsg
          
        sql = """insert into %s(filepath,datatype,detector,add_time,error) values('%s','%s','%s',now(),0)""" %(self.FileTable,filename,datatype,detector )
   
        #print 'sql = %s' %sql 
        c = self._cursor()
        try:
          c.execute(sql)
        except:
          errMsg = 'Error: %s %s<br>SQL statement: %s' % (sys.exc_info()[0], sys.exc_info()[1], sql)
          #print ' addfile, errMsg = %s' %errMsg
          statAdd = 0
          return statAdd,errMsg
          

        c.execute('commit')
        return statAdd,errMsg


    def verifyFileList(self,filedata):

        errList = []
        lines = filedata.readlines()
        fdata = [ln.strip() for ln in lines]
 
# Make sure first each line has 3 args
        ll=1
        for aline in fdata:
           aval = [af.strip() for af in aline.rsplit(',')]
           print('%s, len= %d ' %(aline, len(aval)))
           if len(aval) !=3:
                err = " Error in parsing file at line %d <br> %s " %(ll,aline)
                errList.append(err)
           for av in aval:
              if av == '':
                err = " Error in parsing file at line %d <br> %s " %(ll,aline)
                errList.append(err)

           ll+=1
        if errList:
            return fdata,errList
 
        for aline in fdata:
           aval = [af.strip() for af in aline.rsplit(',')]
             
           afile,dtyp,det = aval
           sf = self.checkFileExist(afile)
           if sf == 1:
             errList.append(" File %s: file already in DB" %afile)

           sd = self.checkDetectors(det)
           #sd = self.checkDetectors(det)
           if sd ==None:
             err = " File %s: detector ='%s' not found in DB" %(afile,det)
             errList.append(err)

           st = self.checkDataTypes(dtyp)
           #st = self.checkDataTypes(dtyp)
           if st ==None:
             err = " File %s: datatype ='%s' not found in DB" %(afile,dtyp)
             errList.append(err)

        return fdata,errList

    def addFileList(self,filedata):

        nfiles = 0
        errMsg = ''
        errFound = []
        saveFiles = []
        errList =[]
        fdata =[]
        
# First validate file
        verifyfile = filedata
        fdata, errList = self.verifyFileList(verifyfile)
        if errList :
            #print 'verifylist failed, %s' %str(errList) 
            return saveFiles,errList
           
        for aline in fdata:
           aval = [af.strip() for af in aline.rsplit(',')]
           afile,dtyp,det = aval
           stat,errMsg = self.addFile(afile,dtyp,det)
           if errMsg:
             return saveFiles,errList.append(errMsg)
           nfiles+=1
           saveFiles.append(afile)  
        return saveFiles,errList



    def initFile(self):
        return MnvFile(self)

    def genFile(self,filename):
        return MnvFile(self,filename)


# ----------------------------------------------------------------------------------------------------------------------------------------------------                    
# ----------------------------------------------------------------------------------------------------------------------------------------------------                    

#class IOVFile:
class MnvFile:
    def __init__(self, db, filename ):
        self.DB = db
        self.Name = filename

        mvitest= False #True #
        if mvitest:
            self.FileTable  = 'mvi_upload_files'
            self.DatypTable = 'mvi_upload_files_datatypes'
            self.DetTable   = 'mvi_upload_files_detectors'
        else:
            self.FileTable  = 'upload_files'
            self.DatypTable = 'upload_files_datatypes'
            self.DetTable   = 'upload_files_detectors'
            self.UsersTable = 'upload_users_privs'
        


    # ------------------------------------------------------------------------------------
    def getInfo(self):
        
        showCols = ['filepath','datatype','detector','loaded','verified','obsolete','add_time']
        
        sql = """ select filepath,datatype,detector,loaded,verified,obsolete,
                  to_char(add_time,'YYYY-MM-DD HH24:MI:SS') from %s where filepath='%s' """ %(self.FileTable,self.Name)
        c = self.DB._cursor()
        c.execute(sql)
        data = c.fetchone()
        return showCols,data

    # ------------------------------------------------------------------------------------
    def getFlags(self):
        
        
        lst =[]
        valFlags=[]
        colFlags = ['loaded','verified','obsolete','error','comment']
        sql = """ select loaded,verified,obsolete,error,comment
                  from %s where filepath='%s' """ %(self.FileTable,self.Name)
        c = self.DB._cursor()
        c.execute(sql)
        data = c.fetchall()
        i=0
        for aval in data:
           #print ' aval = %s ' %str(aval)
           i=0
           for av in aval:
             valFlags.append(av )
             lst.append( (colFlags[i],av))
             i+=1

        return lst
    # ------------------------------------------------------------------------------------
    def checkFlag (self,filename,flagCol):

        data = None
        c = self.DB._cursor()
        sql = "select %s from %s  where filepath = '%s' " %(flagCol,self.FileTable,filename)
        c.execute(sql)
        data = c.fetchone()
        #print ' flags  = %s ' %data[0]
        if data : 
          flag = data[0]
          #print ' flagcol = %s, flag again, %s' %(flagCol,flag)
          return flag
        return None

    def checkLoaded (self):
       val = self.checkFlag (self.Name,'loaded')
       return val

    def checkVerified (self):
       val = self.checkFlag (self.Name,'verified')
       return val

    def checkObsolete (self):
       val = self.checkFlag (self.Name,'obsolete')
       return val

    def checkError (self):
       val = self.checkFlag (self.Name,'error')
       return val


    # ------------------------------------------------------------------------------------
    def updateFlags (self,updateData):


        sqlSetList = []
        for aval in updateData:
                #print 'aval = %s' %str(aval)
                if type(aval[1]) == type(""):
                  clause = "%s='%s'" % (aval[0],aval[1])
                else:
                  clause = "%s=%s" % (aval[0],aval[1])
                
                sqlSetList.append(clause)    
# add modify time too
        sqlSetList.append("modify_time=now()")
        sql= "update %s set %s where filepath = '%s'" % (self.FileTable,string.join(sqlSetList,', '),self.Name)
        print(" update sql = %s" %sql)
        c = self.DB._cursor()
        c.execute(sql)
        c.execute('commit')
        return 1

    # ------------------------------------------------------------------------------------
    def setResetFlag (self,filename,flagCol,flagVal):
        c = self.DB._cursor()
        if type(flagCol) == type(""):
          sql = "update %s set %s = '%s'  where filepath = '%s' " %(self.FileTable,flagCol,flagVal,filename)
        else:
          sql = "update %s set %s = %s  where filepath = '%s' " %(self.FileTable,flagCol,flagVal,filename)
        
        #print ' setResetFlag, sql = %s ' %sql
        c.execute(sql)
        c.execute('commit')
     

    def setLoaded (self):
       self.setResetFlag (self.Name,'loaded',True)

    def setVerified (self):
       self.setResetFlag (self.Name,'verified',True)

    def setObsolete (self):
       self.setResetFlag (self.Name,'obsolete',True)

 
    def resetLoaded (self):
       self.setResetFlag (self.Name,'loaded',False)

    def resetVerified (self):
       self.setResetFlag (self.Name,'verified',False)

    def resetObsolete (self):
       self.setResetFlag (self.Name,'obsolete',False)


    def setError (self,errVal):
       self.setResetFlag (self.Name,'error',errVal)

    def resetError (self):
        c = self.DB._cursor()
        sql = "update %s set error=NULL  where filepath = '%s' " %(self.FileTable,self.Name)
        c.execute(sql)
        c.execute('commit')


    # ------------------------------------------------------------------------------------
    def setComment (self,comment):
       self.setResetFlag (self.Name,'comment',comment)

 
    def exists(self):
        return self.DB._tablesExist(self.FileTable)

# ----------------------------------------------------------------------------------------------------------------------------------------------------                    
                 
if __name__ == '__main__':
    import sys, random
    from datetime import datetime, timedelta
    
    db = MnvFileDB(connstr=sys.argv[1])
    
    print("Testing connection")
