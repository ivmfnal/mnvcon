import os, psycopg2, time
import sys, getopt
from datetime import datetime
from IOVAPI import IOVDB
from FileAPI import MnvFileDB

Usage = """
 python testFileApi.py -d "dbname=mnvcon_int host=minervadbprod port=5432 user=vittone" """


dcn = None
# connect
opts, args = getopt.getopt(sys.argv[1:], 'f:d:c')
for opt, val in opts:
        if opt == '-d': dcn = val

print("\n\n ====  Test to use the FileAPI library ==== \n")
 
print('\n Connecting to postgres database\n')
print('\n connection string = %s ' %dcn)

dbFDB = MnvFileDB(connstr=dcn)   # for file handling tables

dbIOV = IOVDB(connstr=dcn)       # for iov data


if dbFDB.probe():
   print('\n we are connected')
else:
   print('\n error in connection ')

# get list of files ready to upload

# How to get files from db:

# getFilesToUpload  returns a list of tuples containing (filename, datatype, detector)

# this retrieves all the file with the uploaded flag False.

filelist = dbFDB.getFilesToUpload()

print('\n ------------------------------------------------------------\n')
print('\n The following files are ready to be uploaded: \n')

print(' %20s   %15s  %15s ' %('filename','datatype','detector'))
print('\n ------------------------------------------------------------\n')


# You can specify a certain datype or detector passing optional args,
# examples:  
#  filelist = dbFDB.getFilesToUpload(datatype='pedestals')
#  filelist = dbFDB.getFilesToUpload(datatype='pedestals',detector='minerva')
#  filelist = dbFDB.getFilesToUpload(detector='testbeam')



for aval in filelist:
   filename,dtyp,det = aval
   print(' %20s  %15s  %15s ' %(filename,dtyp,det))

print('\n ------------------------------------------------------------\n')
print('\n ------------------------------------------------------------\n')

filelistBeam = dbFDB.getFilesToUpload(detector='testbeam')

print(' \n Files with detector type = testbeam \n')

for aval in filelistBeam:
   filename,dtyp,det = aval
   print(' %20s  %15s  %15s ' %(filename,dtyp,det))


# for each file upload data
for aval in filelist:
   filename,dtyp,det = aval
   
   fg = dbFDB.genFile(filename)    #create file object for operations on file
   
   print('\n ------------------------------------------------------------\n')

   print(' Upload data for file %s ' %filename)
   # code here to read/upload real data 
   # if  all is well, set flag loaded for file
   stat = 1
   if stat:
     fg.setLoaded()

   stat = 0   # somethig went wrong, set error flag
   if stat == 0:
     fg.setError(1)   # setError takes and integer to allow different severity errors..
   # readflag back
   
   val = fg.checkLoaded ()
   if val== True: 
     print(" Check flag, loaded = %s " %val)
   else:
     print(" Loaded flag not set ")
   
   val = fg.checkVerified ()
   if val== True: 
     print(" Check flag, verified = %s " %val)
   else:
     print(" verified flag not set ")
