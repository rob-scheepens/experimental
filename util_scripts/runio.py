#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Author: anirudha.sonar@nutanix.com
#
# Tool to write files of given size at a given location
# Usage:
# python runio.py <size of file in MB> <Number of files to be created> \
# <destination directory path>
#

import os
import subprocess
import json
import collections
from threading import Thread
import time
from thread import start_new_thread, allocate_lock
import sys

if len(sys.argv) < 4:
  print "Missing arguments : Usage - python {0} <size of file in MB> \
  <Number of files to be created> <destination directory path>"\
  .format(sys.argv[0])
  sys.exit(1)

files=[]
fileSize = sys.argv[1]
numfiles = sys.argv[2]
dest_dir = sys.argv[3]
loop=None
print len(sys.argv)

if len(sys.argv) == 5:
  loop = True
actualFileSize = int(fileSize)*1024*1024;
seed = "1092384956781341341234656953214543219"
out_file = dest_dir +"/iodata"
cmd = "sudo dd if=/dev/urandom of=%s bs=1 count=1050" %out_file
words = os.system(cmd)
words = open(out_file, "r").read().replace("\n", '').split()

def fdata():
  a = collections.deque(words)
  b = collections.deque(seed)
  while True:
    yield ' '.join(list(a)[0:1024])
    a.rotate(int(b[0]))
    b.rotate(1)

def startIO(fileName, size=1048576):
  files.append(fileName)
  fileName = dest_dir + "/" + fileName 
  print "Creating file {0}".format(fileName)
  start_time=time.time()
  g = fdata()
  fh = open(fileName, 'w')
  print ("Path %s" %os.path.realpath(fileName))
  while os.path.getsize(fileName) < size:
    fh.write(g.next())
  end_time=time.time()
  diff = end_time - start_time
  minutes = diff//60
  seconds = diff%60
  print "File -  %s is created in - %s min and %s secs" %(fileName, minutes, \
                                                          seconds)

def main(numfiles, actualFileSize):
  listOfThreadLists=[]
  threadNum=0
  threads=[]
  for i in range(int(numfiles)):
    uniqueFileName = time.time()
    print "Starting thread number - %s" %(i)
    uFileName="File_%s_%s" %(uniqueFileName,i)
    try:
      thread = Thread(target = startIO, args = (uFileName,actualFileSize,))
      threads.append(thread)
      threadNum+=1
      if threadNum == 100:
        listOfThreadLists.append(threads)
        threads=[]
        threadNum=0
    except Exception as errtxt:
      print errtxt

  listOfThreadLists.append(threads)
  for threadList in listOfThreadLists:
    print "Starting number of new threads - %s " %(len(threadList))
    [thrd.start() for thrd in threadList]
    [thrd.join for thrd in threadList]
    while len(threadList) > 0:
      time.sleep(2)
      for thread in threadList:
        if not thread.isAlive():
          print "Thread "+thread.getName()+" terminated"
          threadList.remove(thread)
        else:
          #print "Thread %s is still alive" %(thread.getName())
          break
        print "Total live threads - %s" %(len(threadList))

lnum=0
isLoop=True
while isLoop:
  print "Starting data writer loopNumber - {0}".format(lnum)
  main(numfiles, actualFileSize)
  if not loop:
    isLoop = False
    print "Ending program since loop write is disabled"
  if isLoop:
    print "Deleting all the file created in previous run from the present "
    "directory."
    for file in files:
      try:
        os.remove(file)
        print "File is deleted - {0}".format(file)
      except OSError, e:
        print "File Not Found - {0}".format(file)
    files=[]
  lnum += 1
