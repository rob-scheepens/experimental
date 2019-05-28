#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
# Author: gokul.kannan@nutanix.com
#
# Utility to read/write/overwrite and delete files in a directory
import os
import random
import sys, getopt
import time

def main(argv):
  
  opts, args = getopt.getopt(argv, "hr:d:n:s:", ["help=","runtime=","dir="])
  num_files = None
  file_size = None
  runtime = None
  dir_name = None
  for opt, arg in opts:
    if opt in ("-h", "--help"):
      print "python crud.py -r <runtime in seconds> -d <destination directory>"
      sys.exit()
    elif opt in ("-r", "--runtime"):
       runtime = arg
    elif opt in ("-d", "--dir"):
      dir_name = arg
    elif opt in ("-n", "--num_files"):
      num_files = arg
    elif opt in ("-s", "--filesize"):
      file_size = arg

  print "passsed options are %s %s" %(runtime, dir_name)

  if dir_name is None:
    print "ERROR. Specify mandatory param dir_name"
    sys.exit()
  if num_files is None:
    num_files = 1000
  if file_size is None:
    file_size = 50
  if runtime is None:
    runtime = 18000000
  file_size = int(file_size)
  num_files = int(num_files)
  runtime = int(runtime)
  block_count = (file_size * 1024)/32
  for file_num in range(num_files):
    file_name = os.path.join(dir_name, "file_" + str(file_num))
    cmd = "sudo dd if=/dev/urandom of=%s bs=32k count=%s" % (file_name, block_count)
    os.system(cmd)

  start_time = time.time()
  print "start time is %s" % start_time
  while time.time() < (start_time + runtime):
    # write the files
    # Read/delete/overwrite a random file
    ops = ["read", "overwrite", "delete", "truncate"]
    file_num = random.choice(range(num_files))
    op = random.choice(ops)
    file_name = os.path.join(dir_name, "file_" + str(file_num))
    del_file = None
    if op == "read":
      print "read"
      cmd = "sudo dd if=%s of=/dev/null bs=4k" % file_name
    if op == "overwrite":
      print "overwrite"
      offset = random.choice(range(file_size * 1024))
      offset = offset/4
      count = random.choice(range(500))
      cmd = "sudo dd if=/dev/urandom of=%s bs=2k count=%s seek=%s conv=notrunc" % (file_name, count, offset)
    
    if op == "truncate":
      print "truncate"
      offset = random.choice(range(file_size * 1024))
      offset = offset/4
      count = random.choice(range(500))
      cmd = "sudo dd if=/dev/urandom of=%s bs=2k count=%s seek=%s " % (file_name, count, offset)
    if op == "delete":
      print "delete"
      cmd = "sudo rm -rf %s" % file_name
      del_file = file_name
    os.system(cmd)
    if del_file is not None:
      cmd = "sudo dd if=/dev/urandom of=%s bs=32k count=%s" % (del_file, block_count)
      os.system(cmd)
    cmd = "sudo fstrim -v %s" % dir_name
    os.system(cmd)

main(sys.argv[1:])