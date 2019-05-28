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
import subprocess
import sys


def main(argv):
  
  opts, args = getopt.getopt(argv, "c:s:", ["clients=","svm="])
  for opt, arg in opts:
    if opt in ("-c", "--clients"):
      clients = arg
    elif opt in ("-s", "--svm"):
      svmip = arg

  print "passsed options are %s" %(clients)
  clients = clients.split(",")


  sshcmd = "ssh -q -o CheckHostIp=no -o ConnectTimeout=15 -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o UserKnownHostsFile=/dev/null  -o PreferredAuthentications=publickey -o IdentityFile=/home/gkannan/main/.python/util/net/../../../../installer/ssh_keys/ nutanix@"
  for c in clients:
    print "%s" % c
    #cmd = "sudo umount -lr /mnt/*"
    sshcmd +=  c
    cmd = "sudo umount -lr /mnt/*"
    subprocess.call(["ls", "-l"])
    subprocess.call(["%s" % sshcmd, "%s" % cmd])
    result = ssh.stdout.readlines()
    if result == []:
        error = ssh.stderr.readlines()
        print >>sys.stderr, "ERROR: %s" % error
    else:
        print result
    cmd = "sudo iscsiadm -m node -u"  
    ssh = subprocess.Popen(["%s" % sshcmd, "%s" % c, cmd],
                           shell=False)
    result = ssh.stdout.readlines()
    if result == []:
        error = ssh.stderr.readlines()
        print >>sys.stderr, "ERROR: %s" % error
    else:
        print result
    cmd = "sudo iscsiadm -m node -o delete"  
    ssh = subprocess.Popen(["%s" % sshcmd, "%s" % c, cmd],
                           shell=False)
    result = ssh.stdout.readlines()
    if result == []:
        error = ssh.stderr.readlines()
        print >>sys.stderr, "ERROR: %s" % error
    else:
        print result

main(sys.argv[1:])