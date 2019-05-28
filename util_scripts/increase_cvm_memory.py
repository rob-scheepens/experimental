#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
# Author: gokul.kannan@nutanix.com
#
# Utility to read/write/overwrite and delete files in a directory
import os
import random
import sys, getopt
#import paramiko
import time
import subprocess
import sys


def main(argv):
  
  opts, args = getopt.getopt(argv, "c:", ["cluster="])
  for opt, arg in opts:
    if opt in ("-c", "--cluster"):
      cluster = arg

  print "passsed options are %s" %(cluster)


  sshcmd = 'ssh -o CheckHostIp=no -o ConnectTimeout=15 -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=300   -i /home/gkannan/main/.python/util/net/../../../../installer/ssh_keys/nutanix root@'
  i = 1
  while i < 5:
    print "%s" % i
    #cmd = "sudo umount -lr /mnt/*"
    hostname = cluster + "-" + str(i)
    sshcmd +=  hostname
    cmd = "virsh list"
    print sshcmd
    subprocess.Popen(["/usr/bin/ssh ", "-l root","10.5.149.41", "ls"], shell=False,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
    result = ssh.stdout.readlines()
    print result
    #process = subprocess.Popen("/usr/bin/ssh -o CheckHostIp=no -o ConnectTimeout=15 -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=300   -i /home/gkannan/main/.python/util/net/../../../../installer/ssh_keys/nutanix -l root 10.5.149.41 ls", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #stdout, stderr = process.communicate()
    #print stdout
    #res = os.system('/usr/bin/ssh -o CheckHostIp=no -o ConnectTimeout=15 -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=300   -i /home/gkannan/main/.python/util/net/../../../../installer/ssh_keys/nutanix -l root 10.5.149.41 ls')
    #print res
    #result = ssh.stdout.readlines()
    #ssh = paramiko.SSHClient()
    #ssh.connect("10.5.149.41", username="root", password="nutanix/4u")
    #ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("virsh list")
    #if result == []:
    #    error = ssh.stderr.readlines()
    #    print >>sys.stderr, "ERROR: %s" % error
    #else:
    #    print result
    i += 1

main(sys.argv[1:])