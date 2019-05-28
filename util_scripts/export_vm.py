#!/usr/bin/python

# Copyright (c) 2016 Nutanix Inc. All rights reserved.

# Author: ashrith.sheshan@nutanix.com
"""
This file creates a qcow2 image of a vm.
"""

import argparse
import json
import os
import signal
import sys
import textwrap

from paramiko import SSHClient, AutoAddPolicy, Transport, SFTPClient

DEFAULT_CVM_USERNAME = 'nutanix'
DEFAULT_CVM_PASSWD = 'nutanix/4u'
STEP_COUNT = 1


def print_step(msg):
  """Prints a STEP
  Args:
    msg (str): Message to be printed.
  """
  global STEP_COUNT
  print "*" * 80
  print str(STEP_COUNT) + '. ' + msg
  STEP_COUNT = STEP_COUNT + 1


def print_status(result, fatal=False):
  """Prints the result of a STEP
  Args:
    result (dict): output of a cmd
      eg. {
            'rv': 0,
            'stderr': '',
            'stdout': ''
          }
    fatal (bool): To exit the script if rv is non zero.
  """
  if not result['rv']:
    print "Success!"
  else:
    print "ERROR"
    print json.dumps(result, indent=4)
    if fatal:
      print "Failed at critical step. Exiting..."
      sys.exit(1)


def ssh_exec(cmd, ip, username=DEFAULT_CVM_USERNAME,
             password=DEFAULT_CVM_PASSWD, timeout=10, verbose=False):
  """Executes the cmd on remote host"""
  if not username:
    username = DEFAULT_CVM_USERNAME
  if not password:
    password = DEFAULT_CVM_PASSWD
  ssh = SSHClient()
  ssh.set_missing_host_key_policy(AutoAddPolicy())
  ssh.connect(ip, username=username, password=password, timeout=10)
  pad = ' ' * (len(ip) + 5)
  if verbose:
    print textwrap.fill("%s >>> %s" % (ip, cmd), width=80, initial_indent='',
                        subsequent_indent=pad)
  ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd, timeout=timeout)
  rv = ssh_stdout.channel.recv_exit_status()
  output = ssh_stdout.readlines() if rv == 0 else None
  stderr = ssh_stderr.readlines()
  ssh.close()
  if verbose:
    print textwrap.fill("%s <<< RV: %s" % (ip, rv), width=80, initial_indent='',
                        subsequent_indent=pad)
    print textwrap.fill("STDOUT: %s" % output, width=80, initial_indent=pad,
                        subsequent_indent=pad + (' ' * 8))
    print textwrap.fill("STDERR: %s" % stderr, width=80,
                        initial_indent=pad, subsequent_indent=pad + (' ' * 8))
  return {
    'rv'    : rv,
    'stdout': output,
    'stderr': stderr
  }


def scp_remote_to_local(remote_file_path, local_file_path, ip, username, password):
  """Copies a file from remote to local"""
  if not username:
    username = DEFAULT_CVM_USERNAME
  if not password:
    password = DEFAULT_CVM_PASSWD
  transport = Transport((ip, 22))
  transport.connect(username=username, password=password)
  sftp = SFTPClient.from_transport(transport)
  sftp.get(remote_file_path, local_file_path)


def export(args):
  """Main script with steps"""

  print_step("Enabling SFTP on CVM")
  cmd = "source /etc/profile; sudo sed -i '/#Subsystem/c\Subsystem sftp " \
        "internal-sftp' /etc/ssh/sshd_config && sudo service sshd restart"
  result = ssh_exec(cmd=cmd, ip=args.cvm_ip, username=args.cvm_username,
                    password=args.cvm_password, timeout=20,
                    verbose=args.verbose)
  print_status(result, fatal=True)

  print_step("Gathering VM information")
  cmd = "source /etc/profile; ncli vm list name=%s -json=true" % args.vm_name
  result = ssh_exec(cmd=cmd, ip=args.cvm_ip, username=args.cvm_username,
                    password=args.cvm_password, timeout=20,
                    verbose=args.verbose)
  vm_details = json.loads(result['stdout'][0])
  vdisk_path = vm_details['data'][0]['vdiskFilePaths'][0]
  print_status(result, fatal=True)

  print_step("Disabling autologout of CVM to avoid timeout.")
  cmd = "source /etc/profile; sudo rm /etc/profile.d/os-security.sh"
  result = ssh_exec(cmd=cmd, ip=args.cvm_ip, username=args.cvm_username,
                    password=args.cvm_password, verbose=args.verbose)
  print_status(result)

  print_step("Creating qcow2 image of %s" % args.vm_name)
  cmd = "source /etc/profile; qemu-img convert -c nfs://127.0.0.1%s -O qcow2 " \
        "img.qcow2" % vdisk_path
  result = ssh_exec(cmd=cmd, ip=args.cvm_ip, username=args.cvm_username,
                    password=args.cvm_password, timeout=None,
                    verbose=args.verbose)
  print_status(result, fatal=True)

  print_step("Copying created image to local %s" % args.output)
  scp_remote_to_local(remote_file_path="/home/nutanix/img.qcow2",
                      local_file_path=args.output, ip=args.cvm_ip,
                      username=args.cvm_username, password=args.cvm_password)
  print_status({'rv': 0})

  print_step("Removing image from CVM")
  cmd = "source /etc/profile; rm -rf /home/nutanix/img.qcow2"
  result = ssh_exec(cmd=cmd, ip=args.cvm_ip, username=args.cvm_username,
                    password=args.cvm_password, timeout=20,
                    verbose=args.verbose)
  print_status(result)


def signal_handler(signal, frame):
  """Handle Ctrl + C"""
  print "Exiting..."
  os._exit(0)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("cvm_ip", help="CVM ip of the cluster where the VM "
                                     "resides")
  parser.add_argument("vm_name", help="Name of the VM to export")
  parser.add_argument("--output", help="Output file", default="image.qcow2")
  parser.add_argument("--cvm_username",
                      help="Username to be used to login to CVM")
  parser.add_argument("--cvm_password",
                      help="Password to be used to login to CVM")
  parser.add_argument("--verbose", action="store_true",
                      help="Username to be used to login to CVM")
  args = parser.parse_args()
  signal.signal(signal.SIGINT, signal_handler)
  export(args)
