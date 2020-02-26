
#!/usr/bin/python
"""
Copyright (c) 2020 Nutanix Inc. All rights reserved.

Author: gokul.kannan@nutanix.com

This script migrates a given set of iscsi targets to a given SVM.

./migrate_iscsi_sessions.py --targets <list of target names> --dest_svm_ip \
 <svm IP to migrate the targets to> 

"""

import sys
sys.path.append("/home/nutanix/cluster/bin")

import env
import gflags
import json
import subprocess

from cdp.client.stargate.client import StargateClient
from cdp.client.stargate.stargate_interface.stargate_interface_pb2 import (
    IscsiGetRedirectedSessionsArg, IscsiMigrateSessionsArg)
from util.misc.protobuf import pb2json
from zeus.zookeeper_session import ZookeeperSession
from zeus.configuration import Configuration


FLAGS = gflags.FLAGS

gflags.DEFINE_string("targets", None,
                     ("list of iscsi targets to be migrated"))

gflags.DEFINE_string("dest_svm_ip", None,
                     ("The destination SVM IP "))

gflags.DEFINE_bool("dry_run", False,
                     ("The destination SVM IP "))

gflags.DEFINE_bool("print_all_targets", False,
                     ("The destination SVM IP "))


def migrate_iscsi_target(target_name, src_svm_id, dst_svm_id,
                           stargate_master_ip):
    """
    Calls the IscsiMigrateSessions rpc to migrate a given target to some other
    SVM.
    Args:
      target_name(str): Name of the target to migrate
      src_svm_id(str): SVM id of the source SVM where the target currently
                       resides
      dst_svm_id(str): SVM id of the destination SVM
      stargate_master_ip(str): IP of the stargate master
    Returns:
      Empty dict.
    """
    stargate_client = StargateClient("%s:2009" % stargate_master_ip)
    migrate_arg = IscsiMigrateSessionsArg()
    migrate_arg.target_name = target_name
    migrate_arg.from_svm_id = src_svm_id
    migrate_arg.to_svm_id = dst_svm_id
    migrate_arg.forwarded = False
    ret = stargate_client.IscsiMigrateSessions(migrate_arg)
    json_ret = pb2json(ret, b64_bytes=True)
    return json_ret

def get_redirected_iscsi_sessions(svm_ip):
    """
    Get all the iscsi sessions redirected to a given SVM.
    Args:
      svm_ip(str): IP of the svm where the sessions reside.
    Returns:
      Dict of sessions.
    """
    arg = IscsiGetRedirectedSessionsArg()
    arg.include_vdisks = False
    client = StargateClient("%s:2009" % svm_ip)
    ret = client.IscsiGetRedirectedSessions(arg)
    json_ret = pb2json(ret, b64_bytes=True)
    return json_ret

def get_zeus_config():
  """
  Return the zeus config.
  """
  zk_session = ZookeeperSession(connection_timeout=60)
  if not zk_session.wait_for_connection(None):
    zk_session = None

  zeus_config = Configuration().initialize(zk_session = zk_session)
  proto = zeus_config.config_proto()
  return proto


def get_svm_id_from_svm_ip(svm_ip, zeus_config):
  """
  Returns the SVM ID for the given 'svm_ip'.

  Args:
    svm_ip(str): SVM IP.
    zeus_config(ConfigurationProto): Zeus config proto.

  Returns:
    (int|None): ID of the SVM from Zeus config.

  """
  for node in zeus_config.node_list:
    if node.service_vm_external_ip == svm_ip:
      return int(node.service_vm_id)

def get_stargate_master_ip():
  """
  Return the stargate master IP
  """
  rv, stdout, stderr = run_command("links -dump http://0:2009 | grep master")
  if rv:
    print "Failed to find stargate master IP. exiting"
    sys.exit(1)
  ip = stdout.strip().split()[3]
  ip = ip.split("]")[1]
  ip = ip.split(":")[0]
  return ip

def run_command(cmd):
  """
  Runs a command on the localhost and returns output and error values.
  Args:
    cmd(str): The command to be executed.
  Returns the status code, stdout and the stderr.
  """
  proc = subprocess.Popen(
    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
  out, err = proc.communicate()
  return proc.returncode, out, err

def main(argv):
  try:
    argv = FLAGS(argv)
  except gflags.FlagsError, err:
    print "%s\nUsage: %s ARGS\n%s" % (err, argv[0], FLAGS)
    sys.exit(1)

  if (not FLAGS.targets or not FLAGS.dest_svm_ip) and \
      not FLAGS.print_all_targets:
    print "Please pass the targets and dest_svm_ip"
    print "\nUsage: %s ARGS\n%s" % (argv[0], FLAGS)
    sys.exit(1)

  targets_to_be_migrated = FLAGS.targets.strip().split(",")
  # Remove any trailing spaces.
  for xx, _ in enumerate(targets_to_be_migrated):
    targets_to_be_migrated[xx] = targets_to_be_migrated[xx].strip()  

  #print targets_to_be_migrated
  stargate_master_ip = get_stargate_master_ip()
  zeus_config = get_zeus_config()
  dest_svm_id = get_svm_id_from_svm_ip(FLAGS.dest_svm_ip, zeus_config)
  target_src_svm_id = {}
  for node in zeus_config.node_list:
    iscsi_sessions = get_redirected_iscsi_sessions(node.service_vm_external_ip)
    if FLAGS.print_all_targets:
      print "\nTargets on SVM %s:\n" % node.service_vm_external_ip

    for target in iscsi_sessions.get("targets", []):
      if FLAGS.print_all_targets:
        print target["name"]
        continue

      if target["name"] in targets_to_be_migrated:
        target_src_svm_id[target["name"]] = node.service_vm_id

  if FLAGS.print_all_targets:
    sys.exit(1)

  if set(target_src_svm_id.keys()) != set(targets_to_be_migrated):
    print "Could not get source SVM id of all the targets. Exiting"
    print "Targets found: %s" % target_src_svm_id.keys()
    print "Targets provided: %s" % targets_to_be_migrated
    sys.exit(1)

  for target, src_svm_id in target_src_svm_id.iteritems():
    if src_svm_id == dest_svm_id:
      print "Target %s already on %s. Skipping" % (target, FLAGS.dest_svm_ip)
      continue
    if not FLAGS.dry_run:
      migrate_iscsi_target(target, src_svm_id, dest_svm_id, stargate_master_ip)
    else:
      print "(Dry run) Migrating %s to %s" % (target, FLAGS.dest_svm_ip)

  print "Migrations done"

if __name__ == "__main__":
  main(sys.argv)