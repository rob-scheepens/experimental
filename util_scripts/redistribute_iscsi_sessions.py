
#!/usr/bin/python
"""
Copyright (c) 2020 Nutanix Inc. All rights reserved.

Author: gokul.kannan@nutanix.com

This script redistributes all iscsi sessions pertaining to the given VG equally
among all the SVMs in the cluster.

./redistribute_iscsi_sessions.py --vg_name <name of the VG>

"""

import sys
sys.path.append("/home/nutanix/cluster/bin")

import env
import gflags
import json
import subprocess

from cdp.client.stargate.client import StargateClient
from cdp.client.stargate.stargate_interface.stargate_interface_pb2 import (
    IscsiGetRedirectedSessionsArg, IscsiMigrateSessionsArg,
    IscsiGetPreferredSVMArg)
from util.misc.protobuf import pb2json
from zeus.zookeeper_session import ZookeeperSession
from zeus.configuration import Configuration


FLAGS = gflags.FLAGS

gflags.DEFINE_string("vg_name", None,
                     ("Name of the VG for which the seeions have be migrated"))

gflags.DEFINE_bool("dry_run", False,
                  ("When set to true, print the intended distribution"))


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

def get_vg_target_name(vg_name):
  """
  Get the target name of the VG.
  """
  rv, stdout, stderr = run_command("source /etc/profile; acli -o json "
                                   "vg.get %s" % vg_name)
  if rv:
    print("Failed to get details of VG %s. Error: %s" % (vg_name, stdout))
    sys.exit(1)

  vg_data = json.loads(stdout.strip())
  if vg_data["status"]:
    print("Failed to get details of VG %s. Error: %s" % (vg_name, stdout))
    sys.exit(1)

  return vg_data["data"][vg_data["data"].keys()[0]]["iscsi_target_name"]

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

def get_preferred_svm_id(base_target_name, targetname, zeus_config):
  """
  Fetch the preferred SVM for the given target based on the same algorithm used
  by Stargate:
  https://opengrok.eng.nutanix.com/source/xref/cdp-master/cdp/client/stargate/iscsi_adapter/iscsi_util.cc#367
  Args:
    base_target_name(str): Base target name of the VG.
    targetname(str): Name of the target.
    zeus_config(dict): Zeus config of the cluster.
  Returns: The SVM ID of the preferred SVM.
  """
  num_nodes = len(zeus_config.node_list)
  index = hash(base_target_name) % num_nodes
  virtual_target_index = int(targetname.split("-")[-1].split("tgt")[-1])
  index = (index + virtual_target_index) % num_nodes
  return zeus_config.node_list[index].service_vm_id

def main(argv):
  try:
    argv = FLAGS(argv)
  except gflags.FlagsError, err:
    print "%s\nUsage: %s ARGS\n%s" % (err, argv[0], FLAGS)
    sys.exit(1)

  if not FLAGS.vg_name:
    print "Please pass the name of the VG"
    print "\nUsage: %s ARGS\n%s" % (argv[0], FLAGS)
    sys.exit(1)

  vg_target_name = get_vg_target_name(FLAGS.vg_name)
  stargate_master_ip = get_stargate_master_ip()
  zeus_config = get_zeus_config()
  target_src_svm_id = {}
  for node in zeus_config.node_list:
    iscsi_sessions = get_redirected_iscsi_sessions(node.service_vm_external_ip)
    for target in iscsi_sessions.get("targets", []):
      if vg_target_name in target["name"]:
        target_src_svm_id[target["name"]] = node.service_vm_id

  for target, src_svm_id in target_src_svm_id.iteritems():
    dest_svm_id = get_preferred_svm_id(vg_target_name, target, zeus_config)
    if src_svm_id == dest_svm_id:
      print "Target %s already on %s. Skipping" % (target, dest_svm_id)
      continue
    if not FLAGS.dry_run:
      migrate_iscsi_target(target, src_svm_id, dest_svm_id, stargate_master_ip)
    else:
      print "(Dry run) Migrating %s to %s" % (target, dest_svm_id)

  print "Migrations done"

if __name__ == "__main__":
  main(sys.argv)
