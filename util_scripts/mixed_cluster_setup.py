#!/usr/bin/python
#
#  Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
#  Author: Preeti U. Murthy <preetiupendra.murthy@nutanix.com>
#  Date: 12/19/2017
#
#  Script that handles addition of compute only nodes. The script provides
#  the ability to remove a HCI node, convert that to compute only node, and
#  add it back to the cluster.It does not do all this at once, but takes
#  individual commands to perform the actions.
#  Using the script, one can start with a jarvis created cluster, remove nodes that
#  have to be added as compute only, add them back as CO nodes.
#
#  The script takes the following as arguments.
#  Usage:
#    ./mixed_cluster_setup.py <action> <args>
#
#  Action: The action can be one of the following:
#       1. --remove_node: Removes the node assuming it to be a HCI node.
#       2. --kill_cvm: Shutsdown and undefines CVM on the node to be added
#       as CO.
#       3. --monitor_removal: Returns if node removal was success/failure and
#       waits till removal is complete. This is only removal of HCI nodes.
#       4. --add_node: Adds node.
#       5. --remove_co_node: Logs how to remove node.
#       6. --start_cvm: Start cvm on the compute only host in order to add it back
#          as a HCI node.
#
#  Arguments for each of the above actions:
#       1. --remove_node --node_uuid=<node-uuid>
#       2. --kill_cvm --cvm_ip=<cvm_ip> --host_ip=<host_ip>
#       3. --monitor_removal --service_vm_id=<service_vm_id>
#       4. --add_node --node_uuid=<node_uuid> --cvm_ip=<cvm_ip> or
#          --host_ip=<host_ip>
#       5. --remove_co_node: Logs how to remove node.
#       6. --start_cvm --host_ip=<host_ip>
#
#
import sys
sys.path.append("/home/nutanix/cluster/bin")

import env

import gflags
import json
import tempfile
import traceback


from cluster.consts import *
from cluster.genesis.compute_only.consts import *
#from stats.progress_monitor.interface.progress_monitor_interface import *
from cdp.client.progress_monitor.interface.progress_monitor_interface import *
#from stats.progress_monitor.progress_monitor_pb2 import *
from cdp.client.progress_monitor.progress_monitor_pb2 import *
from util.base import log
from util.base.command import timed_command
from util.net.ssh_client import SSHClient
from zeus.configuration import *
from zeus.configuration_pb2 import ConfigurationProto
from zeus.zookeeper_session import ZookeeperSession

FLAGS = gflags.FLAGS

gflags.DEFINE_string(
  "zeus_config_cache_file",
  "/home/nutanix/config/configuration_proto.dat",
  "Cached zeus config is present in this path saved periodically by genesis")
gflags.DEFINE_boolean("help", False, "Show help", short_name="h")
gflags.DEFINE_boolean("remove_node", False, "Remove a given node uuid")
gflags.DEFINE_boolean("add_node", False, "Add a node")
gflags.DEFINE_boolean("monitor_removal", False, "Monitor removal of a node")
gflags.DEFINE_boolean("remove_co_node", False, "Remove a given CO node")
gflags.DEFINE_boolean("kill_cvm", False,
                      "Kill CVM on remote host to make it a CO host")
gflags.DEFINE_boolean("start_cvm", False,
                      "Start CVM on remote host to make it a HCI host")
gflags.DEFINE_string("cvm_ip", "", "CVM IP")
gflags.DEFINE_string("host_ip", "", "Host IP")
gflags.DEFINE_string("node_uuid", "", "Node UUID")
gflags.DEFINE_string("service_vm_id", "", "CVM ID")

def show_help():
  usage="""Usage:
  The script takes the following as an argument.
  Usage:
    ./mixed_cluster_setup.py <action> <args>

  Action: The action can be one of the following:
       1. --remove_node: Removes the node assuming it to be a HCI node.
       2. --kill_cvm: Shutsdown and undefines CVM on the node to be added
       as CO.
       3. --monitor_removal: Returns if node removal was success/failure and
       waits till removal is complete. This is only removal of HCI nodes.
       4. --add_node: Adds node as HCI or CO.
       5. --remove_co_node: Logs how to remove compute only node.
       6. --start_cvm: Start cvm on the compute only host.

  Arguments for each of the above actions:
       1. --remove_node --node_uuid=<node-uuid>
       2. --kill_cvm --cvm_ip=<cvm_ip> --host_ip=<host_ip>
       3. --monitor_removal --service_vm_id=<service_vm_id>
       4. --add_node --node_uuid=<node_uuid> --cvm_ip=<cvm_ip> or
          --host_ip = <host_ip> depending on if the node is HCI or CO.
       5. --remove_co_node: Logs how to remove node.
       6. --start_cvm --host_ip=<host_ip>

  Ideally one would start with a jarvis created cluster and use the script to
  perform the following actions:
  1. Remove a node which has to be added as compute only.
  2. Monitor removal.
  3. Once removal is a success, kill the cvm on the node.
  4. Add the node as compute only.
  """
  log.INFO(usage)
  sys.exit(0)

def add_node(node_uuid, node_ip, compute_only):
  # Add node.
  from cluster.utils.genesis_client import GenesisApiClient
  client = GenesisApiClient()

  ret, error = client.make_rpc("ClusterManager", "add_node",
                  {"node_uuid":node_uuid, "node_svm_ip":node_ip,
                   "compute_only":compute_only}, svm_ips=["localhost"])
  if not ret:
    log.ERROR("Add node for %s failed with error %s" %
              (node_ip, error))
    log.INFO("The node will have to be manually added back")
    return 1
  else:
    log.INFO("Node %s successfully added" % node_ip)
    return 0

def monitor_node_removal(service_vm_id):
  progress_info_id = ProgressInfoProto.ProgressInfoId()
  progress_info_id.operation = ProgressInfoProto.kRemove
  progress_info_id.entity_type = ProgressInfoProto.kNode
  progress_info_id.entity_id = str(service_vm_id)

  progress_interface = ProgressMonitorInterface()
  proto = progress_interface.lookup(progress_info_id)
  num_tasks = len(proto.progress_task_list)

  while True:
    pending_tasks = 0
    for task_id in range(num_tasks):
      task = proto.progress_task_list[task_id]
      if (task.progress_status == ProgressInfoProto.kQueued) or (
         task.progress_status == ProgressInfoProto.kRunning):
        pending_tasks += 1
      elif (task.progress_status == ProgressInfoProto.kFailed) or (
         task.progress_status == ProgressInfoProto.kAborted):
        log.ERROR("Error removing node %s from cluster" % service_vm_id)
        log.INFO("Node has to be removed manually")
        return 1
      elif task.progress_status == ProgressInfoProto.kSucceeded:
        log.INFO("Node %s successfully removed" % service_vm_id)
        return 0
    log.INFO("Waiting for remove node to finish on node vm id %s" %
             service_vm_id)
    time.sleep(10)

def remove_node(node_uuid):
  cmd = "ncli cluster rm-start id=%s" % node_uuid
  ret, out, err = timed_command(cmd)
  if ret != 0:
    log.ERROR("Error %s, out %s, ret %s when %s was called on %s" %
              (err, out, ret, cmd, node_uuid))
    log.WARNING("Please remove this node manually")
    return 1
  else:
    log.INFO("NCLI remove returned %s for node %s" % (out, node_uuid))
    log.INFO("You can check the progress by calling --monitor_removal "
             "<service_vm_id>")
    return 0

def _kill_cvm(cvm_ip, host_ssh_client, hostname):
  # Kill the CVM on each of the removed nodes.
  shutdown_cmd = "virsh shutdown NTNX-%s-CVM" % hostname
  undefine_cmd = "virsh undefine NTNX-%s-CVM" % hostname

  ret, out, err = host_ssh_client.execute(shutdown_cmd)
  if ret != 0:
    log.ERROR("Error %s, out %s, ret %s when shutting down the CVM"
              " %s on host" % (err, out, ret, cvm_ip))
    log.INFO("Please shutdown the CVM manually on this host")
    return 1

  log.INFO("Shutdown cmd for cvm %s success" % cvm_ip)

  ret, out, err = host_ssh_client.execute(undefine_cmd)
  if ret != 0:
    log.ERROR("Error %s, out %s, ret %s when undefining the CVM"
              " %s on host" % (ret, out, err, cvm_ip))
    log.INFO("Please undefine the CVM manually on this host")
    return 1

  log.INFO("Undefine cmd for cvm %s success" % cvm_ip)
  return 0

def _change_permission_bits(client, permission_string, filepath):
  """
  Change permission bits on client for filepath.

  Returns 0 on success, non zero on failure.
  """
  cmd = """chmod %s "%s" """ % (permission_string, filepath)
  ret, out, error = client.execute(cmd)
  if ret != 0:
    log.ERROR("Error %s when changing permission on %s to %s: out %s" %
              (error, filepath, permission_string, out))
  return ret

def kill_cvm(cvm_ip, hypervisor_ip):
  # Get hostname to be used when killing the CVM later.
  host_ssh_client = SSHClient(hypervisor_ip,
                      FLAGS.hypervisor_username,
                      password=FLAGS.default_host_password)
  ret, out, err = host_ssh_client.execute("hostname")
  if ret != 0:
    log.ERROR("Error retrieving hyp hostname from ip %s, error %s"
              "ret %s" % (hypervisor_ip, err, ret))
    return 1
  hostname = out.strip()

  # Transfer the factory_config.json to the host.
  cvm_ssh_client = SSHClient(cvm_ip, "nutanix",
                     password=FLAGS.default_cvm_password)
  with tempfile.NamedTemporaryFile() as tmp:
    # Transfer from the cvm in question to a local tmp path.
    ret, out, error = cvm_ssh_client.execute("cat %s" %
                        FLAGS.factory_config_json_path)
    if ret != 0:
      log.ERROR("Unable to read the factory_config.json from node %s" %
                cvm_ip)
      return 1
    # Modify the contents to add "mode", as foundation would do it.
    factory_config_dict = json.loads(out.strip())
    factory_config_dict["mode"] = "compute only"
    tmp.write(json.dumps(factory_config_dict))
    tmp.flush()

    # Transfer from the local path to the corresponding host.
    ret, out, err = host_ssh_client.transfer_to(tmp.name,
                      FLAGS.factory_config_json_path_on_host)
    if ret != 0:
      log.ERROR("Unable to transfer %s to the host %s" %
                (FLAGS.factory_config_json_path, hypervisor_ip))
      return 1

    # Set the right permission bits.
    permission_string = "644"
    filepath = FLAGS.factory_config_json_path_on_host
    ret = _change_permission_bits(host_ssh_client, permission_string, filepath)
    if ret != 0:
      return 1
  log.INFO("Transferred factory config json to the host")

  # Transfer the hardware config json.
  with tempfile.NamedTemporaryFile() as tmp:
    # Transfer from the cvm in question to a local tmp path.
    ret, out, error = cvm_ssh_client.execute("cat %s" %
                        FLAGS.hardware_config_json_path)
    if ret != 0:
      log.ERROR("Unable to read the hardware_config.json from node %s" %
                cvm_ip)
      return 1
    tmp.write(out.strip())
    tmp.flush()

    # Transfer from the local path to the corresponding host.
    ret, out, err = host_ssh_client.transfer_to(tmp.name,
                      FLAGS.hardware_config_json_path_on_host)
    if ret != 0:
      log.ERROR("Unable to transfer %s to the host %s" %
                (FLAGS.hardware_config_json_path, hypervisor_ip))
      return 1
    # Set the right permission bits.
    permission_string = "644"
    filepath = FLAGS.hardware_config_json_path_on_host
    ret = _change_permission_bits(host_ssh_client, permission_string, filepath)
    if ret != 0:
      return 1
  log.INFO("Transferred hardware config json to the host")
  return _kill_cvm(hypervisor_ip, host_ssh_client, hostname)

def start_cvm(hypervisor_ip):
  # Get hostname to be used when killing the CVM later.
  log.INFO("Starting cvm on host %s" % hypervisor_ip)
  host_ssh_client = SSHClient(hypervisor_ip,
                      FLAGS.hypervisor_username,
                      password=FLAGS.default_host_password)
  ret, out, err = host_ssh_client.execute("hostname")
  if ret != 0:
    log.ERROR("Error retrieving hyp hostname from ip %s, error %s"
              "ret %s" % (hypervisor_ip, err, ret))
    return 1
  hostname = out.strip()

  # Kill the CVM on each of the removed nodes.
  define_cmd = "virsh define /root/NTNX-CVM.xml"
  start_cmd = "virsh start NTNX-%s-CVM" % hostname
  autostart_cmd = "virsh autostart NTNX-%s-CVM" % hostname

  ret, out, err = host_ssh_client.execute(define_cmd)
  if ret != 0:
    log.ERROR("Error %s, out %s, ret %s when defining the CVM"
              " on host %s" % (err, out, ret, hypervisor_ip))
    log.INFO("Please start the CVM manually on this host")
    return 1

  log.INFO("Define cmd for cvm %s success")

  ret, out, err = host_ssh_client.execute(start_cmd)
  if ret != 0:
    log.ERROR("Error %s, out %s, ret %s when starting the CVM"
              "on host %s" % (ret, out, err, hypervisor_ip))
    log.INFO("Please start the CVM manually on this host")
    return 1

  log.INFO("Start cmd for cvm %s success")

  ret, out, err = host_ssh_client.execute(autostart_cmd)
  if ret != 0:
    log.ERROR("Error %s, out %s, ret %s when autostarting the CVM"
              "on host %s" % (ret, out, err, hypervisor_ip))
    log.INFO("Please autostart the CVM manually on this host")
    return 1

  log.INFO("CVM started on host %s" % hypervisor_ip)

  # Remove the configured file on the host which is used to indicate it is part
  # of a cluster.
  rm_cmd = "rm -f /root/configured"
  ret, out, err = host_ssh_client.execute(rm_cmd)
  if ret != 0:
    log.ERROR("Cannot remove /root/configured file from the host. Please "
              "remove the file manually in order to add it back as a CO node")
    return 1
  return 0

def main():
  if FLAGS.help:
    show_help()

  # HCI node removal
  if FLAGS.remove_node:
    if not FLAGS.node_uuid:
      log.ERROR("Node uuid missing. Pass node uuid to initiate removal")
      return 1
    else:
      return remove_node(FLAGS.node_uuid)

  # Monitor removal.
  if FLAGS.monitor_removal:
    if not FLAGS.service_vm_id:
      log.ERROR("Service vm id missing")
      return 1
    else:
      return monitor_node_removal(FLAGS.service_vm_id)

  # Destroy CVM
  if FLAGS.kill_cvm:
    if not FLAGS.cvm_ip:
      log.ERROR("Node cvm ip is missing")
      return 1
    elif not FLAGS.host_ip:
      log.ERROR("Host ip missing")
      return 1
    else:
      return kill_cvm(FLAGS.cvm_ip, FLAGS.host_ip)

  # Add the node.
  if FLAGS.add_node:
    if not FLAGS.node_uuid:
      log.ERROR("Node uuid required to add co node")
      return 1
    if (not FLAGS.cvm_ip) and not (FLAGS.host_ip):
      log.ERROR("Either cvm ip or host ip is required to add the node")
      return 1
    if FLAGS.cvm_ip:
      node_ip = FLAGS.cvm_ip
      compute_only =  False
    else:
      node_ip = FLAGS.host_ip
      compute_only = True
    return add_node(FLAGS.node_uuid, node_ip, compute_only)

  if FLAGS.remove_co_node:
    log.INFO("Please edit zeus config manually using edit_zeus "
             "--editor=/usr/bin/vim. Remove the node info from "
             "compute_node_list and management_server_list")
    return 0

  if FLAGS.start_cvm:
    if not FLAGS.host_ip:
      log.ERROR("Please provide host ip to start cvm on")
      return 1
    return start_cvm(FLAGS.host_ip)

if __name__ == "__main__":
  try:
    global __doc__
    __doc__ = "Usage: %s [flags]\nTry %s --help" % (sys.argv[0], sys.argv[0])
    args = FLAGS(sys.argv)
    FLAGS.logtostderr = True
    log.initialize()
    sys.exit(main())
  except gflags.FlagsError, e:
    log.ERROR("%s\n%s\n" % (str(e), __doc__))
    sys.exit(1)
  except KeyboardInterrupt:
    log.WARNING("Exiting on Ctrl-C")
    sys.exit(1)
  except Exception as ex:
    log.ERROR(traceback.format_exc())
    log.ERROR("Failed to execute action %s error (%s), exiting.." %
              (args[0], str(ex)))
    sys.exit(1)
