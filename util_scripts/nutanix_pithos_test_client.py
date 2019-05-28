#
# Copyright (c) 2013 Nutanix Inc. All rights reserved.
#
# Author: vinayak@nutanix.com
#
# This file implements helper methods for Pithos.
#
# Example usage:
#   # Initialize Pithos client.
#   pithos_test_client = NutanixPithosTestClient(self.cluster)
#
#   # Set 'generate_vblock_copy' flag for vdisk 123 and clear this flag for
#   # vdisk 124.
#   pithos_test_client.update_vdisk_configs("generate_vblock_copy", [123, 124],
#                                           [1, 0])
#
#   # Update parent link of vdisk 123.
#   pithos_test_client.update_vdisk_configs("parent_vdisk_id", [123], ["124"])
#
#   # Remove parent link of vdisk 123.
#   pithos_test_client.update_vdisk_configs("parent_vdisk_id", [123], [""])
#
__all__ = [ "NutanixPithosTestClient" ]

import random
import subprocess
import os
import tempfile

from qa.agave.nutanix_cluster_ssh_util import NutanixClusterSSHUtil
from util.base.build import top_dir
from util.base.log import *
from util.net.ssh_client import SSHClient
from pithos.pithos_pb2 import IscsiClientParams

class NutanixPithosTestClient(object):

  # Port number on which Pithos RPC server is listening.
  PITHOS_PORT = 2016

  # Pithos config editor path in build directory.
  PITHOS_CONFIG_EDITOR_LOCAL_PATH =\
      "build/qa/tools/vdisk_config_editor/vdisk_config_editor"

  # VDisk config parameters supported by pithos_config_editor.
  PITHOS_CONFIG_EDITOR_VDISK_PARAMS =\
      ["to_remove", "generate_vblock_copy", "avoid_vblock_copy_when_leaf",
       "parent_vdisk_id"]

  def __init__(self, cluster):
    self.__cluster = cluster
    self.__copied_pithos_config_editor = False

  def setup_pithos_config_editor(self):
    """
    Copy pithos_config_editor to all SVMs of the cluster.

    Returns True on success, else False.
    """
    svms = self.__cluster.svms()
    remote_dir = "%s/bin" % svms[0].test_dir()
    local_path =\
        os.path.join(top_dir(),
                     self.PITHOS_CONFIG_EDITOR_LOCAL_PATH)
    INFO("Copying %s to all SVMs at path %s" % (local_path, remote_dir))
    success = True
    results = NutanixClusterSSHUtil.execute(svms, "mkdir -p %s" % remote_dir)
    for ii, (rv, stdout, stderr) in enumerate(results):
      if rv != 0:
        success = False
        ERROR("Error creating bin directory on %s. rv: %d, stdout: %s, "
              "stderr: %s" % (svms[ii].ip(), rv, stdout, stderr))
    if not success:
      return success

    p = subprocess.Popen("objdump -p %s | grep -qw RUNPATH" % local_path,
                         shell=True)
    if p.wait() == 0:
      cmd = ["%s/tools/build/push-build" % top_dir(),
             "--root", svms[0].test_dir(),
             "--svm", ",".join([uvm.ip() for uvm in svms]),
             local_path]
      p = subprocess.Popen(cmd)
      if p.wait() != 0:
        ERROR("Push-build %s failed" % local_path)
        return False
    else:
      results = NutanixClusterSSHUtil.rsync_dir_from_files(svms,
                                                           [local_path],
                                                           remote_dir,
                                                           delete_extra=False)
      if not results:
        ERROR("Error rsyncing %s to SVMs at path %s" %
              (local_path, remote_dir))
        return False

    self.__copied_pithos_config_editor = True
    return True

  def update_vdisk_configs(self, vdisk_param, vdisk_ids, values):
    """
    Update 'vdisk_param' of the vdisks specified by 'vdisk_ids' with the values
    specified by 'values'.

    Returns True on success, else False.
    """
    if vdisk_param not in self.PITHOS_CONFIG_EDITOR_VDISK_PARAMS:
      ERROR("Vdisk param %s is not supported by pithos_config_editor" %
            vdisk_param)
      return False

    if len(vdisk_ids) != len(values):
      ERROR("Number of vdisk ids does not match number of values. %d Vs %d" %
            (len(vdisk_ids), len(values)))
      return False

    # Copy the vdisk_config_editor binary to all SVMs, if it is not already
    # copied.
    if not self.__copied_pithos_config_editor and\
       not self.setup_pithos_config_editor():
      return False

    for ii in range(len(vdisk_ids)):
      if not self.__update_vdisk_config(vdisk_param, vdisk_ids[ii],
                                        values[ii]):
        ERROR("Failed to update %s param of vdisk %d with value %s" %
              (vdisk_param, vdisk_ids[ii], values[ii]))
        return False

      INFO("Successfully updated %s param of vdisk %d with value %s" %
           (vdisk_param, vdisk_ids[ii], values[ii]))

    return True

  def mark_all_vdisks_in_container_to_remove(self, container_id,
                                             vdisk_iterator=None):
    """
    Find all the vdisks belonging to a container specified by 'container_id'
    and mark all these vdisks to remove. If 'vdisk_iterator' is specified it
    will be used to iterate over vdisks, else we will fetch vdisk configuration
    from the cluster.

    Returns True on success, else False.
    """
    # Fetch the vdisk configuration if necessary.
    if not vdisk_iterator:
      INFO("Fetching vdisk configuration")
      vdisk_iterator = self.__cluster.create_vdisk_iterator()
      if not vdisk_iterator:
        ERROR("Failed to fetch vdisk configuration")
        return False

    vdisk_configs = list(vdisk_iterator)

    # Find all the vdisks belonging to the 'container_id'.
    to_remove_vdisk_ids = []
    for vdisk_config in vdisk_configs:
      if vdisk_config.container_id == container_id:
        to_remove_vdisk_ids.append(vdisk_config.vdisk_id)

    if len(to_remove_vdisk_ids) == 0:
      INFO("Container %s does not have any vdisk" % str(container_id))
      return True

    INFO("Marking vdisks %s to remove" % to_remove_vdisk_ids)
    param_values = [1] * len(to_remove_vdisk_ids)
    return self.update_vdisk_configs("to_remove",to_remove_vdisk_ids,
                                     param_values)

  def __update_vdisk_config(self, vdisk_param, vdisk_id, value):
    """
    Update 'vdisk_param' of the vdisk specified by 'vdisk_id' with the value
    specified by 'value'.

    Returns True on success, else False.
    """
    CHECK(self.__copied_pithos_config_editor)

    # Randomly select a SVM to update vdisk config.
    svms = self.__cluster.svms()
    svm = svms[random.randint(0, len(svms) - 1)]

    ssh_client = SSHClient(svm.ip(), "nutanix")
    editor_path = "%s/bin/vdisk_config_editor" % svm.test_dir()
    cmd = "%s --vdisk_id %d --param_name %s --value %s" %\
          (editor_path, vdisk_id, vdisk_param, value)
    rv, stdout, stderr = ssh_client.execute(cmd, timeout_secs=60)
    if rv != 0:
      ERROR("Failed to execute %s on SVM %s rv: %d stdout: %s stderr: %s" %
            (cmd, svm.ip(), rv, stdout, stderr))
      return False

    return True

  def get_iscsi_client_params(self):
    """ Get the initiator information from pithos_cli. """
    accessible_svm = None
    for svm in self.__cluster.svms():
      if svm.is_accessible():
        accessible_svm = svm
        break
    #if not accessible_svm:
    #  ERROR("No accessible SVM found")
    #  return None
    accessible_svm = self.__cluster.svms()[0]
    remote_pathname = "/tmp/pithos_iscsi_params"
    cmd = 'source /etc/profile; /usr/local/nutanix/bin/pithos_cli -scan \
          iscsi_client_params -binary_output_file %s' % remote_pathname
    
    results = NutanixClusterSSHUtil.execute([accessible_svm], cmd)[0]
    if results[0]:
      raise NutanixTestException("Error encountered while using pithos_cli %s "
                                 % results[1])
    
    tmp_file = tempfile.NamedTemporaryFile()
    local_pathname = tmp_file.name
    result = NutanixClusterSSHUtil.transfer_from(
        [accessible_svm], [remote_pathname], [local_pathname])
    CHECK(result is not None,
               "Failed to copy pithos cli proto from SVM %s" % accessible_svm.ip())
    #try:
      # Deserialize the proto.
    iscsi_client_params = IscsiClientParams()
      #vdisk_usage = VDiskUsageStatProto()
      #vdisk_usage.ParseFromString(open(tmp_file.name).read())
    iscsi_client_params.ParseFromString(open(tmp_file.name).read())
    return iscsi_client_params
    #except Exception as e:
    #  ERROR("Error getting iscsi client params from pithos %s: %s" %
    #        (self.__cluster.name(), str(e)))
    return None

    
    lines = results[1].split("\n")
    pithos_cli = {}
    for line in lines:      
      if re.search("iscsi_initiator_name", line):
        ret = line.split(" ")
        initiator_name = ret[3].replace("\"","")
        pithos_cli[initiator_name] = {}
        pithos_cli[initiator_name]['Targets'] = []
        pithos_cli[initiator_name]['num_virtual_targets'] = 0
      elif re.search("Target", line):
        ret = line.split(" ")
        pithos_cli[initiator_name]['Targets'].append(ret[1])
      elif re.search("num_virtual_targets", line):
        ret = line.split(" ")
        pithos_cli[initiator_name]['num_virtual_targets'] += ret[3]

    INFO("pithos_cli Info: %s" % pithos_cli)
    return pithos_cli

