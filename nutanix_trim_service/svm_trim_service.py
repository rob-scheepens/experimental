"""
Copyright (c) 2018 Nutanix Inc. All rights reserved.

Author: gokul.kannan@nutanix.com

This module contains the methods for trimming the disks on the nutanix SVM.
"""
import env
import cluster.genesis_utils as genesis_utils
import util.base.log

from util.base import hcl
from util.base.disk import Disk
from util.base.log import DEBUG, ERROR, INFO
from util.infrastructure.cluster import get_cached_zeus_configuration
from vm_trim_service import VMTrim

class SVMTrim(VMTrim):
  """
  Class containing methods for performing fstrim on disks on Nutanix SVM.
  """

  def __init__(self):
    # Initialize logging.
    self.initialize_logging()

  def get_partitions_to_be_trimmed(self):
    """
    See superclass for documentation.
    """
    # Get the disks available on the SVM.
    disks_qualified_for_trim = self._get_disks_qualified_for_fstrim()

    # We want to run fstrim only on the disk partitions being used by
    # stargate. So filter out such disks.
    return self._get_disks_used_by_stargate(disks_qualified_for_trim)

  def initialize_logging(self):
    """
    Set up logging on the SVM.
    """
    util.base.log.initialize()
  
  def log_info(self, msg):
    """
    See superclass for documentation.
    """
    INFO(msg)

  def log_error(self, msg):
    """
    See superclass for documentation.
    """
    ERROR(msg)

  def log_debug(self, msg):
    """
    See superclass for documentation.
    """
    DEBUG(msg)

  def verify_prerequisites(self):
    """
    See superclass for documentation.
    """
    # The preconditions for this feature on the SVM would be managed by
    # AOS/genesis. Return success. In future we could add intelligence here
    # which could check for presence of certain type of drives etc.
    return True

  def _get_disks_qualified_for_fstrim(self):
    """
    Get the disks qualified for running fstrim. Get all the raw devices on the
    SVM. Then refer the hcl.json to filter the disks qualified for fstrim.
    """
    raw_devices = Disk.disks()
    disks_qualified_for_trim = []
    models_with_trim_support = hcl.get_disk_models_with_trim_support()
    for device in raw_devices:
      disk_obj = Disk(device)
      if disk_obj.get_disk_model() in models_with_trim_support:
        disks_qualified_for_trim.append(disk_obj.serial_number())

    return disks_qualified_for_trim

  def _get_disks_used_by_stargate(self, disk_serial_ids):
    """
    Get the disks in use by stargate from the given list of disk serial ids.
    Args:
      disk_serial_ids(list): List of disk serial ids.

    """
    disks_in_use = []
    # Get the cached zeus config.
    zeus_config = get_cached_zeus_configuration()

    # First get the list of offlined disks on the SVM from zeus.
    svm_id = genesis_utils.get_svm_id()
    offline_disk_mount_paths = self.__get_offline_disk_mount_paths(
      zeus_config, svm_id)

    # Now create a dict with disk size and mount point information.
    for disk in zeus_config.disk_list:
      if disk.disk_serial_id in disk_serial_ids and \
        disk.mount_path not in offline_disk_mount_paths:
        disk_info = {}
        disk_info["size"] = disk.statfs_disk_size
        disk_info["mount_path"] = disk.mount_path
        disks_in_use.append(disk_info)

    return disks_in_use

  def __get_offline_disk_mount_paths(self, zeus_config, svm_id):
    """
    Get the disks which are marked offline in zeus.
    Args:
      zeus_config(proto object): Zeus config info of the cluster.
      svm_id(str): SVM id.
    """
    offline_disk_mount_paths = []
    for node in zeus_config.node_list:
      if node.service_vm_id == svm_id:
        if node.offline_disk_mount_paths:
          offline_disk_mount_paths.extend(node.offline_disk_mount_paths)

        return offline_disk_mount_paths
