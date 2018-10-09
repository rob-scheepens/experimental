"""
Copyright (c) 2018 Nutanix Inc. All rights reserved.

Author: gokul.kannan@nutanix.com

This module contains the methods to trim nutanix vdisks attached to UVMs.
"""

import logging
import logging.handlers
import os
import re

from vm_trim_service import VMTrim

NUTANIX_VDISK_IDENTIFIER = "NUTANIX"

class UVMTrim(VMTrim):
  """
  This class provides methods to perform trims on the disks of a Nutanix UVM.
  """

  DISK_NAME_RE = re.compile(r"([^0-9]*)\d*")
  DM_DISK_NAME_RE = re.compile(r"(/dev/mapper/.*)p\d*")

  def __init__(self):
    self.initialize_logging()

  def get_partitions_to_be_trimmed(self):
    """
    Refer superclass for documentation.
    """
    partitions_to_be_trimmed = []
    # Get all the mounted devices with their mount points.
    device_mounts = self._get_all_device_mounts()
    # Check if the device is backed by a nutanix vdisk. We don't want to be
    # touching devices not backed by nutanix vdisk.
    for device, mount_path in device_mounts.iteritems():
      if self._is_device_nutanix_vdisk(device):
        partition = {}
        partition["size"] = self._get_size(device)
        partition["mount_path"] = mount_path
        partitions_to_be_trimmed.append(partition)

    return partitions_to_be_trimmed

  def initialize_logging(self):
    """
    Set up logging on the UVM.
    """
    # Log everything to /var/log/messages via the syslogd module.
    logger = logging.getLogger()
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    handler = logging.handlers.SysLogHandler(address='/dev/log')
    logger.addHandler(handler)

  def log_info(self, msg):
    """
    Refer superclass for documentation.
    """
    logging.info(msg)

  def log_error(self, msg):
    """
    Refer superclass for documentation.
    """
    logging.error(msg)

  def log_debug(self, msg):
    """
    Refer superclass for documentation.
    """
    logging.debug(msg)

  def verify_prerequisites(self):
    """
    Refer superclass for documentation.
    """
    # In case of UVM, we require the user to have sudo privileges. Verify if
    # the current user has it.
    cmd = "sudo -v"
    rv, stdout, stderr = self.run_command(cmd)
    if rv:
      self.log_error("Failed to run command %s to determine sudo access."
                     "stdout: %s stderr:%s\n The service cannot be run on the "
                     "VM without root privileges." % (cmd, stdout, stderr))
      return False

    return True

  def _get_size(self, device):
    """
    Get the size of the given device. Return the size of the disk in bytes.
    Args:
      device(str): The full path of the device.

    Returns -1 on failure.
    """
    while os.path.islink(device):
      device = os.readlink(device)

    # Get size from /proc/partitions. Skip first line.
    # Format is: major minor  #blocks(1K)  name
    for line in open("/proc/partitions"):
      parts = line.split()
      if len(parts) != 4:
        continue
      if parts[3] == os.path.basename(device):
        return int(parts[2]) * 1024

    return -1

  def _get_all_device_mounts(self):
    """
    Get all the partitions/devices mounted on the VM. Returns a dict with the
    partition as the key and mount point as the value.
    """
    # Open /proc/mounts and get the mount points for all the devices. By
    # devices we mean anything starting with a '/dev'.
    devices = {}
    try:
      with open("/proc/mounts") as mounted_partitions_file:
        mounted_partitions = mounted_partitions_file.read().strip().split("\n")
        for mount_info in mounted_partitions:
          # Get the device partition and the mount path from the line.
          fields = mount_info.split()
          part = fields[0]
          path = fields[1]
          # Verify if this is a device and if the mount path is valid.
          if part.startswith("/dev/") and os.path.exists(path):
            devices[part] = path
    except (IOError, OSError) as ex:
      self.log_error("Unable to read /proc/mounts, error %s" % str(ex))
      return None

    return devices

  def _is_device_nutanix_vdisk(self, device):
    """
    Check if the given device is backed by a nutanix vdisk.
    Args:
      device(str): Device path.     
    """
    # If the given device is a symlink, go down the rabbit hole till we get
    # the actual device path.
    while os.path.islink(device):
      link_path = os.readlink(device)
      # If the path is relative, convert it to absolute path.
      if not os.path.isabs(link_path):
        link_path = os.path.join(os.path.dirname(device), link_path)
      device = link_path

    # Use the scsi_id command to get the scsi name of the device. Check if the
    # name has the nutanix identifier. If it does then we have a nutanix vdisk.
    cmd = "sudo /lib/udev/scsi_id --whitelisted %s" % device
    rv, stdout, stderr = self.run_command(cmd)
    if rv:
      # Check if this is a device mapper. If yes, then it is possible that it
      # is backed by more than one physical device. We will find the physical
      # devices backing this dm device and check if that is a nutanix vdisk.
      if os.path.basename(device).startswith("dm-"):
        physical_devices = self._get_backing_devices(device)
        return self._is_device_nutanix_vdisk(physical_devices[0])

      self.log_error("Failed to run command %s on the device. Error: %s, %s" %
                     (cmd, stdout, stderr))
      return False

    if NUTANIX_VDISK_IDENTIFIER not in stdout:
      return False

    return True

  def _get_backing_devices(self, device):
    """
    Find the physical devices backing the given logical device.
    Args:
      device(str): Device path.
    """
    cmd = "sudo dmsetup info %s -c -o blkdevs_used" % device
    rv, stdout, stderr = self.run_command(cmd)
    if rv:
      self.log_error("Could not find the block devices info for the device %s."
                     " Error: %s, %s" % (device, stdout, stderr))
      return False

    # The output will be something similar to the following:
    # [root@localhost ~]# dmsetup info /dev/dm-2  -c -o blkdevs_used
    # BlkDevNamesUsed
    # sdf,sde
    # [root@localhost ~]#
    # Parse this to get the block device names.
    devices = stdout.split("\n")[1].split(",")
    backing_devices = []
    for block in devices:
      backing_dev = "/dev/" + block
      backing_devices.append(backing_dev)

    return backing_devices
