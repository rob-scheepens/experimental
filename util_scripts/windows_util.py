# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
# Author: nandish.mahadev@nutanix.com
#
# Provides methods for windows powershell commands.

from multiprocessing.queues import Queue, Empty
from threading import Thread
import gflags
import json
import math

from qa.util.agave_tools.windows_vm_command_util import WindowsVMCommandUtil
from util.base.log import ERROR, INFO

FLAGS = gflags.FLAGS

class WindowsUtil(object):
  """
  Python methods to interface with windows powershell commands.
  The powershell methods are exectued through the WindowsVMCommandUtil.
  """
  def get_uvm_initiator_name(self, uvm):
    """ Returns the uvm initiator name. """
    get_initiator_cmd = "(Get-InitiatorPort).NodeAddress"
    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
      uvm, get_initiator_cmd, fatal_on_error=False)

    if FLAGS.agave_verbose:
      INFO("Result of executing command %s: ret %s, stdout %s, stderr %s"
           % (get_initiator_cmd, ret, stdout, stderr))
    return ret, stdout, stderr

  def set_uvm_initiator_name(self, uvm, new_initiator):
    """ Set the initiator name for the given uvm. """
    change_initiator_cmd = "Set-InitiatorPort "\
      "-NodeAddress (Get-InitiatorPort).NodeAddress "\
      "-NewNodeAddress {new_initiator}".format(
      new_initiator=new_initiator)
    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(uvm,
      change_initiator_cmd, fatal_on_error=False)

    if FLAGS.agave_verbose:
      INFO("Result of executing command %s: ret %s, stdout %s, stderr %s"
           % (change_initiator_cmd, ret, stdout, stderr))
    return ret, stdout, stderr

  def get_iscsi_session(self, uvm):
    """Get iscsi session"""
    get_iscsi_session_cmd = "iscsicli SessionList"
    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
        uvm, get_iscsi_session_cmd, fatal_on_error=False)

    if FLAGS.agave_verbose:
      INFO("Result of executing command %s: ret %s, stdout %s, stderr %s"
            % (get_iscsi_session_cmd, ret, stdout, stderr))
    return ret, stdout, stderr

  def get_uvm_initiator_map(self, uvms):
    """ Returns a map of the uvm object as key and initiator name as value. """
    uvm_initiator_map = dict()

    for ii, uvm in enumerate(uvms):
      new_initiator_name = self.__class__.__name__+ "-initiator-" + ii
      ret, stdout, stderr = self.set_uvm_initiator_name(
        uvm, new_initiator_name)
      if ret or stderr.strip():
        ERROR("Returning None as setting initiator name failed on UVM %s\n"
              "ret: %s, stdout: %s, stderr: %s" % (
          uvm.name(), ret, stdout, stderr))
        return dict()
      uvm_initiator_map[uvm] = new_initiator_name

    map_output_logger = []
    for uvm, initiator_name in uvm_initiator_map:
      map_output_logger += uvm.name() + ":" + initiator_name + "\n"
    INFO("UVM to initiator name mapping:\n%s" % map_output_logger)
    return uvm_initiator_map

  def mark_disk_ready(self, uvm, disk_number):
    """ Marks a disk ready. """
    undo_readonly_cmd = "Set-Disk -Number {disk_number} -IsReadonly "\
      "$False".format(disk_number=disk_number)

    # First need to disable read only. Marking the disk online when the
    # disk is marked read only will fail.
    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
      uvm, undo_readonly_cmd, fatal_on_error=False)
    if FLAGS.agave_verbose:
      INFO("Result of executing command %s: ret %s, stdout %s, stderr %s"
           % (undo_readonly_cmd, ret, stdout, stderr))
    if ret or stderr.strip():
      ERROR("Undo readonly on disk with number %s failed on UVM %s\n "
            "ret: %s, stdout: %s, stderr: %s" % (disk_number,
            uvm.name(), ret, stderr, stdout))
      return False
    INFO("Marked disk with number %s on UVM %s ready successfully" %
         (disk_number, uvm.name()))
    return True

  def mark_disk_online(self, uvm, disk_number):
    """ Marks a disk online. """
    set_online_cmd = "Set-Disk -Number {disk_number} -IsOffline $False".format(
      disk_number=disk_number)
    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
      uvm, set_online_cmd, fatal_on_error=False)
    if FLAGS.agave_verbose:
      INFO("Result of executing command %s: ret %s, stdout %s, stderr %s"
           % (set_online_cmd, ret, stdout, stderr))
    if ret or stderr.strip():
      ERROR("Marking disk with number %s online failed on UVM %s\n "
            "ret: %s, stdout: %s, stderr: %s" % (disk_number,
            uvm.name(), ret, stderr, stdout))
      return False
    INFO("Marked disk with number %s on UVM %s online successfully" %
         (disk_number, uvm.name()))
    return True

  def clear_disk(self, uvm, disk_number):
    """ Clears the disk of all data. """
    clear_disk_cmd = "Clear-Disk -Number {disk_number} -RemoveData"\
      " -RemoveOEM -confirm:$false".format(disk_number=disk_number)
    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
      uvm, clear_disk_cmd, fatal_on_error=False)
    if FLAGS.agave_verbose:
      INFO("Result of executing command %s: ret %s, stdout %s, stderr %s"
           % (clear_disk_cmd, ret, stdout, stderr))
    if ret or stderr.strip():
      ERROR("Zero out of disk with number %s failed on UVM %s\n "
            "ret: %s, stdout: %s, stderr: %s" % (disk_number,
            uvm.name(), ret, stderr, stdout))
      return False
    INFO("Clear disk with number %s on UVM %s successful" %
         (disk_number, uvm.name()))
    return True

  def initialize_disk(self, uvm, disk_number, format_type="MBR"):
    """
    Intialize disk with a RAW partition style to either MBR or GPT
    partition styles.
    Disks have to be intialized before they are formatted and used
    to store data.
    """
    if format_type == "GPT":
      init_disk_cmd = "Initialize-Disk -Number {disk_number}".format(
        disk_number=disk_number)
    else:
      init_disk_cmd = "Initialize-Disk -Number {disk_number} "\
        " -PartitionStyle \"MBR\"".format(disk_number=disk_number)
    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
      uvm, init_disk_cmd, fatal_on_error=False)
    if FLAGS.agave_verbose:
      INFO("Result of executing command %s: ret %s, stdout %s, stderr %s"
           % (init_disk_cmd, ret, stdout, stderr))
    if ret or stderr.strip():
      ERROR("Initialize of disk with number %s failed on UVM %s\n "
            "ret: %s, stdout: %s, stderr: %s" % (disk_number,
            uvm.name(), ret, stderr, stdout))
      return False
    return True

  def setup_windows_disks(self, uvm, disk_numbers):
    """
    This marks the intended disk with disk_number online. Then
    it intitalizes the disk with MBR style of partition.
    """
    disk_op_status_map = self.get_disk_operational_status_map(uvm)
    disk_readonly_map = self.get_disk_readonly_status_map(uvm)

    for disk_number in disk_numbers:
      if disk_readonly_map[disk_number]:
        status = self.mark_disk_ready(uvm, disk_number)
        if not status:
          return False

      if not disk_op_status_map[disk_number]:
        status = self.mark_disk_online(uvm, disk_number)
        if not status:
          return False

      if not self.initialize_disk(uvm, disk_number):
        return False

      INFO("Setting up windows disk successful for disk_number %s in UVM %s\n"
           % (disk_number, uvm.name()))
    return True

  def get_uvm_disk_properties(self, uvm):
    """
    This retrieves the disk properties of disks attached to the Windows UVM.
    Here is an example for a disk map.

    'UniqueId'           : 'iqn.2010-06.com.nutanix:vol-2,L,0xd6c4a406ddf4a53b'
    'Number'             : '2'
    'Path'               : '\\?\mpio#disk&ven_nutanix&prod_vdisk&rev_0___#1&..'
    'Manufacturer'       : 'NUTANIX'
    'Model'              : 'VDISK'
    'SerialNumber'       : 'NFS_22_0_637_d5beaf51_6b78_49cc_bbe9_cac189f06e25'
    'Size'               : '20 GB'
    'AllocatedSize'      : '2097152'
    'LogicalSectorSize'  : '512'
    'PhysicalSectorSize' : '512'
    'NumberOfPartitions' : '0'
    'PartitionStyle'     : 'MBR'
    'IsReadOnly'         : 'False'
    'IsSystem'           : 'False'
    'IsBoot'             : 'False'

    Returns:
      This function returns a map with key as a linux style device name
      generated based on the disk number.
    """
    get_disks_cmd = "Get-Disk | Format-List"
    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
      uvm, get_disks_cmd, fatal_on_error=False)
    if FLAGS.agave_verbose:
      INFO("Result of executing command %s: ret %s, stdout %s, stderr %s"
           % (get_disks_cmd, ret, stdout, stderr))
    if ret or stderr.strip():
      ERROR("Failed to retrieve disks information for UVM %s" % uvm.name())
      return dict()

    disks_properties_map = dict()
    stdout = stdout.splitlines()
    disk_property_map = dict()
    count = 0

    for line in stdout:
      field_tuple = [ x.strip() for x in line.split(':', 1) ]
      if len(field_tuple) > 1:
        field = field_tuple[0]
        disk_property_map[field_tuple[0]] = field_tuple[1]
        count += 1
      else:
        # UniqueId field spans more than one rows.
        if field == "UniqueId":
          disk_property_map[field] +=  field_tuple[0]
        continue

      if count == 15:
        count = 0
        disk_number = disk_property_map['Number']
        disks_properties_map[disk_number] = disk_property_map
        disk_property_map = dict()

    INFO("Windows Disk information for UVM %s\n %s\n" % (uvm.name(),
         json.dumps(disks_properties_map, indent=4, separators=(',', ': '))))
    return disks_properties_map

  def get_disk_readonly_status_map(self, uvm):
    """
    Gets a map which has the readonly status set to True
    or False.
    """
    disks_properties_map = self.get_uvm_disk_properties(uvm)
    disk_readonly_map = dict()

    for disk_number, disk_property_map in disks_properties_map.iteritems():
      if 'True' in disk_property_map['IsReadOnly']:
        disk_readonly_map[disk_number] = True
      else:
        disk_readonly_map[disk_number] = False

    return disk_readonly_map

  def get_disk_operational_status_map(self, uvm):
    """
    Gets a map which has the operational status of disks with its
    disk_number as a key.
    """
    get_disks_cmd = "Get-Disk"
    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
      uvm, get_disks_cmd, fatal_on_error=False)
    if FLAGS.agave_verbose:
      INFO("Result of executing command %s: ret %s, stdout %s, stderr %s"
           % (get_disks_cmd, ret, stdout, stderr))
    if ret or stderr.strip():
      ERROR("Failed to retrieve disks information for UVM %s" % uvm.name())
      return dict()

    disk_op_status_map = dict()
    lines = stdout.splitlines()[3:]

    for line in lines:
      disk_number = line.split(' ', 1)[0]
      disk_online = False
      if "Online" in line:
         disk_online = True
      disk_op_status_map[disk_number] = disk_online

    return disk_op_status_map

  def get_vdisk_attributes(self, disk_property_map):
    """
    A helper method to translate Windows disks information to
    relevant Acropolis Distributed Storage Fabric data.

    Params:
      disk_property_map: Disk property map.
      {
        'UniqueId'           : 'iqn.2010-06.com.nutanix:vol-2,L,0xd6c4a...',
	'Number'             : 2,
        'Path'               : '\\?\mpio#disk&ven_nutanix&prod_vdisk...',
        'Manufacturer'       : 'NUTANIX',
        'Model'              : 'VDISK',
        'SerialNumber'       : 'NFS_22_0_637_d5beaf51_6b78_49cc_bbe9_ca...',
        'Size'               : 20 GB,
        'AllocatedSize'      : 2097152,
        'LogicalSectorSize'  : 512,
        'PhysicalSectorSize' : 512,
        'NumberOfPartitions' : 0,
        'PartitionStyle'     : 'MBR',
        'IsReadOnly'         : False,
        'IsSystem'           : False,
        'IsBoot'             : False
     }

    Returns:
      dict() : {
                 'target_name': '',
                 'vdisk_name': '',
                 'vmdisk_uuid': ''
               }
    """
    device_mapper = dict()
    device_mapper['target_name'] = disk_property_map['UniqueId'].split(',')[0]
    device_mapper['vdisk_name'] = ":".join(tuple(
      disk_property_map['SerialNumber'].split('_')[:4]))
    device_mapper['vmdisk_uuid'] = "-".join(tuple(
      disk_property_map['SerialNumber'].split('_')[4:]))
    device_mapper['disk_number'] = disk_property_map['Number']

    # Assuming we always create disk greater than 1GB
    numbers = disk_property_map['Size'].split(" ")[0].split(".")
    # In case the capacity is a decimal number.
    if len(numbers) > 1:
      number = float(numbers[0]) + (float(numbers[1])/math.\
                                     pow(10, len(numbers[1])))
    else:
      number = int(numbers[0])
    device_mapper['capacityKB'] = number * 1024 * 1024

    return device_mapper

  def get_uvm_devices_map(self, uvms):
    """
    Has a map of the uvm object to its devices map.

    Returns:
        Dict(uvm, devices_map)
    """
    uvm_devices_map = dict()

    for uvm in uvms:
      devices_map = list()
      disk_properties_map = self.get_uvm_disk_properties(uvm)

      for disk_number, disk_property_map in disk_properties_map.iteritems():
        vdisk_attributes_map = self.get_vdisk_attributes(disk_property_map)
        device_name = "/dev/sd" + chr(ord('a') + int(disk_number))
        if device_name in ['/dev/sda']:
          continue
        devices_map.append(tuple((device_name, vdisk_attributes_map)))

      uvm_devices_map[uvm.ip()] = devices_map

    return uvm_devices_map

  def get_uvm_active_path_map(self, uvms, svm_ips, random=True):
    """
    Creates a map of the uvm object with the target ip serving as an
    active path.
    """
    uvm_active_path_map = dict()

    for xx, uvm in enumerate(uvms):
      uvm_active_path_map[uvm] = (str(uvm.node().svm().ip()),
        str(svm_ips[xx % len(svm_ips)]))[random is True]

    return uvm_active_path_map

  def get_uvm_to_iscsi_target_paths(self, uvms, target_ips, random=True):
    """
    Returns two maps. This can be used in attach_targets method to assign
    these paths to the respective UVMs.
    1) UVM to active path to targets.
    2) UVM to standby paths to targets.
    """
    uvm_standby_paths_map = dict()
    uvm_active_path_map = self.get_uvm_active_path_map(uvms, target_ips, random)

    for uvm in uvms:
      active_path = uvm_active_path_map[uvm]
      standby_paths = [str(target_ip) for target_ip in target_ips \
                        if target_ip != active_path]
      uvm_standby_paths_map[uvm] = ",".join(standby_paths)

    INFO("Active-Standby paths map for UVMs:\n")

    for uvm, active_path, standby_paths in \
      zip(uvm_standby_paths_map.keys(), uvm_active_path_map.values(),
        uvm_standby_paths_map.values()):
      paths_dict = dict()
      uvm_paths_dict = dict()
      paths_dict['active_path'] = active_path
      paths_dict['standby_paths'] = standby_paths
      uvm_paths_dict['uvm_name'] = uvm.name()
      uvm_paths_dict['paths'] = paths_dict
      INFO("%s" % json.dumps(uvm_paths_dict, indent=4,
           separators=(',', ': ')))

    return (uvm_active_path_map, uvm_standby_paths_map)

  def enable_iscsi_multipath(self, uvms):
    """
    Starts up iSCSI service and installs Multipath feature.
    Sets the loadbalancing policy to 'Failover Only'.
    """
    for uvm in uvms:
      install_iscsi_multipath_cmd = (
        "Import-Module NutanixWindowsGuestWorkflows; Install-Iscsi")
      ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
        uvm, install_iscsi_multipath_cmd, fatal_on_error=False)
      if ret or stderr.strip():
        ERROR("Failed to install multipath iSCSI on UVM %s" % uvm.name())
        return False
    return True

  def disable_multipath(self, uvms):
    """ Uninstalls the Multipath feature in the Windows UVM. """
    uninstall_multipath_cmd = \
      "Uninstall-WindowsFeature -Name Multipath-IO 3>&1 | Out-String"

    for uvm in uvms:
      ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
        uvm, uninstall_multipath_cmd, fatal_on_error=False)
      if ret or stderr.strip():
        ERROR("Failed to uninstall multipath iSCSI on UVM %s" % uvm.name())
        return False
    return True

  def add_target_portals(self, uvm, target_portal_ips):
    """ Add target portal IPs to Windows UVM. """
    for target_portal_ip in target_portal_ips.split(","):
      add_target_cmd = "New-IscsiTargetPortal -TargetPortalAddress %s"\
                       " | Out-Null" % target_portal_ip
      ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
        uvm, add_target_cmd, fatal_on_error=False)
      INFO("Result of executing command %s on UVM %s : ret %s, stdout %s,"
           "stderr %s" % (add_target_cmd, uvm.name(),
           ret, stdout, stderr))
      if ret or stderr.strip():
        return False
    return True

  def remove_target_portals(self, uvm, target_portal_ips):
    """ Remove target portal IPs to Windows UVMs. """
    remove_target_cmd = "Remove-IscsiTargetPortal -TargetPortalAddress %s"\
                        " -Confirm:$false| Out-Null" % target_portal_ips
    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
      uvm, remove_target_cmd, fatal_on_error=False)
    INFO("Result of executing command %s on UVM %s : ret %s, stdout %s,"
         "stderr %s" % (remove_target_cmd, uvm.name(),
         ret, stdout, stderr))
    if ret or stderr.strip():
      return False
    return True

  def add_svms_as_target_portal(self, uvm_active_paths, uvm_standby_paths):
    """
    Adds IP part of active paths map  and standby paths map as
    Target Portal on uvm.
    """
    for uvm, active_path, standby_paths in zip(uvm_active_paths.keys(),
      uvm_active_paths.values(), uvm_standby_paths.values()):
      target_portal_ips = ",".join((active_path, standby_paths))
      if not self.add_target_portals(uvm, target_portal_ips):
        return False
    return True

  def add_vip_as_target_portal(self, uvms, data_services_vip):
    """ Add the cluster external data services virtual IP as target portal. """
    for uvm in uvms:
      if not self.add_target_portals(uvm, data_services_vip):
        return False
    return True

  def remove_vip_as_target_portal(self, uvms, data_services_vip):
    """
    Remove the cluster external data services virtual IP as a target portal.
    """
    for uvm in uvms:
      if not self.remove_target_portals(uvm, data_services_vip):
        return False
    return True

  def remove_svms_as_target_portal(self, uvm_active_paths, uvm_standby_paths):
    """
    Adds IP part of active paths map  and standby paths map as
    Target Portal on uvm.
    """
    for uvm, active_path, standby_paths in zip(uvm_active_paths.keys(),
      uvm_active_paths.values(), uvm_standby_paths.values()):
      target_portal_ips =  ",".join((active_path, standby_paths))
      if not self.remove_target_portals(uvm, target_portal_ips):
        return False
    return True

  def connect_vip_targets(self, uvms, data_services_vip):
    """ Connects the discovered targets with the initiator. """
    for uvm in uvms:
      attach_iscsi_target_cmd = (
        "Import-Module NutanixWindowsGuestWorkflows; Connect-VIPScsiTarget "
        "-TargetPath {data_services_vip} -InitiatorIpAddress {ip} "
        "-Verbose".format(data_services_vip=data_services_vip, ip=uvm.ip()))
      ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
        uvm, attach_iscsi_target_cmd, fatal_on_error=False)
      INFO("Result of executing command %s on UVM %s : ret %s, stdout %s,"
           "stderr %s" % (attach_iscsi_target_cmd, uvm.name(),
           ret, stdout, stderr))
      if ret:
        return False
    return True

  def connect_multipath_targets(self, uvm_active_paths, uvm_standby_paths):
    """
    Connects initiator and target with one active path and remaining
    standby paths.
    """
    for uvm, active_paths, standby_paths in zip(uvm_active_paths.keys(),
      uvm_active_paths.values(), uvm_standby_paths.values()):
      target_portal_ips =  ",".join((active_paths, standby_paths))
      attach_iscsi_target_cmd = (
        "Import-Module NutanixWindowsGuestWorkflows; "
        "Connect-MultipathScsiTarget -TargetPaths {target_portal_ips} "
        "-InitiatorIpAddress {ip} -Verbose".format(
        target_portal_ips=target_portal_ips, ip=uvm.ip()))
      ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
        uvm, attach_iscsi_target_cmd, fatal_on_error=False)
      INFO("Result of executing command %s on UVM %s : ret %s, stdout %s,"
           "stderr %s" % (attach_iscsi_target_cmd, uvm.name(),
           ret, stdout, stderr))
      if ret:
        return False
    return True

  def disconnect_targets(self, uvms):
    """ Disconnects the targets from the uvms. """
    for uvm in uvms:
      get_target_list_cmd = "iscsicli ListTargets"

      ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
        uvm, get_target_list_cmd, fatal_on_error=False)
      INFO("Result of executing command %s on UVM %s: ret %s, stdout %s,"
           "stderr %s" % (get_target_list_cmd, uvm.name(), ret, stdout, stderr))
      if ret or stderr.strip():
        return False

      out = stdout.splitlines()[3:-1]
      if not len(out):
        INFO("No targets found on UVM %s" % uvm.name())
        continue

      target_names = list()
      for line in out:
        target_names.append(line.strip())

      remove_iscsi_target_cmd = ("Import-Module NutanixWindowsGuestWorkflows;"
        "Remove-ScsiTarget -TargetNames %s -Verbose" % ",".join(target_names))
      ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
        uvm, remove_iscsi_target_cmd, fatal_on_error=False)
      INFO("Result of executing command %s on UVM %s : ret %s, stdout %s,"
           "stderr %s" % (remove_iscsi_target_cmd, uvm.name(),
           ret, stdout, stderr))
      if ret:
        return False

    return True

  def get_process_count(self, uvm, process_name):
    """ Checks if a process is running or not. """
    process_list_cmd = "Get-Process -processname %s* | Measure-Object |"\
      " Select-Object -ExpandProperty Count" % process_name

    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
      uvm, process_list_cmd, fatal_on_error=False)
    INFO("Result of executing command %s on UVM %s: ret %s, stdout %s,"
         "stderr %s" % (process_list_cmd, uvm.name(), ret, stdout, stderr))
    if ret or stderr.strip():
      return -1

    return int(stdout)

  def stop_process(self, uvm, process_name):
    """ Terminate process with process_name. """
    stop_process_cmd = "Stop-Process -processname %s\*"% process_name

    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
      uvm, stop_process_cmd, fatal_on_error=False)
    INFO("Result of executing command %s on UVM %s: ret %s, stdout %s,"
         "stderr %s" % (stop_process_cmd, uvm.name(), ret, stdout, stderr))
    if ret or stderr.strip():
      return False
    return True

  def set_location(self, uvm, dir_path):
    """ Change the current working directory to 'dir_path'. """
    set_location_cmd = "Set-Location %s" % dir_path

    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
      uvm, set_location_cmd, fatal_on_error=False)
    if FLAGS.agave_verbose:
      INFO("Result of executing command %s on UVM %s: ret %s, "
           "stdout %s, stderr %s" % (set_location_cmd, uvm.name(),
        ret, stdout, stderr))
    if ret or stderr.strip():
      return False
    return True

  def search_string(self, uvm, dir_path, pattern, filter_pattern=None):
    """
    Search for a pattern in a file recursively under all its child
    directories. Optional filter_pattern filters files with specific
    file name pattern.
    """
    if filter_pattern:
      recurse_cmd = "Get-ChildItem %s -recurse -filter %s" % \
        (dir_path, filter_pattern)
    else:
      recurse_cmd = "Get-ChildItem %s -recurse" % dir_path
    search_cmd = recurse_cmd + " | select-string -pattern \"%s\"\
      | Measure-Object| Select-Object -ExpandProperty Count" %  pattern

    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
      uvm, search_cmd, fatal_on_error=False)
    if FLAGS.agave_verbose:
      INFO("Result of executing command %s on UVM %s: ret %s, stdout %s,"
           " stderr %s" % (search_cmd, uvm.name(), ret, stdout, stderr))
    if ret or stderr.strip():
      return -1
    return int(stdout)

  def zero_out_disks(self, uvm, disk_numbers, max_file_sizes,
    block_size=1048576, timeout_secs=3600):
    """
    Write zeroes covering the max_file_size to windows disks.

    Params:
      uvm: windows UVM object.
      disk_numbers: list of disk numbers.
      max_file_sizes: list file sizes.
      block_size: Size of the zero write blocks.
      timeout_secs: Timeout after timeout_secs if command did not return.

    Returns:
      True on success. False, otherwise.
    """
    arg_list = []

    for disk_number, max_file_size in zip(disk_numbers, max_file_sizes):
      count = max_file_size/block_size
      cmd = "C:\\test\\tools\\dd\\dd.exe if=/dev/zero "\
        "of=\\\\?\\Device\\Harddisk{disk_number}\\Partition0 bs={block_size} "\
        "count={count}".format(disk_number=disk_number,
        block_size=block_size, count=count)
      arg_list.append((uvm, cmd, timeout_secs))

    target_list = ['command_execute' for _ in disk_numbers]

    executor = WindowsUtilExecutor(target_list, arg_list)
    executor.start_all()
    executor.join_all()

    if executor.is_failed():
       ERROR("Failed to zero out all devices on UVM %s" % uvm.name())
       return False

    INFO("Sucessfully zeroed out devices attached to the UVM %s" % uvm.name())
    return True

  def command_execute(self, uvm, cmd, timeout_secs=600, check_stdout=False):
    """
    Helper method which allows parallel execution of commands on Windows UVM
    with the help of WindowsUtilExecutor.

    Params
      uvm: UVM object on which the command is to be executed.
      cmd: Command to be executed.
      timeout_secs: Times out after timeout_secs since command execution.
      check_stdout: Checks for True/False in stdout.

    Returns:
      True on success. False, otherwise.
    """
    if FLAGS.agave_verbose:
      INFO("Executing command %s on UVM %s" % (cmd, uvm.name()))
    ret, stdout, stderr = WindowsVMCommandUtil.execute_command(
      uvm, cmd, fatal_on_error=False, timeout_secs=timeout_secs)
    if FLAGS.agave_verbose:
      INFO("Result of executing command %s on UVM %s: ret %s, stdout %s,"
           " stderr %s" % (cmd, uvm.name(), ret, stdout, stderr))
    if ret:
      return False

    if check_stdout:
      if "True" in stdout.strip():
        return True
      else:
        return False

    return True

class WindowsUtilExecutor(object):
  """
  A simple concurrent executor of threads intended to be used for
  parallel execution of commands on disks attached to Windows UVMs.
  """
  def __init__(self, target_list, arg_list):
    """
    Initializes the executor object.

    Params:
    target_list: list of target method names to be executed in parallel.
    arg_list: list of args for the corresponding methods.
    """
    self.target_list = target_list
    self.arg_list = arg_list
    self.executors = []
    self.return_values = []

  def __setup(self):
    """ Sets up the Executor objects to be started. """
    for target, args in zip(self.target_list, self.arg_list):
      executor = Executor(target_name=target, args=args)
      self.executors.append(executor)

  def start_all(self):
    """ Start all executors. """
    self.__setup()
    for xx, executor in enumerate(self.executors):
      executor.start()
      if FLAGS.agave_verbose:
        INFO("Thread started. target_method = %s, target_uvm = %s" % \
             (self.target_list[xx], self.arg_list[xx][0].name()))

  def join_all(self):
    """ Waits for all executors to finish. """
    for xx, executor in enumerate(self.executors):
      executor.join()
      if FLAGS.agave_verbose:
        INFO("Thread stopped. target_method = %s, target_uvm = %s" % \
             (self.target_list[xx], self.arg_list[xx][0].name()))

  def is_failed(self):
    """ Check for command failures. """
    for executor in self.executors:
      if executor.is_failed():
        return True
    return False

  def get_num_failures(self):
    """ Count of failures. """
    count = 0
    for executor in self.executors:
      if executor.is_failed():
       count += 1
    return count

  def get_return_values(self):
    """ Returns a list of return values. """
    for executor in self.executors:
      self.return_values.append(executor.get_return_value())
    return self.return_values

class Executor(Thread):
  """ Implements a thread object. """

  FAILED = "failed"
  FAILED_MSG = {FAILED : True}
  SUCCESS = "success"
  SUCCESS_MSG = {SUCCESS: None}

  def __init__(self, target_name, args=None):
    """ Thread initializer. """
    self.target_name = target_name
    self.args = args
    # Stores the return value of the target method.
    self.out_queue = Queue()
    self.failed = False
    self.return_value = None
    self.windows_util = WindowsUtil()

    super(Executor, self).__init__(target=self.__run)

  def __run(self):

     if getattr(self.windows_util, self.target_name, False):
       rv = getattr(self.windows_util, self.target_name)(*self.args)

       if not rv:
         self.out_queue.put(Executor.FAILED_MSG)
       else:
         self.out_queue.put(Executor.SUCCESS_MSG)
     else:
       # Record failure if invalid method invoked.
       self.out_queue.put(Executor.FAILED_MSG)

  def _check_out_queue(self):
    try:
      while True:
        # Thread out going queue. When queue is empty it
        # will raise exception which will drop the code out
        # of this loop.
        msg = self.out_queue.get_nowait()
        self.failed = msg.get(Executor.FAILED, False)
        self.return_value = msg.get(Executor.SUCCESS, None)
    except Empty:
      pass

  def is_failed(self):
    if not self.failed:
      self._check_out_queue()
    return self.failed

  def get_return_value(self):
    if not self.is_failed():
      return self.return_value
