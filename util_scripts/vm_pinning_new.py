#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Author: gokul.kannan@nutanix.com
#
# Tests for VM Pinning feature

import time
import os
import pprint

from util.base.build import top_dir
from illuminati import perf_util
from qa.agave.agave_util import AgaveUtil
from qa.agave.nutanix_test import NutanixTest
from qa.agave.nutanix_test_uvms_spec import NutanixTestUvmsSpecCreator
from qa.util.agave_tools.vm_disk_util import get_vm_disk_util
from qa.agave.nutanix_cluster_ssh_util import NutanixClusterSSHUtil
from qa.util.agave_tools.nutanix_stargate_util import NutanixStargateUtil
from qa.util.agave_tools.nutanix_curator_util import NutanixCuratorUtil
from qa.util.agave_tools.io_integrity_manager import IOIntegrityManager
from qa.util.agave_tools.nutanix_curator_test_client \
                                          import NutanixCuratorTestClient

class VM_Pinning(NutanixTest):
  """
  This class implements tests to verify VM Pinning feature. The following 
  testcases are implemented:
  - Verify full and partial pinning of vdisk.
  - Verify pinned bytes stay in SSD tier and excess data is downmigrated.
  - Verify pinned bytes are not downmigrated.
  - Verify VM Pinning works on compression enabled containers
  - Verify VM Pinning works on dedupe enabled containers
  """
  # Data population takes its time
  TEST_TIMEOUT = 3600 * 5
  
  @staticmethod
  def tags():
    return set(["stargate"])
  
  @staticmethod
  def uvms_spec():
    creator = NutanixTestUvmsSpecCreator()
    spec = creator.create_hypervisor_uvms_spec(1, 0)
    return spec
  
  @staticmethod  
  def timeout_secs():
    return VM_Pinning.TEST_TIMEOUT

  def setup(self):
    """
    Test setup. Calculates the available space and undoes any previous pinning.
    """
    
    self.info("Get the total SSD capacity and derive the maximum allowed"
              "pinning space")
    self.ssd_capacity = 0
    zeus_config = self.cluster.get_zeus_config()
    for disk in zeus_config.disk_list:
      if disk.storage_tier == "SSD-SATA":
        self.ssd_capacity += disk.disk_size
      elif disk.storage_tier == "SSD-PCIe" and disk.disk_size > 0:
        self.fatal("VM Pinning is not supported in clusters with SSD-PCI")
        
    self.uvms = self.uvms_binding.get_uvm_list()
    self.container = self.uvms_binding.container_map.values()[0]
    rf = self.container.params.replication_factor
    
    # Only max_allowed_pinned_config_pct (25% by default) of the total SSD
    # capacity is available for pinning. This 25% includes the space occupied
    # by all the replicas. Hence do the math here.
    max_pin_percent = zeus_config.StorageTier().\
                      max_allowed_pinned_config_pct
    self.info("Max percent allowed to pin: %s" % max_pin_percent)
    self.max_usable_ssd_capacity = self.ssd_capacity * max_pin_percent // \
                                   (100 * rf)
    self.info("SSD capacity is: %s Max Available for pinning is %s"
              % (self.ssd_capacity, self.max_usable_ssd_capacity))
    self.vdisk_util = get_vm_disk_util(self.cluster)
    
    self.info("Undo any VM previously pinned")
    cmd = "virtual-disk list"
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    pp = pprint.PrettyPrinter(indent=4)    
    for vdisk in result['data']:
      # Putting this here to debug intermittent failure caused when key is not
      # available
      pp.pprint(vdisk)
      if vdisk['virtualDiskTierParams'] is not None and \
      len(vdisk['virtualDiskTierParams']):
        self.__update_pinning(vdisk['virtualDiskId'], 0)
    self.__stargate_util = NutanixStargateUtil(self.cluster)
    self.__curator = NutanixCuratorTestClient(self.cluster)

    gflags_map = {}
    # Reduce ILM tier usage threshold to 25 for down migration to start sooner.
    gflags_map["curator_tier_usage_ilm_threshold_percent"] = "25"
    self.check_eq(
      NutanixCuratorUtil.restart_curator_with_custom_gflags(
        self.cluster, True, # keep_existing_gflags.
        gflags_map),
      0, "Failed to restart Curator with custom gflags")
                              
  def test_basic_pinning_full_partial(self):
    """ 
    Testcase to verify full and partial pinning of vdisk is successful. It also
    verifies if the complete SSD capacity available for pinning can be pinned.
    """
    
    self.info("Starting the test")
    cmd = "virtual-disk list"
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    self.info("Pin the first VM")
    vmid = result['data'][0]['virtualDiskId']
    vmname = result['data'][0]['attachedVMName']
    vmsize = result['data'][0]['diskCapacityInBytes']
    # Pin the vdisk partially.
    if vmsize <= self.max_usable_ssd_capacity :
      bytes_to_pin = vmsize//2
    else:
      bytes_to_pin = self.max_usable_ssd_capacity//2
    
    self.info("Pinning %s bytes of the the first VM with name %s and uuid %s"
              % (bytes_to_pin,vmname, vmid))
    self.__update_pinning(vmid, bytes_to_pin)
    self.info("Reset pinning to 0")
    self.__update_pinning(vmid, 0)
    
    self.info("Try pinning the max allowed size")
    bytes_available = self.max_usable_ssd_capacity
    for vdisk in result['data']:
      if vdisk['diskCapacityInBytes'] <= bytes_available:
        self.__update_pinning(vdisk['virtualDiskId'], \
                              vdisk['diskCapacityInBytes'])
        bytes_available -= vdisk['diskCapacityInBytes']
      elif bytes_available > 0:
        self.__update_pinning(vdisk['virtualDiskId'], bytes_available)
        bytes_available = 0
        break
      
    # Check if all available SSD capacity has been reserved for pinning VMs.
    # If not then attach vdisk to VM and pin it.
    if bytes_available != 0:
      self.info("Attach a disk to the UVM and pin it")
      result = self.__attach_new_vdisk_and_pin(self.uvms[0], \
                                             self.max_usable_ssd_capacity,\
                                             bytes_available)

    return True
  
  def test_verify_pinned_data_in_ssd(self):
    """
    Testcase to verify the data pinned stays in SSD when VMs are powered off and
    on. This also tests runtime increase and decrease of pinned bytes.
    """
    self.info("Attach a disk of size a little more than max_usable_ssd_capacity"
              " to a UVM and pin it")
    # Attach a new vdisk to the UVM and pin it
    disk_size = self.max_usable_ssd_capacity + (self.max_usable_ssd_capacity //\
                                                4)
    result = self.__attach_new_vdisk_and_pin(self.uvms[0], disk_size,\
                                    self.max_usable_ssd_capacity)
    disk = result['disk']
    virtualdisk_id = result['virtual_disk']
    
    # Write 1G more data than what can be max pinned. We have the ilm threshold
    # flag set to 25%. So there will be scan triggered. But the data which is
    # pinned should not be down migrated.
    data_size_to_write_in_mb = (self.max_usable_ssd_capacity//(1024*1024)) +\
                              1048
    self.info("Writing %s MB data to the disk" %data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[0], disk, data_size_to_write_in_mb)
    
    # There might be few bytes lying in the oplog.
    # Drain the oplog and check if bytes pinned are present in SSD.
    svm_ips = [svm.ip() for svm in self.cluster.svms()]
    perf_util.wait_for_oplog_drain(svm_ips)
    test_vdisk_id1 = self.__get_vdiskid_from_dev(self.uvms[0], disk)
    self.__verify_pinned_bytes(test_vdisk_id1, self.max_usable_ssd_capacity)
    
    # Reduce the pinning and pin another vdisk. Current vdisk's data should be
    # downmigrated.
    self.info("Reduce the pinning on the current pinned vdisk and pin another"
              " disk")
    new_bytes_to_pin = self.max_usable_ssd_capacity - (20 * 1024 * 1024 * 1024)
    self.__update_pinning(virtualdisk_id, new_bytes_to_pin)
    
    # Create a 60G disk and pin 20G
    disk_size = 60 * 1024 * 1024 * 1024
    bytes_to_pin = 20 * 1024 * 1024 * 1024
    result = self.__attach_new_vdisk_and_pin(self.uvms[1], disk_size,\
                                             bytes_to_pin)
    
    # Write data to the new disk so that the data from the old disk gets down
    # migrated to HDD
    data_size_to_write_in_mb = (bytes_to_pin//(1024 * 1024)) + 100
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[1], result['disk'], data_size_to_write_in_mb)
    
    # Drain the oplog and check if bytes pinned are present in SSD.
    perf_util.wait_for_oplog_drain(svm_ips)
    test_vdisk_id2 = self.__get_vdiskid_from_dev(self.uvms[1], result['disk'])
    self.__verify_pinned_bytes(test_vdisk_id2, bytes_to_pin)
    self.__verify_pinned_bytes(test_vdisk_id1, new_bytes_to_pin)
    
     # Start a curator scan and verify pinned bytes are still present in the
     # SSD
    self.info("Start a curator scan")
    NutanixCuratorUtil.start_and_wait_till_partial_scans_completed(self.\
                                                                   __curator, 1)
    self.__verify_pinned_bytes(test_vdisk_id2, bytes_to_pin)
    self.__verify_pinned_bytes(test_vdisk_id1, new_bytes_to_pin)
    
    self.info("Increase and decrease the pinned bytes of the two pinned vdisks")
    # Decrease pinning of first disk by 10G.
    new_bytes_to_pin = new_bytes_to_pin - (10 * 1024 * 1024 * 1024)
    self.__update_pinning(virtualdisk_id, new_bytes_to_pin)
    # Increase pinning of second disk by 10G.
    bytes_to_pin = bytes_to_pin + (10 * 1024 * 1024 * 1024)
    self.__update_pinning(result['virtual_disk'], new_bytes_to_pin)

    # Write data to the second disk so the increase in pinning can be verified
    data_size_to_write_in_mb = (bytes_to_pin//(1024 * 1024)) + 100
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[1], result['disk'], data_size_to_write_in_mb)
    
    # Drain the oplog and check if bytes pinned are present in SSD.
    perf_util.wait_for_oplog_drain(svm_ips)
    self.__verify_pinned_bytes(test_vdisk_id2, bytes_to_pin)
    self.__verify_pinned_bytes(test_vdisk_id1, new_bytes_to_pin)

    return True

  def test_verify_pinning_with_dedupe(self):
    '''
    Test to verify VM Pinning works on dedupe enabled containers
    '''
    cmd = "container edit name=%s fingerprint-on-write=on"\
          " on-disk-dedup=PostProcess" % self.container.container_name
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    self.__test_vm_pinning_io_integrity()
  
  def test_verify_pinning_with_compression(self):
    '''
    Test to verify VM Pinning works on compression enabled containers
    '''
    cmd = "container edit name=%s enable-compression=true compression-delay=0"\
          % self.container.container_name
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    self.__test_vm_pinning_io_integrity()

  def test_verify_pinned_not_downmigrated(self):
    """
    Testcase to verify the bytes pinned are not downmigrated
    """
    self.info("Attach a disk of size of 10% of max_usable_ssd_capacity"
              "to a UVM and then pin half of it")
    disk_size = int(self.max_usable_ssd_capacity * .1)
    
    bytes_to_pin_disk1 = disk_size//2
    result = self.__attach_new_vdisk_and_pin(self.uvms[0], disk_size,\
                                    bytes_to_pin_disk1)
    disk = result['disk']
    
    self.info("Write data to the pinned disk and verify data is in SSD")
    # Write data worth half of the disk size
    data_size_to_write_in_mb = bytes_to_pin_disk1//(1024*1024)
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[0], disk, data_size_to_write_in_mb)
    
    # Drain the oplog so that all data gets flushed to disk.
    svm_ips = [svm.ip() for svm in self.cluster.svms()]
    perf_util.wait_for_oplog_drain(svm_ips)
    # Verify if data is present in SSD
    pinned_vdisk_id1 = self.__get_vdiskid_from_dev(self.uvms[0], disk)
    self.__verify_pinned_bytes(pinned_vdisk_id1, bytes_to_pin_disk1)
    
    # Fill up 25% of SSD so that curator scan kicks in and down migration starts
    self.info("Attach a disk of size a little more than "
              "max_usable_ssd_capacity and then fill it up with data")
    disk_size = self.max_usable_ssd_capacity + (self.max_usable_ssd_capacity //\
                                                4)
    disk = self.vdisk_util.create_attach_vdisks_to_vm(
            self.uvms[1],
            self.container.container_name,
            num_disks=1,
            size_kb = disk_size //(1024))
    self.check(disk, "Failed to attach a vdisk")
    data_size_to_write_in_mb = (self.max_usable_ssd_capacity//(1024*1024))
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[1], disk[0], data_size_to_write_in_mb)
    
    self.info("Verify the already pinned bytes are not downmigrated")
    self.__verify_pinned_bytes(pinned_vdisk_id1, bytes_to_pin_disk1)
    
    self.info("Attach a vdisk to another UVM and pin it")
    disk_size = int(self.max_usable_ssd_capacity * .1)
    bytes_to_pin_disk2 = disk_size//3
    result = self.__attach_new_vdisk_and_pin(self.uvms[2], disk_size,\
                                                       bytes_to_pin_disk2)
    disk = result['disk']
    pinned_vdisk_id2 = self.__get_vdiskid_from_dev(self.uvms[2], disk)
    
    self.info("Fill the vdisk and verify the data stays in SSD for both the "
              "pinned vdisk")
    data_size_to_write_in_mb = bytes_to_pin_disk2//(1024*1024)
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[2], disk, data_size_to_write_in_mb)
    
    # Drain the oplog and check if bytes pinned are present in SSD.
    perf_util.wait_for_oplog_drain(svm_ips)
    self.__verify_pinned_bytes(pinned_vdisk_id2, bytes_to_pin_disk2)
    self.__verify_pinned_bytes(pinned_vdisk_id1, bytes_to_pin_disk1)
    
    self.info("Power off and power on the VMs and verify the bytes are still"
              "pinned")
    self.cluster.stop_uvms( self.uvms)
    # Start a curator scan
    NutanixCuratorUtil.start_and_wait_till_partial_scans_completed(self.\
                                                                   __curator, 1)
    
    # Verify the bytes are present in SSD
    self.__verify_pinned_bytes(pinned_vdisk_id2, bytes_to_pin_disk2)
    self.__verify_pinned_bytes(pinned_vdisk_id1, bytes_to_pin_disk1)
    self.cluster.start_uvms( self.uvms)
    
    return True
  
  def teardown(self):
    self.info("Enter tear down. Reset the curator flags ")
    NutanixCuratorUtil.restart_curator_with_default_gflags(self.cluster)
    self.info("Disable compression and dedupe")
    cmd = "container edit name=%s enable-compression=false "\
          "fingerprint-on-write=off on-disk-dedup=none " % \
          self.container.container_name
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    return True
  
  def __test_vm_pinning_io_integrity(self):
    """
    Test to run IO Integrity on pinned vdisks
    """
    # Attach 20GB vdisks to UVM to run IO Integrity on.
    self.info("Creating vdisks on UVMs")
    disk_size = (20 * 1024 * 1024 * 1024)
    bytes_to_pin_disk = disk_size // 2
    uvm_devices_map = {}
    for test_uvm in self.uvms:
      # Let us use the first UVM to populate data to use up the SSD space.
      if test_uvm.name() == self.uvms[0].name():
        continue
      result = self.__attach_new_vdisk_and_pin(test_uvm, disk_size,\
                                    bytes_to_pin_disk)
      uvm_devices_map[test_uvm] = [result['disk']]
    
    self.io_manager = IOIntegrityManager(self.cluster, uvm_devices_map,
                                         (disk_size//(1024 * 1024 * 1024)),
                                         self.out_dir,
                                         self.vdisk_util)
    self.info("Start IO Integrity on the pinned vdisks")
    self.io_manager.start_all()
    
    # Fill up 25% of SSD so that curator scan kicks in and down migration starts
    self.info("Attach a disk of size a little more than "
              "max_usable_ssd_capacity and then fill it up with data")
    disk_size = self.max_usable_ssd_capacity + (self.max_usable_ssd_capacity //\
                                                4)
    disk = self.vdisk_util.create_attach_vdisks_to_vm(
            self.uvms[0],
            self.container.container_name,
            num_disks=1,
            size_kb = disk_size //(1024))
    self.check(disk, "Failed to attach a vdisk")
    data_size_to_write_in_mb = (self.max_usable_ssd_capacity//(1024*1024))
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[0], disk[0], data_size_to_write_in_mb)
    
    self.info("Run a curator scan for ILM migrations to happen")
    NutanixCuratorUtil.start_and_wait_till_partial_scans_completed(self.\
                                                                   __curator, 1)
    
    self.check(not self.io_manager.is_failed(), "IO Integrity Failed")
    self.info("Stop IO integrity")
    self.io_manager.stop_all()
    return True    

  def __update_pinning(self, virtualDiskId, bytes_to_pin):
    """
    This function pins 'bytes_to_pin' bytes of vdisk with uuid 'vmid' and
    verifies if the pinning was successful. 
    Arguments:
      virtualDiskId: The uuid of the vdisk
      bytes_to_pin: Number of bytes to pin
    """
    
    bytes_to_pin_in_G = float(bytes_to_pin)/(1024*1024*1024)
    # Pinned space is precisioned to 2 decimal places. 
    if bytes_to_pin > 0:
      bytes_to_pin_in_G = "%.2f" % bytes_to_pin_in_G
    self.info("Pinning GiB %s" % bytes_to_pin_in_G)
    cmd = "virtual-disk update-pinning id=%s pinned-space=%s \
          tier-name=SSD-SATA" % (virtualDiskId,bytes_to_pin_in_G)
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    
    self.info("Verify the bytes are pinned successfully")
    cmd = "virtual-disk list id=%s" % virtualDiskId
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    if bytes_to_pin > 0:
      bytes_pinned_in_G = float(result['data']['virtualDiskTierParams'][0]\
                                ['pinnedBytes']) / (1024*1024*1024)
      bytes_pinned_in_G = "%.2f" % bytes_pinned_in_G
      self.check_eq(bytes_pinned_in_G, bytes_to_pin_in_G, "Pinned is not equal\
                    to the expected")
    else:
      self.check_eq(len(result['data']['virtualDiskTierParams']), 0)

    return True

  def __attach_new_vdisk_and_pin(self, uvm, disk_size, bytes_to_pin):
    '''
    This function creates a new vdisk, attaches it to the UVM and then pins
    bytes_to_pin bytes of this new vdisk
    Arguments:
    uvm: UVM object
    disk_size: size of the disk to be created in bytes
    bytes_to_pin: Bytes to be pinned
    '''
    cmd = "virtualmachine list name=%s" % uvm.name()
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    current = {}
    for vdiskid in result['data'][0]['nutanixVirtualDiskIds']:
      current[vdiskid] = 1

    disk = self.vdisk_util.create_attach_vdisks_to_vm(
            uvm,
            self.container.container_name,
            num_disks=1,
            size_kb = (disk_size//1024))
    self.check(disk, "Failed to attach a vdisk")
    
    # Sleep for 30 seconds for the disk to shown up through ncli
    time.sleep(30)
    cmd = "virtualmachine list name=%s" % uvm.name()
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    self.check_lt(len(current), \
                  len(result['data'][0]['nutanixVirtualDiskIds']),\
                  " The attached vdisk did not show up in the ncli")
    for vdiskid in result['data'][0]['nutanixVirtualDiskIds']:
      if vdiskid not in current:
        vdisk_id = vdiskid
        self.__update_pinning(vdiskid, bytes_to_pin)
    
    ret = {'disk' : disk[0], 'virtual_disk' : vdisk_id}
    return ret
    
  def __fill_disk(self, uvm, disk, data_size_in_mb):
    '''
    This function fills the given 'disk' of the given 'uvm' with data worth
    'data_size_in_mb' MB
    Arguments:
      uvm: The UVM object on which the disk is mounted on
      disk: The disk attached to the UVM where data has to be written
      data_size_in_mb: Amount of data to be written to the disk
    '''
    
     
    self.__prepare_disk(uvm, disk)
    
    # Copy runio.py tool to UVM. This tool is fast. How fast ? It writes 1 GB
    # worth of data in ~9 seconds.
    io_tool_local_path = os.path.join(top_dir(),
                                      "qa/tools/stargate_tools/runio.py")
    
    results = NutanixClusterSSHUtil.rsync_dir_from_files([uvm],
                                               [io_tool_local_path],
                                               self.__mnt_point, user="root")
    self.check(results, "Copying the runio.py tool using rsync failed")
    
    io_tool = self.__mnt_point + "/runio.py"
    cmd = "sudo python %s %s %s %s" % (io_tool, data_size_in_mb, 1,\
                                      self.__mnt_point)
    results = NutanixClusterSSHUtil.execute([uvm], cmd)
    self.check_eq(results[0][0], 0, "runio tool failed")
    return True
    
  def __prepare_disk(self, uvm, disk):
    '''
    This funtion formats the vdisk 'disk' attached to the UVM 'uvm' and mounts
    it so that IO can be performed on it
    Arguments:
      uvm: UVM object
      disk: The vdisk attached to the UVM
    '''
    
    cmd = "sudo /sbin/mkfs.ext4 -F %s" % disk
    results = NutanixClusterSSHUtil.execute([uvm], cmd)
    self.check_eq(results[0][0], 0, "Command mkfs failed")
    self.__mnt_point = uvm.test_dir()+"/mount"
    cmd = "sudo mkdir -p %s" % self.__mnt_point
    results = NutanixClusterSSHUtil.execute([uvm], cmd)
    self.check_eq(results[0][0], 0, "Command mkdir failed")
    cmd = "sudo mount %s %s " % (disk, self.__mnt_point)
    results = NutanixClusterSSHUtil.execute([uvm], cmd)
    self.check_eq(results[0][0], 0, "Command mount failed")
    return True
  
  def __verify_pinned_bytes(self, vdiskid, bytes_pinned):
    '''
    This function verifies if the vdisk with id "vmid" has 'bytes_pinned' bytes
    residing in the SSD tier.
    Arguments:
    vdiskid: UUID of the vdisk
    bytes_pinned: Amount of bytes pinned
    '''
    vdisk_size_stats = self.__stargate_util.get_vdisk_size_stats(vdiskid)
    self.check(vdisk_size_stats, "Could not get stats for the vdisk: %s" \
               % vdiskid)
    for tier in vdisk_size_stats.tier_usage_list:
            if tier.tier_name == 'SSD-SATA' :
                ssd_usage = tier.usage_bytes
    
    # Account for RF
    bytes_pinned *= self.container.params.replication_factor
    self.info("Pinned bytes: %s  Bytes in SSD: %s" % (bytes_pinned, ssd_usage))
    # Let us keep a 5% error margin
    if ssd_usage < (bytes_pinned - (bytes_pinned *.05)):
      self.fatal("The ssd usage(%s) is less than the bytes pinned(%s)" \
                 % (ssd_usage, bytes_pinned))
    return True

  def __get_vdiskid_from_dev(self, uvm, dev):
    '''
    This function returns the vdisk id corresponding to the disk mounted on
    the given UVM.
    Arguments:
    uvm: UVM object
    dev: device where vdisk is mapped
    '''
    #Get the vdisk name
    dev_to_vdisk_map = self.vdisk_util.get_vm_dev_to_properties_map(uvm)
    self.check(dev_to_vdisk_map)
    for key in dev_to_vdisk_map.keys():
      if(key == dev):    
        test_vdisk_name = dev_to_vdisk_map[key]['vdisk_name']
        break
    
    # From the vdisk name get the vdisk id from Pithos
    vdisk_iterator = self.cluster.create_vdisk_iterator()
    found = False
    for v in vdisk_iterator:
      if v.vdisk_name == test_vdisk_name:
        vdisk_id = v.vdisk_id
        found = True
        break
    self.check(found, "No pithos entry for vdisk name %s" % test_vdisk_name)
    return vdisk_id
    