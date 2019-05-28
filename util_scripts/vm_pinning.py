#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Author: gokul.kannan@nutanix.com
#
# Tests for VM Pinning feature
import math
import os
import random
import time
import pprint
import uuid

from dr_tests.dr_test_lib import DRTestLib
from illuminati import perf_util
from qa.agave.agave_util import AgaveUtil
from qa.agave.nutanix_cluster_ssh_util import NutanixClusterSSHUtil
from qa.agave.nutanix_test import NutanixTest
from qa.agave.nutanix_test_uvms_spec import NutanixTestUvmsSpecCreator
from qa.util.agave_tools.erasure_util import ErasureUtil
from qa.util.agave_tools.io_integrity_manager import IOIntegrityManager
from qa.util.agave_tools.nutanix_curator_util import NutanixCuratorUtil
from qa.util.agave_tools.nutanix_stargate_util import NutanixStargateUtil
from qa.util.agave_tools.nutanix_curator_test_client \
                                          import NutanixCuratorTestClient
from qa.util.agave_tools.nutanix_arithmos_test_client \
                                          import NutanixArithmosTestClient
from qa.util.agave_tools.nutanix_host_vmotion_util import HostVmotion
from qa.util.agave_tools.vdisk_usage_util import VdiskUsageUtil
from qa.util.agave_tools.vm_disk_util import get_vm_disk_util
from qa.util.agave_tools.vmworkflow_util import VMWorkflowUtil
from stats.arithmos.interface.arithmos_type_pb2 import ArithmosEntityProto
from util.base.build import top_dir
from util.nfs.nfs_util import NfsInodeId

GB = (1024 * 1024 * 1024)
MB = (1024 * 1024)

ERASURE_CODE_OPTION = "2/1"

ERASURE_CODE_DELAY = 10

class VM_Pinning(NutanixTest):
  """
  This class implements tests to verify VM Pinning feature. The following
  testcases are implemented:
  - Verify full and partial pinning of vdisk.
  - Verify pinned bytes stay in SSD tier and excess data is downmigrated.
  - Verify pinned bytes are not downmigrated.
  - Verify VM Pinning works on compression enabled containers
  - Verify VM Pinning works on dedupe enabled containers
  - Verify VM Pinning works on EC enabled containers
  """
  # Data population takes its time
  TEST_TIMEOUT = 3600 * 5
  
  VM_PREFIX = "vm_pin_clone"

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
    self.ssd_disks = []
    zeus_config = self.cluster.get_zeus_config()
    for disk in zeus_config.disk_list:
      if disk.storage_tier == "SSD-SATA":
        self.ssd_capacity += disk.disk_size
        self.ssd_disks.append(disk.disk_id)
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
    self.__reset_pinning()
    self.__stargate_util = NutanixStargateUtil(self.cluster)
    self.__curator = NutanixCuratorTestClient(self.cluster)
    self.vdisk_usage_util = VdiskUsageUtil(self.cluster)

    gflags_map = {}
    # Reduce ILM tier usage threshold to 25 for down migration to start sooner.
    gflags_map["curator_tier_usage_ilm_threshold_percent"] = "25"
    self.check_eq(
      NutanixCuratorUtil.restart_curator_with_custom_gflags(
        self.cluster, True, # keep_existing_gflags.
        gflags_map),
      0, "Failed to restart Curator with custom gflags")

    # Is compression/dedupe/EC enabled. By default not enabled.
    self.info("Disable compression, dedupe and EC")
    cmd = "container edit name=%s enable-compression=false "\
          "fingerprint-on-write=off on-disk-dedup=none erasure-code=off" % \
          self.container.container_name
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    self.space_saving_enabled = False
    # Creating Arithmos client to get stats from Arithmos.
    svm_ip = self.cluster.svms()[0].ip()
    self.__arithmos_client = NutanixArithmosTestClient(svm_ip)
    self.__dr = None

  def teardown(self):
    self.info("Teardown: Reset the curator and stargate gflags. Reset pinning."
              "Reset all config to default")
    # Remove pinning.
    self.__reset_pinning()
    if self.space_saving_enabled:
      self.info("Disable compression, dedupe and EC")
      cmd = "container edit name=%s enable-compression=false "\
            "fingerprint-on-write=off on-disk-dedup=none erasure-code=off" % \
            self.container.container_name
      result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    NutanixCuratorUtil.restart_curator_with_default_gflags(self.cluster)
    self.cluster.restart_service_with_default_gflags("stargate")
    
    if self.__dr:
      # Delete the snapshots and then the pd.
      snapshots = self.__dr.pd_list_snapshots(self.cluster, self.__pd_name)
      self.__dr.pd_delete_snapshots(self.cluster, self.__pd_name, snapshots)
      self.__dr.pd_delete(self.cluster, self.__pd_name)

    return True

  def test_basic_pinning_full_partial(self):
    """
    Testcase to verify full and partial pinning of vdisk is successful. It also
    verifies if the complete SSD capacity available for pinning can be pinned.
    """
    self.info("Starting the test")
    cmd = "virtual-disk list"
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    self.info("Pin the first VM")
    for virtual_disk in result['data']:
      vmid = virtual_disk['virtualDiskId']
      vmname = virtual_disk['attachedVMName']
      vmsize = virtual_disk['diskCapacityInBytes']
      # Check if the virtual-disk is part of a VM. Only such disks can be
      # pinned.
      if(vmname and vmsize):
        break
    # Pin the vdisk partially.
    if vmsize <= self.max_usable_ssd_capacity:
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
      # Only vdisks of VM can be pinned.
      if (vdisk['attachedVMName'] and vdisk['diskCapacityInBytes']):
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
    Testcase to verify the data pinned stays in SSD and runtime increase and 
    decrease of pinned bytes.
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
    data_size_to_write_in_mb = (self.max_usable_ssd_capacity//MB) +\
                              1024
    self.info("Writing %s MB data to the disk" %data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[0], disk, data_size_to_write_in_mb)

    # There might be few bytes lying in the oplog.
    # Drain the oplog and check if bytes pinned are present in SSD.
    svm_ips = [svm.ip() for svm in self.cluster.svms()]
    perf_util.wait_for_oplog_drain(svm_ips)
    test_vdisk_id1 = self.__get_vdiskid_from_dev(self.uvms[0], disk)
    self.__verify_pinned_bytes(test_vdisk_id1, self.max_usable_ssd_capacity)

    # Reduce the pinning on current vdisk by 20% and pin another vdisk.
    # Current vdisk's data should be downmigrated.
    self.info("Reduce the pinning on the current pinned vdisk and pin another"
              " disk")
    new_bytes_to_pin = self.max_usable_ssd_capacity - \
                       int(0.2 * self.max_usable_ssd_capacity)
    self.__update_pinning(virtualdisk_id, new_bytes_to_pin)

    # Create a disk of size 50% of max bytes available for pinning and pin 20%
    # max bytes available for pinning.
    disk_size = int(0.5 * self.max_usable_ssd_capacity)
    bytes_to_pin = int(0.2 * self.max_usable_ssd_capacity)
    result = self.__attach_new_vdisk_and_pin(self.uvms[1], disk_size,\
                                             bytes_to_pin)

    # Write data to the new disk so that the data from the old disk gets down
    # migrated to HDD
    data_size_to_write_in_mb = (bytes_to_pin//MB) + 100
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[1], result['disk'], data_size_to_write_in_mb)

    # Drain the oplog and check if bytes pinned are present in SSD.
    perf_util.wait_for_oplog_drain(svm_ips)
    test_vdisk_id2 = self.__get_vdiskid_from_dev(self.uvms[1], result['disk'])
    self.__verify_pinned_bytes(test_vdisk_id2, bytes_to_pin)
    self.__verify_pinned_bytes(test_vdisk_id1, new_bytes_to_pin)

    # Start a curator scan and verify pinned bytes are still present in the
    # SSD.
    self.info("Start a curator scan")
    NutanixCuratorUtil.start_and_wait_till_partial_scans_completed(self.\
                                                                   __curator, 1)
    self.__verify_pinned_bytes(test_vdisk_id2, bytes_to_pin)
    self.__verify_pinned_bytes(test_vdisk_id1, new_bytes_to_pin)

    self.info("Increase and decrease the pinned bytes of the two pinned vdisks")
    # Decrease pinning of first disk by 10%.
    new_bytes_to_pin = new_bytes_to_pin - int(0.1 * new_bytes_to_pin)
    self.__update_pinning(virtualdisk_id, new_bytes_to_pin)
    # Increase pinning of second disk by 10%.
    bytes_to_pin = bytes_to_pin + int(0.1 * new_bytes_to_pin)
    self.__update_pinning(result['virtual_disk'], bytes_to_pin)

    # Write data to the second disk so the increase in pinning can be verified.
    data_size_to_write_in_mb = (bytes_to_pin//MB) + 100
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[1], result['disk'], data_size_to_write_in_mb, \
                     prepare_disk = False)

    # Drain the oplog and check if bytes pinned are present in SSD.
    perf_util.wait_for_oplog_drain(svm_ips)
    self.__verify_pinned_bytes(test_vdisk_id2, bytes_to_pin)
    self.__verify_pinned_bytes(test_vdisk_id1, new_bytes_to_pin)

    return True

  def test_verify_pinned_not_downmigrated(self):
    """
    Testcase to verify the pinned bytes are not downmigrated. 
    """
    self.__test_verify_pinned_not_downmigrated()

  def test_verify_pinning_with_dedupe(self):
    """
    Test to verify VM Pinning works on dedupe enabled containers.
    """
    # Set the gflags for dedupe.
    stargate_gflag_map = \
        {
          "stargate_fingerprint_threshold_size": "32768",
          "stargate_on_disk_dedup_min_ssd_size_GB": "100",
          "stargate_on_disk_dedup_min_ram_size_GB": "8"
        }

    result = self.cluster.restart_service_with_custom_gflags(
        "stargate", True, stargate_gflag_map)
    cmd = "container edit name=%s fingerprint-on-write=on"\
          " on-disk-dedup=POST_PROCESS" % self.container.container_name
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    self.space_saving_enabled = True
    self.__test_verify_pinned_not_downmigrated(dedupe = True)

  def test_verify_pinning_with_compression(self):
    """
    Test to verify VM Pinning works on compression enabled containers.
    """
    cmd = "container edit name=%s enable-compression=true compression-delay=0"\
          % self.container.container_name
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    self.space_saving_enabled = True
    self.__test_verify_pinned_not_downmigrated(compression = True)

  def test_verify_pinning_with_EC(self):
    """
    Test to verify VM Pinning with EC.
    """
    cmd = "container edit name=%s erasure-code=%s"\
          % (self.container.container_name, ERASURE_CODE_OPTION)
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    # Set the gflag to change the minimum time threshold.
    gflags_map = {}
    gflags_map["curator_erasure_code_threshold_seconds"] = "10"
    self.check_eq(
      NutanixCuratorUtil.restart_curator_with_custom_gflags(
        self.cluster, True, # keep_existing_gflags.
        gflags_map),
      0, "Failed to restart Curator with custom gflags")
    self.space_saving_enabled = True
    self.__erasure_util = ErasureUtil(self.cluster)
    self.__test_verify_pinned_not_downmigrated(erasure_coding = True)

  def test_vm_pinning_io_integrity(self):
    """
    Run IO integrity on pinned VMs with EC, compression and dedupe enabled
    """
    # Set stargate and curator flags to enable dedup and EC.
    stargate_gflag_map = \
        {
          "stargate_fingerprint_threshold_size": "32768",
          "stargate_on_disk_dedup_min_ssd_size_GB": "100",
          "stargate_on_disk_dedup_min_ram_size_GB": "8"
        }
    result = self.cluster.restart_service_with_custom_gflags(
        "stargate", True, stargate_gflag_map)
    cmd = "container edit name=%s fingerprint-on-write=on"\
          " on-disk-dedup=POST_PROCESS erasure-code=%s"\
          " enable-compression=true compression-delay=0" \
          % (self.container.container_name, ERASURE_CODE_OPTION)
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    gflags_map = {}
    gflags_map["curator_erasure_code_threshold_seconds"] = "10"
    self.check_eq(
      NutanixCuratorUtil.restart_curator_with_custom_gflags(
        self.cluster, True, # keep_existing_gflags.
        gflags_map),
      0, "Failed to restart Curator with custom gflags")
    self.space_saving_enabled = True
    self.__test_vm_pinning_io_integrity()
    return True
    
  def test_verify_full_pinning_with_snapshots(self):
    self.__test_verify_pinning_with_snapshots(full_pinning=True)

  def test_verify_partial_pinning_with_snapshots(self):
    self.__test_verify_pinning_with_snapshots()

  def test_verify_pinning_with_clones_and_migration(self):
    
    self.info("Attach a disk to the UVM %s " %self.uvms[0].name())
    disk_size = self.max_usable_ssd_capacity//5
    uvm_devices_map = self.vdisk_util.match_and_attach_vdisks([self.uvms[0]],
                                              self.container.container_name,
                                              (disk_size//GB), True,
                                              1)
    
    self.info("Create clones of the VM.")
    self.vmware_workflow_util = VMWorkflowUtil(self.cluster)
    host_ip_list = [node.ip() for node in self.cluster.nodes()]
    clone_status = self.vmware_workflow_util.clone_vms(
        source_vm_names=[self.uvms[0].name()],
        generated_vm_prefix=self.VM_PREFIX,
        total_vm_count=4,
        linked=True,
        host_ips=host_ip_list,
        timeout_secs_clone_vms=1200)
    
    self.info("Getting vm list")
    self.cluster.reload_vm_inventory()
    cloned_vm_list = self.vmware_workflow_util.get_vms_by_name_prefix(
      self.VM_PREFIX)
    virtualdisk_ids = []
    vdisk_ids = []
    uvms_map = {}
    for vm in cloned_vm_list:
      self.info("Getting dev for vm %s and ip %s" % (vm.name(), vm.ip()))
      dev, vdisk_id = self.__get_vdiskid_from_dev(vm, require_dev = True)
      virtualdisk_id = self.__get_vdisk_uuid_from_vdisk_id(vdisk_id)
      virtualdisk_ids.append(virtualdisk_id)
      uvms_map[vm] = {'dev' : dev, 'vdisk_id' : vdisk_id,
                      'virtualdisk_id' : virtualdisk_id}
    self.info("Pin the cloned VMs")
    for virtualdisk_id in virtualdisk_ids:
      self.__update_pinning(virtualdisk_id, -1)
    self.info("Partial pin the VMs")
    bytes_to_pin = disk_size//2
    for virtualdisk_id in virtualdisk_ids:
      self.__update_pinning(virtualdisk_id, bytes_to_pin)

    self.info("Write data to the pinned VMs and verify they remain in SSD ")
    data_size_to_write_in_mb = bytes_to_pin//MB
    for vm in cloned_vm_list:
      for entry in uvms_map[vm]:
        self.__fill_disk(vm, entry['dev'], data_size_to_write_in_mb)
    # Fill up the SSD by writing to some other non pinned disk.
    self.info("Attach a disk of size a little more than "
              "max_usable_ssd_capacity and then fill it up with data.")
    disk_size = self.max_usable_ssd_capacity + (self.max_usable_ssd_capacity //\
                                                4)
    uvm_devices_map = self.vdisk_util.match_and_attach_vdisks([self.uvms[1]],
                                              self.container.container_name,
                                              (disk_size//GB), True,
                                              1)
    disk = uvm_devices_map[self.uvms[1]]
    self.check(disk, "Failed to attach a vdisk.")
    data_size_to_write_in_mb = (self.max_usable_ssd_capacity//MB)
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[1], disk[0], data_size_to_write_in_mb)
    # Run a full curator scan.
    NutanixCuratorUtil.start_and_wait_till_full_scans_completed(
              self.__curator, 1)
    # Verify data of pinned disks remain in SSD.
    for virtualdisk in virtualdisk_ids:
      self.__verify_pinned_bytes(virtualdisk, bytes_to_pin)
    
    self.info("Migrate one of the pinned VM")
    uvm = random.choice.cloned_vm_list()
    target_host = random.choice(self.cluster.nodes())
    while True:
      target_host = random.choice(self.cluster.nodes())
      if (target_host.ip() != uvm.node().ip()):
        break
    host_vmotion = HostVmotion(self.cluster)
    self.info("Migrating UVM with ip %s to host %s" % (uvm.ip(), \
                                                       target_host.ip()))
    result = host_vmotion.host_vmotion(uvm, target_host)
    if not result:
      self.fatal("Vmotion failed with error %s " % result)
    # Verify the pinned bytes stay in SSD and pinning config is intact.
    for virtualdisk in virtualdisk_ids:
      self.__verify_vdisk_is_pinned(virtualdisk, bytes_to_pin)
      self.__verify_pinned_bytes(virtualdisk, bytes_to_pin)
    return True

  def __test_verify_pinning_with_snapshots(self, full_pinning = False):
    """
    Test to verify if vm pinning works with snapshots. This test pins a vdisk,
    writes data to it, takes few snapshots and verifies that pinned bytes stay
    in the SSD tier. This test also verifies if snapshot restore resets the
    pinning config.
    """
    # Due to some temporary issue in agave taking care of 
    #cmd = "sudo service iptables stop"
    #results = NutanixClusterSSHUtil.execute(self.cluster.svms(), cmd)
    #cmd = "sudo chkconfig iptables off"
    #results = NutanixClusterSSHUtil.execute(self.cluster.svms(), cmd)
    self.info("Attach a disk of size of 30% of max_usable_ssd_capacity"
              "to a UVM and then pin it.")
    disk_size = int(self.max_usable_ssd_capacity * 0.3)
    vdiskids = []
    if full_pinning:
      # Precision errors may arise. Hence pin the vdisk with the size reported
      # through ncli.
      result = self.__attach_new_vdisk_and_pin(self.uvms[0], disk_size, -1)
      bytes_to_pin_disk1 = result['pinned_bytes']
    else:
      bytes_to_pin_disk1 = disk_size//2
      result = self.__attach_new_vdisk_and_pin(self.uvms[0], disk_size, \
                                               bytes_to_pin_disk1)

    pinned_disk = result['disk']
    pinned_virtualdisk_id = result['virtual_disk']

    self.info("Write data to the pinned disk and verify data is in SSD.")
    # Write data worth half of the disk size.
    data_size_to_write_in_mb = disk_size//(2 * MB)
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[0], pinned_disk, data_size_to_write_in_mb)
    
    # Drain the oplog so that all data gets flushed to disk.
    svm_ips = [svm.ip() for svm in self.cluster.svms()]
    perf_util.wait_for_oplog_drain(svm_ips)
    # Verify if data is present in SSD.
    pinned_vdisk_id1 = self.__get_vdiskid_from_dev(self.uvms[0], pinned_disk)
    vdiskids.append(pinned_vdisk_id1)
    self.__verify_pinned_bytes(pinned_vdisk_id1, bytes_to_pin_disk1)

    self.info("Take a snapshot of the VM")
    # Create dr lib object, create pd and take a snapshot
    self.__pd_name = "pd_vm_pinning"
    self.__dr = DRTestLib()
    self.__dr.pd_create(self.cluster, self.__pd_name)
    self.__dr.pd_protect(self.cluster, self.__pd_name, vms=[self.uvms[0]])
    self.__dr.pd_add_one_time_snapshot(self.cluster, self.__pd_name)
    pinned_vdisk_id1 = self.__get_vdiskid_from_dev(self.uvms[0], pinned_disk)

    # Verify that, collectively, the vdisk in snapshot and the active vdisk
    # both have pinned bytes in SSD.
    vdiskids.append(pinned_vdisk_id1)
    self.__verify_pinned_bytes_snapshot(vdiskids, bytes_to_pin_disk1)
    
    # Write some more data to the first disk.
    data_size_to_write_in_mb = (disk_size//(4 * MB))
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[0], pinned_disk, data_size_to_write_in_mb, \
                     prepare_disk = False)
    
    self.info("Verify that cumulatively across all vdisks in the snapshot chain"
              " pinned bytes stay in SSD tier.")
    self.__verify_pinned_bytes_snapshot(vdiskids, bytes_to_pin_disk1)
    
    self.info("Create a long (8) snapshot chain.")
    # Write some data and then take a snapshot in a loop. 1 snapshots already
    # present.
    count = 0
    # We have 25% free space still left on the vdisk.
    data_size_to_write_in_mb = disk_size//(4 * MB)
    
    # Divide this remaining space across 10 writes.We will perform only write 7
    # but give some leeway so that we don't go beyond the disk size.
    data_size_to_write_in_mb = data_size_to_write_in_mb//10
    while count < 7:
      self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
      self.__fill_disk(self.uvms[0], pinned_disk, data_size_to_write_in_mb, \
                      prepare_disk = False)
      self.__dr.pd_add_one_time_snapshot(self.cluster, self.__pd_name)
      pinned_vdisk = self.__get_vdiskid_from_dev(self.uvms[0], pinned_disk)
      vdiskids.append(pinned_vdisk)
      count+=1
    
    # Fill up 25% of SSD so that curator scan kicks in and down migration
    # starts.
    self.info("Attach a disk of size a little more than "
              "max_usable_ssd_capacity and then fill it up with data.")
    disk_size = self.max_usable_ssd_capacity + (self.max_usable_ssd_capacity //\
                                                4)
    uvm_devices_map = self.vdisk_util.match_and_attach_vdisks([self.uvms[1]],
                                              self.container.container_name,
                                              (disk_size//GB), True, 1)
    disk = uvm_devices_map[self.uvms[1]]
    self.check(disk, "Failed to attach a vdisk.")
    data_size_to_write_in_mb = (self.max_usable_ssd_capacity//MB)
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[1], disk[0], data_size_to_write_in_mb)
    
    # Start two curator full scan and verify pinned bytes are still present
    # in the SSD.
    self.info("Start two curator full scans")
    NutanixCuratorUtil.start_and_wait_till_full_scans_completed(
              self.__curator, 2)
    self.info("Verify that cumulatively across all vdisks in the snapshot chain"
              " pinned bytes stay in SSD tier.")
    self.__verify_pinned_bytes_snapshot(vdiskids, bytes_to_pin_disk1)
    
    self.info("Protect/Unprotect the VM and verify pinning config stays the "
              "same")
    self.__dr.pd_unprotect(self.cluster, self.__pd_name, vms=[self.uvms[0]])
    self.__verify_vdisk_is_pinned(pinned_virtualdisk_id, bytes_to_pin_disk1)
    self.__dr.pd_protect(self.cluster, self.__pd_name, vms=[self.uvms[0]])
    self.__verify_vdisk_is_pinned(pinned_virtualdisk_id, bytes_to_pin_disk1)

    self.info("Restore a snapshot and verify that the pinning config is intact")
    # Pick a random snapshot to restore.
    snapshots = self.__dr.pd_list_snapshots(self.cluster, self.__pd_name)
    snapid = random.choice(snapshots)
    # Remove the old vdisk id from the list.
    vdisk_id = self.__get_vdiskid_from_dev(self.uvms[0], pinned_disk)
    vdiskids.remove(vdisk_id)
    self.__dr.pd_restore(self.cluster, self.__pd_name, snapid)
    vdisk_id = self.__get_vdiskid_from_dev(self.uvms[0], pinned_disk)
    vdiskids.append(vdisk_id)
    pinned_virtualdisk_id = self.__get_vdisk_uuid_from_vdisk_id(vdisk_id)
    # Verify pinning config is reset.
    self.__verify_vdisk_is_pinned(pinned_virtualdisk_id, 0)

    self.info("Change the pinning config when the VM protected")
    new_bytes_to_pin = int(self.max_usable_ssd_capacity * 0.2)
    self.__update_pinning(pinned_virtualdisk_id, new_bytes_to_pin)

    self.info("Reset the pinning to 0 and then restore a snapshot.")
    self.__update_pinning(pinned_virtualdisk_id, 0)
    snapid = random.choice(snapshots)
    self.__dr.pd_restore(self.cluster, self.__pd_name, snapid)
    vdisk_id = self.__get_vdiskid_from_dev(self.uvms[0], pinned_disk)
    pinned_virtualdisk_id = self.__get_vdisk_uuid_from_vdisk_id(vdisk_id)
    self.__verify_vdisk_is_pinned(pinned_virtualdisk_id, 0)
    return True

  def __test_verify_pinned_not_downmigrated(self, erasure_coding=False,
                                            dedupe = False,
                                            compression = False):
    """
    This function verifies the bytes pinned are not downmigrated when SSD usage
    increases and ILM is triggered. This test also verifies pinned bytes stay
    in SSD when VM is powered off and on.
    """
    # Variable to keep track of bytes down migrated.
    self.down_migrated_bytes = 0
    self.info("Attach a disk of size of 10% of max_usable_ssd_capacity"
              "to a UVM and then pin half of it.")
    disk_size = int(self.max_usable_ssd_capacity * .1)

    bytes_to_pin_disk1 = disk_size//2
    result = self.__attach_new_vdisk_and_pin(self.uvms[0], disk_size,\
                                    bytes_to_pin_disk1)
    disk = result['disk']

    self.info("Write data to the pinned disk and verify data is in SSD.")
    # Write data worth half of the disk size.
    data_size_to_write_in_mb = bytes_to_pin_disk1//MB
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[0], disk, data_size_to_write_in_mb)

    # Drain the oplog so that all data gets flushed to disk.
    svm_ips = [svm.ip() for svm in self.cluster.svms()]
    perf_util.wait_for_oplog_drain(svm_ips)
    # Verify if data is present in SSD.
    pinned_vdisk_id1 = self.__get_vdiskid_from_dev(self.uvms[0], disk)
    self.__verify_pinned_bytes(pinned_vdisk_id1, bytes_to_pin_disk1, \
                               erasure_coding)

    # Fill up 25% of SSD so that curator scan kicks in and down migration
    # starts.
    self.info("Attach a disk of size a little more than "
              "max_usable_ssd_capacity and then fill it up with data.")
    disk_size = self.max_usable_ssd_capacity + (self.max_usable_ssd_capacity //\
                                                4)
    uvm_devices_map = self.vdisk_util.match_and_attach_vdisks([self.uvms[1]],
                                              self.container.container_name,
                                              (disk_size//GB), True,
                                              1)
    disk = uvm_devices_map[self.uvms[1]]
    self.check(disk, "Failed to attach a vdisk.")
    data_size_to_write_in_mb = (self.max_usable_ssd_capacity//MB)
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[1], disk[0], data_size_to_write_in_mb)

    self.info("Verify the already pinned bytes are not downmigrated.")
    self.__verify_pinned_bytes(pinned_vdisk_id1, bytes_to_pin_disk1, \
                               erasure_coding)

    self.info("Attach a vdisk to another UVM and pin it.")
    disk_size = int(self.max_usable_ssd_capacity * .1)
    bytes_to_pin_disk2 = disk_size//3
    result = self.__attach_new_vdisk_and_pin(self.uvms[2], disk_size,\
                                                       bytes_to_pin_disk2)
    disk = result['disk']
    pinned_vdisk_id2 = self.__get_vdiskid_from_dev(self.uvms[2], disk)

    self.info("Fill the vdisk and verify the data stays in SSD for both the "
              "pinned vdisk.")
    data_size_to_write_in_mb = bytes_to_pin_disk2//MB
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[2], disk, data_size_to_write_in_mb)

    # Drain the oplog and check if bytes pinned are present in SSD.
    perf_util.wait_for_oplog_drain(svm_ips)
    self.__verify_pinned_bytes(pinned_vdisk_id2, bytes_to_pin_disk2, \
                               erasure_coding)
    self.__verify_pinned_bytes(pinned_vdisk_id1, bytes_to_pin_disk1, \
                               erasure_coding)

    self.info("Power off and power on the VMs and verify the bytes are still"
              " pinned.")
    self.cluster.stop_uvms( self.uvms)
    # Start a curator scan.
    if erasure_coding:
      # Run three full curator scans and verify erasure coding happens. 
      NutanixCuratorUtil.wait_till_scan_is_completed(self.__curator)
      self.__erasure_util.ec_start_full_scan_wait_for_job_completion(\
                                                                    num_scans=3)
      pre_reduction, post_reduction = self.__get_container_reduction_stats(\
                                            self.container.container_id,
                                            "erasure")
      self.check_lt(post_reduction, pre_reduction, "No savings after erasure "
                      "coding. Pre-reduction bytes: %s Post-reduction bytes: %s"
                      % (pre_reduction, post_reduction))
    elif dedupe:
      # Run 3 full curator scans for dedupe to happen.
      NutanixCuratorUtil.start_and_wait_till_full_scans_completed(
              self.__curator, 3)
      pre_reduction, post_reduction = self.__get_container_reduction_stats(\
                                            self.container.container_id,
                                            "dedup")
      self.check_lt(post_reduction, pre_reduction, "No savings after dedup "
                      "Pre-reduction bytes: %s Post-reduction bytes: %s"
                      % (pre_reduction, post_reduction))
    elif compression:
      # Run 2 full curator scan for compression to happen.
      NutanixCuratorUtil.start_and_wait_till_full_scans_completed(
              self.__curator, 2)
      pre_reduction, post_reduction = self.__get_container_reduction_stats(\
                                            self.container.container_id,
                                            "compression")
      self.check_lt(post_reduction, pre_reduction, "No savings after "
                      "compression. Pre-reduction bytes: %s Post-reduction "
                      "bytes: %s" % (pre_reduction, post_reduction))
    else:
      NutanixCuratorUtil.start_and_wait_till_partial_scans_completed(self.\
                                                                   __curator, 1)

    # Verify the bytes are present in SSD.
    self.__verify_pinned_bytes(pinned_vdisk_id2, bytes_to_pin_disk2, \
                               erasure_coding)
    self.__verify_pinned_bytes(pinned_vdisk_id1, bytes_to_pin_disk1, \
                               erasure_coding)
    self.cluster.start_uvms( self.uvms)

    # Reset the pinning on all pinned Vdisks.
    self.__reset_pinning()

    # Now try pinning the max possible to verify pinned space is really cleared.
    disk_size = self.max_usable_ssd_capacity + (self.max_usable_ssd_capacity //\
                                                4)
    result = self.__attach_new_vdisk_and_pin(self.uvms[1], \
                                             disk_size,\
                                             self.max_usable_ssd_capacity)

    return True

  def __test_vm_pinning_io_integrity(self):
    """
    Test to run IO Integrity on pinned vdisks
    """
    # Attach 20GB vdisks to UVM to run IO Integrity on.
    self.info("Creating vdisks on UVMs")
    disk_size = 20 * GB
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
                                         (disk_size//GB),
                                         self.out_dir,
                                         self.vdisk_util)
    self.info("Start IO Integrity on the pinned vdisks.")
    self.io_manager.start_all()

    # Fill up 25% of SSD so that curator scan kicks in and down migration starts
    self.info("Attach a disk of size a little more than "
              "max_usable_ssd_capacity and then fill it up with data.")
    disk_size = self.max_usable_ssd_capacity + (self.max_usable_ssd_capacity //\
                                                4)
    devices_map = self.vdisk_util.match_and_attach_vdisks([self.uvms[0]],
                                              self.container.container_name,
                                              (disk_size//GB), True,
                                              1)
    self.check(devices_map, "Failed to attach a vdisk.")
    disk = devices_map[self.uvms[0]]

    data_size_to_write_in_mb = self.max_usable_ssd_capacity//MB
    self.info("Writing %s MB data to the disk" % data_size_to_write_in_mb)
    self.__fill_disk(self.uvms[0], disk[0], data_size_to_write_in_mb)

    self.info("Run a curator scan for ILM migrations to happen.")
    NutanixCuratorUtil.start_and_wait_till_partial_scans_completed(self.\
                                                                   __curator, 1)

    self.check(not self.io_manager.is_failed(), "IO Integrity Failed")
    self.info("Stop IO integrity.")
    self.io_manager.stop_all()
    return True    

  def __reset_pinning(self):
    """
    Reset the pinning to 0 on all pinned vdisks.
    """
    cmd = "virtual-disk list"
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    for vdisk in result['data']:
      if vdisk['virtualDiskTierParams'] is not None and \
      len(vdisk['virtualDiskTierParams']):
        self.__update_pinning(vdisk['virtualDiskId'], 0)

  def __update_pinning(self, virtualDiskId, bytes_to_pin):
    """
    This function pins 'bytes_to_pin' bytes of vdisk with uuid 'vmid' and
    verifies if the pinning was successful. 
    Arguments:
      virtualDiskId: The uuid of the vdisk
      bytes_to_pin: Number of bytes to pin. -1 to pin the whole disk.
    """
    if bytes_to_pin < 0:
      # Full pinning case.
      cmd = "virtual-disk list id=%s" % virtualDiskId
      res = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
      bytes_to_pin = res['data']['diskCapacityInBytes']

    bytes_to_pin_in_G = float(bytes_to_pin)/GB
    # Pinned space is precisioned to 2 decimal places. But it isn't rounded off.
    # If the bytes_to_pin_in_G is 20.26845632345 then 20.27 does not work. 20.26
    # works. So we can't use %.2f.
    if bytes_to_pin > 0:
      bytes_to_pin_in_G = math.floor(bytes_to_pin_in_G * 10 ** 2) / 10 ** 2
    self.info("Pinning GiB %s" % bytes_to_pin_in_G)
    cmd = "virtual-disk update-pinning id=%s pinned-space=%s \
          tier-name=SSD-SATA" % (virtualDiskId,bytes_to_pin_in_G)
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)

    self.info("Verify the bytes are pinned successfully.")
    self.__verify_vdisk_is_pinned(virtualDiskId, bytes_to_pin)
    return True

  def __attach_new_vdisk_and_pin(self, uvm, disk_size, bytes_to_pin):
    '''
    This function creates a new vdisk, attaches it to the UVM and then pins
    bytes_to_pin bytes of this new vdisk
    Arguments:
    uvm: UVM object
    disk_size: size of the disk to be created in bytes
    bytes_to_pin: Bytes to be pinned. If -1 pin the complete disk.
    Returns a dict with the device and virtual_disk_id of the pinned vdisk
    '''
    cmd = "virtualmachine list name=%s" % uvm.name()
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    current = {}
    for vdiskid in result['data'][0]['nutanixVirtualDiskUuids']:
      current[vdiskid] = 1
    uvm_devices_map = self.vdisk_util.match_and_attach_vdisks([uvm],
                                              self.container.container_name,
                                              (disk_size//GB), True,
                                              1)
    self.check(uvm_devices_map, "Failed to attach a vdisk")
    disk = uvm_devices_map[uvm]

    # Start polling till the new virtual disk shows up in ncli. Wait max 100s.   
    count = 0
    vdisk_id = 0
    cmd = "virtualmachine list name=%s" % uvm.name()
    while count < 5:
      result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
      for vdiskid in result['data'][0]['nutanixVirtualDiskUuids']:
        if vdiskid not in current:
          vdisk_id = vdiskid
          if bytes_to_pin < 0:
            cmd = "virtual-disk list id=%s" % vdiskid
            res = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
            bytes_to_pin = res['data']['diskCapacityInBytes']
          self.__update_pinning(vdiskid, bytes_to_pin)
      if vdisk_id:
        break
      time.sleep(20)
      count += 1

    self.check(vdisk_id, "The new disk did not show up in ncli.")
    ret = {'disk' : disk[0], 'virtual_disk' : vdisk_id, 'pinned_bytes' : \
          bytes_to_pin}
    return ret

  def __fill_disk(self, uvm, disk, data_size_in_mb, prepare_disk=True):
    """
    This function fills the given 'disk' of the given 'uvm' with data worth
    'data_size_in_mb' MB
    Arguments:
      uvm: The UVM object on which the disk is mounted on
      disk: The disk attached to the UVM where data has to be written
      data_size_in_mb: Amount of data to be written to the disk
    """
    if prepare_disk:
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
    """
    This funtion formats the vdisk 'disk' attached to the UVM 'uvm' and mounts
    it so that IO can be performed on it
    Arguments:
      uvm: UVM object
      disk: The vdisk attached to the UVM
    """
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

  def __verify_pinned_bytes(self, vdiskid, bytes_pinned, erasure_code = False):
    """
    This function verifies if the vdisk with id "vdiskid" has 'bytes_pinned' bytes
    residing in the SSD tier.
    Arguments:
    vdiskid: UUID of the vdisk
    bytes_pinned: Amount of bytes pinned
    """
    vdisk_size_stats = self.__stargate_util.get_vdisk_size_stats(vdiskid)
    self.check(vdisk_size_stats, "Could not get stats for the vdisk: %s" \
               % vdiskid)
    hdd_usage = 0
    for tier in vdisk_size_stats.tier_usage_list:
            if tier.tier_name == 'SSD-SATA' :
              ssd_usage = tier.usage_bytes
            elif tier.tier_name == 'DAS-SATA':
              hdd_usage = tier.usage_bytes

    # Account for RF
    bytes_pinned *= self.container.params.replication_factor
    self.info("Pinned bytes: %s  Bytes in SSD: %s" % (bytes_pinned, ssd_usage))
    self.info("Bytes in HDD: %s vdisk id : %s" % (hdd_usage, vdiskid))
    # Check if the bytes in ssd is greater than or equal to the pinned bytes.
    # If ssd usage is less, then check if data is present in hdd.
    # Let us keep a 5% error margin.
    
    if (ssd_usage < (bytes_pinned - (bytes_pinned *.05))) and not \
                    self.space_saving_enabled and hdd_usage:
      self.fatal("The ssd usage(%s) is less than the bytes pinned(%s) and there\
                  is data in HDD(%s)" % (ssd_usage, bytes_pinned, hdd_usage))
    
    # If we have enabled dedupe or compression or EC we might have much less
    # physical data than what was written to disk. This gives rise to two
    # scenarios:
    # i) Till the pinned space in SSD is not fully used, nothing should be
    #    down migrated to HDD.
    # ii) Condition (i) may not hold true if data was already down migrated to
    #     HDD because pinned space in SSD was used up and then curator scan
    #     kicked in and reduced the bytes by either compression, dedupe or
    #     erasure coding.
    # iii) In case of erasure coding, the parity egroups always go to HDD.
    # Take into account the above conditions while checking.

    elif (ssd_usage < (bytes_pinned - (bytes_pinned *.05))) and \
          self.space_saving_enabled:
      if(hdd_usage and self.down_migrated_bytes):
        self.info("There was already some data in hdd. So this is fine.")
      elif(hdd_usage):
        if(erasure_code):
          # Here we have to make sure all the active egroups of the vdisk are
          # present in SSD and the ones present in HDD are only the parity
          # egroups. This is a time consuming operation.

          egid_disk_id_map = self.vdisk_usage_util.get_egroup_id_disk_ids_map\
                             (str(vdiskid))
          for egid, disks in egid_disk_id_map.iteritems():
            if not(set(disks) <= set(self.ssd_disks)):
              self.fatal("This active Egroup is not in SSD: %s" % egid)
        else:
          self.fatal("The ssd usage(%s) is less than the bytes pinned(%s) and "
                     "there is data downmigrated to HDD(%s)" \
                     % (ssd_usage, bytes_pinned, hdd_usage))
    elif (ssd_usage >= (bytes_pinned - (bytes_pinned *.05))) and \
          self.space_saving_enabled:
        self.down_migrated_bytes = hdd_usage
    return True

  def __get_vdiskid_from_dev(self, uvm, dev=None, require_dev = False):
    """
    This function returns the vdisk id corresponding to the disk mounted on
    the given UVM.
    Arguments:
    uvm: UVM object
    dev: device where vdisk is mapped
    """
    #Get the vdisk name
    dev_to_vdisk_map = self.vdisk_util.get_vm_dev_to_properties_map(uvm)
    self.check(dev_to_vdisk_map)
    for key in dev_to_vdisk_map.keys():
      if(key == dev and (dev is not None)):
        test_vdisk_name = NfsInodeId(int128=int(dev_to_vdisk_map[key]\
                                         ['inode'])).vdisk_name()
        self.info("dev is %s and vdisk name is %s " %(dev,test_vdisk_name))
        break
      elif(dev is None and  not(key.startswith("/dev/sda") or \
                             key.startswith("/dev/vda"))):
        test_vdisk_name = NfsInodeId(int128=int(dev_to_vdisk_map[key]\
                                         ['inode'])).vdisk_name()
        self.info("dev is %s and vdisk name is %s " %(dev,test_vdisk_name))
        break

    # From the vdisk name get the vdisk id from Pithos
    vdisk_iterator = self.cluster.create_vdisk_iterator()
    found = False
    for v in vdisk_iterator:
      self.info("The pithos entry: %s and %s " %(v.vdisk_name, v.vdisk_id))
      if v.vdisk_name == test_vdisk_name:
        vdisk_id = v.vdisk_id
        found = True
        break
    self.check(found, "No pithos entry for vdisk name %s" % test_vdisk_name)
    if require_dev:
      return dev, vdisk_id
    else:
      return vdisk_id

  def __get_container_reduction_stats(self, container_id, category):
    """
    This function takes a container ID and category (erasure, dedup, or
    compression) and will grab conatiner stats by making a call to arithmos.
    Method will return pre_reduction_bytes and post_reduction_bytes of the
    given category. Originally implemented in dedup_compression_stats_test.py
    """
    valid_categories = ["erasure", "dedup", "compression"]
    self.check(category in valid_categories, "Category is invalid")
    savings_values = dict()

    # Fetch Arithmos proto to evaluate container pre_reduction and
    # post_reduction bytes.
    container = ArithmosEntityProto.kContainer
    arithmos_proto = self.__arithmos_client.master_get_entities(
                         entity_type=container)
    user_pre_reduction_bytes = 0
    user_post_reduction_bytes = 0
    for ctr in arithmos_proto.response.entity_list.container:
      if int(ctr.id) == container_id:
        self.info("container_id: %s found, " % ctr.id)
        for stats in ctr.stats.generic_stat_list:
          if category in stats.stat_name:
            if "user_pre_reduction_bytes" in stats.stat_name:
              user_pre_reduction_bytes = int(stats.stat_value)
            if "user_post_reduction_bytes" in stats.stat_name:
              user_post_reduction_bytes = int(stats.stat_value)
        return user_pre_reduction_bytes, user_post_reduction_bytes
    self.info("Container not found, returning -1")
    return False
  
  def __verify_pinned_bytes_snapshot(self, vdiskids, bytes_pinned):
    """
    This function verifies if the vdisks with id "vdiskids" has 'bytes_pinned'
    bytes residing in the SSD tier.
    Arguments:
    vdiskid: UUID of the vdisk
    bytes_pinned: Amount of bytes pinned
    """
    # Account for RF
    bytes_pinned *= self.container.params.replication_factor
    ssd_usage = 0
    hdd_usage = 0
    for vdiskid in vdiskids:
      vdisk_size_stats = self.__stargate_util.get_vdisk_size_stats(vdiskid)
      self.check(vdisk_size_stats, "Could not get stats for the vdisk: %s" \
                 % vdiskid)
      for tier in vdisk_size_stats.tier_usage_list:
              if tier.tier_name == 'SSD-SATA' :
                ssd_usage += tier.usage_bytes
                self.info("Pinned bytes: %s  Bytes in SSD for vdiskid: %s : %s"
                % (bytes_pinned, vdiskid, tier.usage_bytes))
              elif tier.tier_name == 'DAS-SATA':
                hdd_usage += tier.usage_bytes
                self.info("Pinned bytes: %s  Bytes in HDD for vdiskid: %s : %s"
                % (bytes_pinned, vdiskid, tier.usage_bytes))

    # Verify if cumulatively across all vdisks the ssd usage is greater than the
    # pinned bytes and no data has downmigrated. Let us keep a 5% error margin.
    if (ssd_usage < (bytes_pinned - (bytes_pinned *.05)) and hdd_usage):
      self.fatal("The ssd usage(%s) is less than the bytes pinned(%s)"
                % (ssd_usage, bytes_pinned))
    
  def __verify_vdisk_is_pinned(self, virtualDiskId, bytes_pinned):
    """
    Verify if pinning is configured on the given vdisk.
    Arguments:
    virtualDiskId: virtual_disk id.
    bytes_pinned: number of bytes pinned.
    """
    bytes_pinned_in_G = float(bytes_pinned)/GB
    if bytes_pinned > 0:
      bytes_pinned_in_G = math.floor(bytes_pinned_in_G * 10 ** 2) / 10 ** 2
    
    cmd = "virtual-disk list id=%s" % virtualDiskId
    result = AgaveUtil.ncli_cmd_with_check(self.cluster, cmd)
    if bytes_pinned > 0:
      actual_bytes_pinned_in_G = float(result['data']['virtualDiskTierParams']\
                                       [0]['pinnedBytes']) / GB
      actual_bytes_pinned_in_G = "%.2f" % actual_bytes_pinned_in_G
      bytes_pinned_in_G = "%.2f" % bytes_pinned_in_G
      self.check_eq(actual_bytes_pinned_in_G, bytes_pinned_in_G, "Pinned is not"
                    " equal to the expected.")
    else:
      self.check_eq(len(result['data']['virtualDiskTierParams']), 0)
      
  def __get_vdisk_uuid_from_vdisk_id(self, vdisk_id):
    """
    For a given vdisk_id return the virtual disk uuid.
    Arg:
    vdisk_id: vdisk id from the pithos.
    Returns the virtual disk uuid.
    """
    # Get the vdisk name from pithos. Use that vdisk name as the key to find
    # virtual disk id from arithmos. Then use the virtual disk id as key to
    # find virtual disk uuid from arithmos virtual disk entity.
    vdisk_name = None
    vdisk_iterator = self.cluster.create_vdisk_iterator()
    for v in vdisk_iterator:
      self.info("vdisk id in pithos is %s " % v.vdisk_id )
      if v.vdisk_id == vdisk_id:
        self.info("found the vdisk id %s " % v.vdisk_id )
        vdisk_name = v.vdisk_name
        break
    self.check(vdisk_name, "Vdisk name not found for vdisk id %s" % vdisk_id)
    arithmos_proto = self.__arithmos_client.master_get_entities(
                             entity_type=ArithmosEntityProto.kVDisk)
    virtual_disk_id = None
    for vdisk in arithmos_proto.response.entity_list.vdisk:
      if vdisk.id == vdisk_name:
        self.info("Vdisk: %s found, " % vdisk.attach_virtual_disk_id)
        virtual_disk_id = vdisk.attach_virtual_disk_id
        break
    self.check(virtual_disk_id, "Virtual disk id not found for vdisk name %s" %
               vdisk_name)
    arithmos_proto = self.__arithmos_client.master_get_entities(
      entity_type=ArithmosEntityProto.kVirtualDisk)
    virtual_disk_uuid = None
    for vdisk in arithmos_proto.response.entity_list.virtual_disk:
      if vdisk.id == virtual_disk_id:
        for attr in vdisk.generic_attribute_list:
          if "virtual_disk_uuid" in attr.attribute_name:
            self.info("uuid : %s" %attr.attribute_value_str)
            virtual_disk_uuid = attr.attribute_value_str
    self.check(virtual_disk_uuid, "Virtual disk uuid not found for vdisk id %s"
               % virtual_disk_id)
    return virtual_disk_uuid
        
  
  def __get_dev_from_vdiskid(self, vm, vdisk_id):
    """
    For a given vdisk id, get the device on the VM.
    Arguments:
    vm : VM to which the vdisk is attached.
    vdisk_id : vdisk id.
    Returns the device on the VM.
    """
    # From the vdisk id get the vdisk name from Pithos
    vdisk_iterator = self.cluster.create_vdisk_iterator()
    found = False
    for v in vdisk_iterator:
      self.info("The pithos entry: %s and %s " %(v.vdisk_name, v.vdisk_id))
      if v.vdisk_id == vdisk_id:
        vdisk_name = v.vdisk_name
        found = True
        break
    self.check(found, "No pithos entry for vdisk id %s" % vdisk_id)
    # Check the
    dev_to_vdisk_map = self.vdisk_util.get_vm_dev_to_properties_map(vm)
    self.check(dev_to_vdisk_map)
    for key in dev_to_vdisk_map.keys():
      test_vdisk_name = NfsInodeId(int128=int(dev_to_vdisk_map[key]\
                                         ['inode'])).vdisk_name()
      if (test_vdisk_name == vdisk_name):
        return key
    self.fatal("Could not find device for vdisk_id %s" % vdisk_id)
      