#
# Copyright (c) 2014 Nutanix Inc. All rights reserved.
#
# Author: kanan@nutanix.com
#
# Agave test for acropolis via rest api.
# Tests vm_lifecycle, snapshot_lifecycle, vm_clone, vm_disk_create.

from functools import partial
import time

from qa.agave.agave_util import AgaveUtil
from acropolis_base_test import AcropolisBaseTest, ONE_KB, FOUR_KB
from qa.agave.nutanix_test_resources import NutanixTestResourcesSpec
from qa.agave.nutanix_test_uvms_spec import NutanixTestUvmsSpecCreator
from qa.agave.acropolis_rest_util import AcropolisRestClient
from util.base.log import *
from qa.util.agave_tools.nutanix_decorators import get_summary, subtest

class AcropolisRestVMTest(AcropolisBaseTest):

  def setup(self):
    AcropolisBaseTest.setup(self)
    self.uvm_for_clone_test = None
    self.uvm_for_snapshot_test = None

    # The uvms created by uvm_specs() are being put to use here.
    for node in self.uvms_binding.uvm_maps():
      for uvm_spec, uvm in self.uvms_binding.uvm_maps()[node].items():
        self.to_be_deleted_in_teardown.append(uvm.uuid())
        if not self.uvm_for_clone_test:
          self.uvm_for_clone_test = uvm
        if not self.uvm_for_snapshot_test:
          self.uvm_for_snapshot_test = uvm

  def test_acropolis_rest_api(self):
    """
      1) vm_lifecycle
      2) vm_disk_create
      3) vm_clone_test
      4) snapshot_lifecycle
    """
    self.vm_lifecycle()
    self.vm_disk_create()
    self.vm_clone_test()
    self.snapshot_lifecycle()
    get_summary(self, num_expected_failures = 0)

  @subtest
  def vm_lifecycle(self):
    """
    Create a VM using a disk img thats on the agave ctr, power it on.
    Verify the VM is pingable.
    Power off and delete.
    """
    vm_name, vm_id = self.vm_create()
    CHECK(vm_id, "Failed to create VM using rest api for acropolis")
    INFO("Successfully created vm %s" % vm_id)
    self.to_be_deleted_in_teardown.append(vm_id)
    # power ON
    if not self._vm_state_is(vm_name, "on"):
      self.rest_cli.vm_power_on(vm_id)

    INFO("Successfully powered ON vm %s" % vm_id)
    self.ping_guest_vm(vm_name)

  @subtest
  def vm_disk_create(self):
    """
     Test vm_disk create using all of the four methods.
     1) size
     2) Clone existing vmdisk
     3) Clone clone_nfs_file
     4) empty
    """
    vm_name = "vm_%d_disk_create" % time.time()
    vmid = self.rest_cli.vm_create(
        name=vm_name,
        num_vcpus=1,
        memory_mb=FOUR_KB)
    self.to_be_deleted_in_teardown.append(vmid)
    INFO("created vm %s" % vm_name)

    # power ON
    if not self._vm_state_is(vm_name, "on"):
      self.rest_cli.vm_power_on(vmid)
    # 1) size
    INFO("About to create vm_disk using size.")
    status, result = self.rest_cli.vm_disk_create(
        vmid=vmid,
        vmdiskcreate_containerName=self._container_name,
        vmdiskcreate_sizemb=ONE_KB,
        diskAddress_deviceBus="SCSI",
        diskAddress_deviceIndex=0)
    INFO(status)
    INFO("Successfully created first vm_disk.")

    # 2) Clone existing vmdisk
    # get_vm, parse output and obtain the vm_disk uuid
    vm_disk1_uuid = \
        self.rest_cli.get_vm(vmid)["config"]["vmDisks"][0]["vmDiskUuid"]
    INFO("About to create vm_disk by cloning exisiting vmdisk %s." %\
          vm_disk1_uuid)
    status, result = self.rest_cli.vm_disk_create(
        vmid=vmid,
        vmdiskclone_minimum_size=ONE_KB,
        vmdiskclone_vmDiskUuid=vm_disk1_uuid,
        diskAddress_deviceBus="SCSI",
        diskAddress_deviceIndex=1)
    CHECK(status)
    INFO("Successfully created second vm_disk.")

    # power OFF
    self.rest_cli.vm_power_off(vmid)

    # Since hot plugin of cdrom is not allowed, power off first
    # 3) Clone clone_nfs_file
    vm_disk3 = self.rest_cli.construct_vm_disk_proto(
        isCdrom="true",
        vmdiskclone_imagepath=self.img_path,
        diskAddress_deviceBus="IDE",
        diskAddress_deviceIndex=0,
        isScsiPassThrough="true")

    # 4) empty
    vm_disk4 = self.rest_cli.construct_vm_disk_proto(
        vmid=vmid,
        isEmpty="true",
        isCdrom="true")

    INFO("About to create vm_disks by cloning nfs file and empty CDROM drive.")
    vm_disk3["disks"].extend(vm_disk4["disks"])
    status, result = self.rest_cli.vm_disk_create(vmid=vmid, disks=vm_disk3["disks"])
    CHECK(status)

    # Power the VM back on.
    self.rest_cli.vm_power_on(vmid)
    INFO("Successfully created third and fourth vm_disk.")
    CHECK_EQ(len(self.rest_cli.get_vm(vmid)["config"]["vmDisks"]), 4)
    INFO("Successfully verified vm has 4 disks.")

  @subtest
  def vm_clone_test(self):
    vmid, vmname = self.vm_clone(self.uvm_for_clone_test.uuid())
    self.vm_on(vmid, vmname)
    self.to_be_deleted_in_teardown.append(vmid)

  @subtest
  def snapshot_lifecycle(self):
    """
    1) Create snapshot of a VM - snap_name, snap_uuid
    2) Verify snap_uuid is shows up in GET snapshots
    3) Verify that GET for a single snapshot shows the snap_name
    4) Verify snap_uuid shows up in the GET snapshot for vm
    5) Create vm clone using the snapshot
    """
    vmid = str(self.uvm_for_snapshot_test.uuid())
    snap_uuid, snap_name = self.vm_snapshot(vmid)

    # 2) Verify snap_uuid is shows up in GET snapshots
    results = self.rest_cli.get_snapshots() #find uuid
    is_snap_uuid_found = False
    for snap in results:
      DEBUG("snap.values() is %s " % str(snap.values()))
      if snap_uuid in snap.values():
        is_snap_uuid_found = True
        break
    CHECK(is_snap_uuid_found,\
          "Could not find uuid of snapshot %s in response of GET snapshots" %\
          snap_name)

    # 3) Verify that GET for a single snapshot shows the snap_name
    result = self.rest_cli.get_snapshot(snap_uuid)
    CHECK(snap_name in result.values())

    # 4) Verify snap_uuid shows up in the GET snapshot for vm
    result = self.rest_cli.get_vm_snapshots(vmid)
    CHECK_EQ(result["parentSnapshotUuid"], snap_uuid)

    # 5) Create vm clone using the snapshot
    vm_from_snapshot_name = "vm_from_%s" % snap_name
    task_status, task_result = self.rest_cli.vm_clone_from_snapshot(
        snapshot_uuid=snap_uuid,
        numVcpus=1, # this parameter is not needed
        memoryMb=256, # this parameter is not needed
        name=vm_from_snapshot_name)
    snapshotted_vmid = None
    entities = task_result["taskInfo"]["entityList"]
    for entity in entities:
      if entity["entityType"] == "Snapshot":
        CHECK_EQ(entity["uuid"], snap_uuid)
      elif  entity["entityType"] == "VM":
        snapshotted_vmid = entity["uuid"]

    # Power on the VM
    self.rest_cli.vm_power_on(snapshotted_vmid)
    self.to_be_deleted_in_teardown.append(snapshotted_vmid)

    CHECK_EQ(vm_from_snapshot_name,
             self.rest_cli.get_vm(snapshotted_vmid)["config"]["name"])
    self.rest_cli.snapshot_delete(snap_uuid)
