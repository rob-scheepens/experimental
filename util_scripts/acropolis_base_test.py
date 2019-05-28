#
# Copyright (c) 2014 Nutanix Inc. All rights reserved.
#
# Author: bharath.yarlaga@nutanix.com
#
# Parent class of all the Acropolis workflow tests.

from functools import partial
import time

from qa.agave.agave_util import AgaveUtil
from qa.agave.nutanix_test import NutanixTest
from qa.agave.nutanix_test_resources import NutanixTestResourcesSpec
from qa.agave.nutanix_test_uvms_spec import NutanixTestUvmsSpecCreator
from qa.agave.acropolis_rest_util import AcropolisRestClient
from qa.agave.rest_util import PrismRestClient
from util.base.log import *
from qa.util.agave_tools.nutanix_decorators import subtest

FOUR_KB = 4096
ONE_KB = 1024

class AcropolisBaseTest(NutanixTest):

  @staticmethod
  def uvms_spec():
    creator = NutanixTestUvmsSpecCreator()
    spec = creator.create_hypervisor_uvms_spec(1, 0)
    return spec

  @staticmethod
  def resources_spec():
    spec = NutanixTestResourcesSpec()
    spec.set_cluster_types([ NutanixTestResourcesSpec.KVM_CLUSTER_TYPE ])
    return spec

  def setup(self):
    # Cleanup the UVMs created during this test.
    self.to_be_deleted_in_teardown = []
    self.rest_cli = AcropolisRestClient(self.cluster.svms()[0].ip())

    # If for some reason the host maintenance test fails it blocks other
    # tests from running.
    for host in self.rest_cli.get_host_entities():
      self.check(self.rest_cli.host_exit_maintenance_mode(host["uuid"]),
               "Host %s set back to normal mode " % (host["uuid"]))

    container = self.uvms_binding.container_map.values()[0]
    self._container_name = container.container_name

    # Use disk img thats found on ctr created by agave.
    self.img_path = ("/%s/%s/agave/goldimages/Centos64UVM-1.2/"\
                     "CentosUVM-1.2.img" % (self._container_name,
                                            self.cluster.nodes()[0].ip()))


  def teardown(self):

    # If for some reason the host maintenance test fails it blocks other
    # tests from running.
    for host in self.rest_cli.get_host_entities():
      self.check(self.rest_cli.host_exit_maintenance_mode(host["uuid"]),
               "Host %s set back to normal mode " % (host["uuid"]))
    for vmid in self.to_be_deleted_in_teardown:
      try:
        self.rest_cli.vm_power_off(vmid)
      except:
        WARNING("Failed to power off VM with UUID %s" % vmid)
      try:
        self.rest_cli.vm_delete(vmid)
      except:
        WARNING("Failed to delete VM wih UUID %s" % vmid)

#--------------------------------------------------------------
# Utility methods
#--------------------------------------------------------------

  def vm_create(self, vm_name=("vm_%d_vm_lifecycle" % time.time())):
    net_id = self.rest_cli.get_valid_network_uuid()
    vm_disk = self.rest_cli.construct_vm_disk_proto(
      vmdiskclone_imagepath=self.img_path,
      diskAddress_deviceBus="SCSI",
      diskAddress_deviceIndex=0)

    # Create a functional VM
    vm_id = self.rest_cli.vm_create(
        name=vm_name,
        num_vcpus=1,
        memory_mb=FOUR_KB,
        vm_nics_network_uuid=net_id,
        vmDisks=vm_disk["disks"])

    return vm_name, vm_id

  def vm_clone(self, vmid, vm_clone_name=("vm_%d_clone" % time.time())):
    """
      Test vm_clone workflow
    """
    nic_proto = self.rest_cli.construct_nic_proto(
      networkUuid=self.rest_cli.get_valid_network_uuid())
    INFO("About to clone vm %s." % vmid)
    status, result = self.rest_cli.vm_clone(
      vmid=vmid,
      name=vm_clone_name,
      vmNics=[nic_proto])
    CHECK(status)
    self.to_be_deleted_in_teardown.append(vmid)
    INFO("Successfully cloned vm %s." % vmid)
    INFO("Name of the clone is %s." % vm_clone_name)
    cloned_vm_id, cloned_vm = self.rest_cli.get_vm_from_arithmos(vm_clone_name)
    return cloned_vm_id, vm_clone_name

  def vm_on(self, vmid, vmname):
    # power ON
    self.rest_cli.vm_power_on(vmid)
    AgaveUtil.wait_for(partial(self._vm_state_is, vmname, "on"),\
                               "vm to become on", 300)

  def vm_snapshot(self, vmid, snap_name="snapshot_test_1_%d" % time.time()):
    # 1) Create snapshot of a VM - snap_name, snap_uuid
    vm_logical_timstamp = self.rest_cli.get_vm(vmid)["logicalTimestamp"]
    task_status, task_response = self.rest_cli.vm_snapshot_create(
      vmLogicalTimestamp=vm_logical_timstamp,
      vmUuid=vmid,
      snapshotName=snap_name)
    snap_uuid = [et["uuid"] for et in task_response["taskInfo"]["entityList"]\
                 if 'Snapshot' in et.values()][0]
    INFO("Successfully created snapshot with uuid %s " % snap_uuid)

    return snap_uuid, snap_name

  def _vm_has_ip(self, vm_name):
    vmid, vm = self.rest_cli.get_vm_from_arithmos(vm_name)
    return len(vm["ipAddresses"]) > 0

  def _vm_state_is(self, vm_name, expected_state):
    vmid, vm = self.rest_cli.get_vm_from_arithmos(vm_name)
    DEBUG(self.rest_cli.get_vm(vmid))
    return (self.rest_cli.get_vm(vmid)["state"] == expected_state and\
            vm["powerState"] == expected_state)

  def ping_guest_vm(self, vm_name):
    # Verify IP pingable
    AgaveUtil.wait_for(partial(self._vm_has_ip, vm_name),\
                               "ip to be assigned to vm", 300)

    vmid, vm = self.rest_cli.get_vm_from_arithmos(vm_name)
    DEBUG("VM ip address is %s" % vm["ipAddresses"][0])
    vmip = vm["ipAddresses"][0]
    cmd = "ping -c 5 %s;exit ${PIPESTATUS[0]}" % vmip
    rv, stdout, stderr = self.cluster.svms()[0].execute(cmd, timeout_secs=60)
    INFO("On SVM %s, the output of %s is :" %\
         (self.cluster.svms()[0].ip(),cmd))
    INFO("STDOUT: %s " % stdout)
    INFO("STDERR: %s " % stderr)
    if rv != 0:
      FATAL("Could not ping the new VM")
    INFO("Successfully pinged vm %s" % vmid)
