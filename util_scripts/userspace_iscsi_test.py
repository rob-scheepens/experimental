#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Author: kanan@nutanix.com
#
# Test for Acropolis Userspace Iscsi.
# Verify iscsi portal initialization to local Stargate.
# Verify iscsi portal failover to neighboring Stargates.
# Verify iscsi portal failback to original local Stargate.
# Verification is done by parsing the iscsi_redictor log
# (/var/log/iscsi_redirector) on the host on which the VM is created.
# Also check the qemu pids have connections to cvm:3261

import re
import time

from qa.agave.nutanix_test import NutanixTest
from qa.agave.nutanix_test_resources import NutanixTestResourcesSpec
from qa.agave.nutanix_test_uvms_spec import NutanixTestUvmsSpecCreator
from qa.agave.acropolis_rest_util import AcropolisRestClient
from qa.agave.rest_util import HOSTS, PrismRestClient

class UserspaceIscsiTest(NutanixTest):
  @staticmethod
  def uvms_spec():
    creator = NutanixTestUvmsSpecCreator()
    spec = creator.create_hypervisor_uvms_spec(0, 0)
    return spec

  @staticmethod
  def resources_spec():
    spec = NutanixTestResourcesSpec()
    spec.set_cluster_types([ NutanixTestResourcesSpec.KVM_CLUSTER_TYPE ])
    return spec

  def teardown(self):
    self.cluster.start()
    self.acropolis_rest_util.vm_delete(self.vmid)

  def setup(self):
    container = self.uvms_binding.container_map.values()[0]
    self.ctr_name = container.container_name

    self.acropolis_rest_util = AcropolisRestClient(self.cluster.svms()[0].ip())
    self.arithmos_rest_util = PrismRestClient(self.cluster.svms()[0].ip())

    self._nuke_iscsi_redirector_logs()

  def test_userspace_iscsi(self):
    """
    1) Create a VM with 4 vmdisks. Note the host, cvm, iscsi targets and
       iscsi portals for these vmdisks. Verify the iscsi portals are pointing
       to the CVM corresponding to the host on which the vmdisks are created.
    2) Stop Stargate on the CVM corresponding to the host of the VM.
       Verify the iscsi portals failover to other CVMs
    3) Restart the Stargate from step 2. Verify the iscsi portals are
       failback to cvm noted in step 1.
    """
    #Step 1)

    # Create and power on a VM with 4 vdisks
    self.vmid = self._create_and_poweron_vm()

    # Obtain the vmdisk ids, host and corresponding CVM on which the
    # VM is created.
    vminfo = self.acropolis_rest_util.get_vm(self.vmid)
    self.vmdisks = self._get_vmdisk_ids(vminfo)
    self.vm_on_host, self.vm_on_svm = self._get_node_and_svm(vminfo)

    # Obtain the list of the other CVMs
    self.other_cmv_ips = \
        [svm.ip() for svm in self.cluster.svms() if svm != self.vm_on_svm]

    # Initial verification
    ir_log = self._cat_iscsi_redirector_logs()
    for vmdisk in self.vmdisks:
      self.check(vmdisk in ir_log,
                 "vmdisk %s was absent from iscsi_redirector log" % vmdisk)
    self.iscsi_portal_map = {} # Initial iscsi target -> portal mapping
    for (iscsi_target, iscsi_portal) in self._ir_log_parser():
      self.info((iscsi_target, iscsi_portal))
      portal_cvm = iscsi_portal.split(":")[0]
      self.check(portal_cvm == self.vm_on_svm.ip())
      self.iscsi_portal_map[iscsi_target] = iscsi_portal
      self.info("Successfully verified iscsi target %s is pointing to "\
           "portal %s." % (iscsi_target,iscsi_portal))
    self._verify_qemu_connections()

    #Step 2)
    self._nuke_iscsi_redirector_logs()
    self.info("About to stop Stargate on %s." % self.vm_on_svm.ip())
    self.vm_on_svm.execute("source /etc/profile; genesis stop stargate")
    self.info("Successfully stopped Stargate on %s." % self.vm_on_svm.ip())
    starttime = int(time.time())
    failover_ir_log = self._cat_iscsi_redirector_logs()
    skip_str = "Portal %s:3261 is down" % self.vm_on_svm.ip()
    while failover_ir_log.find(skip_str) < 1:
      failover_ir_log = self._cat_iscsi_redirector_logs()
    endtime = int(time.time())
    self.info("Iscsi targets failover took %d ms." % (endtime - starttime))
    self.failover_iscsi_portal_map = {}
    for (iscsi_target, failover_iscsi_portal) in self._ir_log_parser(skip_beyond_str=skip_str):
      self.info((iscsi_target, failover_iscsi_portal))
      failover_cvm = failover_iscsi_portal.split(":")[0]
      if iscsi_target in self.iscsi_portal_map:
        self.check(failover_cvm in self.other_cmv_ips,
                   "Unexpectedly failover portal %s did not belong to "\
                   "cvms %s." % (failover_cvm , ",".join(self.other_cmv_ips)))
      self.info("Successfully verified failover for iscsi target %s." %\
                iscsi_target)
      self.failover_iscsi_portal_map[iscsi_target] = failover_iscsi_portal
    self._verify_qemu_connections(is_failover=True)

    #Step 3)
    self._nuke_iscsi_redirector_logs()
    self.cluster.start()
    starttime = int(time.time())
    failback_ir_log = self._cat_iscsi_redirector_logs()
    while failback_ir_log.count("Redirecting target") < 4:
      failback_ir_log = self._cat_iscsi_redirector_logs()
    endtime = int(time.time())
    self.info("Iscsi targets failback completed in %d ms." %\
              (endtime - starttime))
    self.failback_iscsi_portal_map = {}
    for (iscsi_target, failback_iscsi_portal) in self._ir_log_parser():
      self.info((iscsi_target, failback_iscsi_portal))
      if iscsi_target in self.iscsi_portal_map:
        self.check(
            failback_iscsi_portal == self.iscsi_portal_map[iscsi_target])
      self.failback_iscsi_portal_map[iscsi_target] = failback_iscsi_portal
    self._verify_qemu_connections()
    self.info(self.failback_iscsi_portal_map)

  def _verify_qemu_connections(self, is_failover=False):
    """
    Find the pid of the qemu processes. Verify that those pids are have
    connections to cvm:3261
    """
    node = self.vm_on_host
    cmd = "ps -fp $(pgrep qemu) | grep %s" % self.vmid
    self.debug("\n\n###Executing %s on %s" % (cmd, node.ip()))
    rv, stdout, stderr = node.execute(cmd)
    self.check(len(stdout.splitlines()) == 1,
               "Number of qemu processes failed to match expectation.")
    self.debug("rv is %s" % rv)
    self.debug("stdout is %s" % stdout)
    self.debug("stderr is %s" % stderr)
    pid = stdout.split()[1].strip()
    if is_failover:
      cmd = "netstat -anp | egrep 'tcp.*%s.*%s/qemu-kvm'" % (node.ip(),pid)
    else:
      cmd = "netstat -anp | egrep 'tcp.*%s.*%s:3261.*%s/qemu-kvm'" %\
            (node.ip(),self.vm_on_svm.ip(),pid)
    self.debug("\n\n###Executing %s on %s" % (cmd, node.ip()))
    rv, stdout, stderr = node.execute(cmd)
    self.debug("rv is %s" % rv)
    self.debug("stdout is %s" % stdout)
    self.debug("stderr is %s" % stderr)
    lines = stdout.splitlines()
    self.check(len(lines) == 4, "Unexpectedly did not find 4 connections "\
               "for vmdisks of vm %s" % self.vmid)
    for line in lines:
      self.debug(line)
      cvm = re.match((r'^tcp(.*)%s:(\d+)(\s+)(.*):3261(.*)/qemu-kvm$' %\
                     (self.vm_on_host.ip())), line.strip()).group(4)
      if is_failover:
        self.check(cvm in self.other_cmv_ips)
      else:
        self.check(cvm == self.vm_on_svm.ip())

  def _cat_iscsi_redirector_logs(self):
    node = self.vm_on_host
    self.debug("\n\n###Executing cat /var/log/iscsi_redirector on %s" %\
               node.ip())
    rv, stdout, stderr = node.execute("cat /var/log/iscsi_redirector")
    self.debug("rv is %s" % rv)
    self.debug("stdout is %s" % stdout)
    self.debug("stderr is %s" % stderr)
    return stdout

  def _nuke_iscsi_redirector_logs(self):
    for node in self.cluster.nodes():
      cmd = "cat /dev/null > /var/log/iscsi_redirector"
      rv, stdout, stderr = node.execute(cmd)
      self.debug("rv is %s" % rv)
      self.debug("stdout is %s" % stdout)
      self.debug("stderr is %s" % stderr)
      self.debug("Sucessfully nuked iscsi_redirector log on %s" % node.ip())


  def _get_node_and_svm(self, vminfo):
    host_uuid = vminfo["hostUuid"]
    host_info = self.arithmos_rest_util.get_entities_lookup_by_value(HOSTS,
                                                                     host_uuid)
    reqd_host = None
    for node in self.cluster.nodes():
      if node.ip() == host_info["hypervisorAddress"]:
        reqd_host = node
    return reqd_host, reqd_host.svm()

  def _get_vmdisk_ids(self, vminfo):
    return [vmdisk["vmDiskUuid"] for vmdisk in vminfo["config"]["vmDisks"]]

  def _ir_log_parser(self, skip_beyond_str=None):
    ir_log_lines = self._cat_iscsi_redirector_logs().splitlines()
    idx = 0
    if skip_beyond_str:
      while ir_log_lines[idx].find(skip_beyond_str) < 0:
        idx += 1
    for line in ir_log_lines[idx:]:
      if line.find("iqn") > 0 and line.find("3261") > 0:
        iscsi_target = line.split()[7]
        iscsi_portal = line.split()[-1]
        yield (iscsi_target, iscsi_portal)
    return

  def _create_and_poweron_vm(self):
    def create_vmdisk(bus):
      return self.acropolis_rest_util.construct_vm_disk_proto(
          vmdiskcreate_containerName=self.ctr_name,
          vmdiskcreate_sizemb=1024,
          diskAddress_deviceBus=bus)["disks"][0]
    vmDisks = [create_vmdisk("SCSI"), create_vmdisk("PCI"),
               create_vmdisk("IDE"), create_vmdisk("SATA")]
    vmid = self.acropolis_rest_util.vm_create(
        name=("userspace_iscsi_testvm_%s" % int(time.time())),
        num_vcpus=1,
        memory_mb=1,
        vmDisks=vmDisks)
    self.acropolis_rest_util.vm_power_on(vmid)
    return vmid
