#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Author: gokul.kannan@nutanix.com
#
# Tests for Multipath ALUA (Asymmetric Logical Unit Access)

import time
import random

from qa.agave.agave_util import AgaveUtil
from qa.agave.nutanix_test import NutanixTest
from qa.agave.nutanix_test_uvms_spec import NutanixTestUvmsSpecCreator
from qa.util.agave_tools.vm_disk_util import get_vm_disk_util
from qa.agave.nutanix_cluster_ssh_util import NutanixClusterSSHUtil
from qa.agave.nutanix_acli import NutanixAcli

class Alua(NutanixTest):
  """
  This class implements tests to verify MPIO ALUA feature. The following
  testcases are implemented:
  - Verify basic ALUA.
  TODO: add the tests when implemented
  """

  @staticmethod
  def tags():
    return set(["stargate"])

  @staticmethod
  def uvms_spec():
    creator = NutanixTestUvmsSpecCreator()
    spec = creator.create_hypervisor_uvms_spec(1, 0)
    return spec

  def setup(self):
    """
    Test setup. Prepare the UVMs
    """

    self.uvms = self.uvms_binding.get_uvm_list()
    node = random.choice(self.cluster.nodes())
    cvm = node.svm()
    self.acli = NutanixAcli(cvm.ip())
    self.__prepare_uvms()
    self.__prepare_vgs()

  def test_basic_alua(self):
    self.__discover_luns()
    return True
    
  def __prepare_uvms(self):
    """
    This function prepares the UVMs for the test. It generates and adds a new
    initiator name. It installs the mpio and sg3_utils packages. It then enables
    alua on the UVM. 
    """
    self.initiators = {}
    for uvm in self.uvms:
      cmd = "sudo cat /etc/iscsi/initiatorname.iscsi"
      results = NutanixClusterSSHUtil.execute([uvm], cmd, False, "root")
      self.info("Initiator before in file: %s" %results[0][1])
    
      cmd = "sudo iscsi-iname"
      results = NutanixClusterSSHUtil.execute([uvm], cmd)
      rv, stdout, stderr = results[0]
      self.check_eq(rv, 0, "Failed to generate new initiator name")
      self.initiators[uvm] = stdout
      self.info("The initiator is : %s" %self.initiators[uvm])
      cmd = "sudo echo InitiatorName=%s > /etc/iscsi/initiatorname.iscsi" \
            %stdout
      results = NutanixClusterSSHUtil.execute([uvm], cmd, False, "root")
      self.check_eq(results[0][0], 0, "Failed to change the initiator name")
      cmd = "sudo cat /etc/iscsi/initiatorname.iscsi"
      results = NutanixClusterSSHUtil.execute([uvm], cmd, False, "root")
      self.info("Initiator in file: %s" %results[0][1])
    '''cmd = "sudo yum install -y device-mapper-multipath.x86_64 "
    results = NutanixClusterSSHUtil.execute(self.uvms, cmd, False, "root")
    if(results[0][0] != 0):
      if (results[0][2] eq "")
    cmd = "sudo yum install -y sg3_utils"
    results = NutanixClusterSSHUtil.execute(self.uvms, cmd, False, "root")
    self.check_eq(results[0][0], 0, "Installing sg3_utils failed")
    '''
    contents = 'defaults{\n path_grouping_policy failover\n failback \
               immediate\n prio alua \n}'
    cmd = "sudo echo -e %s > /etc/multipath.conf" %contents
    results = NutanixClusterSSHUtil.execute(self.uvms, cmd, False, "root")
    rv, stdout, stderr = results[0]
    self.check_eq(rv, 0, "Writing to mutipath.conf file failed")
    # Reboot the UVMs
    self.cluster.stop_uvms( self.uvms)
    self.cluster.start_uvms( self.uvms)
  
  def __prepare_vgs(self):
    '''
    This function creates volume groups and attaches the initiators to them.
    '''
    node = random.choice(self.cluster.nodes())
    cvm = node.svm()
    for uvm in self.uvms:
      vg_name = "vg"+ uvm.name()
      vg_name = vg_name.replace("_", "")
      #cmd = "source /etc/profile; /usr/local/nutanix/bin/acli -o json vg.create %s" %vg_name
      cmd = "vg.create %s" %vg_name
      #results = NutanixClusterSSHUtil.execute([cvm.ip()], cmd)
      result = self.acli.execute(cmd)
      #self.check_eq(results[0][0], 0, "Volume Group create failed because : %s"\
      #              %results[0][2])
      #cmd = "source /etc/profile; /usr/local/nutanix/bin/acli -o json vg.attach_external %s %s" \
      #      %(vg_name, self.initiators[uvm])
      cmd = "vg.attach_external %s %s" %(vg_name, self.initiators[uvm])
      #results = NutanixClusterSSHUtil.execute([cvm.ip()], cmd)
      result = self.acli.execute(cmd)
      #self.check_eq(results[0][0], 0, "Attaching initiator to VG failed because"
      #             ": %s" %results[0][2])
      cmd = "sudo cat /etc/iscsi/initiatorname.iscsi"
      results = NutanixClusterSSHUtil.execute([uvm], cmd, False, "root")
      self.info("Initiator in file: %s" %results[0][1])
    
                    
      
  
  def __discover_luns(self):
    '''
    '''
    # Select a random CVM for discovery
    node = random.choice(self.cluster.nodes())
    cvm = node.svm()
    cmd = "iscsiadm -m discovery -t st -p %s:3260" %cvm.ip()
    results = NutanixClusterSSHUtil.execute(self.uvms, cmd, False, "root")
    self.check_eq(results[0][0], 0, "Discovery failed beacause: %s" %results[0][2])
    cmd = "iscsiadm -m node -l"
    results = NutanixClusterSSHUtil.execute(self.uvms, cmd, False, "root")
    self.check_eq(results[0][0], 0, "Target login failed")
    cmd = "multipath -ll"
    results = NutanixClusterSSHUtil.execute(self.uvms, cmd, False, "root")
    self.check_eq(results[0][0], 0, "Multipath command failed")
    self.info("op: %s" %results[0][1])
      
    