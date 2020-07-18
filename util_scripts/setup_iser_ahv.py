#!/usr/bin/python
"""
Copyright (c) 2020 Nutanix Inc. All rights reserved.
Author: gokul.kannan@nutanix.com

This module configures RDMA on the AHV host. This module does a pass through of
NICs and/or VFs to the CVM and assigns IP addresses to the RDMA interfaces
created on the AHV host.
"""

import glob
import json
import logging
import logging.handlers
import os
import re
import subprocess
import sys
import time
import uuid

import xml.etree.ElementTree  as et


# Vendor ID of the NICs on which we support RDMA.
RDMA_CAPABLE_NICS = ["15b3:1013", "15b3:1015"]

# Path to the CVM xml file.
CVM_XML_PATH = "/etc/libvirt/qemu/{}.xml"

# IPs to be used for ISER
AHV_ISER_IP = "192.168.5.3"
CVM_ISER_IP = "192.168.5.4"

# Minimum number of NICS to be present on the node for us to consider
# configuring RDMA.
MIN_RDMA_NICS = 2

# Number of VFs to create on the RDMA capable NIC.
NUM_VFs = 2

# RDMA config file path
RDMA_CONFIG_FILE_PATH = "/run/rdma-config.json"


class PCI_Address(object):
  """
  Class to represent PCI Address.
  """
  def __init__(self, pci_address=None, domain=None, bus=None, slot=None,
               function=None):
    """
    Split the pci address into different components and initialize.
    """
    if pci_address:
      pci_addr_list = re.split(':|\.', pci_address)

      # Pci address can be split into domain, bus, slot and function.
      # In x86_64, domain is usually 0000
      if len(pci_addr_list) == 4:
        domain, bus, slot, function = pci_addr_list
      elif len(pci_addr_list) == 3:
        domain = "0000"
        bus, slot, function = pci_addr_list
      else:
        log.error("Invalid PCI format")

    self.__domain = domain
    self.__bus = bus
    self.__slot = slot
    self.__function = function
    self.pci_address = self.__domain + ":" + self.__bus + ":" + self.__slot + \
                       "." + self.__function

  @staticmethod
  def is_valid_pci_address(pci_address):
    """
    Verify if the given string is a valid pci address.
    """
    pci_addr_list = re.split(':|\.', pci_address)
    if len(pci_addr_list) in [3, 4]:
      # TODO: Add some more checks.
      return True
    return False

  @property
  def pci_address_string(self):
    """
    The PCI address in string format.
    """
    return self.pci_address

  @property
  def domain(self):
    """
    The domain field of PCI id.
    """
    return self.__domain

  @property
  def bus(self):
    """
    The bus field of the PCI id.
    """
    return self.__bus

  @property
  def slot(self):
    """
    The slot field of the PCI id.
    """
    return self.__slot

  @property
  def function(self):
    """
    The function field of the PCI id.
    """
    return self.__function

def run_command(cmd):
  """
  Runs a command on the localhost and returns output and error values.
  Args:
    cmd(str): The command to be executed.
  Returns the status code, stdout and the stderr.
  """
  proc = subprocess.Popen(
    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
  out, err = proc.communicate()
  return proc.returncode, out, err

def get_rdma_capable_devices():
  """
  Find and return all the devices which support RDMA and which have been
  qualified for our solution.
  Returns:
    list of PCI id of the rdma capable ports sorted by PCI ids.
  """
  rdma_nics = []
  rv, stdout, stderr = run_command("lspci -n")
  if rv:
    log.error("Failed to run lspci command. Error:( %s, %s)" % (stdout,
      stderr))
    return None

  for line in stdout.strip().splitlines():
    if line.split()[2].strip() in RDMA_CAPABLE_NICS:
      pci_id = line.split()[0].strip()
      pci_id_obj = PCI_Address(pci_id)
      rdma_nics.append(pci_id_obj.pci_address_string)

  return rdma_nics

def shutdown_vm(vm_domain_name):
  """
  Shutdown the CVM:
  Args:
    vm_domain_name(str): virsh domain name of the VM.
  """
  cmd = "virsh shutdown %s" % vm_domain_name
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to shutdown the VM. Error:( %s, %s)" % (stdout,
      stderr))
    return False

  expired_time = 0
  sleep_time = 10
  total_wait_time = 300
  while expired_time < total_wait_time:
    if is_vm_up(vm_domain_name):
      time.sleep(10)
      expired_time += sleep_time
    else:
      return True

  return False

def start_vm(vm_domain_name):
  """
  Start the VM.
  Args:
    vm_domain_name(str): virsh domain name of the CVM.
  """
  cmd = "virsh start %s" % vm_domain_name
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to start the VM. Error:( %s, %s)" % (stdout,
      stderr))
    return False

  expired_time = 0
  sleep_time = 10
  total_wait_time = 300
  while expired_time < total_wait_time:
    if not is_vm_up(vm_domain_name):
      time.sleep(10)
      expired_time += sleep_time
    else:
      return True

  return False

def is_vm_up(vm_domain_name):
  """
  Check if the given VM is up.
  Args:
    vm_domain_name(str): Virsh domain name.
  """
  cmd = "virsh domstate %s" % vm_domain_name
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to get state of the VM. Error:( %s, %s)" % (stdout,
      stderr))
    return False

  if stdout.strip() != "running":
    return False

  return True

def create_virtual_functions(interface, num_vfs=2):
  """
  Creates VFs on the given interface.
  Args:
    interface(str): Name of the interface.
    num_vfs(int): Number of VFs to create.
  """
  cmd = "echo %d > /sys/class/net/%s/device/sriov_numvfs" % (num_vfs,
      interface)
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to create VFs. Error: (%s, %s)" % (stdout, stderr))
    return False

  return True

def assign_uuid_vf(ib_device_id, vf_index):
  """
  Assign UUID to the Virtual Function.
  Args:
    ib_device_id(str): The infiniband device id.
    vf_index(int): Index of the VF in the IB device.
  """
  new_uuid = uuid.uuid4().int>>64
  it = iter(str(new_uuid))
  vf_uuid = ':'.join(a+b for a,b in zip(it, it))
  cmd = "echo %s > "\
    "/sys/class/infiniband/%s/device/sriov/%d/node" % (vf_uuid, ib_device_id,
                                                       vf_index)
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to assign uuid. Error: (%s, %s)" % (stdout, stderr))
    return False

  return True

def rebind_vfs(pci_addresses):
  """
  Unbind and bind the VFs.
  Args:
    pci_addresses(list): List of pci addresses to rebind.
  """
  cmd_unbind = "echo {0} > /sys/bus/pci/drivers/mlx5_core/unbind"
  cmd_bind = "echo {0} > /sys/bus/pci/drivers/mlx5_core/bind"
  for address in pci_addresses:
    rv, stdout, stderr = run_command(cmd_unbind.format(address))
    if rv:
      log.error("Failed to unbind VF. Error: (%s, %s)" % (stdout, stderr))
      return False

    rv, stdout, stderr = run_command(cmd_bind.format(address))
    if rv:
      log.error("Failed to bind VF. Error: (%s, %s)" % (stdout, stderr))
      return False

  return True

def enable_vf(pf, vf_index):
  """
  Set the VF to enabled state.
  Args:
    pf(str): The physical function interface name.
    vf_index(int): Index number of the VF.
  """
  cmd = "ip link set %s vf %d state enable" % (pf, vf_index)
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to enable VF. Error: (%s, %s)" % (stdout, stderr))
    return False
  return True

def set_interfaces_up(interfaces):
  """
  Bring the interfaces up.
  Args:
    interfaces(list): List of nw interfaces.
  """
  cmd = "ip link set {0} up"
  for iface in interfaces:
    rv, stdout, stderr = run_command(cmd.format(iface))
    if rv:
      log.error("Failed to bring the interface up. Error: (%s, %s)"
        % (stdout, stderr))
      return False

  return True

def set_vf_mac_address(pf, vf_index, mac_address):
  """
  Set the mac address on the VF.
  Args:
    pf(str): The physical function interface name.
    vf_index(int): Index number of the VF.
  """
  cmd = "ip link set {0} vf {1} mac {2}".format(pf, vf_index, mac_address)
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to set mac address on the VF. Error:(%s, %s)" %
              (stdout, stderr))

  return True


def get_vf_details(pf):
  """
  Get the PCI addresses of all the VFs for the given PF.
  Args:
    pf(str): Interface name of the physical function.
  Returns:
    list: A list of dicts containing the interface name, pci address and vf
          index.
  """
  path = "/sys/class/net/%s/device" % pf
  files = os.listdir(path)
  vf_details = []
  if not files:
    log.error("Failed get VF details")
    return False
  for filename in files:
    if "virtfn" in filename:
      vf_index = int(filename.split("virtfn")[1])
      file_path = os.path.join(path, filename)
      target_file = os.readlink(file_path)
      pci_address = os.path.basename(target_file)
      pci_ad_obj = PCI_Address(pci_address)
      interface_name = get_nw_interface_name_from_pci_address(
        pci_ad_obj.pci_address_string)
      vf_details.append({"pci_address": pci_ad_obj.pci_address_string,
                        "interface_name": interface_name,
                        "vf_index": vf_index})

  return vf_details

def get_nw_interface_name_from_pci_address(pci_address):
  """
  Return the name of the interface created on the pci device.
  Args:
    pci_address(str): Address of the pci device
  """
  ethtool_cmd = "ethtool -i {0}"
  ip_link_cmd = "ip link show {0}"
  pci_base = "/sys/devices/pci"
  for intf_path in glob.glob("/sys/class/net/*"):
    # Check the interface is a PCIe device. Ignore if not.
    if not os.path.realpath(intf_path).startswith(pci_base):
      continue

    iface = os.path.basename(intf_path)
    rv, out, err = run_command(ethtool_cmd.format(iface))
    if rv:
      log.error("Failed to run ethtool command. Error: (%s, %s)" % (out,
        err))
      return False

    for entry in out.strip().splitlines():
      if "bus-info" in entry:
        if len(entry.split()) < 2 or \
          not PCI_Address.is_valid_pci_address(entry.split()[1]):
          continue
        pci_id = entry.split()[1]
        pci_id_obj = PCI_Address(pci_id)
        if pci_id_obj.pci_address_string == pci_address:
          return iface
        break

  return None

def get_mac_address(iface_name):
  """
  Return the mac address for the interface.
  Args:
    iface_name(str): Name of the interface.
  """
  file_name = "/sys/class/net/%s/address" % iface_name
  with open(file_name) as fl:
    return  fl.read().strip()

def get_ib_dev_from_interface_name(interface_name):
  """
  Return the infiniband device name for the given interface name.
  Args:
    interface_name(str): Name of the network interface.
  """
  rv, stdout, stderr = run_command("ibdev2netdev")
  if rv:
    log.error("Failed to run ibdev2netdev command. Error: (%s, "
              "%s)" % (stdout, stderr))
    return None

  for line in stdout.strip().splitlines():
    if not line:
      continue
    words = line.split()
    if words[4] == interface_name:
      return words[0]

  return None

def pass_through_device(pci_address, vm_xml_path):
  """
  Pass through the given PCI device to the CVM.
  Args:
    pci_address(str): PCI address of the device to pass through.
    vm_xml_path(str): Path to the VM XML file.
  """
  vm_descriptor = et.parse(vm_xml_path)
  device = vm_descriptor.find("devices")
  hostdevs = device.findall("hostdev")
  slot = 0
  for hostdev in hostdevs:
    address = hostdev.find("address")
    if address is not None:
      if slot < int(address.get("slot"), 16):
        slot = int(address.get("slot"), 16)

  hostdev_slot = slot + 1
  pci_addr_obj = PCI_Address(pci_address)

  hostdev = et.SubElement(device, "hostdev", {  "mode":    "subsystem",
                                                "type":    "pci",
                                                "managed": "yes" })
  source = et.SubElement(hostdev, "source")
  et.SubElement(source , "address",
                { "domain":   "0x%s" % pci_addr_obj.domain,
                  "bus":      "0x%s" % pci_addr_obj.bus,
                  "slot":     "0x%s" % pci_addr_obj.slot,
                  "function": "0x%s" % pci_addr_obj.function })
  et.SubElement(hostdev, "rom", { "bar": "off" })
  et.SubElement(hostdev, "address",
                                   { "domain":    "0x0000",
                                     "bus":       "0x00",
                                     "slot":      "0x%x" % hostdev_slot,
                                     "function":  "0x0",
                                     "type":      "pci"})
  vm_descriptor.write(vm_xml_path)
  return True

def is_device_pass_through(pci_address, vm_xml_path):
  """
  Check if the device with the pci_address has been passed through to the cvm.
  Args:
    pci_address(str): PCI address of the device.
    vm_xml_path(str): Path of the VM xml file.
  """
  vm_descriptor = et.parse(vm_xml_path)
  device = vm_descriptor.find("devices")
  hostdevs = device.findall("hostdev")
  for hostdev in hostdevs:
    elem = hostdev.find("source")
    if elem is not None:
      address = elem.find("address")
      source_pci_address = PCI_Address(domain=address.get("domain")[2:],
          bus=address.get("bus")[2:], slot=address.get("slot")[2:],
          function=address.get("function")[2:])
      if source_pci_address.pci_address_string == pci_address:
        return True

  return False

def remove_pass_through_device(pci_address, vm_xml_path):
  """
  Remove the pass through device from the CVM xml file.
  Args:
    pci_address(str): PCI address of the device.
    vm_xml_path(str): Path of the VM xml file.
  """
  vm_descriptor = et.parse(vm_xml_path)
  device = vm_descriptor.find("devices")
  hostdevs = device.findall("hostdev")
  hostdev_to_remove = None
  for hostdev in hostdevs:
    elem = hostdev.find("source")
    if elem is not None:
      address = elem.find("address")
      source_pci_address = PCI_Address(domain=address.get("domain")[2:],
          bus=address.get("bus")[2:], slot=address.get("slot")[2:],
          function=address.get("function")[2:])
      if source_pci_address.pci_address_string == pci_address:
        hostdev_to_remove = hostdev
        break

  if hostdev_to_remove is not None:
    device.remove(hostdev_to_remove)
    vm_descriptor.write(vm_xml_path)
    return True

  return False

def get_cvm_domain_name():
  """
  Get the CVM domain name.
  """
  rv, stdout, stderr = run_command("virsh list --all")
  if rv:
    log.error("Failed to run virsh list command. Error: (%s, %s)" % (stdout,
      stderr))
  for line in stdout.strip().splitlines():
    if re.match("^Id", line.strip()):
      continue
    if re.match("^---", line.strip()):
      continue
    words = line.strip().split()
    if words[1].startswith("NTNX") and words[1].endswith("CVM"):
        return words[1]
  return None

def reload_vm_xml(vm_xml_path):
  """
  Define the CVM using virsh.
  Args:
    vm_xml_path(str): Path to the VM XML file.
  """
  rv, stdout, stderr = run_command("virsh define %s" % vm_xml_path)
  if rv:
    log.error("Failed to define VM XML Error: (%s, %s)" % (stdout, stderr))
    return False

  return True


def create_configure_vfs(pf_pci_address, num_vfs=2):
  """
  Configure VFs on the given physical interface.
  Args:
    pf_pci_address(str): PCI address of the physical interface.
  """
  pf_interface_name = \
    get_nw_interface_name_from_pci_address(pf_pci_address)

  if not create_virtual_functions(pf_interface_name, num_vfs):
    log.error("Failed to create VFs.")
    return False

  ib_dev_name = get_ib_dev_from_interface_name(pf_interface_name)
  if not ib_dev_name:
    log.error("Failed to get the IB dev name from interface")
    return False

  for index in xrange(num_vfs):
    if not assign_uuid_vf(ib_dev_name, index):
      log.error("Failed to assign uuid to the VFs")
      return False

  vf_details = get_vf_details(pf_interface_name)
  if not vf_details:
    log.error("Failed to get VF details")
    return False

  pci_addresses = []
  vf_interfaces = []
  for vf in vf_details:
    pci_addresses.append(vf["pci_address"])
    vf_interfaces.append(vf["interface_name"])

  if not rebind_vfs(pci_addresses):
    log.error("Failed to rebind VFs")
    return False

  for vf in vf_details:
    if not enable_vf(pf_interface_name, vf["vf_index"]):
      log.error("Failed to enable VF")
      return False

  # Every time VF is intialized the mac address of the VF changes. Hence set
  # the mac address here.
  for vf in vf_details:
    mac_address = get_mac_address(vf["interface_name"])
    if not set_vf_mac_address(pf_interface_name, vf["vf_index"], mac_address):
      log.error("Failed to set mac address for the VF:%s" % vf)
      return False
    vf["mac_address"] = mac_address

  if not set_interfaces_up(vf_interfaces):
    log.error("Failed to bring the interfaces up")
    return False

  return vf_details

def get_bond_interfaces(bond_name=None):
  """
  Return the list of interfaces present in the bond.
  Args:
    bond_name(str): Name of the bond. If None return all interfaces which are
                    part of a bond.
  Returns a list of interfaces.
  """
  if bond_name is not None:
    cmd = "ovs-appctl bond/show %s" % bond_name
  else:
    cmd = "ovs-appctl bond/show"

  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to get the bond information of %s. Error: (%s, %s)" %
      (bond_name, stdout, stderr))

  interfaces = []
  for line in stdout.strip().splitlines():
    if line.startswith("slave"):
      interfaces.append(line.strip().split()[1])
    continue

  return interfaces

def select_ports_for_rdma(rdma_ports):
  """
  This method will hold the logic to select ports for doing RF2 RDMA and iSER.
  RF2 RDMA is CVM to CVM communication over RDMA and iSER is for Frodo to
  Stargate communication over RDMA.
  Args:
    rdma_ports(list): List of RDMA capable ports.
  Returns a dict with rdma and iser ports.
  """
  # The rdma_ports passed are sorted in order of bus ids. The port with lowest
  # bus id is connected to external switch. That port will be used for RF2
  # RDMA. The second port of the same NIC is unconnected and hence will be
  # used for iSER RDMA. This logic assumes that we will always have
  # NICs connected and configured in a certain way. In future we can some more
  # logic here so that we don't have to rely on assumptions.
  rf2_port = rdma_ports[0]
  iser_port = rdma_ports[1]
  rf2_inf = get_nw_interface_name_from_pci_address(rf2_port)
  iser_inf = get_nw_interface_name_from_pci_address(iser_port)
  bond_interfaces = get_bond_interfaces()

  # Check if the interfaces are part of any bonds on the node. When RF2 RDMA is 
  # handled via this script, expand the check below to include rf2_inf also.
  if iser_inf in bond_interfaces:
    log.error("Interface %s is part of a bond. Not configuring iSER on it." % iser_inf)
    return False

  return {"rf2": rf2_port, "iser": iser_port}

def cleanup_sriov(device):
  """
  Cleanup any SRIOV changes done on the device.
  Args:
    device(str): PCI id of the interface to be cleaned.
  """
  pf_interface_name = get_nw_interface_name_from_pci_address(
    device)
  create_virtual_functions(pf_interface_name, 0)

def configure_iser(iser_device, create_vfs=True):
  """
  Configures the given device for iser. Creates VF on the given device if
  needed and passes it through to the CVM.
  Args:
    iser_device(str): PCI id of the device to be used for iser.
    create_vfs(bool): Should VFs be created on this device.
  """
  log.info("Selected NIC with PCI id %s for iSER" % iser_device)
  cvm_domain_name = get_cvm_domain_name()
  cvm_xml_path = CVM_XML_PATH.format(cvm_domain_name)
  if create_vfs:
    vf_details = create_configure_vfs(iser_device, NUM_VFs)
  else:
    # TODO: This is the case where the VFs were already created. Get the interface 
    # details here.
    vf_details = None

  if not vf_details:
    # Undo any VFs created on the interface.
    cleanup_sriov(iser_device)
    log.error("RDMA config failed.\n Exiting")
    return False

  # Remove the device if it has already been passed through.
  if is_device_pass_through(iser_device, cvm_xml_path):
    remove_pass_through_device(iser_device, cvm_xml_path)

  # Pass through the first VF to the SVM for iSER.
  pass_through_vf = vf_details[0]
  ahv_vf = vf_details[1]
  if is_device_pass_through(ahv_vf["pci_address"], cvm_xml_path):
    log.error("VF assigned to AHV has been passed through. Removing it.")
    remove_pass_through_device(ahv_vf["pci_address"], cvm_xml_path)

  if is_device_pass_through(pass_through_vf["pci_address"],
      cvm_xml_path):
    log.error("VF already passed through")
  elif not pass_through_device(pass_through_vf["pci_address"],
      cvm_xml_path):
    log.error("Failed to pass through VF to the CVM")
    return False

  if not reload_vm_xml(cvm_xml_path):
    log.error("Failed to reload the CVM XML")
    remove_pass_through_device(pass_through_vf["pci_address"],
      cvm_xml_path)
    cleanup_sriov(iser_device)
    return False

  # Assign IP address to the AHV VF and add route to the linux routing table.
  cmd ="ifconfig %s %s/24 up" % (ahv_vf["interface_name"], AHV_ISER_IP)
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to assign IP Address to interface on AHV. Error: "
              "(%s, %s)" % (stdout, stderr))
    cleanup_sriov(iser_device)
    return False

  # Add route to the linux routing table.
  cmd = "route add -host %s dev %s" % (CVM_ISER_IP, ahv_vf["interface_name"])
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to add route. Error: (%s, %s)" % (stdout, stderr))
    cleanup_sriov(iser_device)
    return False

  return {"pass_through_vf": pass_through_vf, "ahv_vf": ahv_vf}

def configure_rf2_rdma(rf2_device):
  """
  Args:
    rf2_device(str): PCI id of the devie to be used for rf2 RDMA.
  """
  # This is currently taken care of by foundation.
  pass

def configure_rdma():
  """
  Main method which configures RDMA on AHV node.
  """
  rdma_nics = get_rdma_capable_devices()
  if not rdma_nics:
    log.error("No RDMA capable NICs found on the node. Exiting!")
    sys.exit(1)

  if len(rdma_nics) < MIN_RDMA_NICS:
    log.error("Not enough RDMA NICs to enable iser. Found: %d Need:"
              " %d.\nExiting" % (len(rdma_nics), MIN_RDMA_NICS))
    sys.exit(1)

  rdma_config_ports = select_ports_for_rdma(rdma_nics)
  if not rdma_config_ports:
    log.error("Could not find eligible ports for RDMA. Exiting!")
    sys.exit(1)

  # In future, we might decide not to dedicate the complete ports for iSER
  # and RDMA. Instead we would create the VFs on a given port and then assign
  # those VFs for iSER and RF2 RDMA. In that scenario, we will call
  # create_configure_vfs() method on the port and then select VFs from the
  # returned list.
  selected_device_for_iser = rdma_config_ports["iser"]
  selected_device_for_rf2 = rdma_config_ports["rf2"]
  cvm_domain_name = get_cvm_domain_name()
  if not cvm_domain_name:
    log.error("Failed to get CVM domain name")
    sys.exit(1)

  if is_vm_up(cvm_domain_name):
    log.info("Shutting down the SVM")
    if not shutdown_vm(cvm_domain_name):
      log.error("Failed to shutdown CVM")
      sys.exit(1)

  iser_config = configure_iser(selected_device_for_iser, create_vfs=True)
  rf2_config = configure_rf2_rdma(selected_device_for_rf2)
  iser_cvm_mac_addr = iser_config["pass_through_vf"]["mac_address"]

  if not start_vm(cvm_domain_name):
    log.error("Failed to start the CVM after VF creation")
    sys.exit(1)

  # Finally record the mac address of the passed through interface here for CVM
  # to lookup later.
  rdma_config_json =  {"iser": {"cvm_mac_address": iser_cvm_mac_addr}}
  with open(RDMA_CONFIG_FILE_PATH, "w") as rdma_config_file:
    rdma_config_file.write(json.dumps(rdma_config_json, indent=2))

def main():
  """
  Main method
  """
  configure_rdma()

if __name__ == "__main__":
  logging.basicConfig()
  log = logging.getLogger(os.path.basename(__file__))
  log.setLevel(logging.INFO)
  main()
