#!/usr/bin/python
"""
Copyright (c) 2020 Nutanix Inc. All rights reserved.
Author: gokul.kannan@nutanix.com

This script uses SRIOV and creates Virtual Functions on RDMA capable port. It
then passes through one VF to the CVM. Copy this script to the AHV node and
run it as:
./setup_iser_ahv.py
"""

import glob
import logging
import logging.handlers
import os
import re
import subprocess
import sys
import time
import uuid

import xml.etree.ElementTree  as et


RDMA_CAPABLE_NICS = ["15b3:1013", "15b3:1015"]

CVM_XML_PATH = "/etc/libvirt/qemu/"

AHV_ISER_IP = "192.168.5.3"

CVM_ISER_IP = "192.168.5.4"

MIN_RDMA_NICS = 2

NUM_VFs = 2

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
  Find and return all the devices which are rdma capable.
  Returns:
    list of PCI_Address objects of the rdma capable ports.
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
      # vendor_id = line.split()[2].strip()
      rdma_nics.append(pci_id_obj.pci_address_string)

  return rdma_nics

def shutdown_cvm(cvm_domain_name):
  """
  Shutdown the CVM:
  Args:
    cvm_domain_name(str): virsh domain name of the CVM.
  """
  cmd = "virsh shutdown %s" % cvm_domain_name
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to shutdown the CVM. Error:( %s, %s)" % (stdout,
      stderr))
    return False

  expired_time = 0
  sleep_time = 10
  total_wait_time = 300
  while expired_time < total_wait_time:
    if is_cvm_up(cvm_domain_name):
      time.sleep(10)
      expired_time += sleep_time
    else:
      return True

  return False

def start_cvm(cvm_domain_name):
  """
  Start the CVM.
  Args:
    cvm_domain_name(str): virsh domain name of the CVM.
  """
  cmd = "virsh start %s" % cvm_domain_name
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to start the CVM. Error:( %s, %s)" % (stdout,
      stderr))
    return False

  expired_time = 0
  sleep_time = 10
  total_wait_time = 300
  while expired_time < total_wait_time:
    if not is_cvm_up(cvm_domain_name):
      time.sleep(10)
      expired_time += sleep_time
    else:
      return True

  return False

def is_cvm_up(cvm_domain_name):
  """
  Check if CVM is up.
  """
  cmd = "virsh domstate %s" % cvm_domain_name
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to get state of the CVM. Error:( %s, %s)" % (stdout,
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

def get_vf_details(pf):
  """
  Get the PCI addresses of all the VFs for the given PF.
  Args:
    pf(str): Interface name of the physical function.
  Returns:
    list: A list of dicts containing the interface name and pci address.
  """
  path = "/sys/class/net/%s/device" % pf
  files = os.listdir(path)
  vf_details = []
  if not files:
    log.error("Failed get VF details")
    return False
  for filename in files:
    if "virtfn" in filename:
      file_path = os.path.join(path, filename)
      target_file = os.readlink(file_path)
      pci_address = os.path.basename(target_file)
      pci_ad_obj = PCI_Address(pci_address)
      interface_name = get_nw_interface_name_from_pci_address(
        pci_ad_obj.pci_address_string)
      vf_details.append({"pci_address": pci_ad_obj.pci_address_string,
                        "interface_name": interface_name})

  return vf_details

def get_nw_interface_name_from_pci_address(pci_address):
  """
  Return the name of the interface created on the pci device.
  Args:
    pci_address(str): Address of the pci device
  """
  ethtool_cmd = "ethtool -i {0}"
  ip_link_cmd = "ip link show {0}"
  # First get all the network interfaces on PCi devices.
  pci_base = "/sys/devices/pci"
  for intf_path in glob.glob("/sys/class/net/*"):
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


def pass_through_device(pci_address, cvm_domain):
  """
  Pass through the given device to the CVM.
  Args:
    pci_address(str): PCI address of the device to pass through.
    cvm_domain(str): Name of the CVM domain.
  """
  cmd = "virsh dumpxml %s" % cvm_domain
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to dump the CVM info from virsh. Error: (%s, %s)" %
              (stdout, stderr))
    return False

  #parser = et.XMLParser()
  cvm_xml_path = "%s/%s.xml" % (CVM_XML_PATH, cvm_domain)
  vm_descriptor = et.parse(cvm_xml_path)
  # vm_descriptor = et.fromstring(stdout.strip())
  device = vm_descriptor.find("devices")
  hostdevs = device.findall("hostdev")
  slot = 0
  for hostdev in hostdevs:
    address = hostdev.find("address")
    if address is not None:
      if slot < int(address.get("slot"), 16):
        slot = int(address.get("slot"), 16)

  hostdev_slot = slot + 1
  pci_addr_list = re.split(':|\.', pci_address)
  # Pci address can be split into domain, bus, slot and function.
  # In x86_64, domain is usually 0000
  if len(pci_addr_list) == 4:
    domain, bus, slot, function = pci_addr_list
  else:
    domain = "0000"
    bus, slot, function = pci_addr_list

  hostdev = et.SubElement(device, "hostdev", {  "mode":    "subsystem",
                                                "type":    "pci",
                                                "managed": "yes" })
  source = et.SubElement(hostdev, "source")
  et.SubElement(source , "address",
                                    { "domain":   "0x%s" % domain,
                                      "bus":      "0x%s" % bus,
                                      "slot":     "0x%s" % slot,
                                      "function": "0x%s" % function })
  #et.SubElement(hostdev, "alias", { "name": "hostdev%d" % i })
  et.SubElement(hostdev, "rom", { "bar": "off" })
  et.SubElement(hostdev, "address",
                                   { "domain":    "0x0000",
                                     "bus":       "0x00",
                                     "slot":      "0x%x" % hostdev_slot,
                                     "function":  "0x0",
                                     "type":      "pci"})
  vm_descriptor.write(cvm_xml_path)
  return True

def is_device_pass_through(pci_address, cvm_domain):
  """
  Check if the device with the pci_address has been passed through to the cvm.
  Args:
    pci_address(str): PCI address of the device.
    cvm_domain(str): Name of the CVM domain.
  """
  cvm_xml_path = "%s/%s.xml" % (CVM_XML_PATH, cvm_domain)
  vm_descriptor = et.parse(cvm_xml_path)
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

def remove_pass_through_device(pci_address, cvm_domain):
  """
  Remove the pass through device from the CVM xml file.
  Args:
    pci_address(str): PCI address of the device.
    cvm_domain(str): Name of the CVM domain.
  """
  cvm_xml_path = "%s/%s.xml" % (CVM_XML_PATH, cvm_domain)
  vm_descriptor = et.parse(cvm_xml_path)
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
    vm_descriptor.write("%s/%s.xml" % (CVM_XML_PATH, cvm_domain))
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

def reload_cvm_xml(cvm_domain_name):
  """
  Define the CVM using virsh.
  Args:
    cvm_domain_name(str): Domain Name of the CVM.
  """
  cvm_xml_path = "%s/%s.xml" % (CVM_XML_PATH, cvm_domain_name)
  rv, stdout, stderr = run_command("virsh define %s" % cvm_xml_path)
  if rv:
    log.error("Failed to define CVM XML Error: (%s, %s)" % (stdout, stderr))
    return False

  return True

def get_mac_address(iface_name):
  """
  Return the mac address for the interface.
  Args:
    iface_name(str): Name of the interface.
  """
  file_name = "/sys/class/net/%s/address" % iface_name
  with open(file_name) as fl:
    return  fl.read().strip()

def create_pass_through_vfs(pf_pci_address):
  """
  Configure VFs on the AHV hosts and pass it to the SVM.
  Args:
    pf_pci_address(str): PCI address of the physical interface.
  """
  cvm_domain_name = get_cvm_domain_name()
  if not cvm_domain_name:
    log.error("Failed to get CVM domain name")
    return False

  pf_interface_name = \
    get_nw_interface_name_from_pci_address(pf_pci_address)

  # TODO: Add checks here to make sure the selected interface isn't part of
  # the br0 bond.
  if not create_virtual_functions(pf_interface_name, NUM_VFs):
    log.error("Failed to create VFs.")
    sys.exit(1)

  ib_dev_name = get_ib_dev_from_interface_name(pf_interface_name)
  if not ib_dev_name:
    log.error("Failed to get the IB dev name from interface")
    return False

  for index in xrange(NUM_VFs):
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

  for index in xrange(NUM_VFs):
    if not enable_vf(pf_interface_name, index):
      log.error("Failed to enable VF")
      return False

  if not set_interfaces_up(vf_interfaces):
    log.error("Failed to bring the interfaces up")
    return False

  if is_device_pass_through(pf_pci_address,
      cvm_domain_name):
    remove_pass_through_device(pf_pci_address,
        cvm_domain_name)

  # Pass through the first VF:
  pass_through_vf = vf_details[0]
  ahv_vf = vf_details[1]
  if is_device_pass_through(ahv_vf["pci_address"], cvm_domain_name):
    log.error("VF assigned to AHV has been passed through. Removing it.")
    remove_pass_through_device(ahv_vf["pci_address"], cvm_domain_name)

  if is_device_pass_through(pass_through_vf["pci_address"],
    cvm_domain_name):
    log.error("VF already passed through")
    return {"pass_through_vf": pass_through_vf, "ahv_vf": ahv_vf}

  if not pass_through_device(pass_through_vf["pci_address"],
    cvm_domain_name):
    log.error("Failed to pass through VF to the CVM")
    return False

  if not reload_cvm_xml(cvm_domain_name):
    log.error("Failed to reload the CVM XML")
    remove_pass_through_device(pass_through_vf["pci_address"],
      cvm_domain_name)
    return False

  return {"pass_through_vf": pass_through_vf, "ahv_vf": ahv_vf}

def configure_iser():
  """
  Main method which configures iSER on AHV.
  """
  rdma_nics = get_rdma_capable_devices()
  if not rdma_nics:
    log.error("Could not determine if the node has RDMA NICs. Exiting!")
    sys.exit(1)

  if len(rdma_nics) < MIN_RDMA_NICS:
    log.error("Not enough RDMA NICs to enable iser. Found: %d Need:"
              " %d.\nExiting" % (len(rdma_nics), MIN_RDMA_NICS))
    sys.exit(1)

  # Select the unconnected port of the NIC with lowest bus id.
  selected_device_for_iser = rdma_nics[1]
  cvm_domain_name = get_cvm_domain_name()
  if not cvm_domain_name:
    log.error("Failed to get CVM domain name")
    sys.exit(1)

  if is_cvm_up(cvm_domain_name):
    if not shutdown_cvm(cvm_domain_name):
      log.error("Failed to shutdown CVM")
      sys.exit(1)

  vfs = create_pass_through_vfs(selected_device_for_iser)
  if not vfs:
    # Undo any VFs created on the interface.
    pf_interface_name = get_nw_interface_name_from_pci_address(
      selected_device_for_iser)
    create_virtual_functions(pf_interface_name, 0)
    start_cvm(cvm_domain_name)
    log.error("iSER config failed.\n Exiting")
    sys.exit(1)

  svm_mac_addr = get_mac_address(vfs["pass_through_vf"]["interface_name"])
  if not start_cvm(cvm_domain_name):
    log.error("Failed to start the CVM after VF creation")
    sys.exit(1)

  # Assign IP address to the AHV VF and add route to the linux routing table.
  cmd ="ifconfig %s %s/24 up" % (vfs["ahv_vf"]["interface_name"], AHV_ISER_IP)
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to assign IP Address to interface on AHV. Error: "
              "(%s, %s)" % (stdout, stderr))

  # Add route to the linux routing table.
  cmd = "route add -host %s dev %s" % (CVM_ISER_IP,
    vfs["ahv_vf"]["interface_name"])
  rv, stdout, stderr = run_command(cmd)
  if rv:
    log.error("Failed to add route. Error: (%s, %s)" % (stdout, stderr))

  print ("iSER has been set up on the AHV host. Please assign %s IP to the"
    " interface with mac address %s on the SVM." % (CVM_ISER_IP, svm_mac_addr))

def main():
  """
  Main method
  """
  configure_iser()

if __name__ == "__main__":
  logging.basicConfig()
  log = logging.getLogger(os.path.basename(__file__))
  log.setLevel(logging.INFO)
  main()
