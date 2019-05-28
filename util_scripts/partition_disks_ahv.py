"""
Script to create partitions on AHV hosts and create Jarvis entries of virtual
node.
Author: gokul.kannan@nutanix.com
Copyright (c) 2019 Nutanix Inc. All rights reserved.
"""

import argparse
import glob
import os
import paramiko
import re
import time

GB = 1024 * 1024 * 1024

class PartitionDisks:
  """
  This class contains methods to partition the disks on AHV host.
  """
  def __init__(self, host_ip, username="root", password="nutanix/4u"):
    """
    Initialize stuff.
    """
    self.ssh = paramiko.SSHClient()
    self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    self.host_ip = host_ip
    self.username = username
    self.password = password

  def lsscsi(self):
    """
    This function uses lsscsi to list all the SCSI devices attached to this VM.

    Returns:
      [{
      'scsi_host'         : int,
      'channel'           : int,
      'target_number'     : int,
      'LUN'               : int,
      'peripheral_type'   : str,
      'device_node_name'  : str
      }]
      A list of dictionaries represented in the above format.

    """
    ret = []
    _, result, stderr = self.run_cmd_ssh('lsscsi')
    lines = result.splitlines()
    for line in lines:
      words = line.split()
      host, channel, target, lun = words[0][1:-1].split(':')
      ret.append({
        'scsi_host'         : host,
        'channel'           : channel,
        'target_number'     : target,
        'LUN'               : lun,
        'peripheral_type'   : words[1],
        'device_node_name'  : words[-1]
      })

    return ret

  def get_size(self, device):
    """
    Takes a device name 'device' (partition or disk) and returns the size of the
    disk in bytes.

    Returns -1 on failure.
    """
    # Get size from /proc/partitions. Skip first line.
    # Format is: major minor  #blocks(1K)  name
    device = device.split("/")[-1]
    ret, stdout, stderr = self.run_cmd_ssh("cat /proc/partitions")
    lines = stdout.strip().split("\n")
    for line in lines:
      line = line.strip()
      parts = line.split()
      if len(parts) != 4:
        continue
      if parts[3] == device:
        return int(parts[2]) * 1024

    return -1
  
  def get_disks_eligible_for_partitioning(self, min_disk_size_gb):
    """
    Returns the list of disks which are greated than the specified minimum
    disk size.
    Args:
      min_disk_size_gb(int): Minimum disk size in GB.
    """
    devs = self.lsscsi()
    devs = [dev['device_node_name'] for dev in devs
            if self.get_size(dev['device_node_name']) > (min_disk_size_gb * GB)]
    return devs

  def partitions(self):
    """
    Returns the list of formatted parititions available on the host.
    """
    ret, stdout, stderr = self.run_cmd_ssh(
      "python -c 'import glob; print glob.glob(\"/dev/sd[a-z][0-9]\") + \
      glob.glob(\"/dev/nvme[0-9]n[0-9]p[0-9]\")'")
    stdout = stdout.replace("[", "")
    stdout = stdout.replace("]", "")
    stdout = stdout.replace("'", "")
    stdout = stdout.replace(",", "")
    return stdout.strip().split()

  def make_label(self, disk, part_type="gpt"):
    """
    Set the partition type to the specified type.
    Args:
      disk(str): Device path.
      part_type(str): Type of partition to create. Defaults to gpt.
    """
    # Set the partition type to GPT for partition table for the disk.
    ret, stdout, stderr = self.run_cmd_ssh("sudo parted -s %s mklabel gpt" %
                                           disk)
    if ret:
      print("Unable to set the partition type for the "
            "partition table to GPT, ret %s, stdout %s, stderr %s" %
            (ret, stdout, stderr))
      return None

  def make_partition(self, disk, start, end, fs_type="ext4"):
    """
    Makes a partition on the disk with 'reserved_block_percent' percent blocks
    reserved for the super user from 'start' percentage to 'end' percentage on
    the disk.
    Args:
      disk(str): Path of the device.
      start(int): start location, default unit is %, can be overridden.
      end(int): end location, default unit is %, can be overridden.
      fs_type(str): Filesystem type for the partition. Defaults to ext4.

    Returns the partition name if successful, otherwise, returns None.
    """  
    # Create the partition from 'start' to 'end' on the disk.
    cmd = ("sudo parted -s -a opt %s unit %% mkpart primary %s %d %d" %
           (disk, fs_type, start, end))
    ret, stdout, stderr = self.run_cmd_ssh(cmd)
    if ret:
      print("Unable to create partition, ret %s, stdout %s, stderr %s" %
            (ret, stdout, stderr))
      return None

    partitions = self.partitions()
    return partitions[0]

  def run_cmd_ssh(self, cmd):
    """
    Runs a command on the host and returns the status code and stdout. Picked
    up from Nutest's ssh wrapper.
    Args:
      cmd(str): Command to be executed.

    Returns the status code, stdout and stderr.    
    """
    bufsize = 4096
    ssh_obj = paramiko.SSHClient()

    # Disable host key check
    ssh_obj.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_obj.set_log_channel('')
    
    # Initiate the SSH connection
    connection_attempt = 1
    ssh_obj.connect(
      self.host_ip,
      username=self.username,
      password=self.password,
      port=22
    )
    transport = ssh_obj.get_transport()
    chan = transport.open_session(timeout=10)

    stdout_data = ""
    stderr_data = ""
    chan.exec_command(cmd)

    while not chan.exit_status_ready():
      # read stdout and stderr regularly to prevent hangs when buffer gets
      # full.
      while chan.recv_ready():
        stdout_data += chan.recv(bufsize)
      while chan.recv_stderr_ready():
        stderr_data += chan.recv_stderr(bufsize)
      time.sleep(0.1)

    while True:
      temp_buffer = chan.recv(bufsize)
      # If a string of length zero is returned, the channel stream
      # has closed
      if not len(temp_buffer):
        break
      else:
        stdout_data += temp_buffer
    while True:
      temp_buffer = chan.recv_stderr(bufsize)
      if not len(temp_buffer):
        break
      else:
        stderr_data += temp_buffer

    status = chan.recv_exit_status()

    if status :
      print (status, stdout_data, stderr_data)

    return (status, stdout_data, stderr_data)

  def get_sd_to_id_mapping(self):
    """
    Get the /dev/sd<x> mapping to /dev/disk/by-id device

    Returns:
      dict: Keys of /dev/sd<x> and values of /dev/disk/by-id.
    """
    sd_to_id = {}
    status, stdout, stderr = self.run_cmd_ssh("ls -al /dev/disk/by-id")
    if status:
      print("Unable to fetch by-id paths: {}".format(stdout))
      return sd_to_id

    for line in stdout.splitlines():
      reg = re.search(r'(scsi-.*)\s*->\s*\.\.\/\.\.\/(\w+)', line)

      if reg:
        diskid = "/dev/disk/by-id/{}".format(reg.group(1))
        sd_dev = "/dev/{}".format(reg.group(2))
        sd_to_id[sd_dev.strip()] = diskid.strip()
    return sd_to_id

  def get_host_memory(self):
    """
    Get the total memory of the host.
    """
    cmd = "cat /proc/meminfo | grep MemTotal"
    status, stdout, stderr = self.run_cmd_ssh(cmd)
    return int(stdout.strip().split()[1])

  def is_device_SSD(self, device):
    """
    Check if the given device is an SSD.
    Args:
      device(str): Device path.
    Returns a boolean value indicating if the device is an SSD or not.
    """
    device = device.split("/")[-1]
    cmd = "cat /sys/block/%s/queue/rotational" % device
    status, stdout, stderr = self.run_cmd_ssh(cmd)
    return not int(stdout.strip())

  def get_hostname(self):
    """
    Returns the hostname of the host.
    """
    cmd = "cat /etc/hostname"
    status, stdout, stderr = self.run_cmd_ssh(cmd)
    return stdout.strip()

  def install_utils(self):
    """
    Install the utils needed by this script.
    """
    cmd = "sudo yum -y install parted"
    status, stdout, stderr = self.run_cmd_ssh(cmd)
    cmd = "sudo yum -y install lsscsi"
    status, stdout, stderr = self.run_cmd_ssh(cmd)

  def prepare_host(self):
    """
    Prepare the AHV host for partitioning.
    """
    # Remove the pci passthrough config.
    cmd = "rm -rf /etc/modprobe.d/pci-passthru.conf"
    status, stdout, stderr = self.run_cmd_ssh(cmd)
    # Rerun dracut
    cmd = "dracut -f"
    status, stdout, stderr = self.run_cmd_ssh(cmd)
    # Reboot the host
    cmd = "reboot"
    status, stdout, stderr = self.run_cmd_ssh(cmd)
    time.sleep(10)
    # Wait for the host to be back up.
    self.wait_for_host_to_be_accessible()

  def wait_for_host_to_be_accessible(self):
    """
    Wait for the AHV host to be accessible
    """
    retries = 60
    cmd = "ping -c 1 %s" % self.host_ip
    while retries > 0:
      result = os.system(cmd)
      print "result is %s" % result
      if result:
        time.sleep(5)
        retries -= 1
      else:
        return self.wait_for_ssh_ready()

    return False

  def wait_for_ssh_ready(self):
    """
    Waits till the host is reachable via SSH.
    """
    retries = 10
    cmd = "echo"
    while retries > 0:
      status, stdout, stderr = self.run_cmd_ssh(cmd)
      if status:
        time.sleep(5)
        retries -= 1
      else:
        return True

    return False
    
def main():
  # Parse the arguments.
  parser = argparse.ArgumentParser()
  parser.add_argument(
    "--min_disk_size_gb", action='store_true', default=300,
    help="Minimum disk size for the disks to be partitioned in GB")
  parser.add_argument(
    "--host_ip", action='store', required=True,
    help="IP address of the host")
  parser.add_argument(
    "--base_cvm_ip", action='store', required=True,
    help="IP address of the base CVM")
  parser.add_argument(
    "--num_nodes", action='store', default=2,
    help="Number of virtual nodes to create out of this node")  

  args = parser.parse_args()
  pd = PartitionDisks(args.host_ip)
  print "Installing utils needed for this script"
  pd.install_utils()
  pd.prepare_host()
  disks = pd.get_disks_eligible_for_partitioning(args.min_disk_size_gb)
  disk_partitions = {}
  num_ssds = 0
  num_hdds = 0
  print "Creating partitions"
  for disk in disks:
    if pd.is_device_SSD(disk):
      disk_type = "SSD"
      num_ssds += 1
    else:
      disk_type = "HDD"
      num_hdds += 1

    disk_size  = pd.get_size(disk)
    pd.make_label(disk)
    # Divide the disk into equal sized partitions. We are dealing in
    # percentages. So we divide 100 into parts.
    partition_size = 100 / args.num_nodes
    partition_size_bytes = disk_size / args.num_nodes
    start = 0
    end = partition_size
    for count in xrange(args.num_nodes):
      partition = pd.make_partition(disk, start, end)
      disk_partitions[partition] = {"size": partition_size_bytes,
                                    "disk_type": disk_type}
      start = start + partition_size + 1
      end = end + partition_size

  host_mem = pd.get_host_memory()
  hostname = pd.get_hostname()
  jarvis_data_v1 = {"data":
    { "hardware":
      {
        "storage": [],
        "mem": host_mem/2
      },
    "name": hostname + "_v1",
    "base_cvm_ip": args.base_cvm_ip,
    "base_host_ip": args.host_ip
    }
  }

  jarvis_data_v2 = {"data":
    { "hardware":
      {
        "storage": [],
        "mem": host_mem/2
      },
    "name": hostname + "_v2",
    "base_cvm_ip": args.base_cvm_ip,
    "base_host_ip": args.host_ip
    }
  }

  sd_to_id_mapping = pd.get_sd_to_id_mapping()
  for partition, details in disk_partitions.iteritems():
    details["disk_path"] = sd_to_id_mapping[partition]
    if details["disk_type"] == "SSD" and num_ssds > 0:
      jarvis_data_v1["data"]["hardware"]["storage"].append(details)
      num_ssds -= 1
    elif details["disk_type"] == "HDD" and num_hdds > 0:
      jarvis_data_v1["data"]["hardware"]["storage"].append(details)
      num_hdds -= 1
    else:
      jarvis_data_v2["data"]["hardware"]["storage"].append(details)    

  print jarvis_data_v1
  print jarvis_data_v2

if __name__ == "__main__":
  main()