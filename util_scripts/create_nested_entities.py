"""
Script to create partitions on AHV hosts and create Jarvis entries of virtual
node.
Author: gokul.kannan@nutanix.com, datta.maddikunta@nutanix.com
Copyright (c) 2019 Nutanix Inc. All rights reserved.

Usage:
./partition_disks_ahv.py [--host_ip=<host ip>] --base_cluster_name=<cluster_name>
  --num_nodes=<num nodes to create>

"""
import sys
sys.path.append("/opt/salt/lib/python2.7/site-packages")

import argparse
import glob
import paramiko
import re
import time
import json
import os
import requests
import pdb

from requests.packages.urllib3.exceptions import InsecurePlatformWarning, \
  InsecureRequestWarning, SNIMissingWarning

# Disable emitted warnings due to lack of authentication.
warnings = [InsecurePlatformWarning, InsecureRequestWarning, SNIMissingWarning]

for warning in warnings:
  requests.packages.urllib3.disable_warnings(warning)

GB = 1024 * 1024 * 1024

class Util(object):
  """
    Generic methods for dealing with hosts and partitions.
  """

  def __init__(self, ip, username, password):
    """
    Initialize stuff.
    """

    self.ssh = paramiko.SSHClient()
    self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    self.ip = ip
    self.username = username
    self.password = password

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

    try:
      ssh_obj.connect(
        self.ip,
        username=self.username,
        password=self.password,
        port=22)
    except Exception as e:
      return (1, e, e)
  
    transport = ssh_obj.get_transport()
    chan = transport.open_session(timeout=100)

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


class Cluster(object):
  """
    Collection's of methods to fetch cluster information.
  """  

  # JARVIS_SERVER = "jarvis.eng.nutanix.com"
  JARVIS_SERVER = "10.48.22.175:8002"
  CLUSTER_API_URL = "api/v1/clusters"

  def __init__(self):
    pass 

  def get_cluster_info(self, base_cluster_name):
    """
    Runs a API call on the Jarvis database and fetch the cluster information.
    Get the Object Ids of the base cluster and the nodes from the API response.

    Args:
      base_cluster_name: Base cluster name.

    Returns the cluster information.    
    """

    url = "http://%s/%s/%s" % (Cluster.JARVIS_SERVER, Cluster.CLUSTER_API_URL, base_cluster_name)
    print "DATTA: URL", url
    req = requests.get(url=url, params={}, 
                       timeout=150,
                       verify=False)

    print "Response from Cluster %s: %s" %(base_cluster_name, req.status_code)

    assert req.ok, "Unexpected error %d when fetching %s" % (req.status_code,
                                                             req.url)

    return req.content

  def get_object_id(self, base_cluster_name, host_ip):
    """
    Runs a API call on the Jarvis database and fetch the cluster information.
    Get the Object Ids of the base cluster and the nodes from the API response.

    Args:
      base_cluster_name: Base cluster name.
      host_ip: IP address of CO host(Hypervisor).

    Returns the object Ids.    
    """
    oids = {}

    content = self.get_cluster_info(base_cluster_name)

    resp = json.loads(content)
    for node in resp['data']['nodes']:
      if node['hypervisor']['ip'] == host_ip:
        oids[host_ip] = node['_id']['$oid']

    # Add the base cluster oid
    oids[base_cluster_name] = resp['data']['_id']['$oid']

    return oids

  def get_base_cluster_cvm_ip(self, base_cluster_name):
    """
    Get the base cluster CVM IP address from the cluster info.

    Args:
      base_cluster_name: Base cluster name.

    Returns the base CVM IP address.
    """

    content = self.get_cluster_info(base_cluster_name)

    resp = json.loads(content)

    for node in resp['data']['nodes']:
      # One CVM IP serves the purpose.
      # Return the first such node.

      if node['hypervisor']['node_type'] != "COMPUTE_ONLY":
        return node['svm_ip']

  def get_co_hosts(self, svm_ip):
    """
     Fetches all the Compute Only(CO) hosts on the cluster.
    """   
    hosts = []

    util = Util(svm_ip, username="nutanix", password="nutanix/4u")

    cmd = "/usr/local/nutanix/bin/acli -o json host.list"
    status, stdout, stderr = util.run_cmd_ssh(cmd)

    d = json.loads(stdout)
    for item in d['data']:
      if item["compute_only"]:
        hosts.append(item['hypervisorAddress'])

    return hosts
     

def populate_jarvis_info(args, object_ids, partition_data):
  """ 
    Arrange data for Jarvis API call.
  """ 

  jarvis_data = {"data":
    {   
        "name": "",
        "ram_quota": "",
        "cpu_num_quota": "",
        "base_cvm_ip": "",
        "base_cluster": {
           "$oid": ""
        },
        "node": {
           "$oid": ""
        },
        "partitions": []
   }}
    

  jarvis_data['data']['name'] = partition_data['hostname']
  jarvis_data['data']['ram_quota'] = partition_data['host_mem']
  jarvis_data['data']['cpu_num_quota'] = partition_data['host_cpus']
  jarvis_data['data']['base_cvm_ip'] = args.base_cvm_ip
  jarvis_data['data']['base_cluster']['$oid'] = object_ids[args.base_cluster_name]
  jarvis_data['data']['node']['$oid'] = object_ids[args.host_ip]
  jarvis_data['data']['partitions'] = partition_data['partitions']

  return jarvis_data


def upload_to_jarvis_database(data):
  """
    Upload partition data to Jarvis database.
  """

  # Below are sandbox values, for testing only.

  TMP_JARVIS_SERVER = "10.48.22.175"
  TMP_JARVIS_PORT = 8002   
  TMP_NESTED_URL = "api/v2/nested_entities/"

  url = "http://%s:%s/%s" % (TMP_JARVIS_SERVER, str(TMP_JARVIS_PORT), TMP_NESTED_URL)

  req = requests.post(url=url, 
  		      json=data['data'], 
                      # auth = ('AHV2.0', 'qRSgGZrQ?F%#kcPn2NiT'),
                      auth = ('datta.maddikunta', '2700@Irontiger'),
                      verify=False)

  print "Upload Status", req.content

  assert req.ok, "Unexpected error %d when posting %s" % (req.status_code,
                                                          req.url)
    

class PartitionDisks(Util):
  """
  This class contains methods to partition the disks on AHV host.
  """
  def __init__(self, host_ip, username='root', password='nutanix/4u'):
    """
    Initialize stuff.
    """
    Util.__init__(self, host_ip, username, password)

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

  def get_no_of_cpus(self):
    """
    Get the no of CPUs of the host.
    """
    cmd = "nproc"
    status, stdout, stderr = self.run_cmd_ssh(cmd)
    return int(stdout.strip())

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
    cmd = "ping -c 1 %s" % self.ip

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
    
  def partition_disks(self, args):
    """
      Create virtual nodes on each host.
    """

    jarvis_data = []

    print "Installing utils needed for this script"

    self.install_utils()

    print "Preparing the host %s for partition." %(self.ip)
    self.prepare_host()

    disks = self.get_disks_eligible_for_partitioning(args.min_disk_size_gb)

    print "Creating partitions"

    hostname = self.get_hostname()
    host_mem = self.get_host_memory()
    host_cpus = self.get_no_of_cpus()

    # All the partitions on a single disk will be held by disk_partitions_per_disk.
    disk_partitions_per_disk = []

    for disk in disks:
      disk_type = "SSD" if self.is_device_SSD(disk) else "HDD"

      disk_size  = self.get_size(disk)
      self.make_label(disk)

      # Divide the disk into equal sized partitions. We are dealing in
      # percentages. So we divide 100 into parts.

      partition_size = 100 / args.num_nodes
      partition_size_bytes = disk_size / args.num_nodes

      start = 0
      end = partition_size

      disk_partitions_local = []

      for count in xrange(args.num_nodes):
        partition = self.make_partition(disk, start, end)
        disk_partitions_local.append({"name": partition,
				      "size": partition_size_bytes/GB,
                                      "type": disk_type})

        start = start + partition_size + 1
        end = end + partition_size
      
      disk_partitions_per_disk.append(disk_partitions_local)

    sd_to_id_mapping = self.get_sd_to_id_mapping()

    # Arrange data for the Jarvis Upload API.
    # Arranges one entry for each nested partition.

    for count in xrange(args.num_nodes):
      disk_partition_data = {}

      disk_partition_data['host_ip'] = self.ip
      disk_partition_data['host_cpus'] = host_cpus/args.num_nodes
    
      # Host memory is in KB, convert to GB.
      disk_partition_data['host_mem'] = host_mem/(args.num_nodes * 1024 * 1024)

      # Hostname is <hostname>_NESTED_1 etc
      disk_partition_data['hostname'] = hostname + "-nested-" + str(count)

      disk_partitions_per_nested_entity = []

      for disk_entry in disk_partitions_per_disk:
        # Replace partition name with its disk-path.
        disk_entry[count]['name'] = sd_to_id_mapping[disk_entry[count]['name']]
        disk_partitions_per_nested_entity.append(disk_entry[count])
   
      disk_partition_data['partitions'] = disk_partitions_per_nested_entity 
      jarvis_data.append(disk_partition_data)

    return jarvis_data

def main():
  # Parse the arguments.
  parser = argparse.ArgumentParser()
  parser.add_argument(
    "--min_disk_size_gb", action='store_true', default=300,
    help="Minimum disk size for the disks to be partitioned in GB")
  parser.add_argument(
    "--host_ip", action='store', required=False,
    help="IP address of the host")
  parser.add_argument(
    "--base_cluster_name", action='store', required=True,
    help="Cluster name")
  parser.add_argument(
    "--num_nodes", action='store', default=2,
    help="Number of virtual nodes to create out of this node")  
  parser.add_argument(
    "--base_cvm_ip", action='store',
    help="IP of the base CVM")  


  args = parser.parse_args()

  args.num_nodes = int(args.num_nodes)

  hosts = []
  cluster_jarvis_data = []

  #  Cluster object to interact with the cluster.
  cluster_obj = Cluster()

  # Check host_ip argument is provided.
  if args.host_ip:
    # Use the host provided.
    hosts.append(args.host_ip)
  else:
    # Get the base CVM IP from the cluster name.
    base_cvm_ip = cluster_obj.get_base_cluster_cvm_ip(args.base_cluster_name)
#    args.base_cvm_ip = base_cvm_ip 


    # Get the CO nodes for the SVM IP.
    hosts = cluster_obj.get_co_hosts(args.base_cvm_ip)

  # Partition disks on each host.
  for host in hosts:
    pd = PartitionDisks(host)
    disk_partition_data = pd.partition_disks(args)

    args.host_ip = host
    # Get the object Ids needed for Jarvis upload. 
    object_ids = cluster_obj.get_object_id(args.base_cluster_name, host)

    # Create a message format for Jarvis API for each nested entity.
    for data in disk_partition_data:
      cluster_jarvis_data.append(populate_jarvis_info(args, object_ids, data))

  # Upload each of the partition data to the Jarvis database.
  for data in cluster_jarvis_data:
    print "Uploading to JARVIS:", data
    upload_to_jarvis_database(data)
    print "DONE"
       
if __name__ == "__main__":
  main()
