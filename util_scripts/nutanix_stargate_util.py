#
#  Copyright (c) 2014 Nutanix Inc. All rights reserved.
#
#  Author: jason.chan@nutanix.com
#
"""
#  This class supplies critical elements useful to doing automated stargate
#  testing. This includes functions useful in obtaining inforamtion
#  from oplog store, extent store etc.
"""
#
#  Intialize an instance of the class by supplying agave cluster object and
#  then use that instance to call methods defined here.
from datetime import timedelta
import json
import re
import time
import urllib2
import tempfile

from qa.agave.agave_util import AgaveUtil
from qa.util.agave_tools.nutanix_disk_util import NutanixDiskUtil
from qa.util.agave_tools.nutanix_web import StargateWeb
from qa.util.agave_tools.nutanix_medusa_test_client\
     import NutanixMedusaTestClient
from qa.util.agave_tools.nutanix_curator_test_client\
     import NutanixCuratorTestClient
from qa.util.agave_tools.nutanix_curator_util import NutanixCuratorUtil
from qa.util.agave_tools.vdisk_usage_util import VdiskUsageUtil
from qa.agave.nutanix_cluster_ssh_util import NutanixClusterSSHUtil
from stats.stats_pb2 import VDiskUsageStatProto

from util.base.log import *

class NutanixStargateUtil(object):
  """
  This class supplies functions useful for writing stargate automated tests.

  The public functions available are as below:
    get_episode_list(vdisk_id)
    get_vdisk_oplog_data_size(svm, vdisk_id)
    get_primary_disk_id_to_episode_sequence_map(vdisk_id)
    get_episode_disks_list(episode_list)
    get_episode_count_by_disk_id(episode_list)
    get_disk_serial_id_to_oplog_disk_size_map()
    set_stargate_gflags_using_http(svm, gflag, value)
    map_egroup_id_to_disk_replica_count(svms=None, egroup_ids=None)
    get_egroup_paths_by_egroup_id(egroup_ids=None)
    map_egroup_id_to_disk_serials(svms=None, egroup_ids=None)
    verify_egroup_replica_counts(replica_count_by_egroup_id,
                                 expected_rf, expect_str)
    get_egroup_ids_of_vdisk(vdisk_id)
    map_vdisk_egroup_id_to_replica_count(vdisk_id)
    map_vdisk_egroup_ids_to_replicas(vdisk_id)
    extract_vdisk_egroup_ids_to_vblocks(vdisk_id, file_size)
    get_compressed_egroup_ids_of_vdisk(vdisk_id)
    get_egroup_id_to_egroup_map(egroup_ids=())
    get_egroup_id_to_disk_id_map_of_vdisk(vdisk_id)
    get_egroup_id_to_svm_id_map_of_vdisk(vdisk_id)
    get_egroup_id_to_disk_id_map(egroup_ids=())
    get_egroup_id_to_write_time_map(egroup_ids=())
    get_egroup_id_to_garbage_size_map_of_vdisk(vdisk_id)
    get_vdisk_size_stats(vdisk_id)
    get_vdisk_hosting_svm_ip(self, vdisk_name)
  """

  def __init__(self, cluster):
    """
    This initializes the class.

    Args:
      cluster : Nutanix cluster object.
    """
    self.__cluster = cluster
    self.__zeus_config = self.__cluster.get_zeus_config()
    CHECK(self.__zeus_config, "Failed to get zeus_config_printer")
    self.__medusa_client = NutanixMedusaTestClient(self.__cluster)
    CHECK(self.__medusa_client, "Failed to get medusa_printer")
    self.__disk_util = NutanixDiskUtil(self.__cluster)
    CHECK(self.__disk_util, "Failed to get disk_util")
    self.__vdisk_usage_util = VdiskUsageUtil(self.__cluster)
    CHECK(self.__disk_util, "Failed to get vdisk_usage_util")
    self.__curator_client = NutanixCuratorTestClient(self.__cluster)
    CHECK(self.__curator_client, "Failed to get curator_test_client")

  def teardown(self):
    """
    This method is a place holder for explicit clean up of objects used.
    """
    succeeded = True
    return succeeded

  @staticmethod
  def set_stargate_gflags_using_http(svm, gflag, value):
    """
    Set gflag value for a given service vm object's stargate process.

    Args:
      svm (service vm object) : The object represents the service vm on
                                which the gflag has to be set.
      gflag (string) : This is the name of the gflag which needs to be set.
      value (string/bool/int) : This is the value of the gflag chosen.

    Returns:
      bool : True if successful, False otherwise.

    Example:
      set_stargate_gflags_using_http(svm, "stargate_max_oplog_size_GB", 1)
      The above example will set the stargate_max_oplog_size_GB gflag to value 1
      on the svm provided in the function call.
    """

    gflag_string = "%s=%s" % (gflag, value)
    svm_ip = svm.ip()
    url = "http://%s:2009/h/gflags?%s" % \
          (svm_ip, gflag_string)
    response = urllib2.urlopen(url).read()
    gflag_status = re.search("Successfully set", response)
    if gflag_status:
      INFO("===== Gflag: %s successfully set =====" % gflag_string)
      return True
    INFO("===== Gflag: %s could not be set successfully ====="
         %  gflag_string)
    return False

  def get_disk_serial_id_to_oplog_disk_size_map(self):
    """
    This returns a dict object of SSD serial ID to it's oplog disk size.

    Note:
      We only consider SSD disk serial IDs in this method as oplog store
      resides only on the SSD tier.

    Returns:
      dict (string, int) : map of disk serial ID to oplog disk size on it.
    """
    disk_serial_id_to_oplog_disk_size_map = {}
    for disk in self.__zeus_config.disk_list:
      is_ssd = re.search("SSD", disk.storage_tier)
      if is_ssd:
        disk_serial_id_to_oplog_disk_size_map[disk.disk_serial_id] = \
          disk.oplog_disk_size
    INFO("disk_serial_id_to_oplog_disk_size_map: %s" % \
      disk_serial_id_to_oplog_disk_size_map)
    return disk_serial_id_to_oplog_disk_size_map

  def get_disk_serial_id_to_egroup_disk_usage_map(self):
    """
    Returns a dict with keys as the disk_serial_id and the value as
    egroup_disk_usage.
    """
    return self.get_disk_serial_id_to_star_disk_usage_map("egroup")

  def get_disk_serial_id_to_oplog_disk_usage_map(self):
    """
    Returns a dict with keys as the disk_serial_id and the value as
    oplog_disk_usage.
    """
    return self.get_disk_serial_id_to_star_disk_usage_map("oplog")

  def get_disk_serial_id_to_star_disk_usage_map(self, star_type):
    """
    Returns a dict with keys as the disk_serial_id and the value as
    star_type disk_usage.  star_type is the type of stargate-storage,
    either "oplog" metadata or "egroup" data.
    """
    disk_serial_id_to_star_disk_usage_map = {}
    disk_usage_re = re.compile(r'(\d+)\s+\S+')
    if star_type == "oplog":
      star_path = "/metadata/stargate-storage/oplog-store"
    elif star_type == "egroup":
      star_path = "/data"

    for disk in self.__zeus_config.disk_list:
      # We expect oplog disk_usage to reside solely on the SSD tier.
      if (star_type == "oplog") and (not "SSD" in disk.storage_tier):
        continue
      cmd = "du -sk " + disk.mount_path + star_path
      # Find the right svm to run the du command on
      svm = self.__disk_util.get_svm_of_disk(disk.disk_id)
      if not svm:
        continue  # offline disks have svm_id 0, no svm
      # Run du on the svm of the node which hosts the disk.
      rv, stdout, stderr = svm.execute(cmd)

      m = disk_usage_re.search(stdout)
      if m and m.group(1):
        disk_usage_kb = int(m.group(1))
        disk_serial_id_to_star_disk_usage_map[disk.disk_serial_id] = \
          disk_usage_kb * 1024
        INFO("disk_serial %18s uses %s %9d KB, du -s out:\n\t%s" %
             (disk.disk_serial_id, star_type, disk_usage_kb, stdout))
      else:
        WARNING("disk_usage not found for cmd %s, rv %s, out %s, err %s" %
                (cmd, rv, stdout, stderr))

    return disk_serial_id_to_star_disk_usage_map

  def get_episode_list(self, vdisk_id):
    """
    This provides a list of oplog episode objects for a vdisk.

    Args:
      vdisk_id (int) : Represents the vdisk id of the intended vdisk.

    Returns:
      List (episode objects): list of episode objects if successful,
         None otherwise.
    """
    (ret, medusa_proto) = self.__medusa_client.lookup_vdisk_oplog_map(vdisk_id)
    if medusa_proto and hasattr(medusa_proto, "oplog_map_entry"):
      episode_list = medusa_proto.oplog_map_entry.episodes
      return episode_list
    INFO("Medusa did not return an oplog map (%s, %s)" % (ret, medusa_proto))
    return []

  def get_episode_disks_list(self, episode_list):
    """
    This returns lists of primary and secondary episode replica disk ids.

    Args:
      episode_list (list of episode objects): List of episode objects
      containing some metadate related to oplog episode files.

    Note:
      There is a check to make sure that episode replicas exist on different
      hosts.

    Returns:
      List (List of int lists) :First list has primary replica
        disk ids of the episodes. Second list has secondary replica disk ids
        of the episodes
    """
    all_primary_disks = []
    all_replica_disks = []
    # get_disk_id_to_cvm_id_map will allow us to check the owner CVM for a
    # given disk_id
    disk_id_to_cvm_id_map = self.__disk_util.get_disk_id_to_cvm_id_map()
    for episode in episode_list:
      primary_disk = int(episode.primary_disk)
      replica_disks = []
      hosts_per_replica = []
      for secondary_stripe in episode.secondary_stripes:
        replica_disk = int(secondary_stripe.replica_disks[-1])
        replica_disks.append(replica_disk)

      # Check that primary and replica episodes are on different hosts.
      hosts_per_replica.append(disk_id_to_cvm_id_map[primary_disk])
      for replica_disk in replica_disks:
        hosts_per_replica.append(disk_id_to_cvm_id_map[replica_disk])
      CHECK_EQ(len(set(hosts_per_replica)), len(replica_disks) + 1,
        "Multiple episodes exist on a single host. %s" % episode)

      # After all checks are done, append all replicas to all lists.
      all_primary_disks.append(primary_disk)
      all_replica_disks.extend(replica_disks)
    INFO("episode list of primary episodes: %s" % all_primary_disks)
    INFO("episode list of replica episodes: %s" % all_replica_disks)
    return [all_primary_disks, all_replica_disks]

  @staticmethod
  def get_episode_count_by_disk_id(episode_list):
    """
    This returns the number of episodes per disk.

    Args:
      episode_list (List of episode objects) : List of objects containing
        per episode information of a vdisk.

    Returns:
      map (int , int) : Disk id to number of episodes in its oplog store.
    """
    # Populate a list prior to keeping count.
    episode_count_by_disk_id = {}
    for disk in episode_list:
      if episode_count_by_disk_id.has_key(disk):
        episode_count_by_disk_id[disk] += 1
      else:
        episode_count_by_disk_id[disk] = 1
    INFO("episode_count_by_disk_id %s" % episode_count_by_disk_id)
    return episode_count_by_disk_id

  @staticmethod
  def get_vdisk_oplog_data_size(svm, vdisk_id):
    """
    Get the vdisk oplog data size.

    Args:
      svm (service vm object): The service vm object which hosts the
        vdisk.
      vdisk_id (int) : This represents the vdisk id of the intented vdisk.

    Returns:
       int : Oplop data size in kilobytes for vdisk if found,
       bool: False if not found.
    """
    stargate_page = StargateWeb(svm.ip(), 2009, 600)
    hosted_vdisk_table = stargate_page.get_hosted_vdisk_table()
    hosted_vdisk_dicts = stargate_page.parse_table(hosted_vdisk_table)
    for line in hosted_vdisk_dicts:
      vd_id = line["VDisk Id"]
      if int(vd_id) == vdisk_id:
        index_size = int(line["Oplog - KB"])
        return index_size
      else:
        return False

  def get_primary_disk_id_to_episode_sequence_map(self, vdisk_id):
    """
    Get primary disk id to its episode objects map.

    Args:
      vdisk_id (int) : This represents the vdisk id of the intented vdisk.

    Returns:
      map (int, list of episode objects) : Keys are primary disk-id and the
      values is a list of episode objects for a given vdisk_id.
    """
    disk_id_to_episode_sequence_map = {}
    (ret, medusa_proto) = self.__medusa_client.lookup_vdisk_oplog_map(vdisk_id)
    if not (medusa_proto and hasattr(medusa_proto, "oplog_map_entry")):
      INFO("Medusa did not return an oplog map (%s, %s)" % (ret, medusa_proto))
      return disk_id_to_episode_sequence_map
    episode_list = medusa_proto.oplog_map_entry.episodes
    for episode in episode_list:
      episode_seq_list = []
      current_disk_id = int(episode.primary_disk)
      if disk_id_to_episode_sequence_map.has_key(current_disk_id):
        episode_seq_list = disk_id_to_episode_sequence_map[current_disk_id]
        episode_seq_list.append(episode)
        disk_id_to_episode_sequence_map[current_disk_id] = episode_seq_list
      else:
        episode_seq_list.append(episode)
        disk_id_to_episode_sequence_map[current_disk_id] = episode_seq_list
    return disk_id_to_episode_sequence_map

  def map_egroup_id_to_disk_replica_count(self, svms=None, egroup_ids=None):
    """
    Count the number of replicas on disk for each egroup_id.

    Args:
      svms (List of service vm objects, optional) : Limits the search to disks
        attached to these svms in the cluster.
      egroup_ids (List of int): Limits the search to specified egroup ids.

    Note:
      Each <egid>.egroup file is a replica. It exists on a physical disk,
      and its file size, shown by ls, is often but not always 4MB.
      Disks holding replicas must be in the same storagepool and container,
      but spread across different nodes for resilience.

    Returns:
      dict (int, int): egroup ids to number of replicas it has.
    """
    egroup_id_to_disk_replica_count_map = dict()
    (egroup_id_to_disk_serial_list, _disk_serial_to_egroup_size_map) = \
        self.map_egroup_id_to_disk_serials(svms, egroup_ids)
    for egroup_id in egroup_id_to_disk_serial_list.keys():
      egroup_id_to_disk_replica_count_map[egroup_id] = \
          len(egroup_id_to_disk_serial_list[egroup_id])
    return egroup_id_to_disk_replica_count_map

  def get_egroup_paths_by_egroup_id(self, egroup_ids=None):
    """
    Get egroup disk paths of the specified egroup ids or all.

    Args:
      egroup_ids (list of int, optional) : List of egroup ids.
        Search is limited to specified egroups if passed in the
        argument.

    Returns:
      dict (int, string): map of egroup id as key to egroup paths
        in the cluster.
    """
    if egroup_ids:
      num_egroup_ids = len(egroup_ids)
    else:
      num_egroup_ids = 0  # object NoneType has no len
    INFO("map_egroup_id_to_egroup_paths(egroup_ids=%s) num_egids %d" %
         (egroup_ids, num_egroup_ids))
    egroup_id_to_egroup_paths_map = dict()
    find_err_prune = "-name 'lost+found' -prune -o "
    svms = self.__cluster.svms()
    for svm in svms:
      find_cmd = ("find -L /home/nutanix/data/stargate-storage/disks " +
                 find_err_prune + "-name ")
      if num_egroup_ids == 1:
        find_cmd += ('"%d.egroup"' % egroup_ids[0])
      else:
        find_cmd += '"*.egroup"'

      rv, stdout, stderr = svm.execute(find_cmd)
      if rv == 0:
        INFO("cmd '%s' on SVM %s rv %s stdout\n%s\nstderr\n%s" %
           (find_cmd, svm.ip(), rv, stdout[0:1023], stderr))
      else: # Dont give up just because permission denied on lost+found.
        WARNING("cmd '%s' on SVM %s rv %s stdout\n%s\nstderr\n%s" %
           (find_cmd, svm.ip(), rv, stdout[0:1023], stderr))
      for egroup_path in stdout.split('\n'):
        m = re.search(r'/(\d+)\.egroup$', egroup_path)
        if not m or not m.group(1): # Skip blank and unparsable lines.
          DEBUG("skip egroup_path %s match m %s" % (egroup_path, m))
          continue
        egroup_id = int(m.group(1))

        DEBUG("egroup_id %s, egroup_path %s" % (egroup_id, egroup_path))
        # Create dict of egroup_paths for each egroup_id specified.
        if egroup_id in egroup_id_to_egroup_paths_map.keys():
          egroup_id_to_egroup_paths_map[egroup_id].append(egroup_path)
          # If the caller asked for certain egroup_ids, ignore the unlisted.
        elif (egroup_id in egroup_ids) or (not egroup_ids):
          egroup_id_to_egroup_paths_map[egroup_id] = [egroup_path]

    if not egroup_id_to_egroup_paths_map.keys():
      ERROR("No Egroups found!")
    else:
      INFO("paths found for %d egroup ids" %
           len(egroup_id_to_egroup_paths_map.keys()))
    DEBUG("egroup_id_to_egroup_paths_map %s\n" %
          json.dumps(egroup_id_to_egroup_paths_map, indent=2))
    return egroup_id_to_egroup_paths_map

  def map_egroup_id_to_disk_serials(self, svms=None, egroup_ids=None):
    """
    Get egroup id to disk serial map and disk serial to egroup size map.

    Args:
      svms (service vm objects, optional) : Limit search to svms specified in
        the argument.
      egroup_ids (list of int) : Limit search to egroup ids specified in the
        the argument.

    Returns:
      dict (int, string) : map of egroup id to disk serial id.
      dict (string, int) : map of disk serial id to egroups size on the disk.
    """
    egroup_id_to_disk_serials_map = dict()
    disk_serial_to_egroup_size_map = dict()
    # find lost+found prune lists non-traversed dirname on stdout,
    # which leads to xargs ls permission denied
    if not svms:
      svms = self.__cluster.svms()
    for svm in svms:
      find_cmd = ("cd /home/nutanix/data/stargate-storage/disks;" +
                  'find -L . -name "*.egroup" | ' +
                  "xargs ls -sd --block-size=1024")
      rv, stdout, stderr = svm.execute(find_cmd)
      DEBUG("cmd '%s' on SVM %s rv %s stdout\n%s\nstderr\n%s" %
           (find_cmd, svm.ip(), rv, stdout, stderr))
      if rv != 0:
        WARNING("Unable to find phys egroup files on svm %s: %s" %
                (svm.ip(), stderr))
        continue
      for egroup_path in stdout.split('\n'):
        m = re.search(r'^ *(\d+) \.\/([a-zA-Z0-9:]+).*/(\d+)\.egroup$',
                      egroup_path)
        if not m or not m.group(3):
          if len(egroup_path) > 0:  # Skip blank and unparsable lines.
            INFO("egroup_path %s match m %s" % (egroup_path, m))
          continue
        egroup_size_kb = int(m.group(1))  # str + str is not a sum op
        disk_serial = m.group(2)
        egroup_id = int(m.group(3))

        egroup_size = egroup_size_kb * 1024
        # If the caller asked for certain egroup_ids, ignore the others.
        if egroup_ids and (not egroup_id in egroup_ids):
          continue
        # Create list of disk_serials for each egroup file.
        if egroup_id in egroup_id_to_disk_serials_map.keys():
          egroup_id_to_disk_serials_map[egroup_id].append(disk_serial)
        else:
          egroup_id_to_disk_serials_map[egroup_id] = [disk_serial]

        # Sum the size of egroup files on each disk_serial.
        if disk_serial_to_egroup_size_map.has_key(disk_serial):
          disk_serial_to_egroup_size_map[disk_serial] += egroup_size
        else:
          disk_serial_to_egroup_size_map[disk_serial] = egroup_size

    if not egroup_id_to_disk_serials_map.keys():
      ERROR("No Egroups found!")
    DEBUG("egroup_id_to_disk_serials_map %s\n" %
          json.dumps(egroup_id_to_disk_serials_map, indent=2))
    for disk_serial in disk_serial_to_egroup_size_map.keys():
      INFO("disk_serial %18s has egroup disk usage %9d KB" %
           (disk_serial, disk_serial_to_egroup_size_map[disk_serial] / 1024))
    return (egroup_id_to_disk_serials_map, disk_serial_to_egroup_size_map)

  def check_primary_vs_secondary_vdisk_oplog_usage(
          self, vdisk_ids, disk_serial_to_oplog_size_map, expected_rf):
    """
    For each vdisk in the list of vdisk_ids given,
    check that the vdisk uses the same amount of disk space for each
    oplog replica.
    """
    vdisk_id_to_episode_list_map = dict()
    disk_usage_re = re.compile(r'(\d+)\s+\S+')
    for vdisk_id in vdisk_ids:
      INFO("comparing primary and replica sizes for vdisk %s" % vdisk_id)

      # Call wrapper around medusa_client.lookup_vdisk_oplog_map(vdisk_id)
      vdisk_id_to_episode_list_map[vdisk_id] = \
          self.get_episode_list(vdisk_id)
      (all_primary_disks, all_replica_disks) = \
          self.get_episode_disks_list(
              vdisk_id_to_episode_list_map[vdisk_id])
      primary_disk_usage = 0
      for disk_id in all_primary_disks:
        disk_serial = self.__disk_util.get_disk_serial(disk_id)
        primary_disk_usage += disk_serial_to_oplog_size_map[disk_serial]
        INFO("disk id %5s serial %18s oplog primary_disk_usage_sum %12d" %
             (disk_id, disk_serial, primary_disk_usage))
      replica_disk_usage = 0
      for disk_id in all_replica_disks:
        disk_serial = self.__disk_util.get_disk_serial(disk_id)
        replica_disk_usage += disk_serial_to_oplog_size_map[disk_serial]
        INFO("disk id %5s serial %18s oplog replica_disk_usage_sum %12d" %
             (disk_id, disk_serial, replica_disk_usage))
      # Be aware that primary_disk_usage differs from replica_disk_usage if
      # there are multiple vdisks in play.  Equality test must be vdisk-based
      # rather than phys-disk based.

      vdisk_size_list = []
      # For all primary and replica disk_ids of each vdisk,
      # determine the svm and disk_serial from the disk id,
      # then determine the size of the disk_serial/oplog-store/vdisk.
      # Append each size to vdisk_size_list, in primary, replica order.
      for disk_id in all_primary_disks + all_replica_disks:
        svm = self.__disk_util.get_svm_of_disk(disk_id)
        if not svm:
          continue  # offline disks have svm_id 0, no svm
        disk_serial = self.__disk_util.get_disk_serial(disk_id)
        cmd = ("du -sk /home/nutanix/data/stargate-storage/disks/%s"
               "/metadata/stargate-storage/oplog-store/vdisk-%d" %
               (disk_serial, vdisk_id))
        rv, stdout, stderr = svm.execute(cmd)
        if rv != 0:
          ERROR("svm_ip %s cmd %s rv %d stderr %s" %
                (svm.ip(), cmd, rv, stderr))
          continue
        m = disk_usage_re.search(stdout)
        if m and m.group(1):
          vdisk_size_kb = int(m.group(1))
          INFO("vdisk id %d, kb %s per `du -s` stdout\n\t%s" %
               (vdisk_id, vdisk_size_kb, stdout))
          if vdisk_size_kb:
            vdisk_size_list.append(vdisk_size_kb)
          if len(vdisk_size_list) == 2:
            self.compare_pct_diff("vdisk_size1", vdisk_size_list[0],
                                  "vdisk_size2", vdisk_size_list[1])
          elif len(vdisk_size_list) == 3:
            self.compare_pct_diff("vdisk_size1", vdisk_size_list[0],
                                  "vdisk_size3", vdisk_size_list[2])
      INFO("Found %d disks using oplog-store/vdisk-%d sizes %s" %
              (len(vdisk_size_list), vdisk_id, vdisk_size_list))
      if len(vdisk_size_list) != expected_rf:
        ERROR("Expected %d but found %d disks using oplog-store/vdisk-%d %s"
              % (expected_rf, len(vdisk_size_list), vdisk_id, vdisk_size_list))

  @staticmethod
  def compare_pct_diff(key1_name, key1_value, key2_name, key2_value,
                       max_pct_diff=5.0):
    """ Report PASS if key1 is within max_pct_diff of key2, else FAIL. """
    diff = key2_value - key1_value
    pct_diff = diff * 100.0 / (key1_value or 1)
    INFO("%s %d, %s %d, diff %d, pct %4.2f%%" %
         (key1_name, key1_value, key2_name, key2_value, diff, pct_diff))
    if pct_diff > max_pct_diff:
      ERROR("FAIL! %s exceeds %s by %4.2f%%" %
            (key2_name, key1_name, pct_diff))
    elif pct_diff < -max_pct_diff:
      ERROR("FAIL! %s exceeds %s by %4.2f%%\n" %
            (key1_name, key2_name, -pct_diff))
    else:
      INFO("PASS! %s matches %s within %4.2f%% (< %d%%)\n" %
           (key1_name, key2_name, pct_diff, max_pct_diff))

  def check_primary_vs_replica_disk_usage(self, star_type,
                                          disk_serial_to_star_size_map,
                                          pct_threshhold, expected_rf=None,
                                          excluded_svms=None):
    """
    Check if primary vs secondary egroup or oplog disk usage fits our
    expected percentage.  star_type is "egroup" or "oplog".  primary_svm
    hosts the disks where the writes were local.  disk_serial_to_star_size_map
    must not count offlined egroup or oplog residuals.
    """
    # Keep a list of disk serials for each svm_ip.
    svm_disk_serial_list = dict() # key is svm_ip, value is list of serials
    svm_star_usage = dict()
    if not expected_rf:
      # Base expected_rf on cluster-wide redundancy_factor.
      # Beware that container rf can be 2 or 3 when cluster rf is 3.
      expected_rf = self.__cluster.get_fault_tolerance() + 1
    # Oplogs are on SSD disks only.  egroups can be on SSD or HDD.
    # The svms may not be in primary, secondary, tertiary order.
    total_star_usage = 0
    zeus_config = self.__cluster.get_zeus_config()
    for svm in self.__cluster.svms():
      if excluded_svms and (svm in excluded_svms):
        INFO("Exclude SVM %s for check" % svm.ip())
        continue

      svm_ip = svm.ip()
      if star_type == "oplog": # we care only about the SSD tier
        svm_disk_serial_list[svm_ip] = self.__disk_util.\
            get_ssd_disk_serial_id_list(svm, zeus_config)
      else: # retrieve disk_serial_ids for both SSD and HDD
        svm_disk_serial_list[svm_ip] = self.__disk_util.\
            get_svm_disk_serial_id_list(svm, zeus_config)
      INFO("svm %s has disk serial list %s" %
           (svm_ip, svm_disk_serial_list[svm_ip]))
      # Sum the egroup or oplog usage for all disks on this svm.
      svm_star_usage[svm_ip] = 0
      for disk_serial in svm_disk_serial_list[svm_ip]:
        star_size = disk_serial_to_star_size_map.get(disk_serial, 0)
        INFO("svm_ip %11s disk %18s has %12d bytes %6s usage" %
             (svm_ip, disk_serial, star_size, star_type))
        svm_star_usage[svm_ip] += star_size
      INFO("svm_ip %11s total%18s has %12d bytes %6s usage" %
           (svm_ip, " ", svm_star_usage[svm_ip], star_type))
      total_star_usage += svm_star_usage[svm_ip]
    INFO("svms all have %12d bytes total %6s usage" %
         (total_star_usage, star_type))

    CHECK(len(svm_star_usage) >= 3,
          "%s disk usage found on only %d svms" %
          (star_type, len(svm_star_usage)))
    primary_star_usage = sorted(svm_star_usage.values(), reverse=True)[0]

    if star_type == "egroup":
      # For egroups, the primary (local) write is saved on 1 svm,
      # and the replicas are spread across the remote SVMs.
      # The egroup sizes on all the replica disks put together should
      # equal the egroup sizes on the primary disks within 5% if FT1.
      # If FT2, the egroup size of replicas should be twice the primary.
      self.compare_pct_diff(
          "egroup size on primary SVM", primary_star_usage,
          "size of egroup replicas/FT",
          (total_star_usage - primary_star_usage) / (expected_rf - 1))

    if star_type == "oplog":
      # For oplog, the primary write is saved on the primary svm, and
      # each replica is saved on a separate svm.  We expect the oplog size
      # on all the primary disks put together to equal the oplog sizes on
      # all other disks put together for RF2, or twice that for RF3.
      self.compare_pct_diff(
          "oplog size on primary SVM", primary_star_usage,
          "size of oplog replicas/FT",
          (total_star_usage - primary_star_usage) / (expected_rf - 1))

  def verify_egroup_replica_counts(self, replica_count_by_egroup_id,
                                   expected_rf, repl_test_type,
                                   show_detail=False):
    """
    Verifies if egroup replicas are over, right or under replicated.

    Args:
      replica_count_by_egroup_id (int, int) : map of egroups ids and its
        replica counts.
      expected_rf (int) : expected number of replica copies depending
        upon the rf factor of the cluster.
      expect_str (string) : Value passed could be right, under or over.
        Depending on the value, ERRORs are logged if the replica counts
        are not as expected.

    Returns:
      list (int) : under replicated egroup ids.
      list (int) : right replicated egroup ids.
      list (int) : over replicated egroup ids.
    """
    under_rep_egroups = list()
    right_rep_egroups = list()
    over_rep_egroups = list()

    # Next verify each egroup has the number of replicas we expect.
    for (egroup_id, replicas) in sorted(replica_count_by_egroup_id.items()):
      if isinstance(replicas, int):
        nreplicas = replicas
      else: # replicas is list of disk_serials or medusa_replica_objects
        nreplicas = len(nreplicas)

      if nreplicas < expected_rf:
        under_rep_egroups.append(egroup_id)
      elif nreplicas > expected_rf:
        over_rep_egroups.append(egroup_id)
      else:
        right_rep_egroups.append(egroup_id)

    INFO("Checked %s egroups total for replica count matching expected_rf %d\n"
         "Counts of egroups: %d under, %d right, %d over." %
         (len(replica_count_by_egroup_id), expected_rf,
         len(under_rep_egroups), len(right_rep_egroups), len(over_rep_egroups)))

    if not show_detail:
      return (under_rep_egroups, right_rep_egroups, over_rep_egroups)

    INFO("Found %d egroups under_replicated vs expected_rf %d: %s" %
         (len(under_rep_egroups), expected_rf, under_rep_egroups))
    for egroup_id in under_rep_egroups:
      INFO("egroup_id %s is under-replicated %s vs %d!" %
            (egroup_id, replica_count_by_egroup_id[egroup_id], expected_rf))

    INFO("Found %d egroups over_replicated: %s" %
         (len(over_rep_egroups), over_rep_egroups))
    # We limit loop time because checking 1000's of egroups is slow, verbose.
    start_time = time.time()
    long_enough = 60
    for egroup_id in sorted(over_rep_egroups):
      INFO("Phys disks have %d %d.egroup files, excessive for RF%d!" % (
            replica_count_by_egroup_id[egroup_id], egroup_id, expected_rf))
      # Medusa client is quicker vs. cmd:
      #   medusa_printer --lookup=egid --egroup_id=%d | grep 'replicas {'
      (result, egroup_map) = \
            self.__medusa_client.lookup_extent_group_id_map(int(egroup_id))
      # TEST if StringDebug replaces medusa (rv, stdout, stderr) display.
      INFO("medusa returns egroup_id_map %s" % str(egroup_map))
      medusa_replica_count = len(
          egroup_map.extent_groupid_map_entry.control_block.replicas)
      if medusa_replica_count != expected_rf:
        ERROR("medusa shows egid %d over-replication %d != RF%d" %
              (egroup_id, medusa_replica_count, expected_rf))
      else:
        INFO("medusa shows egid %s has right replica count.\nPresume curator "
             "has not garbage collected excess replicas yet" % egroup_id)
      if (time.time() - start_time) > long_enough:
        INFO("Spent long_enough checking for over_replicated egroups")
        break

    if (repl_test_type == "right"):
      if under_rep_egroups:
        ERROR("We found unexpected under-replication")
      if over_rep_egroups:
        ERROR("We found unexpected over-replication")

    if (repl_test_type == "under") and (not under_rep_egroups):
      INFO("It looks like all the under-replicated egroup files"
           " got replicated while we waited.")

    return (under_rep_egroups, right_rep_egroups, over_rep_egroups)

  def get_egroup_ids_of_vdisk(self, vdisk_id):
    """
    Get list of the extent group IDs on the specified vdisk_id.

    Args:
      vdisk_id (int) : vdisk id of the intended vdisk.

    Returns:
      list (int) : List of egroup ids of the specified vdisk.
    """
    egroup_ids = self.__vdisk_usage_util.get_egroup_ids(vdisk_id)

    if not egroup_ids:
      ERROR("No egroups found for vdisk %s" % vdisk_id)
      return egroup_ids
    INFO("vdisk_id %d has egroup_ids %s" %
         (vdisk_id, egroup_ids))
    return egroup_ids

  def map_vdisk_egroup_id_to_replica_count(self, vdisk_id):
    """
    Generates a map of egroup_id to replica count for a given vdisk.

    Args:
      vdisk_id (int): vdisk id of the intended vdisk.

    Returns:
      dict (int, int) : map of egroup ids of vdisk to its replica counts.
    """
    egroup_id_to_replica_count = dict()
    egroup_disk_map = \
        self.__vdisk_usage_util.get_egroup_id_disk_ids_map(vdisk_id)

    for egid in egroup_disk_map.keys():
      egroup_id_to_replica_count[egid] = len(egroup_disk_map[egid])
    INFO("vdisk_id %d has egroup_id_to_replica_count map %s" %
         (vdisk_id, egroup_id_to_replica_count))
    return egroup_id_to_replica_count

  def map_vdisk_egroup_ids_to_replicas(self, vdisk_id):
    """
    Generate a map of egroup ids and its replica strings.

    Args:
      vdisk_id (int): vdisk id of the intended vdisk.

    Note:
      Each replica string returned is of the pattern.
      [disk_id /service_vm_id/rack].

    Returns:
      dict (int, list) : map of egroup ids to list of replica strings.
    """
    egroup_id_to_replica_map = dict()
    cmd = ("source /etc/profile; vdisk_usage_printer -vdisk_id=%s" % vdisk_id)
    svm = self.__cluster.svms()[0]
    rv, stdout, stderr = svm.execute(cmd)
    if rv != 0:
      ERROR("cmd '%s' on SVM %s rv %s stdout\n%s\nstderr\n%s" %
            (cmd, svm.ip(), rv, stdout, stderr))
      return egroup_id_to_replica_map

    for egroup_line in stdout.split('\n'):
      # 10333 6  6.00 MB  6.38 MB  106%  0.00 KB 0 c=low[53 /4/23][64 /7/23]
      m = re.search(r'^ *(\d+)', egroup_line)
      if not (m and m.group(1)):
        continue  # Skip blank and unparsable lines.
      egroup_id = int(m.group(1))
      DEBUG("egroup_id %d line %s" % (egroup_id, egroup_line))
      if not egroup_id in egroup_id_to_replica_map.keys():
        egroup_id_to_replica_map[egroup_id] = list()
      replica_pattern = re.compile("\[[^]]+\]")
      # sample replica strings:['[53 /4/23]', '[64 /7/23]']
      replica_strings = replica_pattern.findall(egroup_line)
      egroup_id_to_replica_map[egroup_id] = replica_strings

    INFO("egroup_id_to_replica_map %s " % egroup_id_to_replica_map)
    return egroup_id_to_replica_map

  def extract_vdisk_egroup_ids_to_vblocks(self, vdisk_id, file_size):
    """
    Get map of egroup ids to vblock numbers which map to it.

    Args:
      vdisk_id (int) : vdisk id of the intended vdisk.
      file_size (int) : size of the file in bytes.

    Returns:
      map (int, list) : map of egroup id to list of vblock numbers.
    """
    # Method taken from stargate_tests/replication_test.py jaya
    eg_map_vblocks = {}
    block_size = 1048576 # 1MB
    block_count = file_size / block_size
    for vblock_num in xrange(block_count):
      # Parse the vblock regions to find corresponding Extent Group ID.
      DEBUG("Got vblock: %d" % vblock_num)
      egids = self.__medusa_client.find_egroup_ids_from_vblock(vdisk_id,
                                                               vblock_num)
      for egid in egids:
        DEBUG("egid=%d vblock_num=%d" % (egid, vblock_num))
        if egid == -1:
          continue
        if egid in eg_map_vblocks.keys():
          eg_map_vblocks[egid].add(vblock_num)
        else:
          eg_map_vblocks[egid] = set([vblock_num])

    INFO("Dumping egid to vblock map:")
    for key, value in eg_map_vblocks.items():
      INFO("egroup:%d vblocks:%s" % (key, value))

    return eg_map_vblocks

  def get_compressed_egroup_ids_of_vdisk(self, vdisk_id):
    """
    Get list of egroups which are compressed for a given vdisk.

    Args:
      vdisk_id (int) : vdisk id of the intended vdisk.

    Returns:
      list (int) : list of compressed egroup ids or None if vdisk has
        no associated egroups.
    """
    egroup_ids = list()
    egroup_transformation_map = \
        self.__vdisk_usage_util.get_egroups_transformation_map(vdisk_id)

    for egid in egroup_transformation_map.keys():
      if egroup_transformation_map[egid].compressed:
        egroup_ids.extend([int(egid)])

    INFO("Compressed egroup ids of vdisk %s:\n%s\n" % (vdisk_id, egroup_ids))
    return egroup_ids

  def get_dedup_egroup_ids_of_vdisk(self, vdisk_id):
    """
    Get list of egroups which are deduplicated for a given vdisk.

    Args:
      vdisk_id (int) : vdisk id of the intended vdisk.

    Returns:
      list (int) : list of dedup egroup ids or None if vdisk has
        no associated egroups.
    """
    egroup_ids = list()
    egroup_transformation_map = \
        self.__vdisk_usage_util.get_egroups_transformation_map(vdisk_id)

    for egid in egroup_transformation_map.keys():
      if egroup_transformation_map[egid].deduped:
        egroup_ids.extend([int(egid)])

    INFO("Deduplicated egroup ids of vdisk %s:\n%s\n" % (vdisk_id, egroup_ids))
    return egroup_ids

  def get_egroup_id_to_egroup_map(self, egroup_ids=()):
    """
    Get egroup map of egroup id.

    Args:
      egroup_ids (int) : List of egroup ids for which the egroup map
        has to be obtained.

    Returns:
      dict (int, egroup map object) : Returns a map of egroup id to its
        egroup map.
    """
    egroup_id_to_egroup_map = dict()
    if not egroup_ids:
      return egroup_id_to_egroup_map

    for egroup_id in egroup_ids:
      (result, egroup_map) = \
            self.__medusa_client.lookup_extent_group_id_map(int(egroup_id))
      if not egroup_map:
        ERROR("Extent group map not found for egroup %s" % egroup_id)
        continue
      egroup_id_to_egroup_map[egroup_id] = egroup_map
    return egroup_id_to_egroup_map

  def get_egroup_id_to_disk_id_map_of_vdisk(self, vdisk_id):
    """
    Get a map of egroup id to disk id map of its replicas for a vdisk.

    Args:
      vdisk_id (int) :  vdisk id of the intended vdisk.

    Returns:
      dict (int, list) : map of egroup id to list of replica disk ids.
    """
    egroup_id_to_disk_id_map = dict()
    egroup_id_to_disk_id_map = \
        self.__vdisk_usage_util.get_egroup_id_disk_ids_map(vdisk_id)
    return egroup_id_to_disk_id_map

  def get_egroup_id_to_svm_id_map_of_vdisk(self, vdisk_id):
    """
    Get a map of egroup id to service vm id map of its replicas.
    The service vm id is depends on the disks attached to the SVM.

    Args:
      vdisk_id (int) :  vdisk id of the intended vdisk.

    Returns:
      dict (int, list) : map of egroup id to list of replica service vm ids.
    """
    egroup_id_to_svm_id_map = dict()
    egroup_id_to_svm_id_map = \
        self.__vdisk_usage_util.get_egroup_id_svm_ids_map(vdisk_id)
    return egroup_id_to_svm_id_map

  def get_egroup_id_to_disk_id_map(self, egroup_ids=()):
    """
    Get map of egroup id to disk id of specified egroup ids.

    Args:
      egroup_ids (int) : List of egroup ids whose egroup map needs
        to be retrieved.

    Returns:
      dict (int, list) : map of egroup id to list of disk id replicas.
    """
    egroup_id_to_disk_id_map = dict()
    if not egroup_ids:
      return egroup_id_to_disk_id_map

    for egroup_id in egroup_ids:
      (result, egroup_map) = \
          self.__medusa_client.lookup_extent_group_id_map(int(egroup_id))
      if not egroup_map:
        ERROR("Extent group map not found for egroup %s" % egroup_id)
        continue
      egroup_id_to_disk_id_map[egroup_id] = \
        egroup_map.extent_groupid_map_entry.control_block.replicas
    return egroup_id_to_disk_id_map

  def get_egroup_id_to_write_time_map(self, egroup_ids=()):
    """
    Get map of egroup ids to last write time in microseconds.

    Args:
      egroup_ids (int) : List of egroup ids whose egroup map needs
        to be retrieved.

    Returns:
      dict (int, timedelta) : map of egroup id to time delta object
        storing its last write time.
    """
    egroup_id_to_write_time_map = dict()
    if not egroup_ids:
      return egroup_id_to_write_time_map

    for egroup_id in egroup_ids:
      (result, egroup_map) = \
          self.__medusa_client.lookup_extent_group_id_map(int(egroup_id))
      if not egroup_map:
        ERROR("Extent group map not found for egroup %s" % egroup_id)
        continue
      write_time = timedelta(microseconds = \
        egroup_map.extent_groupid_map_entry.control_block.write_time_usecs)
      egroup_id_to_write_time_map[egroup_id] = write_time
    return egroup_id_to_write_time_map

  def get_dedup_job_status(self):
    """
    This a helper method to track dedup tasks in chronos.

    Returns:
      bool : True if success, False otherwise.
    """
    rv, job_status = \
        self.__curator_client.is_deduplicate_vdisk_region_job_completed()
    CHECK(not rv, "Error while checking job status in chronos")
    return job_status

  def dedup_start_full_scan_wait_for_job_completion(self, num_scans=3):
    """
    This provides the functionality to start a full scan and wait
    for chronos dedup code job complete.

    Args:
      num_scans (int) : number of curator full scans.

    Returns:
      bool : True if success, False otherwise.
    """
    for scan_num in range(num_scans):
      if not NutanixCuratorTestClient.wait_till_curator_master_is_initialized(
          self.__cluster.svms()):

        NutanixCuratorUtil.start_and_wait_till_full_scans_completed(
            self.__curator_client, 1)

        AgaveUtil.wait_for(self.get_dedup_job_status,
            "dedup vdisk job to complete", 1200, 60)
    return True

  def get_vdisk_hosting_svm_ip(self, vdisk_name):
    """
    Get the svm ip information from ZK hosting the vdisk.

    Args:
      vdisk_name (string): vdisk name whose svm ip is to be searched.

    Returns:
      svm_ip on success or None on failure.
    """
    svms = self.__cluster.svms()[0]

    INFO("Getting vdisk_id for %s" % vdisk_name)
    # First, use vdisk_config_printer to retrieve the vdisk_id
    # corresponding to the given vdisk_name.
    vdisk_printer_cmd = "source /etc/profile; " \
      "sudo /home/nutanix/bin/vdisk_config_printer --name=\"%s\"" % \
      vdisk_name
    ret, stdout, stderr = NutanixClusterSSHUtil.execute([svms], \
      vdisk_printer_cmd)[0]
    if ret:
      ERROR("Could not run command %s: \nReturn Value: %s, \nSTDOUT: "
            "%s, \nSTDERR: %s" % (vdisk_printer_cmd, str(ret), stdout,
                 stderr))
      return None

    vdisk_id = None
    for line in stdout.splitlines():
      if "vdisk_id" in line:
        vdisk_id = line.split(':')[-1]
        break

    # Unable to find the vDisk.
    if vdisk_id is None:
      ERROR("Vdisk_id is not found for %s" % vdisk_name)
      return None

    INFO("Vdisk %s has vdisk_id %s" % (vdisk_name, vdisk_id))
    # Second, use the vdisk ID to run the following zkls command.
    cmd = "source /etc/profile; /usr/local/nutanix/cluster/bin/zkls " \
          "/appliance/logical/leaders/vdisk-%s" % vdisk_id.strip()
    ret, stdout, stderr = NutanixClusterSSHUtil.execute([svms], cmd)[0]
    if ret or not stdout:
      ERROR("Could not run command %s: \nReturn Value: %s, \nSTDOUT: "
            "%s, \nSTDERR: %s" % (cmd, str(ret), stdout, stderr))
      return None

    # Last, use the following zkcat command to find the stargate hosting
    # the vdisk.
    cmd = "source /etc/profile;  /usr/local/nutanix/cluster/bin/zkcat " \
          "/appliance/logical/leaders/vdisk-%s/%s" % \
          (vdisk_id.strip(), stdout)
    ret, stdout, stderr = NutanixClusterSSHUtil.execute([svms], cmd)[0]
    if ret or not stdout:
      ERROR("Could not run command %s: \nReturn Value: %s, \nSTDOUT: "
            "%s, \nSTDERR: %s" % (cmd, str(ret), stdout, stderr))
      return None

    # Stdout of the form : "Hosted: <stargate_ip> yes/no: yes".
    # We want the stargate_ip.
    stargate_ip = stdout.split()[1]
    # Stargate_ip of the form: "<svm_ip>:<port>:<>".
    # We want the svm_ip.
    if len(stargate_ip):
      svm_ip = stargate_ip.split(':')[0]
      INFO("Vdisk %s is hosted on %s" % (vdisk_name, svm_ip))
      return svm_ip
    else:
      ERROR("Hosting stargate ip was not found for vdisk_name= %s" %
            vdisk_name)

    return None

  def enable_dedup_in_zeus(self, num_retries=3):
    self.enable_fingerprinting_in_zeus(num_retries)
    self.enable_on_disk_dedup_in_zeus(num_retries)

  def enable_fingerprinting_in_zeus(self, num_retries=3):
    """
    Enable fingerprinting in zeus. Return True on success, False
    on failure.
    """
    success = False
    while num_retries > 0:
      zeus_config = self.__cluster.get_zeus_config()
      CHECK(zeus_config is not None, "Failed to get Zeus config")
      if not zeus_config.disable_fingerprinting:
        INFO("Fingerprinting already enabled in Zeus")
        return True
      zeus_config.disable_fingerprinting = False
      logical_timestamp = zeus_config.logical_timestamp + 1
      zeus_config.logical_timestamp = logical_timestamp
      if not self.__cluster.set_zeus_config(zeus_config):
        ERROR("Failed to update Zeus config")
        # Wait for 500ms before retrying to update Zeus config.
        time.sleep(0.5)
      else:
        success = True
        break

    if success:
      INFO("Succeeded to update disable_fingerprinting to false in Zeus")
    else:
      ERROR("Failed to update disable_fingerprinting to false in Zeus")
    return success

  def enable_on_disk_dedup_in_zeus(self, num_retries=3):
    """
    Enable on-disk-dedup in zeus. Return True on success, False
    on failure.
    """
    success = False
    while num_retries > 0:
      zeus_config = self.__cluster.get_zeus_config()
      CHECK(zeus_config, "Failed to get Zeus config")
      if not zeus_config.disable_on_disk_dedup:
        INFO("On Disk Dedup already enabled in Zeus")
        return True
      zeus_config.disable_on_disk_dedup = False
      logical_timestamp = zeus_config.logical_timestamp + 1
      zeus_config.logical_timestamp = logical_timestamp
      if not self.__cluster.set_zeus_config(zeus_config):
        ERROR("Failed to update Zeus config")
        # Wait for 500ms before retrying to update Zeus config.
        time.sleep(0.5)
      else:
        success = True
        break

    if success:
      INFO("Succeeded to update disable_on_disk_dedup to false in Zeus")
    else:
      ERROR("Failed to update disable_on_disk_dedup to false in Zeus")
    return success

  def get_vdisk_size_stats(self, vdisk_id):
    """
    Get the vdisk size stats for the give vdisk_id.  
    """
    svm = self.__cluster.svms()[0]
    if not svm:
      WARNING("No ssh'able SVMs to run stats_tool on")
      return None

    # Get raw disk usage stats.
    remote_pathname = "/tmp/disk_usage_stats.bin"
    usage_cmd = "/home/nutanix/bin/stats_tool -stats_type='vdisk_usage'\
                -stats_key=%d -raw_output_file=%s" % (vdisk_id, remote_pathname)

    INFO("Executing command %s on SVM %s" % (usage_cmd, svm.ip()))
    results = NutanixClusterSSHUtil.execute([svm], usage_cmd)
    CHECK(results is not None, "Error getting disk usage stats")
    rv, stdout, stderr = results[0]
    if rv != 0:
      ERROR("Getting disk usage from %s failed: %d, %s, %s" %
            (self.__cluster.name(), rv, stdout, stderr))
      return None

    tmp_file = tempfile.NamedTemporaryFile()
    local_pathname = tmp_file.name
    result = NutanixClusterSSHUtil.transfer_from(
        [svm], [remote_pathname], [local_pathname])
    CHECK(result is not None,
               "Failed to copy disk usage proto from SVM %s" % svm.ip())
    try:
      # Deserialize disk usage proto.
      vdisk_usage = VDiskUsageStatProto()
      vdisk_usage.ParseFromString(open(tmp_file.name).read())
      return vdisk_usage
    except google.protobuf.message.DecodeError, e:
      FATAL("Error deserializing vdisk usage proto: %s" % str(e))
    except IOError, e:
      FATAL("Error getting vdisk usage %s: %s" %
            (self.__cluster.name(), str(e)))