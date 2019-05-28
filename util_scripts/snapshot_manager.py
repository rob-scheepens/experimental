#
# Copyright (c) 2014 Nutanix Inc. All rights reserved.
#
# Author: ywu@nutanix.com
#
# This class provides a way to start, stop and kill snapshot processes on one
# cluster asynchronously.
#

import datetime
import multiprocessing
import os
import pipes
import sys
import time
from multiprocessing import Process
from multiprocessing.queues import SimpleQueue
from traceback import format_exc

from qa.agave.agave_util import AgaveUtil
from qa.agave.nutanix_cluster_ssh_util import NutanixClusterSSHUtil
from qa.util.agave_tools.nutanix_curator_util import NutanixCuratorUtil
from qa.util.agave_tools.nutanix_curator_test_client\
    import NutanixCuratorTestClient
from qa.util.agave_tools.volume_group_acropolis_util \
    import VolumeGroupAcropolisUtil
from util.base.log import *
from util.data.enum import Enum

class ProcessState(Enum):
  # Snapshot process is done and successfully after the stop event is sent to
  # the snapshot process.
  SUCCEEDED = 0
  # Snapshot process fails at any stage.  When the snapshot process fails,
  # it will retrun with FAILEDl status.
  FAILED = 1
  # Snapshot process has not started yet and the stop event has been sent.
  # The snapshot process is marked as STOPPED.
  STOPPED = 2
  NOT_STARTING = 3
  KILLED = 4

class SnapshotManager(object):
  VM_NAMES_FIELD = "vm-names"
  FILES_FIELD = "files"
  VG_NAMES_FIELD = "vg-names"

  def __init__(self, cluster, out_dir, pd_names_vms_files_to_snapshot=(),
               snapshot_interval=120, disable_curator_scan_before=True,
               full_curator_scan_after=True, pd_names_vgs_to_snapshot=()):
    """
    cluster: nutanix cluster object.  The cluster where to take snapshot.
    out_dir: Output directory path to save snapshot tree configuration.
    pd_names_vms_files_to_snapshot(Dict): a dict of {pd_name: {"vm-names":[],
      "files":[]}}
      key: pd_name(str), the protection domain name,
      value: {"vm-names": list of vm names to snapshot, "files": list of files
      to snapshot}
    pd_names_vgs_to_snapshot(Dict): a dict of {pd_name: {"vg-names":[]}
      key: pd_name(str), the protection domain name,
      value: {"vg-names": list of vg names to snapshot}
    
    snapshot_interval(int): the interval to take snapshot.
    disable_curator_scan_before: whether to disable curator scan on the cluster
      before running any snpashot process.
    full_curator_scan_after: whether to run full curator scan after
      running each snapshot process.
    """
    self.__snapshot_processes = []
    self.__disable_curator_scan_before = disable_curator_scan_before
    self.__cluster = cluster
    self.__pd_names_vms_files_to_snapshot = pd_names_vms_files_to_snapshot
    self.__pd_names_vgs_to_snapshot = pd_names_vgs_to_snapshot
    # Record process status because the out_queue cannot be used once.
    self.__processes_status = {}
    if pd_names_vms_files_to_snapshot is not None:
      for pd_name, vms_files_to_snapshot in \
          pd_names_vms_files_to_snapshot.iteritems():
        stop_event =  multiprocessing.Event()
        out_queue = SimpleQueue()
        out_queue.put(ProcessState.NOT_STARTING)
        init_run = True
        vm_names_to_snapshot = \
            vms_files_to_snapshot.get(SnapshotManager.VM_NAMES_FIELD, None)
        files_to_snapshot = \
            vms_files_to_snapshot.get(SnapshotManager.FILES_FIELD, None)
        snapshot_process = SnapshotProcess(
            cluster, out_dir, pd_name, vm_names_to_snapshot, files_to_snapshot,
            snapshot_interval, full_curator_scan_after, init_run,
            stop_event, out_queue)
        self.__snapshot_processes.append(snapshot_process)
        self.__processes_status[snapshot_process] = ProcessState.NOT_STARTING

    if pd_names_vgs_to_snapshot is not None:
      for pd_name, vgs_to_snapshot in \
          pd_names_vgs_to_snapshot.iteritems():
        stop_event =  multiprocessing.Event()
        out_queue = SimpleQueue()
        out_queue.put(ProcessState.NOT_STARTING)
        init_run = True
        vg_names_to_snapshot = \
            vgs_to_snapshot.get(SnapshotManager.VG_NAMES_FIELD, None)
        snapshot_process = SnapshotProcess(
            cluster, out_dir, pd_name, None, None, snapshot_interval,
            full_curator_scan_after, init_run, stop_event, out_queue,
            vg_names_to_snapshot)
        self.__snapshot_processes.append(snapshot_process)
        self.__processes_status[snapshot_process] = ProcessState.NOT_STARTING

  @staticmethod
  def compose_pd_name_vms_files_to_snapshot(pd_name, vm_names_to_snapshot=(),
                                            files_to_snapshot=()):
    """
    Compose a dict as:
      key: pd_name
      values: "vm-names": list of vm names to snapshot.
              "files": list of files to snapshot.
    Args:
      pd_name(str): porection domain name.
      vm_names_to_snapshot(list): vm names to snapshot.
      files_to_snapshot(list): files to snapshot.
    Returns:
      A dict with
        key: pd_name
        values: "vm-names": list of vm names to snapshot.
                "files": list of files to snapshot.
    """
    result = {}
    result.setdefault(pd_name, {})
    if vm_names_to_snapshot:
      if not result[pd_name].has_key(SnapshotManager.VM_NAMES_FIELD):
        result[pd_name][SnapshotManager.VM_NAMES_FIELD] = vm_names_to_snapshot
      else:
        result[pd_name][SnapshotManager.VM_NAMES_FIELD].extend(
                                                         vm_names_to_snapshot)
    if files_to_snapshot:
      if not result[pd_name].has_key(SnapshotManager.FILES_FIELD):
        result[pd_name][SnapshotManager.FILES_FIELD] = files_to_snapshot
      else:
        result[pd_name][SnapshotManager.FILES_FIELD].extend(
                                                        files_to_snapshot)
    return result

  @staticmethod
  def combine_multiple_pd_name_vms_files_to_snapshot(
     pd_name_vms_files_to_snapshot_list):
    """
    Combine a dict as:
      key: pd_name
      values: "vm-names": list of vm names to snapshot.
              "files": list of files to snapshot.
    Args:
      pd_name_vms_files_to_snapshot_list(list): A list of
        pd_name_vms_files_to_snapshot. Each pd_name_vms_files_to_snapshot as:
        key: pd_name
        values: "vm-names": list of vm names to snapshot.
                "files": list of files to snapshot.
    Returns:
      A dict with
        key: pd_name
        values: "vm-names": list of vm names to snapshot.
                "files": list of files to snapshot.
    """
    result = {}
    for pd_name_vms_files_to_snapshot in pd_name_vms_files_to_snapshot_list:
      for pd_name, vms_files_to_snapshot in \
          pd_name_vms_files_to_snapshot.iteritems():
        result.setdefault(pd_name, {})
        vms_to_snapshot = \
            vms_files_to_snapshot.get(SnapshotManager.VM_NAMES_FIELD, None)
        files_to_snapshot = \
            vms_files_to_snapshot.get(SnapshotManager.FILES_FIELD, None)
        if vms_to_snapshot:
          result[pd_name][SnapshotManager.VM_NAMES_FIELD] = vms_to_snapshot
        if files_to_snapshot:
          result[pd_name][SnapshotManager.FILES_FIELD] = files_to_snapshot
    return result

  @staticmethod
  def create_pds_with_custom_num_vms_per_pd(pd_prefix, vm_names, vms_per_pd):
    """
    Given a pd-prefix and a list of vm_names. Put vms_per_pd in pds and return
    a list of pd and its vms dict.

    Args:
      pd_prefix (string) : protection domain prefix to be used.
      vm_names (list) : list of vm names to be put in multiple
        protection domains.
      vms_per_pd (int) : number of vms to be put in each protection
        domain.

    Returns:
      A dict with
        key: pd_name
        values: "vm-names": list of vm names to snapshot.
    """

    number_vms = len(vm_names)
    number_of_pds = number_vms/vms_per_pd

    pd_vms_dict = dict()
    pds_vms_dict = dict()

    for i in range(number_of_pds):
      pd_name = pd_prefix + str(i)
      pd_vms_dict = SnapshotManager.compose_pd_name_vms_files_to_snapshot(
          pd_name,
          vm_names_to_snapshot=vm_names[i*(vms_per_pd):(i+1)*(vms_per_pd)],
          files_to_snapshot=None)
      pds_vms_dict.update(pd_vms_dict)

    if (number_of_pds * vms_per_pd) < number_vms:
      pd_name = pd_prefix + str(number_of_pds)
      pd_vms_dict = SnapshotManager.compose_pd_name_vms_files_to_snapshot(
          pd_name, vm_names_to_snapshot=\
          vm_names[number_of_pds*(vms_per_pd):number_vms],
          files_to_snapshot=None)
      pds_vms_dict.update(pd_vms_dict)

    return pds_vms_dict
  
  @staticmethod
  def create_pds_with_custom_num_vgs_per_pd(pd_prefix, vg_names, vgs_per_pd):
    """
    Given a pd-prefix and a list of vg_names. Put vgs_per_pd in pds and return
    a list of pd and its VG dict.

    Args:
      pd_prefix (string) : protection domain prefix to be used.
      vg_names (list) : list of vg names to be put in multiple
        protection domains.
      vgs_per_pd (int) : number of vgs to be put in each protection
        domain.

    Returns:
      A dict with
        key: pd_name
        values: "vg-names": list of vg names to snapshot.
    """

    number_vgs = len(vg_names)
    number_of_pds = number_vgs/vgs_per_pd

    pd_vgs_dict = dict()
    pds_vgs_dict = dict()

    for i in range(number_of_pds):
      pd_name = pd_prefix + str(i)
      pd_vgs_dict = SnapshotManager.compose_pd_name_vgs_to_snapshot(
          pd_name,
          vg_names_to_snapshot=vg_names[i*(vgs_per_pd):(i+1)*(vgs_per_pd)])
      pds_vgs_dict.update(pd_vgs_dict)

    if (number_of_pds * vgs_per_pd) < number_vgs:
      pd_name = pd_prefix + str(number_of_pds)
      pd_vgs_dict = SnapshotManager.compose_pd_name_vgs_to_snapshot(
          pd_name, vg_names_to_snapshot=\
          vg_names[number_of_pds*(vgs_per_pd):number_vgs])
      pds_vgs_dict.update(pd_vgs_dict)

    return pds_vgs_dict

  @staticmethod
  def compose_pd_name_vgs_to_snapshot(pd_name, vg_names_to_snapshot=()):
    """
    Compose a dict as:
      key: pd_name
      values: "vg-names": list of vg names to snapshot.
    Args:
      pd_name(str): porection domain name.
      vg_names_to_snapshot(list): vg names to snapshot.
    Returns:
      A dict with
        key: pd_name
        values: "vg-names": list of vg names to snapshot.
    """
    result = {}
    result.setdefault(pd_name, {})
    if vg_names_to_snapshot:
      if not result[pd_name].has_key(SnapshotManager.VG_NAMES_FIELD):
        result[pd_name][SnapshotManager.VG_NAMES_FIELD] = vg_names_to_snapshot
      else:
        result[pd_name][SnapshotManager.VG_NAMES_FIELD].extend(
                                                         vg_names_to_snapshot)
    return result


  def __disable_curator_scan(self):
    # Disable the periodic curator scans complete during the test.
    gflags_map = NutanixCuratorUtil.get_gflags_to_disable_curator_scans()
    gflags_map.update(
        { "curator_experimental_always_generate_copy_blockmap_in_partial_scan":
          "true",
          "v":
          3
       })

    if NutanixCuratorUtil.restart_curator_with_custom_gflags(
        self.__cluster, False, gflags_map) == 0:
      INFO("Disable curator scan successfully.")
      return True
    else:
      ERROR("Failed to disable curator scan.")
      return False

  def start_all(self):
    """
    Start all snapshot processes.
    Returns:
      True: start all snapshot processes successfully.
      False: fail to disable curator scan on the cluster if the
        disable_curator_scan_before is set to True.
    """
    if self.__snapshot_processes:
      if self.__disable_curator_scan_before:
        if not self.__disable_curator_scan():
          return False

      for snapshot_process in self.__snapshot_processes:
        snapshot_process.start()
    else:
      INFO("Snapshot process list is empty.")

    return True

  def are_all_snapshot_processes_finished(self):
    """
    Whether all the snapshot processes have finished either stop, killed or
      succeed.
    Returns:
      True: if all snapshot processes are stopped or snapshot processes list is
        empty.
      False: if any one of the snapshot processes is alive.
    """
    if not self.__snapshot_processes:
      INFO("Snapshot process list is empty.")
      return True
    num_not_running_processes = 0
    for snapshot_process in self.__snapshot_processes:
      if not snapshot_process.is_alive():
        num_not_running_processes += 1
    if num_not_running_processes == len(self.__snapshot_processes):
      return True
    return False

  def terminate_all(self):
    """
    Terminate all snapshot processes.  This method terminates all the snapshot
    processes by killing the processes.  So it is indeterministic.  The process
    might be killed in the middle of operation.  After all processes are kill,
    this method will return.  It is a synchronous call.
    """
    for snapshot_process in self.__snapshot_processes:
      if snapshot_process.is_alive():
        snapshot_process.terminate()
        snapshot_process.join()
        self.__processes_status[snapshot_process] = ProcessState.KILLED

  def stop_all(self):
    """
    Stop all snapshot processes.  This method sends stop event to all the
    snapshot processes.  It is an asynchronous call and the caller needs to
    verify all the snapshot processes are not alive.
    """
    for snapshot_process in self.__snapshot_processes:
      if snapshot_process.is_alive():
        snapshot_process.get_stop_event().set()

  def are_all_snapshot_processes_succeed(self):
    """
    Check whether all snapshot processes succeeded.  If the caller calls
    stop_all and each snapshot process will finish its current task and
    exit as succeeded or failed, then this method could be called to check
    the status.  If this snapshot processes are still running, it will return
    False.
    Returns:
      True if all snapshot processes succeed; False if any one of the snapshot
        process fails or any of the processes is still running.
    """
    if self.are_all_snapshot_processes_finished():
      succeed_count = 0
      for snapshot_process in self.__snapshot_processes:
        snapshot_process_status = None
        # Get from process first and it has the updated info.
        if not snapshot_process.get_process_state_queue().empty():
          snapshot_process_status = \
              snapshot_process.get_process_state_queue().get()
          self.__processes_status[snapshot_process] = snapshot_process_status
        else:
          snapshot_process_status = self.__processes_status[snapshot_process]
        if snapshot_process_status == ProcessState.SUCCEEDED:
          succeed_count += 1
      INFO("%s/%s snapshot process succeed."
           % (succeed_count, len(self.__snapshot_processes)))
      return (succeed_count == len(self.__snapshot_processes))
    else:
      ERROR("Not all snapshot processes have finished yet.")
      return False

  def list_snapshots_summary(self):
    """
    List all the snapshots which have been taken by the snapshot manager.
    This method should be called after all the snapshot processes have finished.
    Returns:
      The output will be a list of:
      {protection_domain_name: xxx
       vm_names: [xxx, xxx]
       files: [XXX, XXX]
       snapshot_id_creation_time:{snapshot_id1: creatoin_time1,
                                  snapshot_id2: creation_time2}
      }
      return empty list for any failure.
    """
    snapshots_list_summary = []
    if not self.are_all_snapshot_processes_finished():
      WARNING("Not all snapshot processes have finished yet.")

    for pd_name, vms_files_to_snapshot in\
        self.__pd_names_vms_files_to_snapshot.iteritems():
      snapshots_summery_per_pd = {}
      ls_pd_name = self.__cluster.ncli("pd ls name=%s" % pd_name)
      if SnapshotManager.is_failed_ncli_result(ls_pd_name):
        ERROR("Failed to run %s" % ls_pd_name)
        continue
      if not ls_pd_name["data"]:
        ERROR("Pd %s doesn't exist." % pd_name)
        continue
      ls_snaps_per_pd = self.__cluster.ncli("pd ls-snaps name=%s" % pd_name)
      if SnapshotManager.is_failed_ncli_result(ls_snaps_per_pd):
        ERROR("Failed to run %s for pd %s" % (ls_snaps_per_pd, pd_name))
        continue

      vm_names_to_snapshot = vms_files_to_snapshot.get(
          SnapshotManager.VM_NAMES_FIELD, [])
      files_to_snapshot = vms_files_to_snapshot.get(
          SnapshotManager.FILES_FIELD, [])

      if not self.__verify_snapshots_per_pd(ls_snaps_per_pd, pd_name,
                                        vm_names_to_snapshot,
                                        files_to_snapshot):
        ERROR("Mismatched pd name, vm names or files found in pd %s" % pd_name)
        continue

      snapshots_summery_per_pd["protection_domain_name"] = pd_name
      if vm_names_to_snapshot:
        snapshots_summery_per_pd[SnapshotManager.VM_NAMES_FIELD] =\
            vm_names_to_snapshot
      if files_to_snapshot:
        snapshots_summery_per_pd[SnapshotManager.FILES_FIELD] =\
            files_to_snapshot
      snapshots_summery_per_pd.setdefault("snapshot_id_creation_time", {})
      for snapshot in ls_snaps_per_pd["data"]:
        snapshot_id = snapshot["snapshotId"]
        creation_time = snapshot["snapshotCreateTimeUsecs"]
        snapshots_summery_per_pd["snapshot_id_creation_time"].update(
            {snapshot_id : creation_time})
      snapshots_list_summary.append(snapshots_summery_per_pd)

    return snapshots_list_summary

  def __verify_snapshots_per_pd(self, ls_snaps_per_pd, pd_name,
                                vm_names_to_snapshot=(), files_to_snapshot=()):
    """
    Verify under each protection domain, the result of pd ls snaps name=pd_name
    has and only has:
      1). protection domain name as pd_name.
      2). vm_names_to_snapshot.
      3). files_to_snapshot.
    Args:
      ls_snaps_per_pd: ls snaps name=pd_name result.
      pd_name: protection domain name.
      vm_names_to_snapshot: vm names to snapshot.
      files_to_snapshot: nfs file paths to snapshot.
    Returns:
      True if pd name, all vm names or files are in the ls snaps name result.
      False: if mismatch protection domain name, missing or extra vm names or
        files in the ls snaps name result.
    """
    vm_names_to_snapshot_set = set(vm_names_to_snapshot)
    files_to_snapshot_set = set(files_to_snapshot)

    for snapshot in ls_snaps_per_pd["data"]:
      if snapshot["protectionDomainName"] != pd_name:
        ERROR("protection domain name %s doesn't exist after cmd: pd ls snaps "
              "name=%s.  It is an ncli issue." % (pd_name, pd_name))
        return False

      vm_names_snapshotted_set = set([vm["vmName"] for vm in snapshot["vms"]])
      files_snapshotted_set = set( nfs_file["nfsFilePath"] for nfs_file
                                  in snapshot["nfsFiles"])

      if len(vm_names_to_snapshot_set) != len(vm_names_snapshotted_set):
        ERROR("vm_names to snapshot %s, vms_names snapshot: %s, size doesn't "
              "match." % (vm_names_to_snapshot_set, vm_names_snapshotted_set))
        return False
      if vm_names_to_snapshot_set.difference(vm_names_snapshotted_set):
        ERROR("vm_names to snapshot %s, vms_names snapshot: %s doesn't match."
              % (vm_names_to_snapshot_set, vm_names_snapshotted_set))
        return False

      if len(files_to_snapshot_set) != len(files_snapshotted_set):
        ERROR("files to snapshot %s, files snapshot: %s, size doesn't match."
              % (files_to_snapshot_set, files_snapshotted_set))
        return False
      if files_to_snapshot_set.difference(files_snapshotted_set):
        ERROR("files to snapshot %s, files snapshot: %s doesn't match."
              % (files_to_snapshot_set, files_snapshotted_set))
        return False

    return True

  @staticmethod
  def remove_pd_and_snapshots(cluster, pd_name):
    """
    Remove protection domain on the cluster and its associated snapshots.
    Args:
      cluster: nutanix cluster
      pd_name: proecttion domain name.
    Returns:
      True: remove protection domain and its snapshots successfully.
      False: Failed to remove protection domain and its snapshots.
    """
    # Check whether pd exists or not.
    ls_pd_name = cluster.ncli("pd ls name=%s" % pd_name)
    if SnapshotManager.is_failed_ncli_result(ls_pd_name):
      return False
    if not ls_pd_name["data"]:
      INFO("Pd %s doesn't exist." % pd_name)
      return True

    # Check snapshot names under pd.
    ls_snaps = cluster.ncli("pd ls-snaps name=%s" % pd_name)
    if SnapshotManager.is_failed_ncli_result(ls_snaps):
      return False
    snap_ids = []
    for snap in ls_snaps["data"]:
      if snap.get("snapshotId", None) is not None:
        snap_ids.append(snap["snapshotId"])
      else:
        INFO("There is no snapshot under pd %s" % pd_name)

    # Remove snapshots under pd.
    if snap_ids:
      rm_snapshot_cmd = "pd rm-snap name=%s snap-ids=%s"\
          % (pd_name, ",".join(map(str, snap_ids)))
      rm_snapshot_cmd_result = cluster.ncli(rm_snapshot_cmd)
      if SnapshotManager.is_failed_ncli_result(rm_snapshot_cmd_result):
        return False

    # Remove pd.
    rm_pd_result = cluster.ncli("pd remove name=%s" % pd_name)
    if SnapshotManager.is_failed_ncli_result(rm_pd_result):
      ERROR("Failed to run pd remove name=%s" % pd_name)
      return False

    return True

  @staticmethod
  def is_failed_ncli_result(ncli_result):
    """
    Check whether the ncli result succeeded or failed.
    True if succeed; False if failed.
    """
    if ncli_result is None or ncli_result["status"] != 0:
      return True
    return False

class SnapshotProcess(Process):
  def __init__(self, cluster, out_dir, pd_name=None, vm_names_to_snapshot=(),
               files_to_snapshot=(), snapshot_interval=120,
               full_curator_scan_after=True, init_run=True, stop_event=None,
               process_state_queue=None, vg_names_to_snapshot=()):
    self.cluster = cluster
    self.out_dir = out_dir
    self.curator_test_client = NutanixCuratorTestClient(self.cluster)
    self.stop_event = stop_event
    self.process_state_queue = process_state_queue
    if init_run:
      super(SnapshotProcess, self).__init__(target=self.__run, args=(
          pd_name, vm_names_to_snapshot, files_to_snapshot, snapshot_interval,
          full_curator_scan_after, stop_event, process_state_queue,
          vg_names_to_snapshot))

  def get_stop_event(self):
    return self.stop_event

  def get_process_state_queue(self):
    return self.process_state_queue

  def __run(self, pd_name, vm_names_to_snapshot, files_to_snapshot,
            snapshot_interval, full_curator_scan_after, stop_event,
            process_status_queue, vg_names_to_snapshot):
    """
    Called by start method in snapshot process as process.
    """
    self.snapshot_files(pd_name, vm_names_to_snapshot, files_to_snapshot,
                        snapshot_interval, full_curator_scan_after, stop_event,
                        process_status_queue, vg_names_to_snapshot)

  def __create_protection_domain(self, pd_name, vm_names_to_snapshot,
                                 files_to_snapshot, remove_existing_pd=True,
                                 vg_names_to_snapshot=None):
    """
    Create a protection domain on cluster.
    Args:
      cluster: nutanix cluster.
      pd_name: protection domain name.
      vm_names_to_snapshot: vm name list to snapshot.
      files_to_snapshot: file list to snapshot under protection domain.
      remove_existing_pd: whether to remove existing protection domain with the
      pd_name before create one or not.
    Returns:
      True: create protection domain successfully;
      False: failed to create protection domain.
    """
    if remove_existing_pd:
      if not SnapshotManager.remove_pd_and_snapshots(self.cluster, pd_name):
        return False

    # Create protection domain and add files to them.
    if SnapshotManager.is_failed_ncli_result(
        self.cluster.ncli("pd create name=%s" % pd_name)):
      return False
    protect_cmd = "pd protect name=%s " % pd_name
    if vm_names_to_snapshot:
      protect_cmd = "%s vm-names=%s"\
          %(protect_cmd,",".join(vm_names_to_snapshot))
    if files_to_snapshot:
      protect_cmd = "%s files=%s" %(protect_cmd,",".join(files_to_snapshot))
    if vg_names_to_snapshot:
      volume_group_uuids = []
      for vg in vg_names_to_snapshot:
        uuid = self.__get_vg_uuid(vg)
        volume_group_uuids.append(uuid)
      protect_cmd = "%s volume-group-uuids=%s" \
                    %(protect_cmd,",".join(volume_group_uuids))
      
    if SnapshotManager.is_failed_ncli_result(
        self.cluster.ncli(protect_cmd)):
      return False

    return True
  
  def __get_vg_uuid(self, vg):
    """
    Return the volume group uuid for a given volume group.
    """
    svm = self.cluster.svms()[0]
    uuids_map = VolumeGroupAcropolisUtil.get_volume_group_uuid_map(svm)
    if (not uuids_map[vg]):
      ERROR("uuid not found for volume group %s" % vg)
    return uuids_map[vg]

  def __add_update_msg_queue(self, simple_queue, process_status):
    """
    Update simple_queue with process status.
    """
    if simple_queue.empty():
      simple_queue.put(process_status)
    else:
      while not simple_queue.empty():
        simple_queue.get()
      simple_queue.put(process_status)

  def snapshot_files(self, pd_name=None, vm_names_to_snapshot=(),
                     files_to_snapshot=(), snapshot_interval=30,
                     full_curator_scan_after=True, stop_event=None,
                     process_status_queue=None, vg_names_to_snapshot=()):
    """
    Take snapshot of files.
    Args:
      pd_name (str): protection domain name.
      vm_names_to_snapshot: vm name list to snapshot.
      files_to_snapshot(list): list of full path files to snapshot.
      snapshot_interval: interval between snapshots.
      full_curator_scan_after: Wehther to start full scan after all
        snapshots have been taken.
      stop_event: event to stop the snapshot process when it is set.
      process_state_queue: queue to store process status.
      vg_names_to_snapshot(list): vg name list to snapshot.
    """
    if pd_name is None:
      ERROR("Protection domain is empty or there is no file to snapshot.")
      self.__add_update_msg_queue(process_status_queue, ProcessState.FAILED)
      return

    if (not vm_names_to_snapshot) and (not files_to_snapshot) \
    and (not vg_names_to_snapshot):
      ERROR("vm name list, vg name list and file list are empty. \
            No snapshot to take.")
      return

    # Wait until current curator_test_client scan is over.
    INFO("Wait until current curator_test_client job is over.")
    NutanixCuratorUtil.wait_till_scan_is_completed(
        self.curator_test_client)
    NutanixCuratorUtil.wait_till_copyblockmap_job_is_completed(
        self.curator_test_client)
    INFO("Current curator_test_client job is over.")

    try:
      if not self.__create_protection_domain(pd_name, vm_names_to_snapshot,
                                             files_to_snapshot,
                                             vg_names_to_snapshot = \
                                                          vg_names_to_snapshot):
        ERROR("Failed to create pd %s with vm names: %s, file list %s, \
              vg names %s" % (pd_name, vm_names_to_snapshot, files_to_snapshot,
                              vg_names_to_snapshot))
        self.__add_update_msg_queue(process_status_queue, ProcessState.FAILED)
        return
      snapshot_count = 0
      if stop_event.is_set():
        self.__add_update_msg_queue(process_status_queue, ProcessState.STOPPED)
        INFO("Stop snapshot process before taking snapshot and after creating"
             " pd")
        return
      while True:
        INFO("Starting %s snapshot" % snapshot_count)
        if not stop_event.is_set():
          # Take a snapshot of the protection domain pd_name.
          if SnapshotManager.is_failed_ncli_result(
              self.cluster.ncli("pd add-one-time-snapshot name=%s" % pd_name)):
            ERROR("Failed to add one-time snapshot under pd %s.Number of run: %s"
                  % (pd_name, snapshot_count))
            ERROR("Failed snapshot process with parameter %s"
                  % self.__print_process_parameters(pd_name,
                                                    vm_names_to_snapshot,
                                                    files_to_snapshot,
                                                    snapshot_interval,
                                                    full_curator_scan_after,
                                                    vg_names_to_snapshot))
            self.__add_update_msg_queue(process_status_queue,
                                        ProcessState.FAILED)
            return
          snapshot_count += 1
          timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S.%f")

          # Save snapshot configuration.
          filename="%s.snapshot_tree.%s.%s" % (self.cluster.name(), timestamp,
                                           "kRunning")
          cmd = "source /etc/profile; snapshot_tree_printer"
          svm = self.cluster.svms()[0]
          rv, stdout, stderr = svm.execute(cmd)

          if rv != 0:
            ERROR("Failed to get cluster snapshot tree from SVM %s" % svm.ip())
          else:
            with open(os.path.join(self.out_dir, filename), "w") as fh:
              fh.write(stdout)
            INFO("Saved snapshot tree to %s" % filename)

          # Vdisk configuration.
          filename = "%s.vdisk_config.%s.%s" % (self.cluster.name(), timestamp,
                                                "kRunning")
          vdisk_iterator = self.cluster.create_vdisk_iterator()
          if vdisk_iterator is None:
            ERROR("Failed to get vdisk config")
          else:
            with open(os.path.join(self.out_dir, filename), "w") as fh:
              fh.write("\n".join([str(vdisk) for vdisk in vdisk_iterator]))
            INFO("Saved vdisk config to %s" % filename)

          if full_curator_scan_after:
            INFO("Starting Curator full scan")
            timestart = time.time()
            NutanixCuratorUtil.start_and_wait_till_full_scans_completed(
                self.curator_test_client, 1)
            INFO("Full curator scan finished after %s seconds." %
                (time.time() - timestart))
        else:
            INFO("Stop snapshot process after %s runs" % snapshot_count)
            INFO("Stopped snapshot process with parameter %s"
                  % self.__print_process_parameters(pd_name,
                                                    vm_names_to_snapshot,
                                                    files_to_snapshot,
                                                    snapshot_interval,
                                                    full_curator_scan_after,
                                                    vg_names_to_snapshot))
            self.__add_update_msg_queue(process_status_queue,
                                        ProcessState.SUCCEEDED)
            return
        time.sleep(snapshot_interval)
    except:
      type_, value_, traceback_ = sys.exc_info()
      ERROR("Failed to run snapshot process %s" % format_exc(traceback_))
      ERROR("Failed snapshot process with parameter %s"
                  % self.__print_process_parameters(pd_name,
                                                    vm_names_to_snapshot,
                                                    files_to_snapshot,
                                                    snapshot_interval,
                                                    full_curator_scan_after,
                                                    vg_names_to_snapshot))

      self.__add_update_msg_queue(process_status_queue, ProcessState.FAILED)

  def __print_process_parameters(self, pd_name=None, vm_names_to_snapshot=(),
                                 files_to_snapshot=(), snapshot_interval=30,
                                 full_curator_scan_after=True,
                                 vg_names_to_snapshot = ()):
      return "pd name: %s, vm names to snapshot: %s, files to snapshot: %s,"\
             "snapshot interval %s, full_curator_scan_after %s,"\
             "vg names to snapshot: %s"\
             % ( pd_name, vm_names_to_snapshot, files_to_snapshot,
                snapshot_interval, full_curator_scan_after,
                vg_names_to_snapshot)

class SnapshotUtil(object):
  """
  Contains a set of snapshot utility methods.
  """
  @staticmethod
  def create_multiple_snapshot_schedules_per_hour(cluster, pd_name,
                                                  num_snapshots_per_hour=1,
                                                  vm_names=None,
                                                  local_retention=12):
    """
    This method creates multiple snapshot schedules so as to simulate
    multiple snapshots per hour.
    :param cluster: Nutanix cluster object
    :param pd_name: Name of the protection domain to be created. Assumes that
                    the pd was not created earlier.
    :param num_snapshots_per_hour: Number of snapshots to be taken in an hour.
    :param vm_names: Names of the VM. If None, all VMs are protected.
    :param local_retention: Number of snapshots to retain for every schedule.
                            These snapshots are stored locally.
    :return:
    """
    # Create a protection domain.
    INFO("Creating protection domain %s" % pd_name)
    pd_create_cmd = ("pd create name=%s" % pd_name)
    AgaveUtil.ncli_cmd_with_check(cluster, pd_create_cmd)

    # Protect all VMs.
    if vm_names is None:
      vm_names = ",".join([vm.name() for vm in cluster.uvms()])

    INFO("Adding VMs %s to protection domain %s" % (vm_names, pd_name))
    pd_protect_cmd = ("pd protect name=%s vm-names=%s" % (pd_name, vm_names))
    AgaveUtil.ncli_cmd_with_check(cluster, pd_protect_cmd)

    # It is possible for the cluster to be at a different time than the machine
    # executing the test. Use the time from the cluster rather than the time
    # from test machine.
    date_cmd = "date"
    svm = cluster.svms()[-1]
    rv, stdout, _ = NutanixClusterSSHUtil.execute([svm], date_cmd)[0]
    if rv != 0:
      ERROR("Unable to determine current time on SVM: %s" % svm.ip())
      return False

    # Create a new snapshot schedule.
    INFO("Current time on SVM %s: %s" % (svm.ip(), stdout))
    INFO("Creating new snapshot schedule")
    svm_time = datetime.datetime.strptime(" ".join(stdout.split()),
                                          "%a %b %d %H:%M:%S %Z %Y")

    minute_delta = int(60/num_snapshots_per_hour)
    # Since the minimum snapshot frequency is 1 hour, to have snapshots
    # happening every minute_delta minutes, multiple snapshot schedules
    # need to be created.
    for delta in range(0, 60, minute_delta):
      schedule_time = svm_time + datetime.timedelta(minutes=delta)
      cmd = "pd add-hourly-schedule  name=%s local-retention=%s start-time=" \
            "'%s'" % (pd_name, str(local_retention),
                      schedule_time.strftime("%m/%d/%Y %H:%M:%S %Z"))
      INFO("Creating new snapshot schedule with cmd: %s" % cmd)
      schedule_cmd = pipes.quote(cmd)
      AgaveUtil.ncli_cmd_with_check(cluster, schedule_cmd)

    return True
