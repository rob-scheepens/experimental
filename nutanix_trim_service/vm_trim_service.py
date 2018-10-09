"""
Copyright (c) 2018 Nutanix Inc. All rights reserved.

Author: gokul.kannan@nutanix.com

This class contains the methods and interfaces for nutanix trim service.
"""
import json
import random
import re
import subprocess
import time
import traceback

from abc import ABCMeta, abstractmethod

class VMTrim(object):
  """
  Class containing the modules required to perform fstrim operations on a VM.
  """
  __metaclass__ = ABCMeta

  TRIMMED_BYTES_RE = re.compile(r"(\d+)\s(bytes)")

  FSTRIM_CMD = "sudo fstrim -v -o {0} -l {1} {2}"

  @abstractmethod
  def log_info(self, msg):
    """
    Log an info level message.
    Args:
      msg(str): The string to printed.

    """
    raise NotImplementedError

  @abstractmethod
  def log_error(self, msg):
    """
    Log an error level message.
    Args:
      msg(str): The string to printed.

    """
    raise NotImplementedError

  @abstractmethod
  def log_debug(self, msg):
    """
    Log an debug level message.
    Args:
      msg(str): The string to printed.

    """
    raise NotImplementedError

  @abstractmethod
  def get_partitions_to_be_trimmed(self):
    """
    Return a list of partitions and their info which need to be trimmed. This
    method returns a list of dicts. Each dict has two keys: "size" and
    "mount_point".
    """
    raise NotImplementedError

  @abstractmethod
  def verify_prerequisites(self):
    """
    Verify if all the conditions are met to start the trim service on the VM.
    Returns true if all the conditions are met. False otherwise.
    """
    raise NotImplementedError

  @abstractmethod
  def initialize_logging(self):
    """
    Set up logging for this tool on the VM.
    """
    raise NotImplementedError

  def start_trim_service(
      self, full_scan_duration_secs, interval_secs_between_trim,
      max_bytes_per_trim_op, trim_alignment):
    """
    This method runs the trim service. For every iteration, it refreshes the
    list of partitions to be trimmed. This is to make sure the information is
    current and we don't act on stale information.
    Args:
      full_scan_duration_secs(int): Number of seconds to complete trimming a
                                    disk/partition.
      interval_secs_between_trim(int): Number of seconds to sleep between the
                                       fstrim commands.
      max_bytes_per_trim_op(int): Maximum number of bytes to trim in a single
                                  fstrim op.
      trim_alignment(int): The block size in bytes to align the trim requests
                           to.

    """
    # Dict to keep track of the last trimmed offsets.
    while True:
      partitions_to_be_trimmed = self.get_partitions_to_be_trimmed()
      # Check if we have anything to trim. If not, go to sleep and check again
      # a while later.
      if not partitions_to_be_trimmed:
        self.log_error(
          "There are no partitions to be trimmed. Will sleep for %s seconds "
          "before checking again" % full_scan_duration_secs)
        time.sleep(full_scan_duration_secs)
        continue

      self.log_info("Partitions selected for trimming: %s"
                    % partitions_to_be_trimmed)
      # Record the start time.
      start_time = time.time()
      # Run fstrim on the partitions.
      trimmed_bytes_info = self.run_fstrim_on_partitions(
        partitions_to_be_trimmed, full_scan_duration_secs,
        interval_secs_between_trim, max_bytes_per_trim_op, trim_alignment)
      scan_duration = time.time() - start_time

      # All disks have been trimmed. Log and then start over.
      self.log_info("Scan took %s seconds to complete trimming all the disks" %
                    scan_duration)

  def run_command(self, cmd):
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

  def run_fstrim_on_mount(self, mount_point, offset, num_bytes):
    """
    Run fstrim on the given mount point.
    Args:
      mount_point(str): Mount point to run fstrim on.
      offset(int): Offset where to start trimming.
      num_bytes(int): Number of bytes to trim.

    Returns the number of bytes trimmed on success, -1 on failure.
    """
    trimmed_bytes = -1
    cmd = self.FSTRIM_CMD.format(offset, num_bytes, mount_point)
    self.log_debug("Trimming %s bytes on mount point %s at offset %s" % (
      num_bytes, mount_point, offset))
    rv, stdout, stderr = self.run_command(cmd)
    if rv:
      self.log_debug("Failed to fstrim disk mounted at %s with error: %s, %s" %
                     (mount_point, stderr, stdout))
    else:
      stdout = stdout.strip()
      self.log_debug("Successfully trimmed %s" % stdout)
      try:
        trimmed_bytes = int(self.TRIMMED_BYTES_RE.search(stdout).group(1))
      except Exception as exception:
        self.log_error(
          "Failed to parse trimmed bytes from %s with error %s. Traceback: %s"
          % (stdout, str(exception), traceback.format_exc()))

    return trimmed_bytes

  def run_fstrim_on_partitions(
      self, partitions, full_scan_duration_secs, interval_secs_between_trim,
      max_bytes_per_trim_op, trim_alignment):
    """
    Run fstrim on the given partitions.
    Args:
      partitions(list): List of partitions to run fstrim on.
      full_scan_duration_secs(int): Time to complete trimming all offsets of a
                                    disk.
      interval_secs_between_trim(int): Number of seconds to sleep between the
                                       fstrim commands.
      max_bytes_per_trim_op(int): Maximum number of bytes to trim in a single
                                  fstrim op.
      trim_alignment(int): The block size in bytes to align the trim requests
                           to.

    """
    # We have to complete scanning the disks within 'full_scan_duration_secs'
    # seconds and also spread out the fstrim operations to avoid any
    # performance issue. For this, we try to keep the number of fstrim
    # operations constant across all the disks but keep the number of bytes
    # trimmed per operation variable, based on the size of the disk. That
    # simply means that we trim bigger disks in bigger chunks and smaller disks
    # in smaller chunks. It is entirely possible that the bigger chunks per
    # trim operations might cause performance issue. Hence we have an upper
    # bound on the number of bytes to trim per operation. This upper bound is
    # controlled via the variable 'max_bytes_per_trim_op'
    # List to track the partions which have been completely trimmed.
    trimmed_partitions = set()

    # Dict to keep track of last trimmed offset.
    last_trimmed_offset_info = {}
    for partition in partitions:
      # Calculate how many bytes we will trim per op and add it to the dict.
      # The number of bytes per trim is calculated based on the size of the
      # disk and the full scan duration. However, we have an upper bound to
      # avoid having to trim a really large chunk in one go.
      # First, calculate the number of bytes to trim based on the size of the
      # partition and the scan duration.
      num_bytes_per_trim_op = partition["size"] / full_scan_duration_secs \
        * interval_secs_between_trim

      # Make sure the number of bytes to trim is not above the max allowed.
      num_bytes_per_trim_op = min(num_bytes_per_trim_op, max_bytes_per_trim_op)

      # Make sure the number of bytes to trim is aligned to the trim_alignment
      # variable(trim block size)
      num_bytes_per_trim_op = num_bytes_per_trim_op - (
        num_bytes_per_trim_op % trim_alignment)

      # Finally, make sure the number of bytes to trim is at least the size of
      # the trim_alignment variable(trim block size).
      partition["num_bytes_per_trim_op"] = max(
        trim_alignment, num_bytes_per_trim_op)

      # Randomize the starting offset.
      random_offset = random.randint(0, partition["size"])

      # Align the starting offset to trim_alignment variable(trim block size).
      starting_offset = random_offset - random_offset % trim_alignment

      # Make sure the starting offset is not negative.
      starting_offset = max(0, starting_offset)
      last_trimmed_offset_info[partition["mount_path"]] = starting_offset

    # Dict to keep track of how many bytes are actually trimmed and how many
    # bytes were requested to be trimmed. Initialize them here.
    actual_trimmed_bytes_info = dict(zip(
      last_trimmed_offset_info.keys(), [0] * len(partitions)))
    trimmed_bytes_info = dict(zip(
      last_trimmed_offset_info.keys(), [0] * len(partitions)))

    # Loop till all the partitions have been trimmed.
    while len(trimmed_partitions) < len(partitions):
      start_time = time.time()
      for partition in partitions:
        # Get the last trimmed offset for the partition and perform fstrim
        # on that offset.
        offset = last_trimmed_offset_info[partition["mount_path"]]
        num_bytes_per_trim_op = partition["num_bytes_per_trim_op"]
        trimmed_bytes = self.run_fstrim_on_mount(
          partition["mount_path"], offset, num_bytes_per_trim_op)
        if trimmed_bytes < 0:
          # Something went wrong. Log it.
          self.log_debug("fstrim failed on mount %s" % partition["mount_path"])
        else:
          # Update the number of bytes trimmed.
          actual_trimmed_bytes_info[partition["mount_path"]] += trimmed_bytes

        # Update the total bytes we have issued trimmed requests for.
        trimmed_bytes_info[partition["mount_path"]] += num_bytes_per_trim_op
        # Update the last trimmed offset.
        last_trimmed_offset_info[partition["mount_path"]] += \
          num_bytes_per_trim_op

        # Check if we have reached end of the partition. If yes, then start
        # over.
        if last_trimmed_offset_info[partition["mount_path"]] >= \
            partition['size']:
          last_trimmed_offset_info[partition["mount_path"]] = 0

        # Check if we have completed trimming the disk. If yes, add the
        # partition to the completed list.
        if trimmed_bytes_info[partition["mount_path"]] >= partition['size']:
          trimmed_partitions.add(partition["mount_path"])

      # Sleep before issuing next set of fstrim ops. We have to ensure that
      # consecutive trim operations happen within the given time period of
      # 'interval_secs_between_trim'. So calculate how much time we actually
      # need to sleep. This is done by calculating how much time the fstrim
      # operations took and then sleep for whatever is left over of
      # 'interval_secs_between_trim'.
      trim_secs = time.time() - start_time
      if trim_secs < interval_secs_between_trim:
        time.sleep(interval_secs_between_trim - trim_secs)

    # Log the stats.
    self.log_info("Trimmed Bytes: %s" % json.dumps(actual_trimmed_bytes_info))
    return actual_trimmed_bytes_info
