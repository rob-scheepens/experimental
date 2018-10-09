#!/usr/bin/python
"""
Copyright (c) 2018 Nutanix Inc. All rights reserved.

Author: gokul.kannan@nutanix.com

This script runs fstrim on the devices of the VM where it is running. The
script has two modes. When the with run_on_uvm flag set to False, the script
gathers all the devices currently in use by Stargate and runs fstrim on those
disks. When the run_on_uvm flag is set to True, the script gets all the Nutanix
devices mounted on the VM and then runs fstrim on those devices. The script
must be run as a user who has sudo permissions. The script runs fstrim on the
mount points in small chunks, max chunk configurable via the parameter
'max_data_mb_per_trim_op'. It sleeps for a certain period, configurable via
'interval_secs_between_trim', between two consecutive fstrim operations. It
runs infinitely and needs to be killed via the process manager to exit.

The script will complete trimming all offsets of the disks within the time
configured via the param 'full_scan_duration_mins'. It spreads out the trim
ops in such a way that small chunks of offsets get trimmed over
full_scan_duration_mins time period. The number of bytes trimmed per trim op
has an upper bound configurable via the param 'max_data_mb_per_trim_op'. If the
parameter 'full_scan_duration_mins' is too less to spread out the trim ops
across this time period without honouring the 'max_data_mb_per_trim_op', the
script will take longer to complete trimming the disks.
"""

import argparse
import sys

from trim_service_factory import VMTrimServiceFactory

def main():
  """
  Main method to start the service.
  """
  # Get the arguments.
  parser = argparse.ArgumentParser()
  parser.add_argument(
    "--run_on_uvm", action='store_true', default=False,
    help="to run on UVM")
  parser.add_argument(
    "--interval_secs_between_trim", action='store', type=int, default=2,
    help="time interval between two consecutive fstrim operations in secs")
  parser.add_argument(
    "--full_scan_duration_mins", action='store', type=int, default=360,
    help="duration, in minutes, to complete trimming all offsets of a disk")
  parser.add_argument(
    "--max_data_mb_per_trim_op", action='store', type=int, default=200,
    help="Maximum data size to trim in a single op in MB")
  parser.add_argument(
    "--trim_alignment_bytes", action='store', type=int, default=4096,
    help="Block size, in bytes, for the trim requests to align to")

  trim_args = parser.parse_args()
  # Convert the duration to seconds and the max trim size to bytes.
  full_scan_duration_secs = trim_args.full_scan_duration_mins * 60
  max_bytes_per_trim_op = trim_args.max_data_mb_per_trim_op * 1024 * 1024
  # Get the correct trim service object.
  trim = VMTrimServiceFactory.get_nutanix_trim_service(trim_args.run_on_uvm)

  # Start the trim service if the pre conditions are met.
  if trim.verify_prerequisites():
    trim.start_trim_service(
      full_scan_duration_secs, trim_args.interval_secs_between_trim,
      max_bytes_per_trim_op, trim_args.trim_alignment_bytes)
  else:
    print "Preconditions not met. Exiting!"
    sys.exit(0)

if __name__ == "__main__":
  main()
