#!/usr/bin/python
#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
# Author: gokul.kannan@nutanix.com
#
# This tool runs a test periodically on nucloud. If any of the runs fail, it
# tries to find the commit which caused the test failure by calling
# auto_bisector.
#

import gflags
import os
import sys
import time

from qa.tools.nucloud.nucloud_rest import *
from qa.tools.jita.jita_rest import JitaRest
from qa.tools.nutanix_auto_bisector import Auto_Bisect
from util.base.log import DEBUG, ERROR, INFO, WARNING, FATAL, initialize

FLAGS = gflags.FLAGS

gflags.DEFINE_string("last_good_commit_file", "last_good_commit",
                     ("File having the last good commit"))

USERNAME = "gokul.kannan"
NODE_POOL = "nucloud_cdp"
POOL_USER = "admin"

def did_nucloud_job_finish(job_id):
  """
  Helper function to check if a particular job has finished execution in
  nucloud.
  """
  nucloud = NuCloudREST()
  response = nucloud.get_jobs(job_id)
  status = response.get_status()
  INFO("Response from nucloud for job id %s is %s" % (job_id, status))
  if status not in ["passed", "killed", "failed", "completed"]:
    return False
  else:
    return True

def run_test_on_nucloud(
  test=None, username=None, node_pool=None, branch=None, build_url=None,
  build_type="opt", preferred_hypervisor=None, commit_id=None):
  """
  Run the given test on nucloud and return the job id if submit succeeds.
  """
  if not test:
    test=FLAGS.test_name
  if not username:
    username=USERNAME
  if not node_pool:
    node_pool=NODE_POOL
  if not branch:
    branch = FLAGS.branch
  if not preferred_hypervisor:
    preferred_hypervisor = FLAGS.hypervisor

  nucloud = NuCloudREST()
  INFO("Submitting job to Nucloud for commit id %s" % commit_id)
  response_obj = nucloud.submit_job(
    [test], username, node_pool, branch=branch, build_url=build_url,
    build_type=build_type, preferred_hypervisor=preferred_hypervisor,
    commit_id=commit_id, skip_email_report=True)

  print response_obj.get_message()
  if response_obj.is_success():
    return response_obj.get_job_id()
  else:
    ERROR("Failed to trigger job in nucloud %s " % response_obj.get_message())
    return False

def wait_for_test_result(job_id, timeout=18000, poll_period=600):
  """
  Wait for results from nucloud and then populate the results.
  """
  expired_time = 0
  nucloud = NuCloudREST()
  while expired_time < timeout:
    if did_nucloud_job_finish(job_id):
      break
    time.sleep(poll_period)
    expired_time += poll_period

  response = nucloud.get_jobs(job_id)
  if response.get_status() != "passed" or response.get_status() != "completed":
      return False
  else:
    response = response.get_test_results(job_id)
    test_results = response.get_test_result_dict()
    for test, result in test_results.iteritems():
      if result.get_status() != "Succeeded":
        INFO("Test %s failed with status %s" % (test, result.get_status()))
        return False

  return True

def get_latest_commit():
  jita = JitaRest()
  commit = jita.get_commits(branch="master")[0]
  return commit['commit_id']

def get_commits_in_range(self, first_commit, last_commit, branch="master"):
    """
    Get all the commits in the range of the two commits.
    """
    jita = JitaRest()
    commits = jita.get_commits(branch=branch, num_commits=50)
    commit_range = []
    flag = False
    for commit in commits:
      if commit['commit_id'] == last_commit:
        flag = True
      elif commit['commit_id'] == first_commit:
        commit_range.append(commit['commit_id'])
        break

      if flag:
        commit_range.append(commit['commit_id'])
    return commit_range

def setup_logging(file_template):
  """ Sets up the log file.
  Args:      file_template (str): Template file name for the log file
  """
  os.path.exists("/tmp") or os.mkdir("/tmp")
  initialize("/tmp/%s.log" % file_template)
  print "Logging to file /tmp/%s.log" % file_template

"""
This function is the entry point to the tool, it parses the inputs and calls
find_offending_commit with the input
"""
def main(argv):
  print "in main"
  try:
    argv = FLAGS(argv)
  except gflags.FlagsError, err:
    print "%s\nUsage: %s ARGS\n%s" % (err, sys.argv[0], FLAGS)
    sys.exit(1)

  file_template = "run_test_log_%d" % (int(time.time()))
  setup_logging(file_template)

  while True:
    top_dir = os.environ['TOP']
    filename = top_dir + "/" + FLAGS.last_good_commit_file
    f = open(filename)
    last_good_commit = f.read()
    f.close()
    latest_commit = get_latest_commit()
    job_id = run_test_on_nucloud(commit_id=latest_commit)
    if not job_id:
      ERROR("Trigerring test on Nucloud failed")
    if not wait_for_test_result(job_id):
      # Trigger auto bisect
      auto_bisect = Auto_Bisect(last_good_commit, latest_commit)
      offending_commit = auto_bisect.find_offending_commit()
      FATAL("Commit %s has broken the test" % offending_commit)
    else:
      f = open(filename, "w")
      f.write(latest_commit)
      f.close()
      last_good_commit = latest_commit
      latest_commit = get_latest_commit()
      commits = get_commits_in_range(last_good_commit, latest_commit)
      while len(commits) < 10:
        # Sleep for 10 mins
        time.sleep(600)
        latest_commit = get_latest_commit()
        commits = get_commits_in_range(last_good_commit, latest_commit)

if __name__ == "__main__":
  main(sys.argv)
