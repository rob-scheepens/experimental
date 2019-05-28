#!/usr/bin/python
#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
# Author: gokul.kannan@nutanix.com
#
# This is a tool to bisect a set of commits for a given test. The tests by
# default would be run on nucloud. If the test is a three node upgrade test then
# please use the jita option. When using Jita, one needs to supply the clusters
# too.
#
import collections
import gflags
import json
import os
import pprint
import sys
import threading
import time

from qa.tools.automation.common.rest_client.jarvis_rest_client import \
  JarvisRestClient
from qa.tools.nucloud.nucloud_rest import *
from qa.tools.automation.common.phoenix.client import PhoenixClient
from qa.tools.automation.common.phoenix.request import ClientPhoenixRequest
from qa.tools.jita.jita_rest import JitaRest
from util.base.log import DEBUG, ERROR, INFO, WARNING, FATAL, initialize

FLAGS = gflags.FLAGS

gflags.DEFINE_string("last_good_commit", None, ("The last good commit"))
gflags.DEFINE_string("latest_bad_commit", None,
                     ("latest bad commit seen"))
gflags.DEFINE_string("commits_file", None,
                     ("File having range of commits and the corresponding build"
                      " to bisect. Note: Each commit should on an new line and"
                      "the build should be separated from the commit by a "
                      "space"))
gflags.DEFINE_string("branch", "master", "The branch on which to search.")
gflags.DEFINE_string("hypervisor", "ahv", "The hypervisor on which to run.")
gflags.DEFINE_string("hypervisor_version", "branch_symlink",
                     "The hypervisor version")
gflags.DEFINE_string("input_file", None, "The filename of input JSON data")
gflags.DEFINE_string("test_name", "vanilla_io_integrity",
                     "The name of the test")
gflags.DEFINE_bool("use_nucloud", True, "Use nucloud if true")
gflags.DEFINE_string("clusters", None, "List of the clusters to use"
                     "Required if use_nucloud is false")
gflags.DEFINE_string("input_type", "build_url", "The type of input. One of "
                     "commit, change_id, build_url")
gflags.DEFINE_bool("upgrade_test", False, "Bool indicating if the the test is"
                     " an upgrade test")
gflags.DEFINE_string("upgrade_base_commit", None, "Base commit to upgrade "
                     "from in case of an upgrade test")
gflags.DEFINE_string("lab", "osl", "Location of the clusters")
gflags.DEFINE_string("test_framework", "nutest",
                     "Framework of the test. One of nutest or agave")

JARVIS_URL = "https://jarvis.eng.nutanix.com"

USERNAME = "gokul.kannan"
# We don't have a dedicated pool yet.
NODE_POOL = "nucloud_cdp"
POOL_USER = "admin"

class Auto_Bisect(object):
  def __init__(self, good_commit=None, bad_commit=None):
    self.commits_dict = collections.OrderedDict()
    self.global_results_dict = {}
    if not good_commit:
      self.good_commit = FLAGS.last_good_commit
    else:
      self.good_commit = good_commit

    if not bad_commit:
      self.last_bad_commit = FLAGS.latest_bad_commit
    else:
      self.last_bad_commit = bad_commit

  def get_commit_search_list(self, commits_file=None, input_type=None):
    """
    Get the commits list from the file and create a dict with commit as the key
    and build_url as the value. If the build_url isnt specified then the value
    would be null
    """
    if self.good_commit:
      commits = self.get_commits_in_range(self.good_commit,
                                          self.last_bad_commit)
      for commit in commits:
        self.commits_dict[commit] = None
    else:
      with open(commits_file) as f:
        lines = f.read().splitlines()
        for line in lines:
          if not line:
            continue 
          commit = line.split()[0]
          if input_type == "build_url":
            self.commits_dict[commit] = line.split()[1]
          else:
            self.commits_dict[commit] = None

    INFO("Commits map: \n%s " % self.commits_dict)

  def did_nucloud_job_finish(self, job_id):
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
    self, test=None, username=None, node_pool=None, branch=None, build_url=None,
    build_type="opt", preferred_hypervisor=None, commit_id=None,
    input_type="build_url", test_upgrade=None, image_commit=None):
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
    if not test_upgrade:
      test_upgrade = FLAGS.upgrade_test
      image_commit = FLAGS.upgrade_base_commit

    if input_type=="change_id":
      change_id = commit_id
      commit_id = None
    else:
      change_id = None

    nucloud = NuCloudREST()
    INFO("Submitting job to Nucloud for commit id %s" % commit_id)
    response_obj = nucloud.submit_job(
      [test], username, node_pool, branch=branch, build_url=build_url,
      build_type=build_type, preferred_hypervisor=preferred_hypervisor,
      commit_id=commit_id, gerrit_change_id=change_id,
      test_upgrade=test_upgrade, image_commit=image_commit,
      skip_email_report=True)

    if response_obj.is_success():
      return response_obj.get_job_id()
    else:
      return False

  def wait_for_test_results(self, job_info, timeout=18000, poll_period=600):
    """
    Wait for results from nucloud and then populate the results.
    """
    expired_time = 0
    unfinished_jobs = job_info.values()

    nucloud = NuCloudREST()
    while len(unfinished_jobs) and expired_time < timeout:
      for job_id in job_info.values():
        if self.did_nucloud_job_finish(job_id):
          if job_id in unfinished_jobs:
            unfinished_jobs.remove(job_id)

      time.sleep(poll_period)
      expired_time += poll_period

    for commit, job_id in job_info.iteritems():
      response = nucloud.get_jobs(job_id)
      if response.get_status() not in ["passed", "completed"]:
        self.global_results_dict[commit] = False
      else:
        response = nucloud.get_test_results(job_id)
        test_results = response.get_test_result_dict()
        for test, result in test_results.iteritems():
          if test != "send_test_results_email" and \
                     result.get_status() != "Succeeded":
            INFO("Test %s failed with status %s" % (test, result.get_status()))
            self.global_results_dict[commit] = False
            break
          self.global_results_dict[commit] = True

    return True

  def binary_search_commit(self, commit_search_list, input_type):
    """
    This is the main function that implements the binary search flow through the
    given commit_search_list.
  
    Params:
        commit_search_list: The list of gerrit change ids or commits to be
                            searched
        test: test set name
        node_pool: The node_pool to use to trigger the tests
        input_type: String to indicate type of input for the commits
        branch: The branch on which this commit appears
        hypervisor: The hypervisor that needs to be used  
    """
    INFO("Below are the list of commits being searched:\n%s" %
         pprint.pformat(commit_search_list))

    if len(commit_search_list) == 0:
      ERROR ("Length of commit_search_list is 0, this is probably a bug")
      return None

    elif len(commit_search_list) == 1:
      INFO("Returning %s " % commit_search_list[0])
      return commit_search_list[0]

    elif len(commit_search_list) == 2:
      if commit_search_list[0] in self.global_results_dict.keys():
        if self.global_results_dict[commit_search_list[0]]:
          return commit_search_list[1]
        else:
          return commit_search_list[0]
      if commit_search_list[1] in self.global_results_dict.keys():
        if self.global_results_dict[commit_search_list[1]]:
          return commit_search_list[0]

      # We reach here if we don't already have the test results for at least the
      # first commit
      response_map = {}
      response_map[commit_search_list[0]] = self.run_test_on_nucloud(
        build_url=self.commits_dict[commit_search_list[0]],
        commit_id=commit_search_list[0], input_type=input_type)
      self.wait_for_test_results(response_map)

      if self.global_results_dict[commit_search_list[0]]:
        return commit_search_list[1]
      else:
        return commit_search_list[0]

    # Get the number of free clusters from the pool.
    free_nodes = self.get_free_nodes_from_jarvis()
    num_free_clusters = len(free_nodes)/3
    INFO("free clusters in nucloud: %s " % num_free_clusters)
    search_list_len = len(commit_search_list)

    # If the number of free clusters is less than the number of commits,
    # let us do a binary search.
    if num_free_clusters < search_list_len:
      mid_commit_index = search_list_len / 2
      first_half_mid_commit_index = mid_commit_index / 2
      second_half_mid_commit_index = (
        mid_commit_index + 1 + search_list_len) / 2
      index_list = [mid_commit_index, first_half_mid_commit_index,
                    second_half_mid_commit_index]
      INFO("Commits selected for verification are: %s, %s and %s" % (
        commit_search_list[mid_commit_index], commit_search_list[
          first_half_mid_commit_index], commit_search_list[
          second_half_mid_commit_index]))

      response_map = {}
      for index in index_list:
        # If we already have the test result for this commit don't run the test
        # again.
        if commit_search_list[index] in self.global_results_dict.keys():
          continue

        response_map[commit_search_list[index]] = self.run_test_on_nucloud(
          build_url=self.commits_dict[commit_search_list[index]],
          commit_id=commit_search_list[index], input_type=input_type)

      self.wait_for_test_results(response_map)
      INFO("Results from the run are: %s" % self.global_results_dict)

      # Based on the test result, call the function again.
      if not self.global_results_dict[commit_search_list[
        first_half_mid_commit_index]]:
        INFO("Narrowing the search based on the results to commits between "
             "%s and %s" % (commit_search_list[0], commit_search_list[
              first_half_mid_commit_index]))
        return self.binary_search_commit(commit_search_list[
          0:(first_half_mid_commit_index+1)], input_type)
      elif not self.global_results_dict[commit_search_list[mid_commit_index]]:
        INFO("Narrowing the search based on the results to commits between "
             "%s and %s" % (commit_search_list[first_half_mid_commit_index],
                            commit_search_list[mid_commit_index]))
        return self.binary_search_commit(
          commit_search_list[first_half_mid_commit_index:(mid_commit_index+1)],
          input_type)
      elif not self.global_results_dict[commit_search_list[
        second_half_mid_commit_index]]:
        INFO("Narrowing the search based on the results to commits between "
             "%s and %s" % (commit_search_list[mid_commit_index],
                            commit_search_list[second_half_mid_commit_index]))
        return self.binary_search_commit(
          commit_search_list[mid_commit_index:(second_half_mid_commit_index+1)],
          input_type)
      else:
        INFO("Narrowing the search based on the results to commits between "
             "%s and %s" % (commit_search_list[second_half_mid_commit_index],
                            commit_search_list[-1]))
        return self.binary_search_commit(
          commit_search_list[second_half_mid_commit_index:], input_type)
    else:
      # We have enough clusters. Trigger all the runs in parallel.
      response_map = {}
      for commit in commit_search_list:
        if commit in self.global_results_dict.keys():
          continue
        response_map[commit] = self.run_test_on_nucloud(
          build_url=self.commits_dict[commit],
          commit_id=commit, input_type=input_type)

      self.wait_for_test_results(response_map)
      INFO("Results from the run are: %s" % self.global_results_dict)

      for commit in commit_search_list:
        if not self.global_results_dict[commit]:
          INFO("Returning the offending commit %s" % commit)
          return commit

  def binary_search_commit_using_jita(self, commit_search_list, input_type,
                                      clusters):
    """
    This is the main function that implements the binary search flow through the
    given commit_search_list.
    Params:
        commit_search_list: The list of gerrit change ids or commits to be
                            searched
        input_type: String to indicate type of input for the commits
        clusters: list of clusters to run the test on
    """
    INFO("Below are the list of commits being searched:\n%s" %
         pprint.pformat(commit_search_list))

    if len(commit_search_list) == 0:
      print "Length of commit_search_list is 0, this is probably a bug"
      return None

    elif len(commit_search_list) == 1:
      INFO ("returning the only commit passed to the binary_search is %s " %
            commit_search_list[0])
      return commit_search_list[0]

    elif len(commit_search_list) == 2:
      if commit_search_list[0] in self.global_results_dict.keys():
        if self.global_results_dict[commit_search_list[0]]:
          return commit_search_list[1]
        else:
          return commit_search_list[0]
      if commit_search_list[1] in self.global_results_dict.keys():
        if self.global_results_dict[commit_search_list[1]]:
          return commit_search_list[0]
        else:
          return commit_search_list[1]

      # We reach here if we don't already have the test results for either of
      # the commits.Run the test on jita.
      response_map = {}
      job_ids_list = [None]
      self.run_test_on_jita(clusters[0], commit_search_list[0],
                       build_url=self.commits_dict[commit_search_list[0]],
                       job_ids_list=job_ids_list, index=0)

      response_map[commit_search_list[0]] = job_ids_list[0]
      self.wait_for_jita_test_results(response_map)

      if self.global_results_dict[commit_search_list[0]]:
        return commit_search_list[1]
      else:
        return commit_search_list[0]

    search_list_len = len(commit_search_list)
    # If there are less free clusters, let us do a binary search.
    if len(clusters) < search_list_len and len(clusters) >= 3:
      mid_commit_index = search_list_len / 2
      first_half_mid_commit_index = mid_commit_index / 2
      second_half_mid_commit_index = (
        mid_commit_index + 1 + search_list_len) / 2
      index_list = [mid_commit_index, first_half_mid_commit_index,
                    second_half_mid_commit_index]

      response_map = {}
      threads = [None] * search_list_len
      job_ids_list = [None] * search_list_len
      for index in index_list:
        # If we already have the test result for this commit don't run the test
        # again.
        if commit_search_list[index] in self.global_results_dict.keys():
          continue

        threads[index] = threading.Thread(
          target=self.run_test_on_jita, name="jita_thread",
          args=(clusters.pop(), commit_search_list[index],
                self.commits_dict[commit_search_list[index]], job_ids_list,
                index))
        threads[index].start()

      for index in index_list:
        threads[index].join()
        if job_ids_list[index]:
          return None
        response_map[commit_search_list[index]] = job_ids_list[index]

      self.wait_for_jita_test_results(response_map)

      # Based on the test result, call the function again.
      if not self.global_results_dict[commit_search_list[
        first_half_mid_commit_index]]:
        return self.binary_search_commit_using_jita(
          commit_search_list[0:(first_half_mid_commit_index+1)],
          input_type, clusters)
      elif not self.global_results_dict[commit_search_list[mid_commit_index]]:
        return self.binary_search_commit_using_jita(
          commit_search_list[first_half_mid_commit_index:(mid_commit_index+1)],
          test, username, node_pool, branch, hypervisor, input_type)
      elif not self.global_results_dict[commit_search_list[
        second_half_mid_commit_index]]:
        return self.binary_search_commit_using_jita(
          commit_search_list[mid_commit_index:(second_half_mid_commit_index+1)],
          input_type, clusters)
      else:
        return self.binary_search_commit_using_jita(
          commit_search_list[second_half_mid_commit_index:], input_type,
          clusters)
    elif len(clusters) >= search_list_len:
      # We have enough clusters. Trigger all the runs in parallel.
      response_map = {}
      threads = []
      job_ids_list = [None] * search_list_len
      for index in xrange(len(commit_search_list)):
        threads[index] = threading.Thread(
          target=self.run_test_on_jita, name="jita_thread",
          args=(clusters.pop(), commit_search_list[index],
                self.commits_dict[commit_search_list[index]], job_ids_list,
                index))
        threads[index].start()

      for index in index_list:
        threads[index].join()
        response_map[commit_search_list[index]] = job_ids_list[index]

      self.wait_for_jita_test_results(response_map)
      for commit in commit_search_list:
        if not self.global_results_dict[commit]:
          return commit
    else:
      # We just have one cluster. This is going to take forever.
      mid_commit_index = search_list_len / 2
      response_map = {}
      job_ids_list = [None]
      self.run_test_on_jita(
        clusters[0], commit_search_list[mid_commit_index],
        build_url=self.commits_dict[commit_search_list[mid_commit_index]],
        job_ids_list=job_ids_list, index=0)

      response_map[commit_search_list[mid_commit_index]] = job_ids_list[0]
      self.wait_for_jita_test_results(response_map)

      if not self.global_results_dict[commit_search_list[mid_commit_index]]:
        self.binary_search_commit_using_jita(
          commit_search_list[0:(mid_commit_index+1)], input_type, clusters)
      else:
        self.binary_search_commit_using_jita(
          commit_search_list[mid_commit_index:], input_type, clusters)

  def find_offending_commit(self, commits_file=None):
    input_type = FLAGS.input_type
    self.get_commit_search_list(commits_file, input_type)
    if FLAGS.use_nucloud:
      INFO("Using nucloud to run the tests")
      offending_commit = self.binary_search_commit(
        self.commits_dict.keys(), input_type)
    else:
      clusters = FLAGS.clusters.split(",")
      INFO("Using Jita to run the tests on clusters %s" % clusters)
      offending_commit = self.binary_search_commit_using_jita(
        self.commits_dict.keys(), input_type, clusters)

    if offending_commit == None:
      INFO("The commit cannot be None! BUG.")
    return offending_commit

  def get_free_nodes_from_jarvis(self, pool=NODE_POOL):
    jarvis = JarvisRestClient()
    url = "/api/v1/nodes/orphans?from_pool=%s&pool_user=%s" % (pool, POOL_USER)
    result = jarvis.get(url)
    res = json.loads(result.content)
    free_nodes = []
    for node in res['data']:
      free_nodes.append(node)

    return free_nodes  

  def run_test_on_jita(
    self, cluster, commit_id=None, build_url=None, job_ids_list=None,
    index=None, hypervisor=None, hypervisor_version=None, branch=None,
    input_type="build_url", build_type="opt", test=None, username=None,
    test_upgrade=None, base_commit=None):

    # NEEDS CHANGE
    if not hypervisor:
      hypervisor = FLAGS.hypervisor 
    if not hypervisor_version:
      hypervisor_version = FLAGS.hypervisor_version
    if not branch:
      branch = FLAGS.branch
    if not test:
      test = FLAGS.test_name
    if not username:
      username = USERNAME
    test_framework = FLAGS.test_framework

    if test_upgrade is None:
      test_upgrade = FLAGS.upgrade_test
      base_commit=FLAGS.upgrade_base_commit

    if test_upgrade:
      phoenix_commit = base_commit
    else:
      phoenix_commit = commit_id

    if not self.image_cluster( cluster, phoenix_commit, build_url, USERNAME,
                              hypervisor, hypervisor_version, branch,
                              build_type):
      job_ids_list[index] = False
      ERROR("Imaging failed on cluster %s" % cluster)
      return False

    jita = JitaRest()
    jita_test = jita.get_test(test,test_framework)
    if test_upgrade:
      svm_installer_url = self.get_svm_installer(commit_id, branch, build_type)
      INFO("Found build %s " % svm_installer_url)
      if not svm_installer_url:
        return False

      args_map = {"target_rel" : svm_installer_url}
    else:
      args_map = None

    INFO("Going to submit task for commit id: %s" % commit_id)
    response_obj = jita.submit_task(
      cluster, commit_id, branch, [jita_test.get_id()],
      test_framework=test_framework, label="auto_bisector", args_map=args_map)

    if response_obj.is_success():
      job_ids_list[index] = response_obj.get_task_id()
      return True
    else:
      ERROR("Task submit failed: %s" % response_obj.get_message())
      return False

  def image_cluster(self, cluster, commit_id, build_url, user_email, hyp_type,
                    hyp_version, svm_version, build_type):
    if build_url:
      request = ClientPhoenixRequest(
        user_email=user_email, cluster=cluster, commit_id=commit_id,
        svm_installer_url=build_url,
        hyp_type=hyp_type, hyp_version=hyp_version,
        svm_version=svm_version, build_type=build_type)
    else:
      build_url = self.get_svm_installer(commit_id, svm_version, build_type)

      if not build_url:
        INFO("Could not get build url to phoenix")
        return False

      request = ClientPhoenixRequest(
        user_email=user_email, cluster=cluster, hyp_type=hyp_type,
        hyp_version=hyp_version, svm_version=svm_version,
        build_type=build_type, svm_installer_url=build_url)

    phoenix = PhoenixClient()
    INFO("Imaging cluster %s to build %s" % (cluster, svm_version))
    if not phoenix.image_cluster(request):
      return False

    return True

  def get_svm_installer(self, commit_id, branch, build_type):
    """
    Get the build location for th given commit id and branch.
    """  
    INFO("Getting installer for commit %s" % commit_id)
    filer = FLAGS.lab
    jita=JitaRest()
    build_url = jita.get_build_url(commit_id, build_type, filer)
    if build_url:
      INFO("Found build %s" % build_url)
      return build_url

    INFO("Checking if build is in progress")
    build_task_id = jita.check_build_in_progress(commit_id, branch, build_type,
                                                 filer)

    if build_task_id:
      status = jita.get_build_task(build_task_id).get_status()

    if not build_task_id or status in ["failed", "aborted", "skipped"]:
      INFO("Could not find an existing build. Hence requesting build")
      res = jita.request_build(commit_id, branch, build_type, filer)
      if res.get_status():
        build_task_id = res.get_build_task_id()
      else:
        ERROR("Build request for commit id %s failed" % commit_id)
        return False

    if self.wait_for_build_to_complete(build_task_id):
      return jita.get_build_url(commit_id, build_type, filer)

    return False

  def is_build_complete(self, build_task_id):
    jita=JitaRest()
    status = jita.get_build_task(build_task_id).get_status()
    if status not in ["passed", "failed", "skipped", "aborted"]:
      return False
    else:
      return True
  
  def wait_for_build_to_complete(self, build_task_id, timeout=7200,
                                 poll_period=180):
    expired_time = 0
    while expired_time < timeout:
      if self.is_build_complete(build_task_id):
        break
      time.sleep(poll_period)
      expired_time += poll_period

    jita=JitaRest()
    status = jita.get_build_task(build_task_id).get_status()
    if status != "passed":
      ERROR("Timed out waiting for build to complete or build failed. Status:"
            " %s" % status)
      return False
    return True

  def wait_for_jita_test_results(self, job_info, timeout=18000,
                                 poll_period=600):
    # Sleep for 10 minutes before starting polling for results.
    expired_time = 0
    unfinished_jobs = job_info.values()

    jita = JitaRest()
    while len(unfinished_jobs) and expired_time < timeout:
      for job_id in job_info.values():
        if self.did_jita_task_finish(job_id):
          if job_id in unfinished_jobs:
            unfinished_jobs.remove(job_id)

      time.sleep(poll_period)
      expired_time += poll_period

    for commit, job_id in job_info.iteritems():
      response = jita.get_task(job_id)
      if response.get_status() not in ["passed", "completed"]:
        self.global_results_dict[commit] = False
      else:
        test_results = response.get_test_results()
        #test_results = response.get_test_result_dict()
        for test, result in test_results.test_result_dict.iteritems():
          if result.get_status() != "Succeeded":
            INFO("Test %s failed with status %s" % (test, result.get_status()))
            self.global_results_dict[commit] = False
            break
          self.global_results_dict[commit] = True

    return True

  def did_jita_task_finish(self, job_id):
    jita = JitaRest()
    response = jita.get_task(job_id)
    status = response.get_status()
    if status in ["passed", "killed", "failed", "completed"]:
      return True
    else:
      return False

  def setup_logging(self, file_template):
    """ Sets up the log file.
    Args:
      file_template (str): Template file name for the log file
    """
    os.path.exists("/tmp") or os.mkdir("/tmp")
    initialize("/tmp/%s.log" % file_template)
    print "Logging to file /tmp/%s.log" % file_template

  def get_commits_in_range(self, first_commit, last_commit, branch="master"):
    """
    Get all the commits in the range of the two commits.
    """
    jita = JitaRest()
    commits = jita.get_commits(branch=branch, num_commits=100)
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

    commit_range.reverse()
    return commit_range

def main(argv):
  try:
    argv = FLAGS(argv)
  except gflags.FlagsError, err:
    print "%s\nUsage: %s ARGS\n%s" % (err, sys.argv[0], FLAGS)
    sys.exit(1)

  if not FLAGS.commits_file and not (
    FLAGS.last_good_commit and FLAGS.latest_bad_commit):
    print "Exiting! One of commits_file or last_good_commit and "
    "latest_bad_commit is a required parameter."
    sys.exit(1)

  file_template = "auto_bisector_log_%d" % (int(time.time()))
  bisect = Auto_Bisect()
  bisect.setup_logging(file_template)
  commits_file = FLAGS.commits_file
  offending_commit = bisect.find_offending_commit(commits_file)

  INFO("Found the offending commit: %s" %
       offending_commit)

if __name__ == "__main__":
  main(sys.argv)
