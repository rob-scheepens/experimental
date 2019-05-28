"""
Copyright (c) 2018 Nutanix Inc. All rights reserved.

Author: gokul.kannan@nutanix.com

Currently, cluster level encryption can only be enabled on
clusters which are empty. This script provides a workaround for existing AHV
customers to enable cluster level encryption when the cluster already has data.
This script must be used under supervision of Nutanix Support or Nutanix
Engineering. The script must be copied to /home/nutanix/cluster/bin
directory of the CVM and run from there.

Before running this script, the Key Management Server and the associated
certificates must be setup by following the Data At Rest Encryption guide under
Security Management section of the Prism Web Console Guide.

The script can be runs as follows:
./enable_cluster_encryption_on_existing_ahv --username "username"
--password "password".
"""

import env
import gflags
import json
import requests
import sys

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from zeus.zookeeper_session import ZookeeperSession
from zeus.configuration import Configuration
from zeus.configuration_pb2 import ConfigurationProto

SERVER = "127.0.0.1:9440"
DARE_URL = "/PrismGateway/services/rest/v2.0/data_at_rest_encryption"
CLUSTER_URL = "/PrismGateway/services/rest/v2.0/cluster"

FLAGS = gflags.FLAGS

gflags.DEFINE_string("username", None,
                     ("prism username"))

gflags.DEFINE_string("password", None,
                     ("prism password"))

MARKER = "\n" + "#" * 100 + "\n"

def print_error(msg):
  print "%sERROR: %s" % (MARKER, msg)

def print_info(msg):
  print "%sINFO: %s" % (MARKER, msg)

def verify_hypervisor_is_ahv():
  url = "https://%s%s" % (SERVER, CLUSTER_URL)
  auth = (FLAGS.username, FLAGS.password)
  response = requests.get(url, auth=auth, verify=False)
  if not response.ok:
    print_error("Could not get cluster status from Prism. Error code %s Url: %s"
                "\nAborting!!\n" (response.status_code, response.url))
    sys.exit(1)

  for hypervisor_type in response.json()['hypervisor_types']:
    if hypervisor_type != "kKvm":
      print_error("Not an AHV hypervisor. Aborting!! \n")
      sys.exit(1)

  return True

def verify_cluster_has_data():
  url = "https://%s%s" % (SERVER, DARE_URL)
  auth = (FLAGS.username, FLAGS.password)
  response = requests.get(url, auth=auth, verify=False)
  assert response.ok, "Unexpected error %d when posting %s" % (
    response.status_code, response.url)

  if response.json()['is_cluster_empty']:
    print_error("The Cluster has no data in it. Cluster level encryption can be "
               "enabled via Prism UI. Aborting!!\n")
    sys.exit(1)

  return True

def enable_container_encryption():
  if is_encryption_enabled(scope="CLUSTER"):
    print_error("Encryption is enabled at cluster level. Aborting!!\n")
    sys.exit(1)

  if is_encryption_enabled():
    print_info("Encryption is already enabled at container level.")
    return True

  url = "https://%s%s/enable" % (SERVER, DARE_URL)
  params = {}
  params["software_encryption_scope"] = "STORAGE_CONTAINER"
  auth = (FLAGS.username, FLAGS.password)
  response = requests.post(url, data=json.dumps(params), auth=auth, verify=False)
  if not response.ok:
    print_error("Encryption at container level could not be enabled. "
          "Unexpected http error %d when posting %s. Raw error message: %s. \n "
          "Aborting!!\n" % (response.status_code, response.url, response.json())
          )
    sys.exit(1)

def is_encryption_enabled(scope="STORAGE_CONTAINER"):
  url = "https://%s%s" % (SERVER, DARE_URL)
  auth = (FLAGS.username, FLAGS.password)
  response = requests.get(url, auth=auth, verify=False)
  assert response.ok, "Unexpected error %d when posting %s" % (
    response.status_code, response.url)
  if response.json()['software_encryption_scope'] != scope:
    print_info("Current encryption level %s " % response.json()[
      'software_encryption_scope'])
    return False

  return True

def enable_cluster_encryption_in_zeus():
  print_info("Setting Cluster encryption in Zeus.")
  zk_session = ZookeeperSession(connection_timeout=60)
  if not zk_session.wait_for_connection(None):
    zk_session = None

  zeus_config = Configuration().initialize(zk_session = zk_session)
  proto = zeus_config.config_proto()
  proto.cluster_encryption_params.encryption_scope = 0
  zeus_config.commit(proto)

try:
  argv = FLAGS(sys.argv)
except gflags.FlagsError, err:
  print "%s\nUsage: %s ARGS\n%s" % (err, sys.argv[0], FLAGS)
  sys.exit(1)

if not FLAGS.username or not FLAGS.password:
  print_error("Please provide the prism credentials. \nUsage: %s ARGS\n%s" % (
    sys.argv[0], FLAGS))
  sys.exit(1)

# This workaround is supported only on AHV cluster.
verify_hypervisor_is_ahv()

# If the cluster is empty, there is no need for this workaround.
verify_cluster_has_data()

enable_container_encryption()
enable_cluster_encryption_in_zeus()
print_info("Success!! Cluster level encryption was successfully enabled."
           "Please create new containers and move the VMs to the new "
           "containers. Delete the old containers once the VMs have been moved."
           )
