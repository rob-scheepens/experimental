#!/usr/bin/python
"""
Copyright (c) 2019 Nutanix Inc. All rights reserved.

Author: gokul.kannan@nutanix.com

This script deletes all the volume groups on the cluster. Copy this script to
/home/nutanix/cluster/bin directory, add executable permission to the file and
invoke it with the following command:

delete_vgs --username <prism username> --password <prism password>

"""
import env
import gflags
import json
import requests
import sys

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

SERVER = "127.0.0.1:9440"
VG_LIST_URL = "/PrismGateway/services/rest/v2.0/volume_groups"
VG_UPDATE_URL = "/PrismGateway/services/rest/v2.0/volume_groups/{0}"
VG_DETACH_URL = "/PrismGateway/services/rest/v2.0/volume_groups/{0}/detach"

FLAGS = gflags.FLAGS

gflags.DEFINE_string("username", None,
                     ("prism username"))

gflags.DEFINE_string("password", None,
                     ("prism password"))

gflags.DEFINE_list("vg_names_to_skip", None,
                     ("The VGs containing this name will be skipped"))

gflags.DEFINE_bool("ask_for_confirmation", True,
                     ("Ask for confirmation every time before deleting VG"))

MARKER = "\n" + "#" * 100 + "\n"

def remove_vgs():
  """
  List all the VGs on the cluster.
  """
  url = "https://%s%s" % (SERVER, VG_LIST_URL)
  auth = (FLAGS.username, FLAGS.password)
  response = requests.get(url, auth=auth, verify=False)
  if not response.ok:
    print("Could not get list of VGs from Prism. Error code %s Url: %s"
          "\nAborting!!\n" (response.status_code, response.url))
    sys.exit(1)

  for entity in response.json()['entities']:
    print MARKER
    url = "https://%s%s" % (SERVER, VG_UPDATE_URL.format(entity["uuid"]))
    if FLAGS.vg_names_to_skip:
      if entity["name"] in FLAGS.vg_names_to_skip:
        print "Not deleting VG %s" % entity["name"]
        continue

    if FLAGS.ask_for_confirmation:
      choice = raw_input("WARNING: Deleting VG %s. \nOK to continue?(y/n):" %
                         entity["name"])
      if choice != 'y':
        print("User did not respond with \"y\".Skippping VG %s !!" %
              entity["name"])
        continue

    params = {}
    params["attached_clients"] = []
    params["uuid"] = entity["uuid"]
    params["name"] = entity["name"]
    auth = (FLAGS.username, FLAGS.password)
    print "Removing attachments from VG %s" % params["name"]
    response = requests.put(url, data=json.dumps(params), auth=auth,
                            verify=False)
    if not response.ok:
      print("Could not update the VG from Prism. Error code %s Url: %s"
            "\nAborting!!\n" (response, url))
      sys.exit(1)

    url = "https://%s%s/%s" % (SERVER, VG_LIST_URL, entity["uuid"])
    auth = (FLAGS.username, FLAGS.password)
    response = requests.get(url, auth=auth, verify=False)
    if not response.ok:
      print("Could not get the VG from Prism. Error code %s Url: %s"
            "\nAborting!!\n" (response, url))
      sys.exit(1)
    for attachment in response.json().get("attachment_list", []):
      if "vm_uuid" in attachment.keys():
        url = "https://%s%s" % (SERVER, VG_DETACH_URL.format(entity["uuid"]))
        params = {"vm_uuid": attachment["vm_uuid"]}
        response = requests.post(url, auth=auth, data=json.dumps(params),
                                 verify=False)
        if not response.ok:
          print("Could not remove Vm attachment from the VG. Error code %s "
                "Url: %s\nAborting!!\n" (response, url))
          sys.exit(1)

    url = "https://%s%s" % (SERVER, VG_UPDATE_URL.format(entity["uuid"]))
    auth = (FLAGS.username, FLAGS.password)
    print "Deleting VG %s" % entity["name"]
    response = requests.delete(url, auth=auth, verify=False)
    if not response.ok:
      print("Could not delete the VG from Prism. Error code %s Url: %s"
            "\nAborting!!\n" (response, url))
      sys.exit(1)

  return True

def main(argv):
  try:
    argv = FLAGS(argv)
  except gflags.FlagsError, err:
    print "%s\nUsage: %s ARGS\n%s" % (err, argv[0], FLAGS)
    sys.exit(1)

  # Give adequate warning.
  choice = raw_input(
    "%sWARNING: This script will permanently delete all the volume groups in "
    "the cluster except VGs containing the name %s. \nOK to continue?(y/n):"
    % (MARKER, FLAGS.vg_names_to_skip))

  if choice != 'y':
    print("User did not respond with \"y\".Exiting !!")
    sys.exit(1)

  remove_vgs()

if __name__ == "__main__":
  main(sys.argv)