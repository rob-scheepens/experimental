import env

from serviceability.license_pb2 import *
from zeus.zookeeper_session import ZookeeperSession

with ZookeeperSession() as zks:
  if not zks.wait_for_connection(120):
    print("Failed to connect to zookeeper - Check zookeeper config.")
  else:
    zkpath = "/appliance/logical/license/license_file"
    license_proto = LicenseProto()
    license_proto.cluster_license_type.license_category = "Ultimate"
    license_proto.cluster_license_type.license_class = "appliance"
    license_proto.license_version = 2

    license_info_proto = LicenseProto.LicenseInfo()
    license_info_proto.license_type.license_category = "Ultimate"
    license_info_proto.license_class = "appliance"
    license_proto.license_list.extend([license_info_proto])

    addon = LicenseType()
    addon.license_category = "Software_Encryption"
    addon.license_sub_category = "addon"
    license_proto.cluster_license_type.addon_list.extend([addon])
    addon_license_proto = LicenseProto.LicenseInfo()
    addon_license_proto.license_type.license_category = "Software_Encryption"
    addon_license_proto.license_type.license_sub_category = "addon"
    addon_license_proto.license_class = "appliance"
    license_proto.addon_license_list.extend([addon_license_proto])

    # Delete the zk node and then set the params.
    old_logical_timestamp = license_proto.logical_timestamp
    license_proto.logical_timestamp += 1
    zks.delete(zkpath)
    if not  zks.create(zkpath, ""):
      print("Unable to create zk node %s" % zk_node)

    if not zks.set(zkpath, license_proto.SerializeToString(),
                          version=old_logical_timestamp):
      print("Unable to set zk node %s" % zkpath)