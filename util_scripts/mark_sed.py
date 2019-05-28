import env

from zeus.zookeeper_session import ZookeeperSession
from zeus.configuration import Configuration
from zeus.configuration_pb2 import ConfigurationProto

config = Configuration()
config.initialize()
proto = config.config_proto()
for disk in proto.disk_list:
  zeus_sed = ConfigurationProto.Disk.SelfEncryptingDrive()
  disk.self_encrypting_drive.CopyFrom(zeus_sed)

config.commit(proto)
