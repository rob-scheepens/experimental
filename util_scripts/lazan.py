# Copyrights (c) Nutanix Inc. 2017
#
# Author: prachi.gupta@nutanix.com
"""Module to abstract Lazan component."""

from .nos_component import NOSComponent
from framework.entities.cluster.nos_cluster import BaseCluster
from framework.exceptions.nutest_error import NuTestError
from framework.interfaces.rpc.rpc_client import RPCClient

class Lazan(NOSComponent):
  """Lazan component."""

  BINARY_PATH = "/home/nutanix/bin/lazan"
  NAME = "lazan"
  PORT = 2038

  def __init__(self, entity):
    """Constructor.

    Args:
      entity (object): NOSCluster or NOSVM entity object.
    """
    super(Lazan, self).__init__(entity)
    if isinstance(entity, BaseCluster):
      self.cluster = entity
      self.rpc_helper = None

  def get_lazan_task(self, task):
    self.rpc_helper = self.__setup_rpc()
    return self.rpc_helper.get_lazan_task(task)

  def __setup_rpc(self):
    """Sets up the RPC client to talk to the RPC server for Lazan.
    Args:
      None
    Returns:
      rpc_helper (object): rpc_helper handler object to invoke RPC's.
    Raises:
      NutestError: If there is a failure in setting up the RPC server.
    """
    if hasattr(self, "cluster"):
      # Check if rpc_helper is already init
      if self.rpc_helper is not None:
        return self.rpc_helper

      # Create the rpc helper object for issuing RPCs.
      rpc = RPCClient(self.cluster)
      rpc.proxy_client.register_class_functions(\
        [("/home/nutanix/rpc/rpc_helpers/lazan/lazan_rpc.py",
          {"LazanRPCHelper": 0})])

      self.rpc_helper = rpc.proxy_client.LazanRPCHelper
      return self.rpc_helper
    else:
      message = ("Cannot setup RPC support as Lazan is not initialized "
                 "with cluster object")
      raise NuTestError(message=message)
