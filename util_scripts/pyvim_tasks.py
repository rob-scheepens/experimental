from pyVim.connect import SmartConnectNoSSL, Disconnect
from pyVmomi import vim, vmodl

hosts = ["10.5.66.146", "10.5.66.147", "10.5.66.148", "10.5.66.149"]
vcenter = "10.4.66.64"

for host in hosts:
  si = SmartConnectNoSSL(user="root", pwd="nutanix/4u", host=host ,port=443,
                          connectionPoolTimeout=-1)
  for datacenter in si.content.rootFolder.childEntity:
    if isinstance(datacenter, vim.Datacenter):
      ds=datacenter
      break
  for compute_resource in ds.hostFolder.childEntity:
    if isinstance(compute_resource, vim.ComputeResource):
      break

  host_obj = compute_resource.host[0]
  hostds_system = host_obj.configManager.datastoreSystem
#  for dst in hostds_system.datastore:
#    if dst.name == "ctr1":
#      hostds_system.RemoveDatastore(dst)
  
  for vm in host_obj.vm:
    print vm.name

  


    