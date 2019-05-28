
from pyVim import connect       
service_instance = connect.SmartConnect(host="10.4.66.64", user="administrator@vsphere.local", pwd="Nutanix/4u", port=443)
content = service_instance.RetrieveContent()
dcname = "ans_K"
clustername = "drs"
children = content.rootFolder.childEntity
for child in children:
    if str(child.name) in dcname and dcname in str(child.name):
      dcmor = child
      break
if dcmor is None:
    print "No good"
for cluster in dcmor.hostFolder.childEntity:
      if cluster.name == clustername:
        clustermor=cluster
for host in cluster.host:
    print host.name
    print host
#for virtual_machine in clustermor.vmFolder.childEntity:
#    print virtual_machine.name
    #if virtual_machine.name == basevmname:
    #  vmmor=virtual_machine
    #  break
