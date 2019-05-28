import re
import subprocess
import os

cmd = "multipath -ll"
#results = NutanixClusterSSHUtil.execute([uvm], cmd)[0]
r  = os.popen(cmd)
results = r.read()
#self.check_eq(results[0], 0, "Command  failed")
lines = results.split("\n")
multipaths = {}
active_devices = []
found_active = 0
# Parse through each line and find the vdisk name and then the device which
# is currently active. The output is expected to be something like below:
###########################################################################
# 1NUTANIX_NFS_1_0_5100_b29f577f_1354_4c4a_ae12_f3809dea44cf dm-3 NUTANIX
# size=19G features='0' hwhandler='0' wp=rw
# |-+- policy='service-time 0' prio=1 status=active
# | `- 17:0:0:1 sdh 8:112 active ready running
# |-+- policy='service-time 0' prio=1 status=enabled
# | `- 16:0:0:1 sde 8:64  active ready running
# |-+- policy='service-time 0' prio=1 status=enabled
# | `- 18:0:0:1 sdi 8:128 active ready running
# `-+- policy='service-time 0' prio=1 status=enabled
# `- 15:0:0:1 sdd 8:48  active ready running
###########################################################################

for line in lines:
    if not line:
        continue
    line = line.strip()
    print("line is %s" % line)
    if  re.search("1NUTANIX_NFS", line):
        out = re.split("_", line)
        vdisk_name = ":".join([out[1],out[2],out[3],out[4]])
        multipaths[vdisk_name] = {}
        continue
    if re.search("status=active", line):
        # fail if found_active is still set
        if not found_active:
            print("Error in parsing or the command output not as expected")
        found_active = 1
        continue
    if re.search("\`-",line) and found_active:
            out = re.split("\s", line)
            # This should be the only active device for the vdisk.
            if 'active_device' in multipaths[vdisk_name].keys():
                print("Error in parsing or the command output not "
                      "as expected. There are more than one active devices"
                      "in the multipath -ll output: %s" % results[1])
            multipaths[vdisk_name] = {'active_device':out[3] }
            active_devices.append(out[3])
            found_active = 0
            continue
# Perform some sanity checks.
for key, value in multipaths.iteritems():
    if 'active_device' not in multipaths[key].keys():
        print("No active paths found for vdisk %s" %key)
# We have found the devices which are active. Now find the corresponding
# CVM ip.
cmd = "iscsiadm -m session -P 3"
results = NutanixClusterSSHUtil.execute([uvm], cmd)[0]
self.check_eq(results[0], 0, "Command  failed")
lines = results[1].split("\n")
for line in lines:
    if re.search("Target:", line):
        line = line.lstrip()
        out = re.split("\s", line)
        target = out[1]
        continue
    if re.search("Current Portal:", line):
        out = re.split("\s", line)
        cvm_ip = re.split(":", out[3])[0]
        print cvm_ip
        continue
    if re.search("Attached scsi disk", line):
        line = line.lstrip()
        out = re.split("\s", line)
        device = out[3]
        if device in active_devices:
            for key, value in multipaths.iteritems():
                if multipaths[key]['active_device'] == device:
                    multipaths[key].update({'active_path' : cvm_ip})
                    multipaths[key].update({'target' : target})

for key, value in multipaths.iteritems():
    if 'active_path' not in multipaths[key].keys():
        print("No active paths found for vdisk %s" %key)






