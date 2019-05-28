disks=[36,37,51,52,58,59,69,70,1256,1257,1397,1398,1408,1407,1665,1666]
import os
from subprocess import Popen, PIPE
egids=open("test", "r").read()
egid_arr=egids.split("\n")
for line in egid_arr:
    egid=line.split("   ")[1]
    #print egid
    p = Popen("medusa_printer --lookup egid --egroup_id {0} | grep -w disk_id | cut -d\":\" -f2 | xargs".format(egid), shell=True, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    arr=out.rstrip().split()
    #print arr
    for d in arr:
        d = int(d)
        print "disk:{0}".format(d)
        if d in disks:
            print "In SSD : {0}, in disk {1}".format(egid, d)
        else:
            print"In HDD : {0}, in disk {1}".format(egid, d)


