cluster=$1
CMD="ssh -o CheckHostIp=no -o ConnectTimeout=15 -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=300   -i $TOP/.python/util/net/../../../../installer/ssh_keys/nutanix nutanix@"
host="$cluster-$i"
SSHCMD=$CMD$host
SSH="$SSHCMD vdisk_config_printer | grep '^vdisk_id:' | sed 's/[^0-9]*//g' > vdisks"
out=$($SSH)
#vdisk_config_printer | grep '^vdisk_id:' | sed 's/[^0-9]*//g' > vdisks
cat vdisks | while read vdisk
do
vdisk_usage_printer -vdisk_id=$vdisk | sed -e 's/^[ \s]*//' | grep '^[0-9]' | grep -v "X"
done
