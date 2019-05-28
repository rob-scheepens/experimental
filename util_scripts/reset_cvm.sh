#! /bin/sh 
cluster=$1
echo Working on cluster $cluster

CMD="ssh -o CheckHostIp=no -o ConnectTimeout=15 -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=300   -i $TOP/.python/util/net/../../../../installer/ssh_keys/nutanix root@"
#NUM=1
#HOST="$cluster-$NUM"
#echo host 
#i="1"
while true;
do
i="1"
while [ $i -lt 4 ]
do
  host="$cluster-$i"
  SSHCMD=$CMD$host
  SSH="$SSHCMD virsh list --all"
  out=$($SSH)
  out1=($out)
  cvm=${out1[5]}
  SSH="$SSHCMD virsh reset $cvm"
  echo "shutting down"
  $SSH
  sleep 300
  i=$[$i+1]
done
done
