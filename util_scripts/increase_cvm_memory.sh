#! /bin/sh 
cluster=$1
echo Working on cluster $cluster

CMD="ssh -o CheckHostIp=no -o ConnectTimeout=15 -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=300   -i $TOP/.python/util/net/../../../../installer/ssh_keys/nutanix root@"
#NUM=1
#HOST="$cluster-$NUM"
#echo host 
i="1"
while [ $i -lt 5 ]
do
  host="$cluster-$i"
  SSHCMD=$CMD$host
  SSH="$SSHCMD virsh list --all"
  out=$($SSH)
  out1=($out)
  cvm=${out1[5]}
  SSH="$SSHCMD virsh shutdown $cvm"
  echo "shutting down"
  $SSH
#  sleep 120
#  SSH="$SSHCMD virsh setmaxmem $cvm 32G --config"
#  $SSH
#  SSH="$SSHCMD virsh setmem $cvm 32G --config"
#  $SSH
#  SSH="$SSHCMD virsh start $cvm "
#  echo "Starting up"
#  $SSH
  i=$[$i+1]
done
sleep 120
i="1"
while [ $i -lt 5 ]
do
  host="$cluster-$i"
  SSHCMD=$CMD$host
  SSH="$SSHCMD virsh list --all"
  out=$($SSH)
  out1=($out)
  cvm=${out1[5]}
  SSH="$SSHCMD virsh setmaxmem $cvm 32G --config"
  $SSH
  SSH="$SSHCMD virsh setmem $cvm 32G --config"
  $SSH
  SSH="$SSHCMD virsh start $cvm "
  echo "Starting up"
  $SSH
  i=$[$i+1]
done
