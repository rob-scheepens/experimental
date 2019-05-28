#!/bin/bash
 
default_pw="nutanix/4u"
cvm_list=()
ahv_list=()
cvm_pw=""
ahv_pw=""
 
for i in "$@"; do
case $i in
  --cvm=*)
  cvm_input="${i#*=}"
  cvm_list=(${cvm_input//,/ })
  shift # past argument=value
  ;;
 
  --cpw=*)
  cvm_pw="${i#*=}"
  shift # past argument=value
  ;;
 
  --ahv=*)
  ahv_input="${i#*=}"
  ahv_list=(${ahv_input//,/ })
  shift # past argument=value
  ;;
 
  --apw=*)
  ahv_pw="${i#*=}"
  shift # past argument=value
  ;;
  *)
  # unknown option
  ;;
esac
done
 
for cvm in "${cvm_list[@]}"; do
  echo Resetting $cvm password
  sshpass -p $cvm_pw ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no nutanix@$cvm "sudo sed -i \"s/even_deny_root//g\" /etc/pam.d/system-auth-local"
  sshpass -p $cvm_pw ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no nutanix@$cvm "sudo sed -i \"s/enforce_for_root//g\" /etc/pam.d/system-auth-local"
  sshpass -p $cvm_pw ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no nutanix@$cvm "echo -e \"$default_pw\n$default_pw\" | sudo passwd nutanix"
done
 
for ahv in "${ahv_list[@]}"; do
  echo Resetting $ahv password
  sshpass -p $ahv_pw ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no root@$ahv "sudo sed -i \"s/even_deny_root//g\" /etc/pam.d/system-auth-local"
  sshpass -p $ahv_pw ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no root@$ahv "sudo sed -i \"s/enforce_for_root//g\" /etc/pam.d/system-auth-local"
  sshpass -p $ahv_pw ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no root@$ahv "echo -e \"$default_pw\n$default_pw\" | passwd"
done
