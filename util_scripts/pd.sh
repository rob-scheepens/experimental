#!/bin/bash
while true;
do
source /etc/profile; ncli pd add-one-time-snapshot name=pd1 remote-sites=scorpion03 retention-time=10000
sleep 900
done
