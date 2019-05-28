i=0
while [  $i -lt 319715200 ]; do
  echo The lba is $i
  sg_unmap --lba=$i --num=28672 /dev/sdc
  let i=i+28672
  done

