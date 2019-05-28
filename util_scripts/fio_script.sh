for x in {a..z}
do
    echo "$x"
    fio --bs=4k --ioengine=libaio --iodepth=512 --rw=randrw --filename=/dev/sd${x} --name=t1 --size=5g --runtime=60 --time_based
done
for x in {a..c}
do
  for y in {a..z}
  do
    echo "$x$y"
    fio --bs=4k --ioengine=libaio --iodepth=512 --rw=randrw --filename=/dev/sd${x}${y} --name=t1 --size=1g --runtime=60 --time_based
  done
done
~
