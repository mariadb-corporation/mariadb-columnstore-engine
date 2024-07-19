#!/bin/bash

set -x
export target="10.6.18-14-cs-05"
export box="rocky_9_libvirt"
~/mdbci/mdbci generate-product-repositories --product mdbe_ci --product-version $target
export provider=`echo ${box##*_}`
./columnstore_vm.py --machine-name $target-${box} --box ${box} --maxscale-box rocky_8_$provider --target $target --server-product MariaDBEnterprise --num 3 --install-test-package True
./columnstore.py --machine-name $target-${box} --num 3 --storage gluster
# use "./columnstore.py --machine-name $target-${box} --num 3 --storage gluster --aws True" if $box is AWS box

./run_mtr.py --machine-name $target-${box} --node-name cs000 --mtr-param mtr-columnstore-multinode-setup-test --target $target --product MariaDBEnterprise --use-packages True --install-packages False --multinode True
./run_mtr.py --machine-name $target-${box} --node-name max --mtr-param mtr-columnstore-multinode-maxscale-test --target $target --product MariaDBEnterprise --use-packages True --install-packages False --multinode True


