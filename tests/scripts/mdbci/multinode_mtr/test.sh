#!/bin/bash
set -x
export target=$1
export DRONE_BUILD_NUMBER=$2
export box=$3
export link_suff=$4
~/mdbci/mdbci generate-product-repositories --product mdbe_ci --product-version $target
export provider=`echo ${box##*_}`
./columnstore_vm.py --machine-name $target-${box} --box ${box} --maxscale-box rocky_8_$provider --target $target --server-product MariaDBEnterprise --num 3 --install-test-package True
./columnstore.py --machine-name $target-${box} --num 3 --storage gluster --aws True
./run_mtr.py --machine-name $target-${box} --node-name cs000 --mtr-param mtr-columnstore-test --target $target --product MariaDBEnterprise --use-packages True --install-packages False --multinode True --log-path $HOME/multinode_logs/ --build-number $DRONE_BUILD_NUMBER  --log-server localhost --log-server-user `whoami` 

