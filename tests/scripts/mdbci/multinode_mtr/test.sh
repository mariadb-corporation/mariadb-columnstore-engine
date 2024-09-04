#!/bin/bash

# This script can be used by Drone to run multinode tests

# $1 - target (directory name) in the CI reposiotory:
#     For Drone repo: e.g. "columnstore/custom/10690/10.6-enterprise/"
#     For Jenkins repo: e.g. "ENTERPRISE/10.6.18-14-columnstore-23.10/4cd9db27766249eeb868f1c68456aefde5d9a413" (see https://es-repo.mariadb.net/jenkins/)
#     For BuildBot repo: e.g. "10.6.18-14-latest" (see https://mdbe-ci-repo.mariadb.net/MariaDBEnterprise/)
# $2 - build number (used as a name for logs directory)
# $3 - MDBCI box, e.g. "rocky_9_aws"
# $4 - suffix for logs path (logs are stored to $HOME/multinode_logs/$2/$4)


set -x
export target=$1
export DRONE_BUILD_NUMBER=$2
export box=$3
export link_suff=$4
~/mdbci/mdbci generate-product-repositories --product mdbe_ci --product-version $target
export provider=`echo ${box##*_}`
./columnstore_vm.py --machine-name $target-${box} --box ${box} --maxscale-box rocky_8_$provider --target $target --server-product MariaDBEnterprise --num 3 --install-test-package True
./columnstore.py --machine-name $target-${box} --num 3 --storage gluster --aws True
./run_mtr.py --machine-name $target-${box} --node-name cs000 --mtr-param mtr-columnstore-multinode-test --target $target --product MariaDBEnterprise --use-packages True --install-packages False --multinode True --log-path $HOME/multinode_logs/ --build-number $DRONE_BUILD_NUMBER  --log-server localhost --log-server-user `whoami` 

