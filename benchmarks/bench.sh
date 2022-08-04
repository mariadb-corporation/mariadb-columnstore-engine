#!/usr/bin/env bash

set -Eeo pipefail



SCRIPT_LOCATION=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)
MDB_SOURCE_PATH=$(realpath $SCRIPT_LOCATION/../../..)


source $MDB_SOURCE_PATH/columnstore/columnstore/build/utils.sh

if [ "$EUID" -ne 0 ]
    then error "Please run this script as root"
    exit 1
fi

message "A script to run benchmark(for now) to compare the pefromanceo of min/max
calculation for two provided branches. Runs the provided .lua script using sysbench.
"
optparse.define short=a long=branch1 desc="Branch1 to compare" variable=BRANCH1 default="develop"
optparse.define short=b long=branch2 desc="Branch2 to compare" variable=BRANCH2
optparse.define short=s long=script-bench desc="Lua benchmark script" variable=SCRIPT
optparse.define short=g long=gen desc="Data generator for the test table(to be used by cpimport)" variable=GEN
optparse.define short=n long=name desc="Name of the test table" variable=TABLE default="t1"
optparse.define short=t long=time desc="Time (in seconds) to run the benchmark" variable=TIME default=120

source $( optparse.build )

export TABLE
trap cleanup SIGINT SIGTERM ERR EXIT

cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
  sysbench $SCRIPT --mysql-socket=/run/mysqld/mysqld.sock \
        --db-driver=mysql \
        --mysql-db=test \
        cleanup > /dev/null
  if [ -f "$DATA" ] ; then
    sudo rm "$DATA"
  fi
  unset TABLE
}


die() {
  local msg=$1
  local code=${2-1} # default exit status 1
  echo "$msg"
  exit "$code"
}

cd $PWD
LUA_PATH=$MDB_SOURCE_PATH/columnstore/columnstore/benchmarks/?.lua
export LUA_PATH
DATA=$(sudo mktemp -p /var)
eval ./$GEN > "$DATA"

git checkout $BRANCH1
sudo $MDB_SOURCE_PATH/columnstore/columnstore/build/bootstrap_mcs.sh -t RelWithDebInfo
echo "Build done; benchmarking $BRANCH1 now"
git checkout with_benchmarks
#Prepare should only create the table, we will fill it with cpimport
sysbench $SCRIPT \
        --mysql-socket=/run/mysqld/mysqld.sock \
        --db-driver=mysql \
        --mysql-db=test \
        prepare

sudo cpimport test "$TABLE" "$DATA"

BRANCH1_DATA=$(sysbench $SCRIPT \
        --mysql-socket=/run/mysqld/mysqld.sock \
        --db-driver=mysql \
        --mysql-db=test \
        --time=$TIME run | tail -n +12)

git checkout $BRANCH2
sudo $MDB_SOURCE_PATH/columnstore/columnstore/build/bootstrap_mcs.sh -t RelWithDebInfo
echo "Build done; benchmarking $BRANCH2 now"
git checkout with_benchmarks
sysbench $SCRIPT \
        --mysql-socket=/run/mysqld/mysqld.sock \
        --db-driver=mysql \
        --mysql-db=test \
        prepare

sudo cpimport test "$TABLE" "$DATA"

BRANCH2_DATA=$(sysbench $SCRIPT \
        --mysql-socket=/run/mysqld/mysqld.sock \
        --db-driver=mysql \
        --mysql-db=test \
        --time=$TIME run | tail -n +12)

cd $MDB_SOURCE_PATH/columnstore/columnstore/benchmarks
python3 parse_bench.py "$BRANCH2" "$BRANCH1" "$BRANCH2_DATA" "$BRANCH1_DATA" "$TIME"
