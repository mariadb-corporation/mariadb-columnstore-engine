#!/usr/bin/env bash

set -Eeuo pipefail



trap cleanup SIGINT SIGTERM ERR EXIT

SCRIPT_LOCATION=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)
MDB_SOURCE_PATH=$(realpath $SCRIPT_LOCATION/../../..)

BRANCH="$1"
SCRIPT="$2"
DATA="$3"
TABLE="t1"
export TABLE

cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
  if [ -f $DATA ]
     then
         sudo rm $DATA
  fi
  if [ -f "${BRANCH}_bench.txt" ]
     then
         sudo rm "${BRANCH}_bench.txt"
  fi
  if [ -f "develop_bench.txt" ]
     then
         sudo rm "develop_bench.txt"
  fi
  sysbench $SCRIPT --mysql-socket=/run/mysqld/mysqld.sock \
        --db-driver=mysql \
        --mysql-db=test \
        cleanup > /dev/null
  unset TABLE
}


source $MDB_SOURCE_PATH/columnstore/columnstore/build/utils.sh

if [ "$EUID" -ne 0 ]
    then error "Please run this script as root"
    exit 1
fi

message "Usage: $(basename "${BASH_SOURCE[0]}") branch lua_script data [-h] [-t t1]

A script to run benchmark(for now) to compare the pefromanceo of min/max
calculation on develop and provided branch. Runs the provided .lua script using sysbench.
"
optparse.define short=t long=table desc="Name of the test table" variable=TABLE

source $( optparse.build )

die() {
  local msg=$1
  local code=${2-1} # default exit status 1
  echo "$msg"
  exit "$code"
}

cd $MDB_SOURCE_PATH/columnstore/columnstore/benchmarks

git checkout $BRANCH
sudo $MDB_SOURCE_PATH/columnstore/columnstore/build/bootstrap_mcs.sh -t RelWithDebInfo
echo "Build done; benchmarking $BRANCH now"
git checkout with_benchmarks
#Prepare should only create the table, we will fill it with cpimport
sysbench $SCRIPT \
        --mysql-socket=/run/mysqld/mysqld.sock \
        --db-driver=mysql \
        --mysql-db=test \
        prepare

sudo cpimport test "$TABLE" "$DATA"

sysbench $SCRIPT \
        --mysql-socket=/run/mysqld/mysqld.sock \
        --db-driver=mysql \
        --mysql-db=test \
        --time=120 run | tail -n +12 > "${BRANCH}_bench.txt"

git checkout develop
sudo $MDB_SOURCE_PATH/columnstore/columnstore/build/bootstrap_mcs.sh -t RelWithDebInfo
echo "Build done; benchmarking develop now"
git checkout with_benchmarks
sysbench $SCRIPT \
        --mysql-socket=/run/mysqld/mysqld.sock \
        --db-driver=mysql \
        --mysql-db=test \
        prepare

sudo cpimport test "$TABLE" "$DATA"

sysbench $SCRIPT \
        --mysql-socket=/run/mysqld/mysqld.sock \
        --db-driver=mysql \
        --mysql-db=test \
        --time=120 run | tail -n +12 > develop_bench.txt

python3 parse_bench.py "$BRANCH" "${BRANCH}_bench.txt" "develop_bench.txt"
