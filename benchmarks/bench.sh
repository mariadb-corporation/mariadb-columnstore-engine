#!/usr/bin/env bash

set -Eeuo pipefail

trap cleanup SIGINT SIGTERM ERR EXIT
source ../build/utils.sh
usage() {
  cat <<EOF
Usage: $(basename "${BASH_SOURCE[0]}") branch lua_script [-h] [-d data.tbl] [-s 1000000]

A script to run benchmark(for now) to compare the pefromanceo of min/max
calculation on develop and provided branch. Runs the provided .lua script using sysbench.

Available options:

-h, --help      Print this help and exit
-d, --data      Data for table that will be given to cpimport; if no name provided it will be generated.
-s, --size      Size of the dataset to generate
-t, --table     Name of the table
EOF
  exit
}

SCRIPT_LOCATION=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)
MDB_SOURCE_PATH=$(realpath $SCRIPT_LOCATION/../../..)
DATA=""

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
        cleanup
  unset TABLE
}

die() {
  local msg=$1
  local code=${2-1} # default exit status 1
  echo "$msg"
  exit "$code"
}

parse_params() {

  args=("$@")
  # check required params and arguments
  [[ ${#args[@]} -eq 0 ]] && die "Missing script arguments"

  BRANCH="$1"

  if [ $BRANCH == "--help" ] || [ $BRANCH == "-h" ]
     then
         usage
  fi

  [[ ${#args[@]} < 2 ]] && die "Missing script arguments"

  SCRIPT="$2"
  RANGE=1000000
  TABLE="t1"

  while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
    -d | --data) DATA="$2"
                 shift
                 ;;
    -s | --size) RANGE="$2"
                 shift
                 ;;
    -t | --table) TABLE="$2"
                  shift
                  ;;
    -?*) die "Unknown option: $key" ;;
    *) break ;;
    esac
    shift
  done

  return 0
}

parse_params "$@"
export TABLE
cd $MDB_SOURCE_PATH/columnstore/columnstore/benchmarks
if [[ $DATA == "" ]]
  then
    DATA="data.tbl"
    seq 1 $RANGE > "$DATA"
fi

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
        --time=30 run | tail -n +12 > "${BRANCH}_bench.txt"

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
        --time=30 run | tail -n +12 > develop_bench.txt

python3 parse_bench.py "$BRANCH" "${BRANCH}_bench.txt" "develop_bench.txt"
