#!/bin/bash

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
COLUMNSTORE_SOURCE_PATH=$(realpath "$SCRIPT_LOCATION"/../)
MDB_SOURCE_PATH=$(realpath "$SCRIPT_LOCATION"/../../../..)
CMAPI_PATH="/usr/share/columnstore/cmapi"
ETC_PATH="/etc/columnstore"

source "$SCRIPT_LOCATION"/utils.sh

optparse.define short=c long=container-name desc="Name of the Docker container where cmapi tests will run" variable=CONTAINER_NAME
optparse.define short=f long=pkg-format desc="Package format" variable=PKG_FORMAT
source $(optparse.build)
echo "Arguments received: $@"

for flag in CONTAINER_NAME PKG_FORMAT; do
  if [[ -z "${!flag}" ]]; then
    error "Missing required flag: -${flag:0:1} / --${flag,,}"
    exit 1
  fi
done

prepare_environment() {
  echo "Preparing for cmapi test run..."

  if [[ "$PKG_FORMAT" == "deb" ]]; then
    execInnerDocker $CONTAINER_NAME "apt-get clean && apt-get update -y && apt-get install -y mariadb-columnstore-cmapi"
  else
    # Note: no 'yum update' here is deliberate. 'apt-get update'
    # above only refreshes the package list (apt requires that before
    # install), but 'yum update' would upgrade EVERY installed package (more
    # like apt-get upgrade, not apt-get update). The full system upgrade
    # already ran for this container in prepare_test_container.sh, as
    # 'yum --nobest update': --nobest keeps an older package when the
    # newest cannot be installed consistently, e.g. when the repo has
    # moved to a newer glibc than the container's base image while some
    # sub-package lags behind (seen on rockylinux:9).
    # Specific 'update' (as opposed to 'upgrade') is not needed here either,
    # since yum/dnf re-fetches a stale package list automatically before install.
    execInnerDocker $CONTAINER_NAME "yum install -y MariaDB-columnstore-cmapi"
  fi

  cd cmapi

  for i in mcs_node_control cmapi_server failover; do
    docker cp "${i}/test" "$CONTAINER_NAME:${CMAPI_PATH}/${i}/"
  done

  docker cp run_tests.py "$CONTAINER_NAME:${CMAPI_PATH}/"
  execInnerDocker $CONTAINER_NAME "systemctl start mariadb-columnstore-cmapi"

  # set API key to /etc/columnstore/cmapi_server.conf
  execInnerDocker $CONTAINER_NAME "mcs cluster set api-key --key somekey123"
  # copy cmapi conf file for test purposes (there are api key already set inside)
  execInnerDocker $CONTAINER_NAME "cp ${ETC_PATH}/cmapi_server.conf ${CMAPI_PATH}/cmapi_server/"
  execInnerDocker $CONTAINER_NAME "systemctl stop mariadb-columnstore-cmapi"
}

run_cmapi_test() {
  execInnerDocker $CONTAINER_NAME "cd ${CMAPI_PATH} && python/bin/python3 run_tests.py"
}

prepare_environment
run_cmapi_test
