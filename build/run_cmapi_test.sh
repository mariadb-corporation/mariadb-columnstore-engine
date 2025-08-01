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
    execInnerDocker $CONTAINER_NAME "yum update -y && yum install -y MariaDB-columnstore-cmapi"
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
