#!/bin/bash

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

CONTAINER_NAME="$1"

echo "Arguments received: $@"

if [[ "$EUID" -ne 0 ]]; then
  error "Please run script as root"
  exit 1
fi

if [[ -z $(docker ps -q --filter "name=${CONTAINER_NAME}") ]]; then
  error "Container '${CONTAINER_NAME}' is not running."
  exit 1
fi

message "Running smoke checks..."

# start mariadb and mariadb-columnstore services and run simple query
execInnerDocker "$CONTAINER_NAME" 'systemctl start mariadb'
execInnerDocker "$CONTAINER_NAME" '/usr/bin/mcsSetConfig SystemConfig CGroup just_no_group_use_local'
execInnerDocker "$CONTAINER_NAME" 'systemctl restart mariadb-columnstore'

execInnerDocker "$CONTAINER_NAME" 'mariadb -e "CREATE DATABASE IF NOT EXISTS test; \
CREATE TABLE test.t1 (a INT) ENGINE=Columnstore; \
INSERT INTO test.t1 VALUES (1); \
SELECT * FROM test.t1;"'

# restart both services, wait a bit, then insert and select again
execInnerDocker "$CONTAINER_NAME" 'systemctl restart mariadb'
execInnerDocker "$CONTAINER_NAME" 'systemctl restart mariadb-columnstore'

sleep 10

execInnerDocker "$CONTAINER_NAME" 'mariadb -e "INSERT INTO test.t1 VALUES (2); SELECT * FROM test.t1;"'
