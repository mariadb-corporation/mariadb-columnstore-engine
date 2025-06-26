#!/bin/bash

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

optparse.define short=c long=container-name desc="Name of the Docker container where mtr tests will run" variable=CONTAINER_NAME
optparse.define short=r long=result-path desc="Path for logs and results" variable=RESULT
source $(optparse.build)

echo "Arguments received: $@"

for flag in CONTAINER_NAME RESULT; do
  if [[ -z "${!flag}" ]]; then
    error "Missing required flag: -${flag:0:1} / --${flag,,}"
    exit 1
  fi
done

if [[ -z $(docker ps -q --filter "name=${CONTAINER_NAME}") ]]; then
  error "Container '${CONTAINER_NAME}' is not running."
  exit 1
fi

#Collect the logs on exit event
collect_logs(){
  "${SCRIPT_LOCATION}/report_test_stage.sh" --container-name "${CONTAINER_NAME}" --result-path "${RESULT}" --stage "smoke"
}
trap collect_logs EXIT

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

