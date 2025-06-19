#!/bin/bash

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

optparse.define short=c long=container-name desc="Name of the Docker container where regression tests will be executed" variable=CONTAINER_NAME
optparse.define short=t long=regression-timeout desc="Name of the Docker container where regression tests will be executed" variable=REGRESSION_TIMEOUT
optparse.define short=n long=test-name desc="Test name" variable=TEST_NAME
source $(optparse.build)

echo "Arguments received: $@"

if [[ "$EUID" -ne 0 ]]; then
    error "Please run script as root"
    exit 1
fi

if [[ -z "${CONTAINER_NAME}" ]]; then
    echo "Please provide mtr container name as a parameter, e.g. ./run_mtr.sh -c mtr183"
    exit 1
fi

if [[ -z $(docker ps -q --filter "name=${CONTAINER_NAME}") ]]; then
    error "Container '${CONTAINER_NAME}' is not running."
    exit 1
fi

execInnerDocker "$CONTAINER_NAME" "cd /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest && mkdir -p reg-logs"

execInnerDocker "$CONTAINER_NAME" "bash -c 'sleep 4800 && bash /save_stack.sh /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/reg-logs/' &"

execInnerDockerNoTTY "$CONTAINER_NAME" \
  "export PRESERVE_LOGS=true && cd /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest && \
   bash -c \"timeout -k 1m -s SIGKILL --preserve-status ${REGRESSION_TIMEOUT} ./go.sh --sm_unit_test_dir=/storage-manager --tests=${TEST_NAME} \
     || ./regression_logs.sh ${TEST_NAME}\""
