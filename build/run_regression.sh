#!/bin/bash

set -o pipefail

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

optparse.define short=c long=container-name     desc="Name of the Docker container where regression tests will run" variable=CONTAINER_NAME
optparse.define short=b long=regression-branch  desc="Branch from regression tests repo"                            variable=REGRESSION_BRANCH
optparse.define short=d long=distro             desc="Linux distro for which regression is executed"                variable=DISTRO
optparse.define short=t long=regression-timeout desc="Timeout for the regression test run"                          variable=REGRESSION_TIMEOUT
optparse.define short=n long=test-name          desc="Name of regression test to execute"                           variable=TEST_NAME
source "$(optparse.build)"

for flag in CONTAINER_NAME REGRESSION_BRANCH DISTRO REGRESSION_TIMEOUT TEST_NAME; do
  if [[ -z "${!flag}" ]]; then
    error "Missing required flag: -${flag:0:1} / --${flag,,}"
    exit 1
  fi
done

if [[ -z $(docker ps -q --filter "name=${CONTAINER_NAME}") ]]; then
  error "Container '${CONTAINER_NAME}' is not running."
  exit 1
fi

BUILD_DIR="verylongdirnameforverystrangecpackbehavior"

prepare_regression() {
  if execInnerDocker "${CONTAINER_NAME}" "test -f /mariadb-columnstore-regression-test/mysql/queries/queryTester.cpp"; then
    message "Preparation for regression tests is already done — skipping"
    return 0
  fi

  message "Running one-time preparation for regression tests"

  # Set config path prefix based on distro
  if [[ "$DISTRO" == *rocky* ]]; then
    CONFIG_PATH_PREFIX="/etc/my.cnf.d/"
  else
    CONFIG_PATH_PREFIX="/etc/mysql/mariadb.conf.d/50-"
  fi

  git clone --recurse-submodules --branch "${REGRESSION_BRANCH}" --depth 1 https://github.com/mariadb-corporation/mariadb-columnstore-regression-test
  cd mariadb-columnstore-regression-test
  git rev-parse --abbrev-ref HEAD && git rev-parse HEAD
  cd ..

  docker cp mariadb-columnstore-regression-test "${CONTAINER_NAME}:/"
  docker cp "/mdb/${BUILD_DIR}/storage/columnstore/columnstore/storage-manager" "${CONTAINER_NAME}:/"

  #copy test data for regression test suite
  execInnerDocker "${CONTAINER_NAME}" 'bash -c "wget -qO- https://cspkg.s3.amazonaws.com/testData.tar.lz4 | lz4 -dc - | tar xf - -C mariadb-columnstore-regression-test/"'

  # set mariadb lower_case_table_names=1 config option
  execInnerDocker "${CONTAINER_NAME}" "sed -i '/^\[mariadb\]/a lower_case_table_names=1' ${CONFIG_PATH_PREFIX}server.cnf"

  # set default client character set to utf-8
  execInnerDocker "${CONTAINER_NAME}" "sed -i '/^\[client\]/a default-character-set=utf8' ${CONFIG_PATH_PREFIX}client.cnf"

  # Start services and build queryTester
  execInnerDocker "${CONTAINER_NAME}" "systemctl start mariadb"
  execInnerDocker "${CONTAINER_NAME}" "systemctl restart mariadb-columnstore"
  execInnerDocker "${CONTAINER_NAME}" "g++ /mariadb-columnstore-regression-test/mysql/queries/queryTester.cpp -O2 -o /mariadb-columnstore-regression-test/mysql/queries/queryTester"

  message "Regression preparation complete"
}

run_test() {
  message "Running test: ${TEST_NAME:-<none>}"

  execInnerDocker "${CONTAINER_NAME}" "sleep 4800 && bash /save_stack.sh /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/reg-logs/ &"

  execInnerDockerNoTTY "${CONTAINER_NAME}" \
    "export PRESERVE_LOGS=true && cd /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest && \
     timeout -k 1m -s SIGKILL --preserve-status ${REGRESSION_TIMEOUT} ./go.sh --sm_unit_test_dir=/storage-manager --tests=${TEST_NAME} \
       || ./regression_logs.sh ${TEST_NAME}"
}

on_exit() {
  exit_code=$?
  if [[ $exit_code -eq 0 ]]; then
    message "Regression finished successfully"
  else
    message "Some of regression tests has failed"
  fi
}
trap on_exit EXIT

prepare_regression
run_test

