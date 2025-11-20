#!/bin/bash

set -o pipefail

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

optparse.define short=c long=container-name     desc="Name of the Docker container where regression tests will run" variable=CONTAINER_NAME
optparse.define short=b long=regression-branch  desc="Branch from regression tests repo"                            variable=REGRESSION_BRANCH
optparse.define short=d long=distro             desc="Linux distro for which regression is executed"                variable=DISTRO
optparse.define short=t long=regression-timeout desc="Timeout for the regression test run"                          variable=REGRESSION_TIMEOUT default=2h
optparse.define short=n long=test-name          desc="Name of regression test to execute"                           variable=TEST_NAME
source "$(optparse.build)"

for flag in CONTAINER_NAME REGRESSION_BRANCH DISTRO TEST_NAME; do
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

  # Clone regression test repo (requires GitHub token)
  REPO_URL="https://github.com/mariadb-corporation/mariadb-columnstore-regression-test"
  if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    REPO_URL="https://${GITHUB_TOKEN}@github.com/mariadb-corporation/mariadb-columnstore-regression-test"
  fi

  rm -rf mariadb-columnstore-regression-test
  git clone --recurse-submodules --branch "${REGRESSION_BRANCH}" --depth 1 "${REPO_URL}"
  cd mariadb-columnstore-regression-test
  git rev-parse --abbrev-ref HEAD && git rev-parse HEAD
  cd ..

  docker cp mariadb-columnstore-regression-test "${CONTAINER_NAME}:/"

  # Copy memory monitoring script
  docker cp "${SCRIPT_LOCATION}/monitor_memory.sh" "${CONTAINER_NAME}:/"
  execInnerDocker "${CONTAINER_NAME}" "chmod +x /monitor_memory.sh"

  # Copy storage-manager (try CI path first, fallback to local)
  SM_PATH="/mdb/${BUILD_DIR}/storage/columnstore/columnstore/storage-manager"
  [[ ! -d "$SM_PATH" ]] && SM_PATH="${SCRIPT_LOCATION}/../storage-manager"
  
  if [[ -d "$SM_PATH" ]]; then
    docker cp "$SM_PATH" "${CONTAINER_NAME}:/"
  else
    warn "storage-manager not found, some tests may fail"
  fi

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
  execInnerDocker "${CONTAINER_NAME}" "mkdir -p /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest"

  message "Regression preparation complete"
}

run_test() {
  local test_dir="/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest"
  
  message "Running test: ${TEST_NAME}"

  execInnerDockerNoTTY "${CONTAINER_NAME}" "nohup /monitor_memory.sh \"${TEST_NAME%.sh}\" >/dev/null 2>&1 &"

  sleep 1
  if ! execInnerDocker "${CONTAINER_NAME}" "ps aux | grep -q '[m]onitor_memory.sh'"; then
    warn "Memory monitor failed to start for test: ${TEST_NAME}"
  fi

  execInnerDocker "${CONTAINER_NAME}" "sleep 4800 && bash /save_stack.sh ${test_dir}/reg-logs/ &"

  execInnerDockerNoTTY "${CONTAINER_NAME}" \
    "export PRESERVE_LOGS=true && cd ${test_dir} && \
     timeout -k 1m -s SIGKILL --preserve-status ${REGRESSION_TIMEOUT} \
     ./go.sh --sm_unit_test_dir=/storage-manager --tests=${TEST_NAME} \
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

