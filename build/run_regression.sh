#!/bin/bash

set -o pipefail

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

optparse.define short=c long=container-name     desc="Name of the Docker container where regression tests will run" variable=CONTAINER_NAME
optparse.define short=b long=regression-branch  desc="Branch from regression tests repo"                            variable=REGRESSION_BRANCH
optparse.define short=d long=distro             desc="Linux distro for which regression is executed"                variable=DISTRO
optparse.define short=t long=regression-timeout desc="Timeout for the regression test run"                          variable=REGRESSION_TIMEOUT default=2h
optparse.define short=n long=test-name          desc="Name of regression test to execute"                           variable=TEST_NAME
optparse.define short=i long=ignore-cores       desc="Mark this test's daemon cores as expected so the stage core gate skips them and stage is not marked as failed because of them, needed when a specific failing test needs to be ignored (cores are still gdb-formatted and published)" variable=IGNORE_CORES default=false value=true
optparse.define short=r long=test-count         desc="Run the test this many times (flaky-test hunting)"            variable=TEST_COUNT default=1
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

  # Detect cnf drop-in dir inside the container (set_cnf_path runs on host and can't see container fs).
  # For deb distros prefer /etc/mariadb.conf.d/ over /etc/my.cnf.d/ when both exist.
  if [[ "$DISTRO" == *ubuntu* ]] || [[ "$DISTRO" == *debian* ]]; then
    CONFIG_PATH_PREFIX=$(execInnerDockerStripped "${CONTAINER_NAME}" "
      if [[ -d /etc/mariadb.conf.d ]]; then echo /etc/mariadb.conf.d/50-
      elif [[ -d /etc/mysql/mariadb.conf.d ]]; then echo /etc/mysql/mariadb.conf.d/50-
      elif [[ -d /etc/my.cnf.d ]]; then echo /etc/my.cnf.d/
      else echo /etc/mysql/mariadb.conf.d/50-
      fi
    ")
  else
    CONFIG_PATH_PREFIX=$(execInnerDockerStripped "${CONTAINER_NAME}" "
      if [[ -d /etc/my.cnf.d ]]; then echo /etc/my.cnf.d/
      elif [[ -d /etc/mariadb.conf.d ]]; then echo /etc/mariadb.conf.d/50-
      else echo /etc/mysql/mariadb.conf.d/50-
      fi
    ")
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

start_monitors() {
  local test_dir="/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest"

  execInnerDockerNoTTY "${CONTAINER_NAME}" "nohup /monitor_memory.sh \"${TEST_NAME%.sh}\" >/dev/null 2>&1 &"

  sleep 1
  if ! execInnerDocker "${CONTAINER_NAME}" "ps aux | grep -q '[m]onitor_memory.sh'"; then
    warn "Memory monitor failed to start for test: ${TEST_NAME}"
  fi

  execInnerDocker "${CONTAINER_NAME}" "sleep 4800 && bash /save_stack.sh ${test_dir}/reg-logs/ &"
}

run_test() {
  local test_dir="/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest"

  message "Running test: ${TEST_NAME}"

  # For a tolerated test, snapshot cores that already exist (left by earlier tests) so we can later
  # record ONLY the cores THIS test produces. A pre-existing core must still fail the stage, so it
  # must NOT end up in .expected_cores.
  if [[ "${IGNORE_CORES}" == "true" ]]; then
    execInnerDocker "${CONTAINER_NAME}" "ls -1 /core/*_core_dump.* 2>/dev/null > /core/.cores_before || true"
  fi

  execInnerDockerNoTTY "${CONTAINER_NAME}" \
    "export PRESERVE_LOGS=true && cd ${test_dir} && \
     timeout -k 1m -s SIGKILL --preserve-status ${REGRESSION_TIMEOUT} \
     ./go.sh --sm_unit_test_dir=/storage-manager --tests=${TEST_NAME} \
       || ./regression_logs.sh ${TEST_NAME}"
  local test_rc=$?

  # A test the Drone config marks tolerated (--ignore-cores) produces EXPECTED daemon cores. Record ONLY the cores
  # that appeared DURING it (diff vs the pre-test snapshot) -- do NOT delete them; they are still
  # gdb-formatted and published as diagnostics. The end-of-stage core gate (core_dump_drop.sh) then
  # skips exactly these, while a core from any OTHER test (including ones before this) still fails the
  # stage. asan.*/ubsan.* reports are untouched, so check_sanitizer_reports.sh keeps gating real leaks.
  if [[ "${IGNORE_CORES}" == "true" ]]; then
    execInnerDocker "${CONTAINER_NAME}" "ls -1 /core/*_core_dump.* 2>/dev/null | grep -vxF -f /core/.cores_before >> /core/.expected_cores 2>/dev/null; rm -f /core/.cores_before || true"
  fi

  return "${test_rc}"
}

# Run the test TEST_COUNT times (default 1). Used for flaky-test hunting:
# a failing iteration does not stop the loop, its test directory (diff.txt,
# main*.log, ...) is preserved under reg-logs/<test>_iterN for the artifact
# upload, and the final exit code reflects whether any iteration failed.
run_test_loop() {
  local test_dir="/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest"
  local failures=0
  local i

  for ((i = 1; i <= TEST_COUNT; i++)); do
    if [[ "${TEST_COUNT}" -gt 1 ]]; then
      message "=== ${TEST_NAME} iteration ${i}/${TEST_COUNT} ==="
    fi

    if run_test; then
      [[ "${TEST_COUNT}" -gt 1 ]] && message "=== ${TEST_NAME} iteration ${i}/${TEST_COUNT}: PASSED ==="
    else
      failures=$((failures + 1))
      warn "=== ${TEST_NAME} iteration ${i}/${TEST_COUNT}: FAILED (failures so far: ${failures}) ==="
      # Preserve this iteration's diffs/logs before the next run overwrites them.
      execInnerDocker "${CONTAINER_NAME}" \
        "cd ${test_dir} && mkdir -p reg-logs && \
         cp -r ${TEST_NAME%.sh} reg-logs/${TEST_NAME%.sh}_iter${i} 2>/dev/null; true" || true
    fi
  done

  if [[ "${TEST_COUNT}" -gt 1 ]]; then
    message "=== ${TEST_NAME}: ${failures}/${TEST_COUNT} iterations failed ==="
  fi

  return $((failures > 0 ? 1 : 0))
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
start_monitors
run_test_loop

