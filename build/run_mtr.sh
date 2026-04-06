#!/bin/bash

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

optparse.define short=c long=container-name desc="Name of the Docker container where mtr tests will run" variable=CONTAINER_NAME
optparse.define short=d long=distro desc="Linux distro for which mtr is runned" variable=DISTRO
optparse.define short=e long=triggering-event desc="Event that triggers testrun" variable=EVENT
optparse.define short=E long=run-as-extern desc="Run MTR with --extern flag" variable=EXTERN default=false value=true
optparse.define short=F long=full-mtr desc="Run Full Mtr" variable=FULL_MTR default=false

source $(optparse.build)

# Define test suite lists
# 'future' suite is run separately because it requires innodb_queries_use_mcs=ON
# (a READONLY startup variable for Query Accelerator / RBO tests).
MTR_BASIC_SUITE_LIST="basic,bugfixes"
MTR_FULL_SUITE_LIST="basic,bugfixes,devregression,autopilot,extended,multinode,oracle,1pmonly"
MTR_FUTURE_SUITE="future"

for flag in CONTAINER_NAME DISTRO EVENT; do
    if [[ -z "${!flag}" ]]; then
        error "Missing required flag: -${flag:0:1} / --${flag,,}"
        exit 1
    fi
done

if [[ "${EVENT}" == "cron" ]]; then
    FULL_MTR=true
fi

if [[ $FULL_MTR = true ]]; then
    MTR_SUITE_LIST="$MTR_FULL_SUITE_LIST"
    SETUP_DATA=true
    EXTERN=true
else
    MTR_SUITE_LIST="$MTR_BASIC_SUITE_LIST"
    SETUP_DATA=false
fi

echo "Arguments received: $@"

if [[ "$EUID" -ne 0 ]]; then
    error "Please run script as root"
    exit 1
fi

if [[ -z $(docker ps -q --filter "name=${CONTAINER_NAME}") ]]; then
    error "Container '${CONTAINER_NAME}' is not running."
    exit 1
fi

CONFIG_PATH_PREFIX=$(set_cnf_path)
echo "Put lower_case_table_names=2 into ${CONFIG_PATH_PREFIX}lower_case.cnf"
execInnerDocker "${CONTAINER_NAME}" "printf '[mysqld]\nlower_case_table_names=2\n' > ${CONFIG_PATH_PREFIX}lower_case.cnf"

# Enable innodb_queries_use_mcs=ON for the future suite (Query Accelerator / RBO tests).
# This READONLY startup variable routes InnoDB queries to Columnstore's select handler.
# We write it before the first restart so it takes effect for ALL suites.  Non-QA suites
# are unaffected because they use ENGINE=ColumnStore tables directly.  The loose- prefix
# ensures MariaDB 10.6 (where the variable does not exist) starts without error.
echo "Put innodb_queries_use_mcs=ON into ${CONFIG_PATH_PREFIX}queryacc.cnf"
execInnerDocker "${CONTAINER_NAME}" "printf '[mysqld]\nloose-columnstore_innodb_queries_use_mcs=ON\n' > ${CONFIG_PATH_PREFIX}queryacc.cnf"

select_pkg_format ${DISTRO}

message "Running mtr tests..."

# disable systemd 'ProtectSystem' (we need to write to /usr/share/)
execInnerDocker "${CONTAINER_NAME}" "sed -i /ProtectSystem/d \$(systemctl show --property FragmentPath mariadb | sed s/FragmentPath=//) || true"
execInnerDocker "${CONTAINER_NAME}" "systemctl daemon-reload"
execInnerDocker "${CONTAINER_NAME}" "systemctl restart mariadb"

# Set RAM consumption limits to avoid RAM contention b/w mtr and regression steps.
execInnerDocker "${CONTAINER_NAME}" "/usr/bin/mcsSetConfig SystemConfig CGroup just_no_group_use_local"
execInnerDocker "${CONTAINER_NAME}" "mariadb -e \"create database if not exists test;\""
execInnerDocker "${CONTAINER_NAME}" "systemctl restart mariadb-columnstore"

VERSION_GREATER_THAN_10=$(execInnerDockerStripped "${CONTAINER_NAME}" "mariadb -N -s -e 'SELECT (sys.version_major(), sys.version_minor(), sys.version_patch()) >= (11, 4, 0);'")
SOCKET_PATH=$(execInnerDockerStripped "${CONTAINER_NAME}" "mariadb -e \"show variables like 'socket';\" | grep socket | cut -f2")

SERVERNAME="mysql"
if [[ $VERSION_GREATER_THAN_10 == "1" ]]; then
    SERVERNAME="mariadb"
fi

if [[ "$PKG_FORMAT" == "rpm" ]]; then
    MTR_PATH="/usr/share/${SERVERNAME}-test"
else
    MTR_PATH="/usr/share/${SERVERNAME}/${SERVERNAME}-test"
fi

message "Running mtr tests from $MTR_PATH with $SOCKET_PATH and version >=11.4 $VERSION_GREATER_THAN_10"

execInnerDocker "${CONTAINER_NAME}" "chown -R mysql:mysql ${MTR_PATH}"

if [[ $SETUP_DATA = true ]]; then
    execInnerDocker "${CONTAINER_NAME}" "wget -qO- https://cspkg.s3.amazonaws.com/mtr-test-data.tar.lz4 | lz4 -dc - | tar xf - -C /"
    execInnerDocker "${CONTAINER_NAME}" "cd ${MTR_PATH} && ./mtr --extern socket=${SOCKET_PATH} --force --print-core=detailed --print-method=gdb --max-test-fail=0 --suite=columnstore/setup"
fi

EXTERN_FLAG=""

if [[ $EXTERN = true ]]; then
    EXTERN_FLAG="--extern socket=${SOCKET_PATH}"
fi

MTR_RUN_COMMAND="cd ${MTR_PATH} && ./mtr ${EXTERN_FLAG} --force --print-core=detailed --print-method=gdb --max-test-fail=0 --big-test \
                                  --verbose-restart --skip-test=rocksdb_hotbackup* \
                                  --suite=columnstore/${MTR_SUITE_LIST//,/,columnstore/}"

MTR_EXIT=0
execInnerDocker "${CONTAINER_NAME}" "${MTR_RUN_COMMAND}" || MTR_EXIT=$?

# Run 'future' suite separately.  innodb_queries_use_mcs=ON was already written
# into queryacc.cnf before the first restart above, so no extra restart is needed.
# Verify the variable is still ON (sanity check).
if ! execInnerDocker "${CONTAINER_NAME}" "mariadb -N -s -e \"SELECT @@global.columnstore_innodb_queries_use_mcs\"" 2>/dev/null | tr -d '\r' | grep -qw "ON"; then
  warn "innodb_queries_use_mcs does NOT appear to be ON — future suite tests will likely fail"
  # Show diagnostics: what cnf file contains and what server sees
  execInnerDocker "${CONTAINER_NAME}" "cat ${CONFIG_PATH_PREFIX}queryacc.cnf 2>/dev/null || echo 'queryacc.cnf NOT FOUND'" || true
  execInnerDocker "${CONTAINER_NAME}" "mariadb -N -s -e \"SHOW VARIABLES LIKE 'columnstore_innodb%'\"" 2>/dev/null || true
fi

execInnerDocker "${CONTAINER_NAME}" "${MTR_RUN_COMMAND% --suite=*} --suite=columnstore/${MTR_FUTURE_SUITE}" || MTR_EXIT=$?

exit ${MTR_EXIT}
