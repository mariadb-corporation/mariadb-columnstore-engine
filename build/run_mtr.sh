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
MTR_BASIC_SUITE_LIST="basic,bugfixes,autopilot"
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
    SETUP_DATA=true
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
set +eo pipefail
execInnerDocker "${CONTAINER_NAME}" "${MTR_RUN_COMMAND}" || MTR_EXIT=$?
set -eo pipefail

# Run 'future' suite separately — it opts into innodb_queries_use_mcs=on via
# its own suite.opt (mariadbd restarted). Under --extern mode .opt files are
# ignored; tests skip via include/have_innodb_queries_use_mcs.inc.
if [[ $PKG_FORMAT == "deb" ]]; then
    CNF_PATH="/etc/mysql/mariadb.conf.d/50-"
    CNF_FIND_PATH="/etc/mysql/*"
else
    CNF_PATH="/etc/my.cnf.d/"
    CNF_FIND_PATH="/etc/my.cnf*"
fi
listToDelete=(
  "skip-partition"
  "skip-sequence"
  "loose-skip-partition"
  "loose-skip-sequence"
  "columnstore_innodb_queries_use_mcs"
  "loose-columnstore_innodb_queries_use_mcs"
)
set +eo pipefail
message "Cleaning .cnf"
for itemToDelete in "${listToDelete[@]}"; do
  # Pattern matches optional leading whitespace, key name, and optional value
  pattern="^[[:space:]]*${itemToDelete}([[:space:]]*=.*)?$"
  execInnerDocker "${CONTAINER_NAME}" "find $CNF_FIND_PATH -type f -exec sed -i -E \"/${pattern}/d\" {} + 2>/dev/null"
done

message "Set .cnf parameters for 'future' test suite"
execInnerDocker "${CONTAINER_NAME}" "cat << 'EOF' > ${CNF_PATH}future.cnf
[mysqld]
columnstore_innodb_queries_use_mcs=on
skip-partition=0
skip-sequence=0
EOF"
message "Running install_mcs_mysql.sh"
execInnerDocker "${CONTAINER_NAME}" "install_mcs_mysql.sh"

message "Restart MariaDB and Columnstore before 'future' suite to apply .cnf changes"
execInnerDocker "${CONTAINER_NAME}" "systemctl daemon-reload"
execInnerDocker "${CONTAINER_NAME}" "systemctl restart mariadb"
execInnerDocker "${CONTAINER_NAME}" "systemctl restart mariadb-columnstore"

execInnerDocker "${CONTAINER_NAME}" "${MTR_RUN_COMMAND} --suite=columnstore/${MTR_FUTURE_SUITE} --extern socket=${SOCKET_PATH}" 
if [[ $? != 0 ]]; then
    MTR_EXIT=$?
fi
exit ${MTR_EXIT}
