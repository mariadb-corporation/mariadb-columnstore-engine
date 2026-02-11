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
MTR_BASIC_SUITE_LIST="basic,bugfixes,future"
MTR_FULL_SUITE_LIST="basic,bugfixes,devregression,autopilot,extended,multinode,oracle,1pmonly,future"

set_cnf_path

execInnerDocker "${CONTAINER_NAME}" "echo '[mysqld]' > ${CONFIG_PATH_PREFIX}lower_case.cnf"
execInnerDocker "${CONTAINER_NAME}" "echo 'lower_case_table_names=2' >> ${CONFIG_PATH_PREFIX}lower_case.cnf"

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

for flag in CONTAINER_NAME DISTRO EVENT; do
    if [[ -z "${!flag}" ]]; then
        error "Missing required flag: -${flag:0:1} / --${flag,,}"
        exit 1
    fi
done

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

execInnerDocker "${CONTAINER_NAME}" "${MTR_RUN_COMMAND}"
