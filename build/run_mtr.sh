#!/bin/bash

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

optparse.define short=c long=container-name desc="Name of the Docker container where mtr tests will run" variable=CONTAINER_NAME
optparse.define short=d long=distro desc="Linux distro for which mtr is runned" variable=DISTRO
optparse.define short=s long=suite-list desc="Comma-separated list of test suites to run" variable=MTR_SUITE_LIST
optparse.define short=e long=triggering-event desc="Event that triggers testrun" variable=EVENT
source $(optparse.build)

MTR_FULL_SET="basic,bugfixes,devregression,autopilot,extended,multinode,oracle,1pmonly"

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

if [[ "$DISTRO" == *rocky* ]]; then
    SOCKET_PATH="/var/lib/mysql/mysql.sock"
    MTR_PATH="/usr/share/mysql-test"
else
    SOCKET_PATH="/run/mysqld/mysqld.sock"
    MTR_PATH="/usr/share/mysql/mysql-test"
fi

message "Running mtr tests..."

execInnerDocker "${CONTAINER_NAME}" "chown -R mysql:mysql ${MTR_PATH}"

# disable systemd 'ProtectSystem' (we need to write to /usr/share/)
execInnerDocker "${CONTAINER_NAME}" "sed -i /ProtectSystem/d \$(systemctl show --property FragmentPath mariadb | sed s/FragmentPath=//) || true"
execInnerDocker "${CONTAINER_NAME}" "systemctl daemon-reload"
execInnerDocker "${CONTAINER_NAME}" "systemctl start mariadb"

# Set RAM consumption limits to avoid RAM contention b/w mtr and regression steps.
execInnerDocker "${CONTAINER_NAME}" "/usr/bin/mcsSetConfig SystemConfig CGroup just_no_group_use_local"
execInnerDocker "${CONTAINER_NAME}" "mariadb -e \"create database if not exists test;\""
execInnerDocker "${CONTAINER_NAME}" "systemctl restart mariadb-columnstore"

if [[ "${EVENT}" == "custom" || "${EVENT}" == "cron" ]]; then
    execInnerDocker "${CONTAINER_NAME}" "wget -qO- https://cspkg.s3.amazonaws.com/mtr-test-data.tar.lz4 | lz4 -dc - | tar xf - -C /"
    execInnerDocker "${CONTAINER_NAME}" "cd ${MTR_PATH} && ./mtr --extern socket=${SOCKET_PATH} --force --print-core=detailed --print-method=gdb --max-test-fail=0 --suite=columnstore/setup"
fi

if [[ "${EVENT}" == "cron" ]]; then
    execInnerDocker "${CONTAINER_NAME}" "cd ${MTR_PATH} && ./mtr --extern socket=${SOCKET_PATH} --force --print-core=detailed --print-method=gdb --max-test-fail=0 --suite=columnstore/${MTR_FULL_SET//,/,columnstore/}"
else
    execInnerDocker "${CONTAINER_NAME}" "cd ${MTR_PATH} && ./mtr --extern socket=${SOCKET_PATH} --force --print-core=detailed --print-method=gdb --max-test-fail=0 --suite=columnstore/${MTR_SUITE_LIST//,/,columnstore/}"
fi