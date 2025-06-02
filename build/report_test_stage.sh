#!/bin/bash

set -eo pipefail

CONTAINER_NAME=$1
RESULT=$2
STAGE=$3

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

echo "Arguments received: $@"

if [[ "$EUID" -ne 0 ]]; then
    error "Please run script as root"
    exit 1
fi

if [[ -z $(docker ps -q --filter "name=${CONTAINER_NAME}") ]]; then
    error "Container '${CONTAINER_NAME}' is not running."
    exit 1
fi

if [[ "$RESULT" == *rocky* ]]; then
    SYSTEMD_PATH="/usr/lib/systemd/systemd"
    MTR_PATH="/usr/share/mysql-test"
else
    SYSTEMD_PATH="systemd"
    MTR_PATH="/usr/share/mysql/mysql-test"
fi

echo "Reporting test stage: ${STAGE} executed in ${CONTAINER_NAME} container"

if [[ "${CONTAINER_NAME}" == *smoke* ]] || [[ "${CONTAINER_NAME}" == *mtr* ]] || [[ "${CONTAINER_NAME}" == *cmapi* ]]; then
    # common logs for smoke, mtr, cmapi
    echo "---------- start mariadb service logs ----------"
    execInnerDocker "$CONTAINER_NAME" 'journalctl -u mariadb --no-pager || echo "mariadb service failure"'
    echo "---------- end mariadb service logs ----------"
    echo
    echo "---------- start columnstore debug log ----------"
    execInnerDocker "$CONTAINER_NAME" 'cat /var/log/mariadb/columnstore/debug.log || echo "missing columnstore debug.log"'
    echo "---------- end columnstore debug log ----------"

    if [[ "${CONTAINER_NAME}" == *mtr* ]]; then
        echo
        docker cp "${CONTAINER_NAME}:${MTR_PATH}/var/log" "/drone/src/${RESULT}/mtr-logs" || echo "missing ${MTR_PATH}/var/log"
    fi

    if [[ "${CONTAINER_NAME}" == *cmapi* ]]; then
        echo
        echo "---------- start cmapi log ----------"
        execInnerDocker "$CONTAINER_NAME" 'cat /var/log/mariadb/columnstore/cmapi_server.log || echo "missing cmapi_server.log"'
        echo "---------- end cmapi log ----------"
    fi

elif [[ "${CONTAINER_NAME}" == *upgrade* ]]; then
    # nothing to report here for upgrade
    :

elif [[ "${CONTAINER_NAME}" == *regression* ]]; then
    echo "---------- start columnstore regression short report ----------"
    execInnerDocker "$CONTAINER_NAME" 'cd /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest; cat go.log || echo "missing go.log"'
    echo "---------- end columnstore regression short report ----------"
    echo
    docker cp "${CONTAINER_NAME}:/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/reg-logs/" "/drone/src/${RESULT}/" || echo "missing regression logs"
    docker cp "${CONTAINER_NAME}:/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/testErrorLogs.tgz" "/drone/src/${RESULT}/" || echo "missing testErrorLogs.tgz"

    execInnerDocker "$CONTAINER_NAME" 'tar czf regressionQueries.tgz /mariadb-columnstore-regression-test/mysql/queries/'
    execInnerDocker "$CONTAINER_NAME" 'cd /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest; tar czf testErrorLogs2.tgz *.log /var/log/mariadb/columnstore || echo "failed to grab regression results"'
    docker cp "${CONTAINER_NAME}:/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/testErrorLogs2.tgz" "/drone/src/${RESULT}/" || echo "missing testErrorLogs2.tgz"
    docker cp "${CONTAINER_NAME}:regressionQueries.tgz" "/drone/src/${RESULT}/" || echo "missing regressionQueries.tgz"

else
    echo "Unknown stage's container provided: ${CONTAINER_NAME}"
    exit 1
fi

execInnerDocker "$CONTAINER_NAME" "/logs.sh ${STAGE}"
execInnerDocker "$CONTAINER_NAME" "/core_dump_check.sh core /core/ ${STAGE}"

docker cp "${CONTAINER_NAME}:/core/" "/drone/src/${RESULT}/"
docker cp "${CONTAINER_NAME}:/unit_logs/" "/drone/src/${RESULT}/"

execInnerDocker "$CONTAINER_NAME" "/core_dump_drop.sh core"
echo "Saved artifacts:"
ls -R "/drone/src/${RESULT}/"
echo "Done reporting ${STAGE}"

cleanup() {
    if [[ -n $(docker ps -q --filter "name=${CONTAINER_NAME}") ]]; then
        echo "Cleaning up container ${CONTAINER_NAME}..."
        docker rm -f "${CONTAINER_NAME}" || echo "Can't remove container ${CONTAINER_NAME}!"
    fi
}
#Remove the container on exit
trap cleanup EXIT
