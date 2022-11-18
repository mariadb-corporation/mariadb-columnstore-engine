#!/bin/bash

CPACK_PACKAGE_DESCRIPTION_SUMMARY="Mariadb Columnstore Cluster Manager API"
SVC_NAME="mariadb-columnstore-cmapi"

SVC_CMD=$1
arg_2=${2}

UNIT_PATH=/usr/lib/systemd/system/${SVC_NAME}.service
TEMPLATE_PATH=./service.template
TEMP_PATH=./service.temp
SYSTEMD_ENV_FILE=/etc/columnstore/systemd.env

CMAPI_DIR=$(pwd)
CMAPI_USER=root
CONFIG_FOLDER=/etc/columnstore
CONFIG_FILENAME=cmapi_server.conf

user_id=$(id -u)

# systemctl must run as sudo
if [ $user_id -ne 0 ]; then
    echo "Must run as sudo"
    exit 1
fi

function failed()
{
   local error=${1:-Undefined error}
   echo "Failed: $error" >&2
   exit 1
}

if [ ! -f "${TEMPLATE_PATH}" ]; then
    failed "Must run from package folder or install is corrupt"
fi

# check if we run as root
if [[ $(id -u) != "0" ]]; then
    echo "Failed: This script requires to run with sudo." >&2
    exit 1
fi

function install()
{
    echo "Creating service in ${UNIT_PATH}"
    if [ -f "${UNIT_PATH}" ]; then
        failed "error: exists ${UNIT_PATH}"
    fi

    if [ -f "${TEMP_PATH}" ]; then
      rm "${TEMP_PATH}" || failed "failed to delete ${TEMP_PATH}"
    fi

    # can optionally use username supplied
    #run_as_user=${arg_2:-$SUDO_USER}
    #echo "Run as user: ${run_as_user}"

    #run_as_uid=$(id -u ${run_as_user}) || failed "User does not exist"
    #echo "Run as uid: ${run_as_uid}"

    #run_as_gid=$(id -g ${run_as_user}) || failed "Group not available"
    #echo "gid: ${run_as_gid}"

    sed "s/\${CPACK_PACKAGE_DESCRIPTION_SUMMARY}/${CPACK_PACKAGE_DESCRIPTION_SUMMARY}/g; s/\${CMAPI_USER}/${CMAPI_USER}/g; s/\${CMAPI_DIR}/$(echo ${CMAPI_DIR} | sed -e 's/[\/&]/\\&/g')/g;" "${TEMPLATE_PATH}" > "${TEMP_PATH}" || failed "failed to create replacement temp file"
    mv "${TEMP_PATH}" "${UNIT_PATH}" || failed "failed to copy unit file"

    if [ ! -d "${CONFIG_FOLDER}" ]; then
        mkdir $CONFIG_FOLDER || failed "failed to create configuration folder"
    fi

    if [ ! -f "${CONFIG_FOLDER}/${CONFIG_FILENAME}" ]; then
        cp cmapi_server/cmapi_server.conf.default "${CONFIG_FOLDER}/${CONFIG_FILENAME}" || failed "failed to copy config file"
    fi

    # Unit file should not be executable and world writable
    chmod 664 ${UNIT_PATH} || failed "failed to set permissions on ${UNIT_PATH}"

    # Since we started with sudo, files will be owned by root. Change this to specific user
    #chown -R ${run_as_uid}:${run_as_gid} $CMAPI_DIR || failed "failed to set owner for $CMAPI_DIR"

    systemctl enable ${SVC_NAME} || failed "failed to enable ${SVC_NAME}"

    # chown ${run_as_uid}:${run_as_gid} ${CONFIG_FOLDER}/${CONFIG_FILENAME} || failed "failed to set permission for ${CONFIG_FOLDER}/${CONFIG_FILENAME}"
    echo PYTHONPATH=${CMAPI_DIR}/deps > ${SYSTEMD_ENV_FILE}

    systemctl daemon-reload || failed "failed to reload daemons"
}

function start()
{
    systemctl start ${SVC_NAME} || failed "failed to start ${SVC_NAME}"
    status
}

function stop()
{
    systemctl stop ${SVC_NAME} || failed "failed to stop ${SVC_NAME}"
    status
}

function uninstall()
{
    stop
    systemctl disable ${SVC_NAME} || failed "failed to disable ${SVC_NAME}"
    rm "${UNIT_PATH}" || failed "failed to delete ${UNIT_PATH}"
    rm "${SYSTEMD_ENV_FILE}" || failed "failed to delete ${SYSTEMD_ENV_FILE}"
    systemctl daemon-reload || failed "failed to reload daemons"
}

function status()
{
    if [ -f "${UNIT_PATH}" ]; then
        echo
        echo "${UNIT_PATH}"
    else
        echo
        echo "not installed"
        echo
        return
    fi

    systemctl --no-pager status ${SVC_NAME}
}

function usage()
{
    echo
    echo Usage:
    echo "./install.sh [install, start, stop, status, uninstall]"
    echo "Commands:"
    #echo "   install [user]: Install as Root or specified user"
    echo "   install: Install"
    echo "   start: Manually start"
    echo "   stop: Manually stop"
    echo "   status: Display intallation status"
    echo "   uninstall: Uninstall"
    echo
}

case $SVC_CMD in
   "install") install;;
   "status") status;;
   "uninstall") uninstall;;
   "start") start;;
   "stop") stop;;
   "status") status;;
   *) usage;;
esac

exit 0
