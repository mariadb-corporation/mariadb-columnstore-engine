#!/bin/bash

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

optparse.define short=i long=columnstore-image-name desc="Name of columnstore docker image" variable=MCS_IMAGE_NAME
optparse.define short=d long=distro desc="Linux distro for which multinode mtr is executed" variable=DISTRO
source $(optparse.build)

echo "Arguments received: $@"

if [[ "$EUID" -ne 0 ]]; then
    error "Please run script as root"
    exit 1
fi

for flag in MCS_IMAGE_NAME DISTRO; do
  if [[ -z "${!flag}" ]]; then
    error "Missing required flag: -${flag:0:1} / --${flag,,}"
    exit 1
  fi
done

select_pkg_format ${DISTRO}

# Determine server name (mysql vs mariadb) and correct paths based on version >= 11.4
# We query inside the primary container (mcs1) after it starts.

SERVERNAME="mysql"
VERSION_GE_114=""
SOCKET_PATH=""
MTR_PATH=""

message "Running multinode mtr tests..."

cd docker
cp .env_example .env
sed -i "/^MCS_IMAGE_NAME=/s|=.*|=${MCS_IMAGE_NAME}|" .env
sed -i "/^MAXSCALE=/s|=.*|=false|" .env

docker-compose up -d
docker exec mcs1 provision mcs1 mcs2 mcs3

# Detect MariaDB version >= 11.4 and socket path inside the container
VERSION_GE_114=$(docker exec -t mcs1 bash -lc "mariadb -N -s -e 'SELECT (sys.version_major(), sys.version_minor(), sys.version_patch()) >= (11, 4, 0);'" | tr -d '\r')
if [[ "${VERSION_GE_114}" == "1" ]]; then
  SERVERNAME="mariadb"
fi

if [[ "${PKG_FORMAT}" == "rpm" ]]; then
  MTR_PATH="/usr/share/${SERVERNAME}-test"
else
  MTR_PATH="/usr/share/${SERVERNAME}/${SERVERNAME}-test"
fi

SOCKET_PATH=$(docker exec -t mcs1 bash -lc "mariadb -N -B -e \"SHOW VARIABLES LIKE 'socket'\" | awk '{print \$2}'" | tr -d '\r')

message "Multinode MTR path: ${MTR_PATH}, socket: ${SOCKET_PATH}, version >=11.4: ${VERSION_GE_114}"

if [[ "${VERSION_GE_114}" != "1" ]]; then
  message "Copying local columnstore suite into container (server < 11.4)"
  docker cp ../mysql-test/columnstore mcs1:"${MTR_PATH}/suite/"
else
  message "Skipping suite copy (server >= 11.4, tests expected in package)"
fi
docker exec -t mcs1 chown -R mysql:mysql "${MTR_PATH}"
docker exec -t mcs1 mariadb -e "CREATE DATABASE IF NOT EXISTS test;"

docker exec -t mcs1 bash -c "\
  cd '${MTR_PATH}' && \
  ./mtr \
    --extern socket='${SOCKET_PATH}' \
    --force \
    --print-core=detailed \
    --print-method=gdb \
    --max-test-fail=0 \
    --suite=columnstore/basic,columnstore/bugfixes \
"
