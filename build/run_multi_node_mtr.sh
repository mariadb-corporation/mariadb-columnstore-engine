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


if [[ "$DISTRO" == *rocky* ]]; then
    SOCKET_PATH="/var/lib/mysql/mysql.sock"
    MTR_PATH="/usr/share/mysql-test"
else
    SOCKET_PATH="/run/mysqld/mysqld.sock"
    MTR_PATH="/usr/share/mysql/mysql-test"
fi

message "Running multinode mtr tests..."

cd docker
cp .env_example .env
sed -i "/^MCS_IMAGE_NAME=/s|=.*|=${MCS_IMAGE_NAME}|" .env
sed -i "/^MAXSCALE=/s|=.*|=false|" .env

docker-compose up -d
docker exec mcs1 provision mcs1 mcs2 mcs3
docker cp ../mysql-test/columnstore mcs1:"${MTR_PATH}/suite/"
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
