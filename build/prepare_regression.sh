#!/bin/bash
set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

optparse.define short=c long=container-name desc="Name of the Docker container where regression tests will run" variable=CONTAINER_NAME
optparse.define short=b long=regression-branch desc="Branch from regression tests repo" variable=REGRESSION_BRANCH
optparse.define short=d long=distro desc="Linux distro for which regression is executed" variable=DISTRO
source $(optparse.build)

for var in CONTAINER_NAME REGRESSION_BRANCH DISTRO; do
  if [[ -z "${!var}" ]]; then
    error "Missing required flag: -${var:0:1} / --${var,,}"
    exit 1
  fi
done

if [[ -z $(docker ps -q --filter "name=${CONTAINER_NAME}") ]]; then
  error "Container '${CONTAINER_NAME}' is not running."
  exit 1
fi

if [[ "$DISTRO" == *rocky* ]]; then
    CONFIG_PATH_PREFIX="/etc/my.cnf.d/"
else
    CONFIG_PATH_PREFIX="/etc/mysql/mariadb.conf.d/50-"
fi

BUILD_DIR="verylongdirnameforverystrangecpackbehavior"

git clone --recurse-submodules --branch "${REGRESSION_BRANCH}" --depth 1 https://github.com/mariadb-corporation/mariadb-columnstore-regression-test

cd mariadb-columnstore-regression-test
git rev-parse --abbrev-ref HEAD && git rev-parse HEAD
cd ..

docker cp mariadb-columnstore-regression-test "${CONTAINER_NAME}:/"

echo "list storage manager binary:"
ls -la "/mdb/${BUILD_DIR}/storage/columnstore/columnstore/storage-manager"

docker cp "/mdb/${BUILD_DIR}/storage/columnstore/columnstore/storage-manager" "${CONTAINER_NAME}:/"

#copy test data for regression test suite
execInnerDocker "$CONTAINER_NAME" 'bash -c "wget -qO- https://cspkg.s3.amazonaws.com/testData.tar.lz4 | lz4 -dc - | tar xf - -C mariadb-columnstore-regression-test/"'

# set mariadb lower_case_table_names=1 config option
execInnerDocker "$CONTAINER_NAME" "sed -i '/^\[mariadb\]/a lower_case_table_names=1' ${CONFIG_PATH_PREFIX}server.cnf"

# set default client character set to utf-8
execInnerDocker "$CONTAINER_NAME" "sed -i '/^\[client\]/a default-character-set=utf8' ${CONFIG_PATH_PREFIX}client.cnf"


execInnerDocker "$CONTAINER_NAME" "systemctl start mariadb"
execInnerDocker "$CONTAINER_NAME" "systemctl restart mariadb-columnstore"

# Set RAM consumption limits to avoid RAM contention b/w mtr and regression steps.
execInnerDocker "$CONTAINER_NAME" '/usr/bin/mcsSetConfig SystemConfig CGroup just_no_group_use_local'

# Compile queryTester inside container
execInnerDocker "$CONTAINER_NAME" "g++ /mariadb-columnstore-regression-test/mysql/queries/queryTester.cpp -O2 -o /mariadb-columnstore-regression-test/mysql/queries/queryTester"

echo "Prepare_regression stage has completed"
