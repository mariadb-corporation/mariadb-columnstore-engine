#!/usr/bin/env bash

source ./utils.sh

set -xeuo pipefail

VERSION="$1"
RESULT="$2"
ARCH="$3"
LINK="$4"
UPGRADE_TOKEN="$5"

yum clean all
yum install -y wget which procps-ng diffutils rsyslog
wget https://dlm.mariadb.com/enterprise-release-helpers/mariadb_es_repo_setup -O mariadb_es_repo_setup
chmod +x mariadb_es_repo_setup
bash -c "./mariadb_es_repo_setup --token=${UPGRADE_TOKEN} --apply --mariadb-server-version=${VERSION} --skip-maxscale --skip-tools --skip-check-installed"
yum repo-pkgs mariadb-es-main list
yum -y install MariaDB-server MariaDB-client MariaDB-columnstore-engine MariaDB-columnstore-engine-debuginfo

systemctl start mariadb
systemctl start mariadb-columnstore

INITIAL_VERSION=$(mariadb -e "select @@version;")

bash -c "./upgrade_data.sh"
bash -c "./upgrade_verify.sh"

bash -c "./setup-repo.sh"

yum repo-pkgs repo list
yum -y update MariaDB-server MariaDB-client MariaDB-columnstore-engine MariaDB-columnstore-engine-debuginfo

UPGRADED_VERSION=$(mariadb -e "select @@version;")

if [[ "$INITIAL_VERSION" == "$UPGRADED_VERSION" ]]; then
  error "The upgrade didn't happen!"
  exit 1
else
  message_splitted "The upgrade from "$INITIAL_VERSION" to "$UPGRADED_VERSION" succeded!"
  bash -c "./upgrade_verify.sh"
fi
