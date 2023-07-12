#!/usr/bin/env bash

set -xeuo pipefail

VERSION="$1"
RESULT="$2"
ARCH="$3"
LINK="$4"
UPGRADE_TOKEN="$5"

yum clean all
rm -rf /var/cache/dnf/*
yum install -y wget which procps-ng diffutils rsyslog
wget https://dlm.mariadb.com/enterprise-release-helpers/mariadb_es_repo_setup -O mariadb_es_repo_setup
chmod +x mariadb_es_repo_setup
bash -c "./mariadb_es_repo_setup --token=${UPGRADE_TOKEN} --apply --mariadb-server-version=${VERSION} --skip-maxscale --skip-tools --skip-check-installed"
dnf repo-pkgs mariadb-es-main list
dnf -y install MariaDB-server MariaDB-client MariaDB-columnstore-engine MariaDB-columnstore-engine-debuginfo

systemctl start mariadb
systemctl start mariadb-columnstore
bash -c "./upgrade_data.sh"
bash -c "./upgrade_verify.sh"

bash -c "./setup-repo.sh"

dnf repo-pkgs repo list
dnf -y update MariaDB-server MariaDB-client MariaDB-columnstore-engine MariaDB-columnstore-engine-debuginfo
bash -c "./upgrade_verify.sh"
