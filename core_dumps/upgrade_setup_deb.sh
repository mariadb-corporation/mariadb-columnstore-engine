#!/usr/bin/env bash

set -xeuo pipefail

VERSION="$1"
RESULT="$2"
ARCH="$3"
LINK="$4"
UPGRADE_TOKEN="$5"

apt install --yes rsyslog
sed -i "s/exit 101/exit 0/g" /usr/sbin/policy-rc.d
bash -c "apt update --yes && apt install -y procps wget curl"
wget https://dlm.mariadb.com/enterprise-release-helpers/mariadb_es_repo_setup -O mariadb_es_repo_setup
chmod +x mariadb_es_repo_setup
bash -c "./mariadb_es_repo_setup --token=${UPGRADE_TOKEN} --apply --mariadb-server-version=${VERSION} --skip-maxscale --skip-tools"
apt update --yes
apt install --yes mariadb-server mariadb-client mariadb-plugin-columnstore
systemctl start mariadb
systemctl start mariadb-columnstore
bash -c "./upgrade_data.sh"
bash -c "./upgrade_verify.sh"

touch /etc/apt/auth.conf
cat << EOF > /etc/apt/auth.conf
machine ${LINK}${RESULT}/
EOF

bash -c "./setup-repo.sh"

apt upgrade --yes
bash -c "./upgrade_verify.sh"
