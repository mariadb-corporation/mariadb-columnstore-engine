#!/usr/bin/env bash

source ./utils.sh

set -xeuo pipefail

VERSION="$1"
RESULT="$2"
ARCH="$3"
LINK="$4"
UPGRADE_TOKEN="$5"

DEBIAN_FRONTEND=noninteractive
UCF_FORCE_CONFNEW=1

apt install --yes rsyslog
sed -i "s/exit 101/exit 0/g" /usr/sbin/policy-rc.d
bash -c "apt update --yes && apt install -y procps wget curl"
wget https://dlm.mariadb.com/enterprise-release-helpers/mariadb_es_repo_setup -O mariadb_es_repo_setup
chmod +x mariadb_es_repo_setup
bash -c "./mariadb_es_repo_setup --token=${UPGRADE_TOKEN} --apply --mariadb-server-version=${VERSION} --skip-maxscale --skip-tools"
apt update --yes
apt install --yes -oDebug::RunScripts=1 mariadb-server mariadb-client mariadb-plugin-columnstore
systemctl start mariadb
systemctl start mariadb-columnstore

INITIAL_VERSION=$(mariadb -e "select @@version;")

bash -c "./upgrade_data.sh"
bash -c "./upgrade_verify.sh"

touch /etc/apt/auth.conf
cat << EOF > /etc/apt/auth.conf
machine ${LINK}${RESULT}/
EOF

bash -c "./setup-repo.sh"


# Configuration file '/etc/columnstore/Columnstore.xml'
#  ==> Modified (by you or by a script) since installation.
#  ==> Package distributor has shipped an updated version.
#    What would you like to do about it ?  Your options are:
#     Y or I  : install the package maintainer's version
#     N or O  : keep your currently-installed version
#       D     : show the differences between the versions
#       Z     : start a shell to examine the situation
#  The default action is to keep your current version.

# the -o options are used to make choise of keep your currently-installed version without interactive prompt

apt-get --yes --with-new-pkgs -oDebug::RunScripts=1 -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" upgrade

UPGRADED_VERSION=$(mariadb -e "select @@version;")

if [[ "$INITIAL_VERSION" == "$UPGRADED_VERSION" ]]; then
  error "The upgrade didn't happen!"
  exit 1
else
  message_splitted "The upgrade from "$INITIAL_VERSION" to "$UPGRADED_VERSION" succeded!"
  bash -c "./upgrade_verify.sh"
fi
