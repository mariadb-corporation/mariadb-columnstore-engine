#!/usr/bin/env sh

set -ex

REPOFILE='mariadb-columnstore'

if [ -n "$1" ]; then
    REPOFILE=$1
fi

REPO_PACKAGES_URL=${PACKAGES_URL}

if [ -n "$2" ]; then
    REPO_PACKAGES_URL=$2
fi

. /etc/os-release

ARCH=$(expr "$(uname -m)" : "arm64\|aarch64" > /dev/null && echo "arm64" || echo "amd64")

case "$ID" in
ubuntu|debian)
    apt update -y
    apt install -y ca-certificates
    echo "deb [trusted=yes] ${REPO_PACKAGES_URL}/${ARCH}/ ${OS}/" > /etc/apt/sources.list.d/${REPOFILE}.list
    cat /etc/apt/sources.list.d/${REPOFILE}.list
    cat << EOF > /etc/apt/preferences
Package: *
Pin: origin cspkg.s3.amazonaws.com
Pin-Priority: 1700
EOF
    apt update -y
    ;;
rocky|centos)
    cat << EOF > /etc/yum.repos.d/${REPOFILE}.repo
[repo]
name=${REPOFILE}
baseurl=${REPO_PACKAGES_URL}/${ARCH}/${OS}
enabled=1
gpgcheck=0
module_hotfixes=1
EOF
    ;;
*)
    echo "$ID is unknown!"
    exit 1
    ;;
esac
