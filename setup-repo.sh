#!/usr/bin/env bash

set -ex

. /etc/os-release

ARCH=$(expr "$(uname -m)" : "arm64\|aarch64" > /dev/null && echo "arm64" || echo "amd64")

ARCH_SUFFIX=""
if [[ $ARCH == "arm64" ]]; then ARCH_SUFFIX="-arm"; fi

case "$ID" in
ubuntu|debian)
    apt update -y
    apt install -y ca-certificates
    MAIN_URL=""
    if [[ -v EXTERNAL_PACKAGES_URL ]]; then
        cat << EOF > /etc/apt/auth.conf
machine $(echo "${EXTERNAL_PACKAGES_URL}"| awk -F[/:] '{print $4}')
login ${EXTERNAL_PACKAGES_USERNAME}
password ${EXTERNAL_PACKAGES_PASSWORD}
EOF
        echo "deb [trusted=yes] ${EXTERNAL_PACKAGES_URL}/DEB/ ${ID}-${VERSION_ID//./}${ARCH_SUFFIX}/" > /etc/apt/sources.list.d/repo.list
        MAIN_URL=$EXTERNAL_PACKAGES_URL
    else
        echo "deb [trusted=yes] ${PACKAGES_URL}/${ARCH}/ ${OS}/" > /etc/apt/sources.list.d/repo.list
        MAIN_URL=$PACKAGES_URL
    fi
    PACKAGES_DOMAIN=`echo $MAIN_URL | sed -e 's/[^/]*\/\/\([^@]*@\)\?\([^:/]*\).*/\2/'`
    cat /etc/apt/sources.list.d/repo.list
    cat << EOF > /etc/apt/preferences
Package: *
Pin: origin $PACKAGES_DOMAIN
Pin-Priority: 1700
EOF
    cat /etc/apt/preferences
    apt update -y
    ;;
rocky|centos)
    if [[ -v EXTERNAL_PACKAGES_URL ]]; then
        cat << EOF > /etc/yum.repos.d/repo.repo
[repo]
name=repo
baseurl=${EXTERNAL_PACKAGES_URL}/RPMS/rhel-${VERSION_ID%%.*}${ARCH_SUFFIX}
enabled=1
gpgcheck=0
username=${EXTERNAL_PACKAGES_USERNAME}
password=${EXTERNAL_PACKAGES_PASSWORD}
EOF
    else
        cat << EOF > /etc/yum.repos.d/repo.repo
[repo]
name=repo
baseurl=${PACKAGES_URL}/${ARCH}/${OS}
enabled=1
gpgcheck=0
module_hotfixes=1
EOF
    fi
    ;;
*)
    echo "$ID is unknown!"
    exit 1
    ;;
esac
