#!/usr/bin/env bash
set -eo pipefail

source /etc/os-release

ARCH=$([[ "$(uname -m)" =~ arm64|aarch64 ]] && echo "-arm" || echo "")

if [ "${VERSION_ID%%.*}" -eq 8 ]; then dnf module -y disable mysql mariadb; fi

case "$ID" in
ubuntu|debian)
    cat << EOF > /etc/apt/auth.conf
machine $(echo "${EXTERNAL_PACKAGES_URL}"| awk -F[/:] '{print $4}')
login ${EXTERNAL_PACKAGES_USERNAME}
password ${EXTERNAL_PACKAGES_PASSWORD}
EOF
    apt update -y
    apt install -y ca-certificates
    echo "deb [trusted=yes] ${EXTERNAL_PACKAGES_URL}/DEB/ ${ID}-${VERSION_ID//./}${ARCH}/" > /etc/apt/sources.list.d/repo.list
    apt update -y
    ;;
rocky|centos)
    cat << EOF > /etc/yum.repos.d/repo.repo
[repo]
name=repo
baseurl=${EXTERNAL_PACKAGES_URL}/RPMS/rhel-${VERSION_ID%%.*}${ARCH}
enabled=1
gpgcheck=0
username=${EXTERNAL_PACKAGES_USERNAME}
password=${EXTERNAL_PACKAGES_PASSWORD}
EOF
    ;;
*)
    echo "$ID is unknown!"
    exit 1
    ;;
esac
