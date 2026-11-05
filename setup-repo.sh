#!/usr/bin/env sh

set -ex

. /etc/os-release

ARCH=$(expr "$(uname -m)" : "arm64\|aarch64" > /dev/null && echo "arm64" || echo "amd64")

case "$ID" in
ubuntu|debian)
    if [ "${VERSION_CODENAME:-}" = "bullseye" ]; then
        # Debian 11 reached EOL on 2026-08-31; packages were moved to
        # archive.debian.org. Repoint sources before apt-get update so the
        # stale index does not keep requesting pruned +deb11uN files (404s).
        printf '%s\n' \
            'deb http://archive.debian.org/debian bullseye main' \
            'deb http://archive.debian.org/debian bullseye-updates main' \
            'deb http://archive.debian.org/debian-security bullseye-security main' \
            > /etc/apt/sources.list
        rm -rf /var/lib/apt/lists/*
        echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99archive
    fi
    apt update -y
    apt install -y ca-certificates
    echo "deb [trusted=yes] ${PACKAGES_URL}/${ARCH}/ ${OS}/" > /etc/apt/sources.list.d/repo.list
    cat /etc/apt/sources.list.d/repo.list
    cat << EOF > /etc/apt/preferences
Package: *
Pin: origin cspkg.s3.amazonaws.com
Pin-Priority: 1700
EOF
    apt update -y
    ;;
rocky|centos)
    cat << EOF > /etc/yum.repos.d/repo.repo
[repo]
name=repo
baseurl=${PACKAGES_URL}/${ARCH}/${OS}
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
