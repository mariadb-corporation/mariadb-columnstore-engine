#!/bin/bash

SCRIPT_LOCATION=$(dirname "$0")

. /etc/os-release

case "$VERSION_CODENAME" in
 focal)
    OS_SHORTNAME=ubuntu20.04
    ;;
  jammy)
    OS_SHORTNAME=ubuntu22.04
    ;;
  noble)
    OS_SHORTNAME=ubuntu24.04
    ;;
esac

case "$PLATFORM_ID" in
   platform:el8)
    OS_SHORTNAME=rockylinux8
    ;;
   platform:el9)
    OS_SHORTNAME=rockylinux8
    ;;
esac

PACKAGES_URL="https://cspkg.s3.amazonaws.com/FoundationDB" OS=$OS_SHORTNAME sudo -E $SCRIPT_LOCATION/../../setup-repo.sh mariadb-columnstore-foundationdb

install_for_deb() {
    sudo apt install -y "$@"
}

install_for_rpm() {
    sudo yum install -y "$@"
}

if [ -f /etc/redhat-release ]; then
    install_for_rpm "foundationdb-clients foundationdb-server"
elif [ -f /etc/debian_version ]; then
    install_for_deb "foundationdb-clients foundationdb-server"
else
    echo "Unsupported distribution. This script only supports RPM-based and DEB-based systems."
    exit 1
fi
