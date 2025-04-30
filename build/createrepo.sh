#!/bin/bash

# This script creates a repository from packages in result directory
# Should be executed by root

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")


source "$SCRIPT_LOCATION"/utils.sh
optparse.define short=D long=distro desc="OS" variable=OS
source $(optparse.build)
echo "Arguments received: $@"

BUILDDIR="verylongdirnameforverystrangecpackbehavior";
COLUMNSTORE_RPM_PACKAGES_PATH="/mdb/${BUILDDIR}/*.rpm"
CMAPI_RPM_PACKAGES_PATH="/mdb/${BUILDDIR}/storage/columnstore/columnstore/cmapi/*.rpm"

COLUMNSTORE_DEB_PACKAGES_PATH="/mdb/*.deb"
CMAPI_DEB_PACKAGES_PATH="/mdb/${BUILDDIR}/storage/columnstore/columnstore/cmapi/*.deb"
RESULT=$(echo "$OS" | sed 's/://g' | sed 's/\//-/g')

if [ "$EUID" -ne 0 ]; then
    error "Please run script as root"
    exit 1
fi

if [[ -z "${OS:-}" ]]; then
  echo "Please provide provide --distro parameter, e.g. ./createrepo.sh --distro ubuntu:24.04"
  exit 1
fi

pkg_format="deb"
if [[ "$OS" == *"rocky"* ]]; then
    pkg_format="rpm"
fi

cd "/mdb/${BUILDDIR}"

if [[ "${pkg_format}" == "rpm" ]]; then
  dnf install -q -y createrepo

  mv -v ${COLUMNSTORE_RPM_PACKAGES_PATH} ${CMAPI_RPM_PACKAGES_PATH} "./${RESULT}/"
  createrepo "./${RESULT}"

else
  apt update && apt install -y dpkg-dev
  mv -v ${COLUMNSTORE_DEB_PACKAGES_PATH} ${CMAPI_DEB_PACKAGES_PATH} "./${RESULT}/"
  dpkg-scanpackages "${RESULT}" | gzip > "./${RESULT}/Packages.gz"
fi

mkdir -p "/drone/src/${RESULT}"
cp -vrf "./${RESULT}" "/drone/src/${RESULT}"
