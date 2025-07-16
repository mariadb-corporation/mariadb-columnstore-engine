#!/bin/bash

# This script creates a repository from packages in result directory
# Should be executed by root

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")

source "$SCRIPT_LOCATION"/utils.sh
optparse.define short=R long=result desc="Directory with packages" variable=RESULT
source $(optparse.build)
echo "Arguments received: $@"

BUILDDIR="verylongdirnameforverystrangecpackbehavior"
COLUMNSTORE_RPM_PACKAGES_PATH="/mdb/${BUILDDIR}/*.rpm"
CMAPI_RPM_PACKAGES_PATH="/mdb/${BUILDDIR}/storage/columnstore/columnstore/cmapi/*.rpm"

COLUMNSTORE_DEB_PACKAGES_PATH="/mdb/*.deb"
COLUMNSTORE_DEBUG_DEB_PACKAGES_PATH="/mdb/*.ddeb"

CMAPI_DEB_PACKAGES_PATH="/mdb/${BUILDDIR}/storage/columnstore/columnstore/cmapi/*.deb"

if [ "$EUID" -ne 0 ]; then
  error "Please run script as root"
  exit 1
fi

if [[ -z "${RESULT:-}" ]]; then
  echo "Please provide provide --result parameter, e.g. ./createrepo.sh --result ubuntu24.04"
  exit 1
fi

cd "/mdb/${BUILDDIR}"

if ! compgen -G "/mdb/${BUILDDIR}/*.rpm" >/dev/null &&
  ! compgen -G "/mdb/${BUILDDIR}/storage/columnstore/columnstore/cmapi/*.rpm" >/dev/null &&
  ! compgen -G "/mdb/${BUILDDIR}/*.deb" >/dev/null &&
  ! compgen -G "/mdb/${BUILDDIR}/storage/columnstore/columnstore/cmapi/*.deb" >/dev/null; then
  echo "None of the cmapi or columnstore packages found. Failing!"
  exit 1
fi

echo "Adding columnstore packages to repository..."
if [[ $(compgen -G "$COLUMNSTORE_RPM_PACKAGES_PATH") ]]; then
  mv -v $COLUMNSTORE_RPM_PACKAGES_PATH "./${RESULT}/"
elif [[ $(compgen -G "$COLUMNSTORE_DEB_PACKAGES_PATH") ]]; then
  mv -v $COLUMNSTORE_DEB_PACKAGES_PATH "./${RESULT}/"
  mv -v $COLUMNSTORE_DEBUG_DEB_PACKAGES_PATH "./${RESULT}/" || true
else
  echo "Columnstore packages are not found!"
fi

echo "Adding cmapi packages to repository..."
if [[ $(compgen -G "$CMAPI_RPM_PACKAGES_PATH") ]]; then
  mv -v $CMAPI_RPM_PACKAGES_PATH "./${RESULT}/"
elif [[ $(compgen -G "$CMAPI_DEB_PACKAGES_PATH") ]]; then
  mv -v $CMAPI_DEB_PACKAGES_PATH "./${RESULT}/"
else
  echo "Cmapi packages are not found!"
fi

if [[ $(compgen -G "./${RESULT}/*.rpm") ]]; then
  retry_eval 5 "dnf install -q -y createrepo"
  createrepo "./${RESULT}"
else
  retry_eval 5 "apt update && apt install -y dpkg-dev"

  dpkg-scanpackages "${RESULT}" >Packages
  dpkg-scanpackages --type ddeb "${RESULT}" >>Packages
  gzip -c Packages >"./${RESULT}/Packages.gz"
fi

mkdir -p "/drone/src/${RESULT}"
cp -vrf "./${RESULT}/." "/drone/src/${RESULT}"
