#!/bin/bash

# This script installs dependencies and builds cmapi package
# Should be executed by root
# Should reside in build/ (or storage/columnstore/columnstore/build), as all the paths a relative to script's location

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
COLUMNSTORE_SOURCE_PATH=$(realpath "$SCRIPT_LOCATION"/../)
MDB_SOURCE_PATH=$(realpath "$SCRIPT_LOCATION"/../../../..)

source "$SCRIPT_LOCATION"/utils.sh

optparse.define short=d long=distro desc="distro" variable=OS
source $(optparse.build)
echo "Arguments received: $@"

if [ "$EUID" -ne 0 ]; then
  error "Please run script as root"
  exit 1
fi

if [[ -z "${OS:-}" ]]; then
  echo "Please provide provide --distro parameter, e.g. ./build_cmapi.sh --distro ubuntu:22.04"
  exit 1
fi

select_pkg_format ${OS}

if [[ "$(arch)" == "arm64" || "$(arch)" == "aarch64" ]]; then
  export CC=gcc
fi

on_exit() {
  if [[ $? -eq 0 ]]; then
    echo "Cmapi package has been build successfully."
  else
    echo "Cmapi package build failed!"
  fi
}
trap on_exit EXIT

install_deps() {
  echo "Installing dependencies..."

  cd "$COLUMNSTORE_SOURCE_PATH"/cmapi

  if is_rocky_version $OS 9; then
    retry_eval 5 "dnf install -q -y libxcrypt-compat yum-utils"
    retry_eval 5 "dnf config-manager --set-enabled devel && dnf update -q -y" #to make redhat-lsb-core available for rocky 9
  fi

  #no redhat-lsb-release for rockylinux >=10
  if is_rocky_version $OS && ! is_rocky_version_ge $OS 10; then
    retry_eval 5 "dnf update -q -y && dnf install -q -y redhat-lsb-core"
  fi


  if is_rocky_version_ge $OS 10; then
    retry_eval 5 "dnf update -q -y && dnf install -q -y libxcrypt-compat"
  fi

  if [[ "$PKG_FORMAT" == "rpm" ]]; then
    retry_eval 5 "dnf update -q -y && dnf install -q -y epel-release wget zstd findutils gcc cmake make rpm-build libarchive"
  else
    retry_eval 5 "apt-get update -qq -o Dpkg::Use-Pty=0 && apt-get install -qq -o Dpkg::Use-Pty=0 wget zstd findutils gcc cmake make dpkg-dev lsb-release"
  fi

  if [ "$(arch)" == "x86_64" ]; then
    PYTHON_URL="https://github.com/indygreg/python-build-standalone/releases/download/20220802/cpython-3.9.13+20220802-x86_64_v2-unknown-linux-gnu-pgo+lto-full.tar.zst"
  elif [[ "$(arch)" == "arm64" || "$(arch)" == "aarch64" ]]; then
    PYTHON_URL="https://github.com/indygreg/python-build-standalone/releases/download/20220802/cpython-3.9.13+20220802-aarch64-unknown-linux-gnu-noopt-full.tar.zst"
  else
    echo "Unsupported architecture: $(arch)"
    exit 1
  fi

  rm -rf python pp
  wget -qO- "$PYTHON_URL" | tar --use-compress-program=unzstd -xf - -C ./

  mv python pp
  mv pp/install python
  chown -R root:root python

  python/bin/pip3 install -t deps --only-binary :all -r requirements.txt
  cp cmapi_server/cmapi_server.conf cmapi_server/cmapi_server.conf.default
}

build_cmapi() {
  cd "$COLUMNSTORE_SOURCE_PATH"/cmapi
  ./cleanup.sh
  cmake -D"${PKG_FORMAT^^}"=1 -DSERVER_DIR="$MDB_SOURCE_PATH" . && make package
}
install_deps
build_cmapi
