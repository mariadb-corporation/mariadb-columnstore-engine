#!/bin/bash

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

VERSION="$1"

if [[ $# -ne 1 ]]; then
  echo "Please pass clang-version as a first parameter"
  exit 1
fi

change_ubuntu_mirror us

message "Installing libc++-${VERSION}"

retry_eval 5 apt-get clean && apt-get update && apt-get install -y libc++-${VERSION}-dev libc++abi-${VERSION}-dev
