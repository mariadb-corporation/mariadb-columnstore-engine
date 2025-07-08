#!/bin/bash

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

CLANG_VERSION="$1"

if [[ $# -ne 1 ]]; then
  echo "Please pass clang-version as a first parameter"
  exit 1
fi

change_ubuntu_mirror us

message "Installing clang-${CLANG_VERSION}"

retry_eval 5 apt-get clean && apt-get update && apt-get install -y wget curl lsb-release software-properties-common gnupg
wget https://apt.llvm.org/llvm.sh
bash llvm.sh $CLANG_VERSION
export CC=/usr/bin/clang
export CXX=/usr/bin/clang++





