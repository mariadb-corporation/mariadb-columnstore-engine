#!/bin/bash

# Single source of truth for the python-build-standalone release bundled with
# CMAPI. Sourced by build_cmapi.sh (to download the interpreter) and by
# generate_cmapi_sbom.sh (to record it in the SBOM). Bump both fields together.

PBS_RELEASE="20260127"
PBS_CPYTHON_VERSION="3.13.11"

# Sets PBS_URL and PBS_SHA for the current architecture. Exits on unsupported.
cmapi_python_release_for_arch() {
  local a="$(arch)"
  if [[ "$a" == "x86_64" ]]; then
    PBS_URL="https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_RELEASE}/cpython-${PBS_CPYTHON_VERSION}+${PBS_RELEASE}-x86_64_v2-unknown-linux-gnu-pgo+lto-full.tar.zst"
    PBS_SHA="b5d915b716609b9f60e17ab8654cdf0bd810f52f710b743d0fd75bb85db1cbac"
  elif [[ "$a" == "arm64" || "$a" == "aarch64" ]]; then
    PBS_URL="https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_RELEASE}/cpython-${PBS_CPYTHON_VERSION}+${PBS_RELEASE}-aarch64-unknown-linux-gnu-pgo+lto-full.tar.zst"
    PBS_SHA="c0065a0e7afe48bad9471b7f9daafe3331afa900e19051510ef7696926df759e"
  else
    echo "Unsupported architecture: $a"
    exit 1
  fi
}
