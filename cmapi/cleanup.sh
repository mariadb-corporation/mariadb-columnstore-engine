#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

rm -rf  \
    cmapi_server/test/tmp.xml \
    systemd.env \
    *.service \
    prerm \
    postinst \
    CMakeCache.txt \
    CMakeFiles \
    CMakeScripts \
    Makefile \
    cmake_install.cmake \
    install_manifest.txt \
    *CPack* \
#    buildinfo.txt

find . -type d -name __pycache__ -exec rm -rf {} +
find . -type f -iname '*.swp' -exec rm -rf {} +
