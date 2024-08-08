#!/bin/bash

set -e
FDB_VERSION=7.1.63
GCC_VERSION='11'
BUILD_COMMAND='make -j2'
. /etc/os-release


message()
{
    color_normal=$(tput sgr0 -T xterm-256color )
    color_cyan=$(tput setaf 87 -T xterm-256color)

    echo "${color_cyan}・・・・・・・・・・・${color_normal} $@"
}

print_env()
{
    message "ID=${ID}"
    message "VERSION_ID=${VERSION_ID}"
    message "GENERATOR=${GENERATOR}"
    message "PACKAGES_TYPE=${PACKAGES_TYPE}"
    message "PKG_MANAGER=${PKG_MANAGER}"
    message "PACKAGES_SUFFIX=${PACKAGES_SUFFIX}"
}

make_lz4()
{
    message "Compiling static lz4"
    curl -Ls https://github.com/lz4/lz4/archive/refs/tags/v1.9.3.tar.gz -o lz4.tar.gz && \
    echo "030644df4611007ff7dc962d981f390361e6c97a34e5cbc393ddfbe019ffe2c1  lz4.tar.gz" > lz4-sha.txt && \
    sha256sum --quiet -c lz4-sha.txt && \
    mkdir lz4 && \
    tar --strip-components 1 --no-same-owner --directory lz4 -xf lz4.tar.gz && \
    cd lz4 && \
    make && \
    make install && \
    cd ../ && \
    rm -rf /tmp/*
}

if [[ ${ID} == 'ubuntu' || ${ID} == 'debian' ]]; then
    message "Preparing dev requirements for ubuntu|debian"
    GENERATOR='DEB'
    PACKAGES_TYPE='deb'
    PKG_MANAGER='dpkg -i'
    PACKAGES_SUFFIX="-DDEB=${VERSION_CODENAME}"
    print_env
    ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
    DEBIAN_FRONTEND=noninteractive apt install -y -qq automake cmake curl g++ gcc git jq libjemalloc-dev libssl-dev mono-devel patch python3-dev unzip

elif [[ ${ID} == "rocky" ]]; then
    PKG_MANAGER='yum install -y'
    OS_SHORTCUT=$(echo $PLATFORM_ID | cut -f2 -d ':')
    PACKAGES_SUFFIX="-DRPM=${OS_SHORTCUT}"
    PACKAGES_TYPE='rpm'
    dnf -y update
    dnf install -y -q ncurses

    if [[ ${VERSION_ID} == "9.3" ]]; then
        message "Preparing dev requirements for Rockylinux 9"
        dnf install -y -q epel-release scl-utils yum-utils
        dnf install -y -q --allowerasing gcc-c++
    else
        message "Preparing dev requirements for Rockylinux 8"
        dnf install -y -q 'dnf-command(config-manager)' && dnf config-manager --set-enabled powertools
        dnf install -y -q epel-release gcc-toolset-${GCC_VERSION}
        . /opt/rh/gcc-toolset-${GCC_VERSION}/enable
        rpmkeys --import "http://keyserver.ubuntu.com/pks/lookup?op=get&search=0x3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF"
        curl https://download.mono-project.com/repo/centos8-stable.repo | tee /etc/yum.repos.d/mono-centos8-stable.repo
    fi

    dnf install -y -q --allowerasing automake cmake curl dnf gcc git jemalloc-devel jq mono-devel openssl-devel patch python3-devel unzip

else
    echo "Unsupported distribution. This script only supports Rocky[8|9], Ubuntu [20.04|22.04|24.04] Debian[11|12]"
fi

message "Downloading sources"
wget https://github.com/apple/foundationdb/archive/refs/tags/${FDB_VERSION}.zip
unzip -q ${FDB_VERSION}.zip

message "Patching sources"
sed -i "s/O_WRONLY | O_CREAT | O_TRUNC/O_WRONLY | O_CREAT | O_TRUNC, 0666/g" foundationdb-${FDB_VERSION}/fdbbackup/FileDecoder.actor.cpp

message "Configuring cmake"
mkdir -p fdb_build
cd fdb_build

make_lz4

export CLICOLOR_FORCE=1

cmake  -DWITH_PYTHON=ON \
       -DWITH_C_BINDING=ON \
       -DWITH_PYTHON_BINDING=ON \
       -DWITH_JAVA_BINDING=OFF \
       -DWITH_GO_BINDING=OFF \
       -DWITH_RUBY_BINDING=IFF \
       -DWITH_TLS=OFF \
       -DWITH_DOCUMENTATION=OFF \
       -DWITH_ROCKSDB_EXPERIMENTAL=OFF \
       -DWITH_AWS_BACKUP=OFF \
       ${PACKAGES_SUFFIX} \
            ../foundationdb-${FDB_VERSION}

message "Compiling sources"



message "Compiling fdbserver"
cd fdbserver
${BUILD_COMMAND}
cd -
message "Compiling fdbcli"
cd fdbcli
${BUILD_COMMAND}
cd -
message "Compiling fdbclient"
cd fdbclient
${BUILD_COMMAND}
cd -
message "Compiling rest"
${BUILD_COMMAND}

message "Generating packages"
cpack -G ${GENERATOR}
message "Installing packages"
${PKG_MANAGER} packages/*.${PACKAGES_TYPE}
message "Checking statuses"
fdbcli --exec 'status json'  | jq .client
service foundationdb status
