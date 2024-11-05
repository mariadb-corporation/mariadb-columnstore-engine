#!/bin/bash

set -e
FDB_VERSION=7.3.43
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

make_openssl()
{
    message "Compiling static openssl"
    curl -Ls https://www.openssl.org/source/openssl-1.1.1m.tar.gz -o openssl.tar.gz && \
    echo "f89199be8b23ca45fc7cb9f1d8d3ee67312318286ad030f5316aca6462db6c96  openssl.tar.gz" > openssl-sha.txt && \
    sha256sum --quiet -c openssl-sha.txt && \
    mkdir openssl && \
    tar --strip-components 1 --no-same-owner --directory openssl -xf openssl.tar.gz && \
    cd openssl && \
    ./config CFLAGS="-fPIC -O3" --prefix=/usr/local && \
    make -j`nproc` && \
    make -j1 install && \
    cd ../ && \
    rm -rf /tmp/*
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

make_jemalloc()
{
    message "Compiling jemalloc 5.3"
    curl -Ls https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2 -o jemalloc.tar.bz2 && \
    echo "2db82d1e7119df3e71b7640219b6dfe84789bc0537983c3b7ac4f7189aecfeaa  jemalloc.tar.bz2" > jemalloc-sha.txt && \
    sha256sum --quiet -c jemalloc-sha.txt && \
    mkdir -p /opt/jemalloc_5.3.0 && \
    tar --strip-components 1 --no-same-owner --no-same-permissions --directory /opt/jemalloc_5.3.0 -xjf jemalloc.tar.bz2 && \
    cd /opt/jemalloc_5.3.0 && \
    ./configure --enable-static --disable-cxx --enable-prof && \
    make -j32 && \
    cd .. && \
    rm -rf /tmp/*
}

setup_sphinx()
{
    message "installing Sphinx"
    python3 -m pip install setuptools==65.3.0 sphinx-bootstrap-theme==0.8.1 docutils==0.19 sphinx==5.1.1 sphinx-autobuild Jinja2==3.1.2 urllib3==2.0.2
}



if [[ ${ID} == 'ubuntu' || ${ID} == 'debian' ]]; then
    message "Preparing dev requirements for ubuntu|debian"
    GENERATOR='DEB'
    PACKAGES_TYPE='deb'
    PKG_MANAGER='dpkg -i'
    PACKAGES_SUFFIX="-DDEB=${VERSION_CODENAME}"
    print_env
    ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
    DEBIAN_FRONTEND=noninteractive apt install -y -qq automake cmake curl file g++ gcc git jq libjemalloc-dev libssl-dev mono-devel patch python3 python3-pip python3-dev unzip

elif [[ ${ID} == "rocky" ]]; then
    PKG_MANAGER='yum install -y'
    OS_SHORTCUT=$(echo $PLATFORM_ID | cut -f2 -d ':')
    PACKAGES_SUFFIX="-DRPM=${OS_SHORTCUT}"
    PACKAGES_TYPE='rpm'
    GENERATOR='RPM'

    dnf -y update
    dnf install -y -q ncurses
    dnf install -y -q python3 python3-pip

    if [[ ${VERSION_ID} == "9.3" ]]; then
        message "Preparing dev requirements for Rockylinux 9"
        dnf install -y -q epel-release scl-utils yum-utils
        dnf install -y -q gcc-c++
        dnf install -y -q --enablerepo devel libstdc++-static
    else
        message "Preparing dev requirements for Rockylinux 8"
        dnf install -y -q 'dnf-command(config-manager)' && dnf config-manager --set-enabled powertools
        dnf install -y -q epel-release gcc-toolset-${GCC_VERSION}
        . /opt/rh/gcc-toolset-${GCC_VERSION}/enable
        rpmkeys --import "http://keyserver.ubuntu.com/pks/lookup?op=get&search=0x3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF"
        curl https://download.mono-project.com/repo/centos8-stable.repo | tee /etc/yum.repos.d/mono-centos8-stable.repo
    fi

    dnf install -y -q --allowerasing automake cmake curl dnf gcc git jemalloc-devel jq mono-devel patch perl python3-devel rpm-build unzip
    make_openssl

    OPENSSL_FLAGS=' -DOPENSSL_ROOT_DIR=/usr/local/openssl/ '

else
    echo "Unsupported distribution. This script only supports Rocky[8|9], Ubuntu [20.04|22.04|24.04] Debian[11|12]"
fi

message "Downloading sources"
wget https://github.com/apple/foundationdb/archive/refs/tags/${FDB_VERSION}.zip
unzip -q ${FDB_VERSION}.zip

message "Configuring cmake"
mkdir -p fdb_build
cd fdb_build

make_lz4
make_jemalloc
setup_sphinx

export CLICOLOR_FORCE=1

cmake  -DWITH_PYTHON=ON \
       -DWITH_C_BINDING=ON \
       -DWITH_PYTHON_BINDING=ON \
       -DWITH_JAVA_BINDING=OFF \
       -DWITH_GO_BINDING=OFF \
       -DWITH_RUBY_BINDING=IFF \
       -DWITH_TLS=ON \
       -DDISABLE_TLS=OFF \
       -DWITH_DOCUMENTATION=OFF \
       -DWITH_ROCKSDB_EXPERIMENTAL=OFF \
       -DWITH_AWS_BACKUP=ON \
       -DFDB_RELEASE=ON \
       ${PACKAGES_SUFFIX} \
       ${OPENSSL_FLAGS} \
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
