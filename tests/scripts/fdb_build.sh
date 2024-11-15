#!/bin/bash

set -e
FDB_VERSION=7.3.43
GCC_VERSION='11'
CLANG_VERSION='16'
BUILD_COMMAND='make -j2'
SCRIPT_LOCATION=$(dirname "$0")

. /etc/os-release


message()
{
    color_normal=$(tput sgr0 -T xterm-256color )
    color_cyan=$(tput setaf 87 -T xterm-256color)

    echo "${color_cyan}・・・・・・・・・・・${color_normal} $@"
}

error()
{
    color_normal=$(tput sgr0 -T xterm-256color )
    color_red=$(tput setaf 1 -T xterm-256color)
    echo "${color_red}・・・・・・・・・・・${color_normal} $@"
    exit 1
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
    message "Done ・ Compiling static openssl"
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
    message "Done ・ Compiling static lz4"
}

make_jemalloc()
{
    message "Compiling jemalloc 5.3"
    curl -Ls https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2 -o jemalloc.tar.bz2 && \
    echo "2db82d1e7119df3e71b7640219b6dfe84789bc0537983c3b7ac4f7189aecfeaa  jemalloc.tar.bz2" > jemalloc-sha.txt && \
    sha256sum --quiet -c jemalloc-sha.txt && \
    mkdir jemalloc && \
    tar --strip-components 1 --no-same-owner --no-same-permissions --directory jemalloc -xjf jemalloc.tar.bz2 && \
    cd jemalloc && \
    ./configure --enable-static --disable-cxx --enable-prof && \
    make && \
    make install && \
    cd .. && \
    rm -rf /tmp/*
    message "Done ・ Compiling jemalloc 5.3"

}

install_cmake()
{
    message "Installing CMake "
    if [ "$(uname -m)" == "aarch64" ]; then \
        CMAKE_SHA256="4a750db7becfa426a37f702fa87267e836dda6720f2b768e31828f4cb5e2e24b"; \
    else \
        CMAKE_SHA256="b1dfd11d50e2dfb3d18be86ca1a369da1c1131badc14b659491dd42be1fed704"; \
    fi && \
    curl -Ls https://github.com/Kitware/CMake/releases/download/v3.26.1/cmake-3.26.1-$(uname -s)-$(uname -m).tar.gz -o cmake.tar.gz && \
    echo "${CMAKE_SHA256}  cmake.tar.gz" > cmake-sha.txt && \
    sha256sum --quiet -c cmake-sha.txt && \
    mkdir cmake && \
    tar --strip-components 1 --no-same-owner --directory cmake -xf cmake.tar.gz && \
    cp -rf cmake/* /usr/ && \
    rm -rf /tmp/*
    message "Done ・ Installing CMake "

}


install_clang()
{
    message "Installing clang"
    OS_ID=$1
    if [[ ${OS_ID} == 'ubuntu' ]]; then
        CLANG_REPO_URL="deb http://apt.llvm.org/focal/ llvm-toolchain-focal-${CLANG_VERSION}"
    elif [[ ${OS_ID} == 'debian' ]]; then
        CLANG_REPO_URL="deb http://apt.llvm.org/bullseye/ llvm-toolchain-bullseye-${CLANG_VERSION}"
    else
        error "Clang if not avaliable of ${OS_ID}"
    fi

    apt update && apt install -y gnupg wget \
        && echo "${CLANG_REPO_URL} main" >>  /etc/apt/sources.list \
        && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && apt update && apt install -y clang-${CLANG_VERSION}

     update-alternatives --install /usr/bin/clang clang /usr/bin/clang-${CLANG_VERSION} 100 --slave /usr/bin/clang++ clang++ /usr/bin/clang++-${CLANG_VERSION} \
     && update-alternatives --install /usr/bin/cc cc /usr/bin/clang 100 \
     && update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++ 100
    export CC=/usr/bin/clang; export CXX=/usr/bin/clang++

    message "Installing clang done "
}


if [[ ${ID} == 'ubuntu' || ${ID} == 'debian' ]]; then
    message "Preparing dev requirements for ubuntu|debian"
    GENERATOR='DEB'
    PACKAGES_TYPE='deb'
    PKG_MANAGER='dpkg -i'
    PACKAGES_SUFFIX="-DDEB=${VERSION_CODENAME}"
    print_env
    ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
    DEBIAN_FRONTEND=noninteractive apt install -y -qq automake cmake curl file g++ gcc git jq libjemalloc-dev libssl-dev mono-devel patch python3-dev python3-venv unzip

    if [[ ${ID} == 'ubuntu' && ${VERSION_ID} == '20.04' || ${ID} == 'debian' && ${VERSION_ID} == '11' ]]; then
        install_clang ${ID}
    fi

elif [[ ${ID} == "rocky" ]]; then
    PKG_MANAGER='yum install -y'
    OS_SHORTCUT=$(echo $PLATFORM_ID | cut -f2 -d ':')
    PACKAGES_SUFFIX="-DRPM=${OS_SHORTCUT}"
    PACKAGES_TYPE='rpm'
    GENERATOR='RPM'

    dnf -y update
    dnf install -y -q ncurses
    #dnf install -y -q python3 python3-pip

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

    dnf install -y -q --allowerasing automake cmake curl dnf gcc git jemalloc-devel jq mono-devel patch perl python3-devel python3-pip rpm-build unzip
    pip3 install --upgrade pip
    pip3 install --user virtualenv
    make_openssl

    OPENSSL_FLAGS=' -DOPENSSL_ROOT_DIR=/usr/local/openssl/ '

else
    echo "Unsupported distribution. This script only supports Rocky[8|9], Ubuntu [20.04|22.04|24.04] Debian[11|12]"
fi

install_cmake
make_lz4
make_jemalloc

cd /

message "Downloading sources"
wget https://github.com/apple/foundationdb/archive/refs/tags/${FDB_VERSION}.zip
unzip -q ${FDB_VERSION}.zip


message "Patching sources"
cd foundationdb-${FDB_VERSION}
patch -p1 -i ${SCRIPT_LOCATION}/mariadb_foundationdb-${FDB_VERSION}_gcc.patch
cd -


message "Configuring cmake"
mkdir -p fdb_build
cd fdb_build

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


message "Compiling actorcompiler"
${BUILD_COMMAND} actorcompiler

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
