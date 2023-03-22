#!/bin/bash

#TODO pass deb type as argumant, see CMAKEFLAGS
#TODO pass platform as argument


SCRIPT_LOCATION=$(dirname "$0")
MDB_SOURCE_PATH=$(realpath $SCRIPT_LOCATION/../../../..)
CURRENT_DIR=`pwd`

cd $MDB_SOURCE_PATH

sed -i 's|.*-d storage/columnstore.*|elif [[ -d storage/columnstore/columnstore/debian ]]|' debian/autobake-deb.sh
sed -i 's/mariadb-server/mariadb-server-10.6/' storage/columnstore/columnstore/debian/control
sed -i '/-DPLUGIN_COLUMNSTORE=NO/d' debian/rules

# Disable dh_missing strict check for missing files
sed -i s/--fail-missing/--list-missing/ debian/rules

#Tweak debian packaging stuff
for i in mariadb-backup mariadb-plugin libmariadbd;
    do sed -i "/Package: $i.*/,/^$/d" debian/control;
done

sed -i 's/Depends: galera.*/Depends:/' debian/control
for i in galera wsrep ha_sphinx embedded; do
    sed -i /$i/d debian/*.install;
done

apt-cache madison liburing-dev | grep liburing-dev || \
    sed 's/liburing-dev/libaio-dev/g' -i debian/control \
    && sed '/-DIGNORE_AIO_CHECK=YES/d' -i debian/rules  \
    && sed '/-DWITH_URING=yes/d' -i debian/rules \
    && apt-cache madison libpmem-dev | grep 'libpmem-dev' || \
    sed '/libpmem-dev/d' -i debian/control \
    && sed '/-DWITH_PMEM/d' -i debian/rules \
    && sed '/libfmt-dev/d' -i debian/control

apt-get -y update

apt install -y lto-disabled-list && \
    for i in mariadb-plugin-columnstore mariadb-server mariadb-server-core mariadb mariadb-10.6; do
        echo "$i any" >> /usr/share/lto-disabled-list/lto-disabled-list;
    done \
    && grep mariadb /usr/share/lto-disabled-list/lto-disabled-list

apt install --yes libasan6 build-essential automake libboost-all-dev \
    bison cmake libncurses5-dev libaio-dev libsystemd-dev libpcre2-dev \
    libperl-dev libssl-dev libxml2-dev libkrb5-dev flex libpam-dev git \
    libsnappy-dev libcurl4-openssl-dev libgtest-dev libcppunit-dev googletest \
    libsnappy-dev libjemalloc-dev liblz-dev liblzo2-dev liblzma-dev liblz4-dev \
    libbz2-dev libbenchmark-dev libdistro-info-perl
apt install --yes --no-install-recommends build-essential devscripts git ccache equivs eatmydata libssl-dev
mk-build-deps debian/control -t 'apt-get -y -o Debug::pkgProblemResolver=yes --no-install-recommends' -r -i

export DEBIAN_FRONTEND='noninteractive',
export DEB_BUILD_OPTIONS='parallel=20'
export DH_BUILD_DDEBS='1'
export BUILDPACKAGE_FLAGS='-b'  # Save time and produce only binary packages, not source

rm -rf CMakeCache.txt CMakeFiles

sudo -E CMAKEFLAGS='-DCMAKE_BUILD_TYPE=RelWithDebInfo
            -DBUILD_CONFIG=mysql_release
            -DCMAKE_C_COMPILER_LAUNCHER=sccache
            -DCMAKE_CXX_COMPILER_LAUNCHER=sccache
            -DPLUGIN_COLUMNSTORE=YES
            -DWITH_UNITTESTS=YES
            -DPLUGIN_MROONGA=NO
            -DPLUGIN_ROCKSDB=NO
            -DPLUGIN_TOKUDB=NO
            -DPLUGIN_CONNECT=NO
            -DPLUGIN_OQGRAPH=NO
            -DPLUGIN_GSSAPI=NO
            -DPLUGIN_SPIDER=NO
            -DPLUGIN_SPHINX=NO
            -DWITH_EMBEDDED_SERVER=NO
            -DWITH_WSREP=NO
            -DWITH_COREDUMPS=ON
            -DWITH_ASAN=ON
            -DWITH_COLUMNSTORE_ASAN=ON
            -DWITH_COLUMNSTORE_REPORT_PATH=/core
            -DCMAKE_VERBOSE_MAKEFILE=ON
            -DDEB=jammy' debian/autobake-deb.sh> >(tee -a $CURRENT_DIR/build.log) 2> >(tee -a $CURRENT_DIR/build.err >&2)