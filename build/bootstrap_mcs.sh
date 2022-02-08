#!/bin/sh

# This script compiles/installs MCS from scratch and it makes some assumptions:
# - the server's source code is two directories above the MCS engine source.
# - the script is to be run under root.
# - it is run MDB_MDB_SOURCE_PATH/storage/columnstore/columnstore, e.g. ./build/bootstrap_mcs.sh

DISTRO=$1
MDB_BUILD_TYPE=$2
MDB_SOURCE_PATH=`pwd`/../../../
MCS_CONFIG_DIR=/etc/columnstore
# Needs systemd to be installed obviously.
# Feel free to ask in MariaDB Zulip how to bootstrap MCS in containers or other systemd-free environments.
systemctl stop mariadb-columnstore
MDB_GIT_URL=https://github.com/MariaDB/server.git
MDB_GIT_TAG=10.8

if [ -z "$DISTRO" ]; then
    echo "Choose a distro"
    exit 1
fi

if [ $DISTRO = 'bionic' ]; then
    sudo apt-get -y update
    apt-get -y install build-essential automake libboost-all-dev bison cmake \
    libncurses5-dev libaio-dev libsystemd-dev libpcre2-dev \
    libperl-dev libssl-dev libxml2-dev libkrb5-dev flex libpam-dev git \
    libsnappy-dev libcurl4-openssl-dev
elif [ $DISTRO = 'focal' ]; then
    sudo apt-get -y update
    apt-get -y install build-essential automake libboost-all-dev bison cmake \
    libncurses5-dev libaio-dev libsystemd-dev libpcre2-dev \
    libperl-dev libssl-dev libxml2-dev libkrb5-dev flex libpam-dev git \
    libsnappy-dev libcurl4-openssl-dev
elif [ $DISTRO = 'centos' ]; then
    yum -y install epel-release \
    && yum -y groupinstall "Development Tools" \
    && yum -y install bison ncurses-devel readline-devel perl-devel openssl-devel cmake libxml2-devel gperf libaio-devel libevent-devel python-devel ruby-devel tree wget pam-devel snappy-devel libicu \
    && yum -y install vim wget strace ltrace gdb  rsyslog net-tools openssh-server expect boost perl-DBI libicu boost-devel initscripts jemalloc-devel libcurl-devel
elif [ $DISTRO = 'leap' ]; then
    zypper install -y bison ncurses-devel readline-devel libopenssl-devel cmake libxml2-devel gperf libaio-devel libevent-devel python-devel ruby-devel tree wget pam-devel snappy-devel libicu-devel \
    && zypper install -y libboost_system-devel libboost_filesystem-devel libboost_thread-devel libboost_regex-devel libboost_date_time-devel libboost_chrono-devel \
    && zypper install -y vim wget strace ltrace gdb  rsyslog net-tools expect perl-DBI libicu boost-devel jemalloc-devel libcurl-devel \
    && zypper install -y gcc gcc-c++ git automake libtool
fi

if [ ! -d $MDB_SOURCE_PATH ]; then
    git clone $MDB_GIT_URL $MDB_SOURCE_PATH -b $MDB_GIT_TAG
fi

if [ ! -d $MCS_CONFIG_DIR ]; then
    mkdir $MCS_CONFIG_DIR
fi

if [ -z "$(grep mysql /etc/passwd)" ]; then
    echo "Adding user mysql into /etc/passwd"
    useradd -r -U mysql -d /var/lib/mysql
    exit 1
fi

if [ -z "$(grep mysql /etc/group)" ]; then
    echo "You need to manually add mysql group into /etc/group, e.g. mysql:x:999"
    exit 1
fi

MCS_INSTALL_PREFIX=/var/lib/
rm -rf /var/lib/columnstore/data1/*
rm -rf /var/lib/columnstore/data/
rm -rf /var/lib/columnstore/local/
rm -f /var/lib/columnstore/storagemanager/storagemanager-lock
rm -f /var/lib/columnstore/storagemanager/cs-initialized

MCS_TMP_DIR=/tmp/columnstore_tmp_files
TMP_PATH=/tmp
CPUS=$(getconf _NPROCESSORS_ONLN)

# script
rm -rf $MCS_TMP_DIR/*
rm -rf /var/lib/mysql

cd $MDB_SOURCE_PATH
if [[ "$DISTRO" = 'bionic' || "$DISTRO" = 'focal' ]]; then
    #MDB_CMAKE_FLAGS='-DWITH_SYSTEMD=yes -DPLUGIN_TOKUDB=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_MROONGA=NO -DPLUGIN_GSSAPI=NO -DWITH_MARIABACKUP=NO -DDEB=bionic -DPLUGIN_COLUMNSTORE=YES'
    MDB_CMAKE_FLAGS='-DWITH_SYSTEMD=yes -DPLUGIN_COLUMNSTORE=YES -DPLUGIN_MROONGA=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_CONNECT=NO -DPLUGIN_SPIDER=NO -DPLUGIN_OQGRAPH=NO -DPLUGIN_SPHINX=NO -DBUILD_CONFIG=mysql_release -DWITH_WSREP=OFF -DWITH_SSL=system -DDEB=bionic'
    # Some development flags
    #MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DWITH_GTEST=1 -DWITH_ROWGROUP_UT=1 -DWITH_DATACONVERT_UT=1 -DWITH_ARITHMETICOPERATOR_UT=1 -DWITH_ORDERBY_UT=1 -DWITH_CSDECIMAL_UT=1 -DWITH_SORTING_COMPARATORS_UT=1"
    MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DWITH_MICROBENCHMARKS=YES -DWITH_BRM_UT=YES" #-DWITH_GTEST=1 -DWITH_UNITTESTS=YES
    cmake . -DCMAKE_BUILD_TYPE=$MDB_BUILD_TYPE ${MDB_CMAKE_FLAGS} && \
    make -j $CPUS install
elif [ $DISTRO = 'centos' ]; then
    MDB_CMAKE_FLAGS='-DWITH_SYSTEMD=yes -DPLUGIN_AUTH_GSSAPI=NO -DPLUGIN_COLUMNSTORE=YES -DPLUGIN_MROONGA=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_CONNECT=NO -DPLUGIN_SPIDER=NO -DPLUGIN_OQGRAPH=NO -DPLUGIN_SPHINX=NO -DBUILD_CONFIG=mysql_release -DWITH_WSREP=OFF -DWITH_SSL=system -DRPM=CentOS7'
    cmake . -DCMAKE_BUILD_TYPE=$MDB_BUILD_TYPE $MDB_CMAKE_FLAGS && \
    make -j $CPUS install
fi

if [ $? -ne 0 ]; then
    return 1
fi

# These two lines are to handle file layout difference b/w RPM- and DEB-based distributions.
# One of the lines always fails.
mv /usr/lib/mysql/plugin/ha_columnstore.so /tmp/ha_columnstore_1.so
mv /usr/lib64/mysql/plugin/ha_columnstore.so /tmp/ha_columnstore_2.so


/usr/bin/mysql_install_db --rpm --user=mysql

mv /tmp/ha_columnstore_1.so /usr/lib/mysql/plugin/ha_columnstore.so
mv /tmp/ha_columnstore_2.so /usr/lib64/mysql/plugin/ha_columnstore.so

cp -r /etc/mysql/conf.d /etc/my.cnf.d/
cp $MDB_SOURCE_PATH/storage/columnstore/columnstore/oam/etc/Columnstore.xml /etc/columnstore/Columnstore.xml
cp $MDB_SOURCE_PATH/storage/columnstore/oam/etc/Columnstore.xml /etc/columnstore/Columnstore.xml
cp $MDB_SOURCE_PATH/storage/columnstore/columnstore/storage-manager/storagemanager.cnf /etc/columnstore/storagemanager.cnf
cp $MDB_SOURCE_PATH/storage/columnstore/storage-manager/storagemanager.cnf /etc/columnstore/storagemanager.cnf

if [[ "$DISTRO" = 'bionic' || "$DISTRO" = 'focal' ]]; then
    cp ./support-files/*.service /lib/systemd/system/
    cp ./storage/columnstore/columnstore/oam/install_scripts/*.service /lib/systemd/system/
    cp ./storage/columnstore/oam/install_scripts/*.service /lib/systemd/system/
    cp ./debian/additions/debian-start.inc.sh /usr/share/mysql/debian-start.inc.sh
    cp ./debian/additions/debian-start /etc/mysql/debian-start
    systemctl daemon-reload
    rm -f /etc/mysql/my.cnf
    cp -r /etc/mysql/conf.d/ /etc/my.cnf.d
    cp -rp /etc/mysql/mariadb.conf.d/ /etc/my.cnf.d
    mkdir /var/lib/columnstore/data1
    mkdir /var/lib/columnstore/data1/systemFiles
    mkdir /var/lib/columnstore/data1/systemFiles/dbrm
    chown -R mysql:mysql /var/lib/mysql
    chown -R mysql:mysql /var/lib/mysql
    chown -R mysql.mysql /var/run/mysqld 
    chown -R mysql:mysql /data/columnstore/*
    chmod +x /usr/bin/mariadb*
    cp /etc/my.cnf.d/mariadb.conf.d/columnstore.cnf /etc/my.cnf.d/

    ldconfig    
    columnstore-post-install
    chown -R mysql:mysql /data/columnstore/*

    /usr/sbin/install_mcs_mysql.sh

    chown -R syslog.syslog /var/log/mariadb/ 
    chmod 777 /var/log/mariadb/ 
    chmod 777 /var/log/mariadb/columnstore 

elif [ $DISTRO = 'centos' ]; then
    cp ./support-files/*.service /lib/systemd/system/
    cp ./storage/columnstore/columnstore/oam/install_scripts/*.service /lib/systemd/system/
    cp ./storage/columnstore/oam/install_scripts/*.service /lib/systemd/system/
    systemctl daemon-reload
    rm -f /etc/mysql/my.cnf
    cp -r /etc/mysql/conf.d/ /etc/my.cnf.d
    mkdir /var/lib/columnstore/data1
    mkdir /var/lib/columnstore/data1/systemFiles
    mkdir /var/lib/columnstore/data1/systemFiles/dbrm
    chown -R mysql:mysql /var/lib/mysql
    chown -R mysql:mysql /data/columnstore/*
    chown -R mysql.mysql /var/run/mysqld 
    chmod +x /usr/bin/mariadb*

    ldconfig 
    columnstore-post-install
    chown -R mysql:mysql /data/columnstore/*

    /usr/sbin/install_mcs_mysql.sh

    /usr/sbin/install_mcs_mysql.sh
    mkdir /var/lib/columnstore/data1
    mkdir /var/lib/columnstore/data1/systemFiles
    mkdir /var/lib/columnstore/data1/systemFiles/dbrm

    chown -R mysql:mysql /var/log/mariadb/ 
    chmod 777 /var/log/mariadb/ 
    chmod 777 /var/log/mariadb/columnstore 
fi


exit 0 
