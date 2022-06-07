#!/bin/bash

# This script compiles/installs MCS from scratch and it makes some assumptions:
# - the server's source code is two directories above the MCS engine source.
# - the script is to be run under root.



SCRIPT_LOCATION=$(dirname "$0")
MDB_SOURCE_PATH=$(realpath $SCRIPT_LOCATION/../../../..)

source $SCRIPT_LOCATION/utils.sh


if [ "$EUID" -ne 0 ]
    then error "Please run script as root to install MariaDb to system paths"
    exit 1
fi

message "Building Mariadb Server from $color_yellow$MDB_SOURCE_PATH$color_normal"

BUILD_TYPE_OPTIONS=("Debug" "RelWithDebInfo")
DISTRO_OPTIONS=("Ubuntu" "CentOS" "Debian" "openSUSE" "Rocky")
BRANCHES=($(git branch --list --no-color| grep "[^* ]+" -Eo))

optparse.define short=t long=build-type desc="Build Type: ${BUILD_TYPE_OPTIONS[*]}" variable=MCS_BUILD_TYPE
optparse.define short=d long=distro desc="Choouse your OS: ${DISTRO_OPTIONS[*]}" variable=OS
optparse.define short=s long=skip-deps desc="Skip install dependences" variable=SKIP_DEPS default=false value=true
optparse.define short=C long=force-cmake-reconfig desc="Force cmake reconfigure" variable=FORCE_CMAKE_CONFIG default=false value=true
optparse.define short=S long=skip-columnstore-submodules desc="Skip columnstore submodules initialization" variable=SKIP_SUBMODULES default=false value=true
optparse.define short=b long=branch desc="Choouse git branch ('none' for menu)" variable=BRANCH

source $( optparse.build )

if [[ ! " ${BUILD_TYPE_OPTIONS[*]} " =~ " ${MCS_BUILD_TYPE} " ]]; then
    getChoice -q "Select your Build Type" -o BUILD_TYPE_OPTIONS
    MCS_BUILD_TYPE=$selectedChoice
fi

if [[ ! " ${DISTRO_OPTIONS[*]} " =~ " ${OS} " || $OS = "CentOS" ]]; then
    detect_distro
fi

INSTALL_PREFIX="/usr/"
DATA_DIR="/var/lib/mysql/data"
CMAKE_BIN_NAME=cmake

select_branch()
{
    if [[ ! " ${BRANCHES[*]} " =~ " ${BRANCH} " ]]; then
        if [[ $BRANCH = 'none' ]]; then
            getChoice -q "Select your branch" -o BRANCHES
            BRANCH=$selectedChoice
        fi
        cd $SCRIPT_LOCATION
        message "Selecting $BRANCH branch for Columnstore"
        git checkout $BRANCH
        cd -

        message "Turning off Columnstore submodule auto update via gitconfig"
        cd $MDB_SOURCE_PATH
        git config submodule.storage/columnstore/columnstore.update none
        cd -
    fi

    cd $SCRIPT_LOCATION
    CURRENT_BRANCH=$(git branch --show-current)
    cd -
    message "Columnstore will be built from $color_yellow$CURRENT_BRANCH$color_normal branch"
}

install_deps()
{
    message "Installing deps"
    if [[ $OS = 'Ubuntu' || $OS = 'Debian' ]]; then
        sudo apt-get -y update
        apt-get -y install build-essential automake libboost-all-dev bison cmake \
        libncurses5-dev libaio-dev libsystemd-dev libpcre2-dev \
        libperl-dev libssl-dev libxml2-dev libkrb5-dev flex libpam-dev git \
        libsnappy-dev libcurl4-openssl-dev libgtest-dev libcppunit-dev googletest libsnappy-dev libjemalloc-dev
    elif [[ $OS = 'CentOS' || $OS = 'Rocky' ]]; then
        yum -y install epel-release \
        && yum -y groupinstall "Development Tools" \
	&& yum config-manager --set-enabled powertools \
        && yum -y install bison ncurses-devel readline-devel perl-devel openssl-devel libxml2-devel gperf libaio-devel libevent-devel tree wget pam-devel snappy-devel libicu \
        && yum -y install vim wget strace ltrace gdb  rsyslog net-tools openssh-server expect boost perl-DBI libicu boost-devel initscripts jemalloc-devel libcurl-devel gtest-devel cppunit-devel systemd-devel
        if [[ "$OS_VERSION" == "7" ]]; then
            yum -y install cmake3
            CMAKE_BIN_NAME=cmake3
        else
            yum -y install cmake
        fi
        if [ $OS = 'Rocky' ]; then
	    yum install -y checkpolicy
        fi
    elif [ $OS = 'openSUSE' ]; then
        zypper install -y bison ncurses-devel readline-devel libopenssl-devel cmake libxml2-devel gperf libaio-devel libevent-devel python-devel ruby-devel tree wget pam-devel snappy-devel libicu-devel \
        && zypper install -y libboost_system-devel libboost_filesystem-devel libboost_thread-devel libboost_regex-devel libboost_date_time-devel libboost_chrono-devel libboost_atomic-devel \
        && zypper install -y vim wget strace ltrace gdb  rsyslog net-tools expect perl-DBI libicu boost-devel jemalloc-devel libcurl-devel liblz4-devel lz4 \
        && zypper install -y gcc gcc-c++ git automake libtool gtest cppunit-devel pcre2-devel systemd-devel libaio-devel snappy-devel cracklib-devel policycoreutils-devel ncurses-devel \
        && zypper install -y make flex libcurl-devel  automake libtool rpm-build lsof iproute pam-devel perl-DBI expect createrepo
    fi
}

stop_service()
{
    message "Stopping MariaDB services"
    systemctl stop mariadb
    systemctl stop mariadb-columnstore
}

check_service()
{
    if systemctl is-active --quiet $1; then
        message "$1 service started$color_green OK $color_normal"
    else
        error "$1 service failed"
        service $1 status
    fi
}

start_service()
{
    message "Starting MariaDB services"
    systemctl start mariadb-columnstore
    systemctl start mariadb

    check_service mariadb-columnstore
    check_service mariadb
}

clean_old_installation()
{
    message "Cleaning old installation"
    rm -rf /var/lib/columnstore/data1/*
    rm -rf /var/lib/columnstore/data/
    rm -rf /var/lib/columnstore/local/
    rm -f /var/lib/columnstore/storagemanager/storagemanager-lock
    rm -f /var/lib/columnstore/storagemanager/cs-initialized
    rm -rf /tmp/*
    rm -rf /var/lib/mysql
    rm -rf /var/run/mysqld
    rm -rf $DATA_DIR
    rm -rf /etc/mysql
    rm -r /etc/my.cnf.d/provider_* 
}

build()
{
    message "Building sources in $color_yellow$MCS_BUILD_TYPE$color_normal mode"

    local MDB_CMAKE_FLAGS="-DWITH_SYSTEMD=yes
                     -DPLUGIN_COLUMNSTORE=YES
                     -DPLUGIN_MROONGA=NO
                     -DPLUGIN_ROCKSDB=NO
                     -DPLUGIN_TOKUDB=NO
                     -DPLUGIN_CONNECT=NO
                     -DPLUGIN_SPIDER=NO
                     -DPLUGIN_OQGRAPH=NO
                     -DPLUGIN_SPHINX=NO
                     -DWITH_EMBEDDED_SERVER=OFF
                     -DBUILD_CONFIG=mysql_release
                     -DWITH_WSREP=OFF
                     -DWITH_SSL=system
                     -DWITH_UNITTESTS=YES
                     -DWITH_BRM_UT=YES
                     -DCMAKE_INSTALL_PREFIX:PATH=$INSTALL_PREFIX"

    cd $MDB_SOURCE_PATH

    if [[ $SKIP_SUBMODULES = true ]] ; then
        warn "Skipping initialization of columnstore submodules"
    else
	    message "Initialization of columnstore submodules"
	    cd storage/columnstore/columnstore
	    git submodule update --init
	    cd -
    fi

    if [[ $FORCE_CMAKE_CONFIG = true ]] ; then
        warn "Erasing cmake cache"
        rm -f "$MDB_SOURCE_PATH/CMakeCache.txt"
        rm -rf "$MDB_SOURCE_PATH/CMakeFiles"
    fi

    if [[ "$OS" = 'Ubuntu' || "$OS" = 'Debian' ]]; then
        MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DDEB=bionic"
    elif [ $OS = 'CentOS' ]; then
        MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DRPM=CentOS7"
    elif [ $OS = 'Rocky' ]; then
        MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DRPM=CentOS7"
    elif [ $OS = 'openSUSE' ]; then
        MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DRPM=sles15"
    fi

    message "building with flags $MDB_CMAKE_FLAGS"

    local CPUS=$(getconf _NPROCESSORS_ONLN)
    ${CMAKE_BIN_NAME} . -DCMAKE_BUILD_TYPE=$MCS_BUILD_TYPE $MDB_CMAKE_FLAGS && \
    make -j $CPUS install

    if [ $? -ne 0 ]; then
        error "!!!! BUILD FAILED"
        exit 1
    fi
    cd -
}


check_user_and_group()
{
    if [ -z "$(grep mysql /etc/passwd)" ]; then
        message "Adding user mysql into /etc/passwd"
        useradd -r -U mysql -d /var/lib/mysql
    fi

    if [ -z "$(grep mysql /etc/group)" ]; then
        GroupID = `awk -F: '{uid[$3]=1}END{for(x=100; x<=999; x++) {if(uid[x] != ""){}else{print x; exit;}}}' /etc/group`
        message "Adding group mysql with id $GroupID"
        groupadd -g GroupID mysql
    fi
}

install()
{
    message "Installing MariaDB"

    check_user_and_group

    mkdir -p /etc/my.cnf.d

    bash -c 'echo "[client-server]
socket=/run/mysqld/mysqld.sock" > /etc/my.cnf.d/socket.cnf'

    mv $INSTALL_PREFIX/lib/mysql/plugin/ha_columnstore.so /tmp/ha_columnstore_1.so || mv $INSTALL_PREFIX/lib64/mysql/plugin/ha_columnstore.so /tmp/ha_columnstore_2.so
    message "Running mysql_install_db"
    mysql_install_db --rpm --user=mysql
    mv /tmp/ha_columnstore_1.so $INSTALL_PREFIX/lib/mysql/plugin/ha_columnstore.so || mv /tmp/ha_columnstore_2.so $INSTALL_PREFIX/lib64/mysql/plugin/ha_columnstore.so

    mkdir -p /etc/columnstore

    cp $MDB_SOURCE_PATH/storage/columnstore/columnstore/oam/etc/Columnstore.xml /etc/columnstore/Columnstore.xml
    cp $MDB_SOURCE_PATH/storage/columnstore/columnstore/storage-manager/storagemanager.cnf /etc/columnstore/storagemanager.cnf

    cp $MDB_SOURCE_PATH/support-files/*.service /lib/systemd/system/
    cp $MDB_SOURCE_PATH/storage/columnstore/columnstore/oam/install_scripts/*.service /lib/systemd/system/

    if [[ "$OS" = 'Ubuntu' || "$OS" = 'Debian' ]]; then
        mkdir -p /usr/share/mysql
        mkdir -p /etc/mysql/
        cp $MDB_SOURCE_PATH/debian/additions/debian-start.inc.sh /usr/share/mysql/debian-start.inc.sh
        cp $MDB_SOURCE_PATH/debian/additions/debian-start /etc/mysql/debian-start
        > /etc/mysql/debian.cnf
    fi

    systemctl daemon-reload

    if [ -d "/etc/mysql/mariadb.conf.d/" ]; then
        message "Copying configs from /etc/mysql/mariadb.conf.d/ to /etc/my.cnf.d"
        cp -rp /etc/mysql/mariadb.conf.d/* /etc/my.cnf.d
    fi

    if [ -d "/etc/mysql/conf.d/" ]; then
        message "Copying configs from /etc/mysql/conf.d/ to /etc/my.cnf.d"
        cp -rp /etc/mysql/conf.d/* /etc/my.cnf.d
    fi

    mkdir -p /var/lib/columnstore/data1
    mkdir -p /var/lib/columnstore/data1/systemFiles
    mkdir -p /var/lib/columnstore/data1/systemFiles/dbrm
    mkdir -p /run/mysqld/

    mkdir -p $DATA_DIR
    chown -R mysql:mysql $DATA_DIR
    chown -R mysql:mysql /var/lib/columnstore/
    chown -R mysql:mysql /run/mysqld/

    chmod +x $INSTALL_PREFIX/bin/mariadb*

    ldconfig

    message "Running columnstore-post-install"
    mkdir -p /var/lib/columnstore/local
    columnstore-post-install --rpmmode=install
    message "Running install_mcs_mysql"
    install_mcs_mysql.sh

    chown -R syslog:syslog /var/log/mariadb/
    chmod 777 /var/log/mariadb/
    chmod 777 /var/log/mariadb/columnstore
}


select_branch

if [[ $SKIP_DEPS = false ]] ; then
    install_deps
fi

stop_service
clean_old_installation
build
install
start_service
message "$color_green FINISHED $color_normal"
