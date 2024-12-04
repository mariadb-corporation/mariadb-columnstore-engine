#!/bin/bash

# This script compiles/installs MCS from scratch and it makes some assumptions:
# - the server's source code is two directories above the MCS engine source.
# - the script is to be run under root.

set -o pipefail
SCRIPT_LOCATION=$(dirname "$0")
MDB_SOURCE_PATH=$(realpath $SCRIPT_LOCATION/../../../..)

source $SCRIPT_LOCATION/utils.sh

if [ "$EUID" -ne 0 ]
    then error "Please run script as root to install MariaDb to system paths"
    exit 1
fi

message "Building Mariadb Server from $color_yellow$MDB_SOURCE_PATH$color_normal"

BUILD_TYPE_OPTIONS=("Debug" "RelWithDebInfo")
DISTRO_OPTIONS=("Ubuntu" "CentOS" "Debian" "Rocky")

cd $SCRIPT_LOCATION
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
BRANCHES=($(git branch --list --no-color| grep "[^* ]+" -Eo))
cd - > /dev/null


optparse.define short=t long=build-type desc="Build Type: ${BUILD_TYPE_OPTIONS[*]}" variable=MCS_BUILD_TYPE
optparse.define short=d long=distro desc="Choose your OS: ${DISTRO_OPTIONS[*]}" variable=OS
optparse.define short=D long=install-deps desc="Install dependences" variable=INSTALL_DEPS default=false value=true
optparse.define short=C long=force-cmake-reconfig desc="Force cmake reconfigure" variable=FORCE_CMAKE_CONFIG default=false value=true
optparse.define short=S long=skip-columnstore-submodules desc="Skip columnstore submodules initialization" variable=SKIP_SUBMODULES default=false value=true
optparse.define short=u long=skip-unit-tests desc="Skip UnitTests" variable=SKIP_UNIT_TESTS default=false value=true
optparse.define short=B long=run-microbench="Compile and run microbenchmarks " variable=RUN_BENCHMARKS default=false value=true
optparse.define short=b long=branch desc="Choose git branch. For menu use -b \"\"" variable=BRANCH default=$CURRENT_BRANCH
optparse.define short=W long=without-core-dumps desc="Do not produce core dumps" variable=WITHOUT_COREDUMPS default=false value=true
optparse.define short=v long=verbose desc="Verbose makefile commands" variable=MAKEFILE_VERBOSE default=false value=true
optparse.define short=A long=asan desc="Build with ASAN" variable=ASAN default=false value=true
optparse.define short=T long=tsan desc="Build with TSAN" variable=TSAN default=false value=true
optparse.define short=U long=ubsan desc="Build with UBSAN" variable=UBSAN default=false value=true
optparse.define short=P long=report-path desc="Path for storing reports and profiles" variable=REPORT_PATH default="/core"
optparse.define short=N long=ninja desc="Build with ninja" variable=USE_NINJA default=false value=true
optparse.define short=G long=draw-deps desc="Draw dependencies graph" variable=DRAW_DEPS default=false value=true
optparse.define short=M long=skip-smoke desc="Skip final smoke test" variable=SKIP_SMOKE default=false value=true
optparse.define short=n long=no-clean-install desc="Do not perform a clean install (keep existing db files)" variable=NO_CLEAN default=false value=true
optparse.define short=j long=parallel desc="Number of paralles for build" variable=CPUS default=$(getconf _NPROCESSORS_ONLN)
optparse.define short=F long=show-build-flags desc="Print CMake flags, while build" variable=PRINT_CMAKE_FLAGS default=false value=true
optparse.define short=c long=cloud desc="Enable cloud storage" variable=CLOUD_STORAGE_ENABLED default=false value=true
optparse.define short=f long=do-not-freeze-revision desc="Disable revision freezing, or do not set 'update none' for columnstore submodule in MDB repository" variable=DO_NOT_FREEZE_REVISION default=false value=true
optparse.define short=a long=build-path variable=MARIA_BUILD_PATH default=$MDB_SOURCE_PATH/../MariaDBBuild
optparse.define short=o long=recompile-only variable=RECOMPILE_ONLY default=false value=true
optparse.define short=r long=restart-services variable=RESTART_SERVICES default=true value=false


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
CTEST_BIN_NAME=ctest
RPM_CONFIG_DIR="/etc/my.cnf.d"
DEB_CONFIG_DIR="/etc/mysql/mariadb.conf.d"
CONFIG_DIR=$RPM_CONFIG_DIR

if [[ $OS = 'Ubuntu' || $OS = 'Debian' ]]; then
    CONFIG_DIR=$DEB_CONFIG_DIR
fi

export CLICOLOR_FORCE=1


disable_git_restore_frozen_revision()
{
    cd $MDB_SOURCE_PATH
    git config submodule.storage/columnstore/columnstore.update none
    cd - > /dev/null
}

select_branch()
{
    cd $SCRIPT_LOCATION
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

    if [[ ! " ${BRANCHES[*]} " =~ " ${BRANCH} " ]]; then
        if [[ $BRANCH = "" ]]; then
            getChoice -q "Select your branch" -o BRANCHES
            BRANCH=$selectedChoice
        fi
        if [[ $BRANCH != $CURRENT_BRANCH ]]; then
            message "Selecting $BRANCH branch for Columnstore"
            git checkout $BRANCH
        fi

        message "Turning off Columnstore submodule auto update via gitconfig"
    fi

    cd - > /dev/null
    message "Columnstore will be built from $color_yellow$CURRENT_BRANCH$color_cyan branch"
}

install_deps()
{
    message_split
    message "Installing deps"
    if [[ $OS = 'Ubuntu' || $OS = 'Debian' ]]; then
        apt-get -y update
        apt-get -y install build-essential automake libboost-all-dev bison cmake \
        libncurses5-dev libaio-dev libsystemd-dev libpcre2-dev \
        libperl-dev libssl-dev libxml2-dev libkrb5-dev flex libpam-dev git \
        libsnappy-dev libcurl4-openssl-dev libgtest-dev libcppunit-dev googletest libsnappy-dev libjemalloc-dev \
        liblz-dev liblzo2-dev liblzma-dev liblz4-dev libbz2-dev libbenchmark-dev graphviz

    elif [[ $OS = 'CentOS' || $OS = 'Rocky' || $OS = 'Fedora' ]]; then
        if [[ "$OS_VERSION" == "7" ]]; then
            yum -y install cmake3 epel-release centos-release-scl
            CMAKE_BIN_NAME=cmake3
            CTEST_BIN_NAME=ctest3
        else
            yum -y install cmake
        fi
        if [ $OS = 'Rocky' ]; then
            if [[ "$OS_VERSION" == "9" ]]; then
                dnf install -y 'dnf-command(config-manager)'
                dnf config-manager --set-enabled crb
            fi
           yum -y groupinstall "Development Tools" && yum config-manager --set-enabled powertools
           yum install -y checkpolicy
        fi
        if [[ $OS != 'Fedora' ]]; then
	    yum -y install epel-release
	fi

        yum install -y bison ncurses-devel readline-devel perl-devel openssl-devel libxml2-devel gperf libaio-devel libevent-devel tree wget pam-devel snappy-devel libicu \
            vim wget strace ltrace gdb rsyslog net-tools openssh-server expect boost perl-DBI libicu boost-devel initscripts \
            jemalloc-devel libcurl-devel gtest-devel cppunit-devel systemd-devel lzo-devel xz-devel lz4-devel bzip2-devel \
            pcre2-devel flex graphviz libaio-devel openssl-devel flex
    else
	error "Unsupported OS $OS"
	exit 17
    fi
}

stop_service()
{
    message_split
    message "Stopping MariaDB services"
    systemctl stop mariadb
    systemctl stop mariadb-columnstore
    systemctl stop mcs-storagemanager
}

check_service()
{
    if systemctl is-active --quiet $1; then
        message "$1 $color_normal[$color_green OK $color_normal]"
    else
        message "$1 $color_normal[$color_red Fail $color_normal]"
        service $1 status
    fi
}

start_service()
{
    message_split
    message "Starting MariaDB services"
    systemctl start mariadb-columnstore
    systemctl start mariadb

    check_service mariadb
    check_service mariadb-columnstore
    check_service mcs-controllernode
    check_service mcs-ddlproc
    check_service mcs-dmlproc
    check_service mcs-primproc
    check_service mcs-workernode@1
    check_service mcs-writeengineserver
}

start_storage_manager_if_needed()
{
  if [[ $CLOUD_STORAGE_ENABLED = true ]]; then
    export MCS_USE_S3_STORAGE=1;
    message_split
    message "Starting Storage Manager service"
    systemctl start mcs-storagemanager
    check_service mcs-storagemanager
  fi
}

clean_old_installation()
{
    message_split
    message "Cleaning old installation"
    rm -rf /var/lib/columnstore/data1/*
    rm -rf /var/lib/columnstore/data/
    rm -rf /var/lib/columnstore/local/
    rm -rf /var/lib/columnstore/storagemanager/*
    rm -rf /var/log/mariadb/columnstore/*
    rm -rf /etc/mysql/mariadb.conf.d/columnstore.cnf /etc/my.cnf.d/columnstore.cnf
    rm -rf /tmp/*
    rm -rf $REPORT_PATH
    rm -rf /var/lib/mysql
    rm -rf /var/run/mysqld
    rm -rf $DATA_DIR
    rm -rf /etc/mysql
    rm -rf /etc/my.cnf.d/columnstore.cnf
    rm -rf /etc/mysql/mariadb.conf.d/columnstore.cnf
}

build()
{
    MARIA_BUILD_PATH=$(realpath $MARIA_BUILD_PATH)
    message_split
    message "Building sources in $color_yellow$MCS_BUILD_TYPE$color_cyan mode"
    message "Compiled artifacts will be written to $color_yellow$MARIA_BUILD_PATH$color_cyan"
    mkdir -p $MARIA_BUILD_PATH

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
                     -DCMAKE_INSTALL_PREFIX:PATH=$INSTALL_PREFIX
                     -DCMAKE_EXPORT_COMPILE_COMMANDS=1
                     "


    if [[ $SKIP_UNIT_TESTS = true ]] ; then
        warn "Unittests are not build"

    else
        MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DWITH_UNITTESTS=YES"
        message "Buiding with unittests"
    fi

    if [[ $DRAW_DEPS = true ]] ; then
        warn "Generating dependendies graph to mariadb.dot"
        MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} --graphviz=mariadb.dot"
    fi

    if [[ $USE_NINJA = true ]] ; then
        warn "Using Ninja instead of Makefiles"
        MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -GNinja"
    fi

    if [[ $ASAN = true ]] ; then
        warn "Building with Address Sanitizer "
        MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DWITH_ASAN=ON -DWITH_COLUMNSTORE_ASAN=ON -DWITH_COLUMNSTORE_REPORT_PATH=${REPORT_PATH}"
    fi

    if [[ $TSAN = true ]] ; then
        warn "Building with Thread Sanitizer"
        MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DWITH_TSAN=ON -DWITH_COLUMNSTORE_REPORT_PATH=${REPORT_PATH}"
    fi

    if [[ $UBSAN = true ]] ; then
        warn "Building with UB Sanitizer"
        MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DWITH_UBSAN=ON -DWITH_COLUMNSTORE_REPORT_PATH=${REPORT_PATH}"
    fi

    if [[ $WITHOUT_COREDUMPS = true ]] ; then
        warn "Cores are not dumped"
    else
        MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DWITH_COREDUMPS=ON"
        warn Building with CoreDumps: /proc/sys/kernel/core_pattern changed to ${REPORT_PATH}/core_%e.%p
        echo "${REPORT_PATH}/core_%e.%p" > /proc/sys/kernel/core_pattern
    fi

    if [[ $MAKEFILE_VERBOSE = true ]] ; then
        warn "Verbosing Makefile Commands"
        MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DCMAKE_VERBOSE_MAKEFILE:BOOL=ON"
    fi

    if [[ $RUN_BENCHMARKS = true ]] ; then
        if [[ $MCS_BUILD_TYPE = 'Debug' ]] ; then
            error "Benchmarks will not be build in run in Debug build Mode"
            MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DWITH_MICROBENCHMARKS=NO"
            $RUN_BENCHMARKS = false
        elif [[ $OS != 'Ubuntu' && $OS != 'Debian' ]] ; then
            error "Benchmarks are now avaliable only at Ubuntu or Debian"
            MAKE_FLAGS="${MDB_CMAKE_FLAGS} -DWITH_MICROBENCHMARKS=NO"
            $RUN_BENCHMARKS = false
        else
            message "Compile with microbenchmarks"
            MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DWITH_MICROBENCHMARKS=YES"
        fi
    else
        MDB_CMAKE_FLAGS="${MDB_CMAKE_FLAGS} -DWITH_MICROBENCHMARKS=NO"
        message "Buiding without microbenchmarks"
    fi

    cd $MDB_SOURCE_PATH

    if [[ $SKIP_SUBMODULES = true ]] ; then
        warn "Skipping initialization of columnstore submodules"
    else
        message "Initialization of columnstore submodules"
        cd storage/columnstore/columnstore
        git submodule update --init
        cd - > /dev/null
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

    if [[ $PRINT_CMAKE_FLAGS = true ]] ; then
        message "Building with flags"
        newline_array ${MDB_CMAKE_FLAGS[@]}
    fi

    message "Configuring cmake silently"
    ${CMAKE_BIN_NAME} -DCMAKE_BUILD_TYPE=$MCS_BUILD_TYPE $MDB_CMAKE_FLAGS -S$MDB_SOURCE_PATH -B$MARIA_BUILD_PATH | spinner
    message_split

    ${CMAKE_BIN_NAME} --build $MARIA_BUILD_PATH -j $CPUS | onelinearizator && \
    message "Installing silently" &&
    ${CMAKE_BIN_NAME} --install $MARIA_BUILD_PATH | spinner 30

    if [ $? -ne 0 ]; then
        message_split
        error "!!!! BUILD FAILED !!!!"
        message_split
        exit 1
    fi
    cd - > /dev/null
}

check_user_and_group()
{
    user=$1
    if [ -z "$(grep $user /etc/passwd)" ]; then
        message "Adding user $user into /etc/passwd"
        useradd -r -U $user -d /var/lib/mysql
    fi

    if [ -z "$(grep $user /etc/group)" ]; then
        GroupID = `awk -F: '{uid[$3]=1}END{for(x=100; x<=999; x++) {if(uid[x] != ""){}else{print x; exit;}}}' /etc/group`
        message "Adding group $user with id $GroupID"
        groupadd -g $GroupID $user
    fi
}

run_unit_tests()
{
    message_split
    if [[ $SKIP_UNIT_TESTS = true ]] ; then
        warn "Skipping unittests"
    else
        message "Running unittests"
        cd $MARIA_BUILD_PATH
        ${CTEST_BIN_NAME} . -R columnstore: -j $(nproc) --progress --output-on-failure
        cd - > /dev/null
    fi
}

run_microbenchmarks_tests()
{
    message_split
    if [[ $RUN_BENCHMARKS = false ]] ; then
        warn "Skipping microbenchmarks"
    else
        message "Runnning microbenchmarks"
        cd $MDB_SOURCE_PATH
        ${CTEST_BIN_NAME} . -V -R columnstore_microbenchmarks: -j $(nproc) --progress
        cd - > /dev/null
    fi
}

disable_plugins_for_bootstrap()
{
    find /etc -type f -exec sed -i 's/plugin-load-add=auth_gssapi.so//g' {} +
    find /etc -type f -exec sed -i 's/plugin-load-add=ha_columnstore.so//g' {} +
}

enable_columnstore_back()
{
    echo plugin-load-add=ha_columnstore.so >> $CONFIG_DIR/columnstore.cnf
    sed -i '/\[mysqld\]/a\plugin-load-add=ha_columnstore.so' $CONFIG_DIR/columnstore.cnf
}

fix_config_files()
{
    message Fixing config files

    THREAD_STACK_SIZE="20M"

    SYSTEMD_SERVICE_DIR="/usr/lib/systemd/system"
    MDB_SERVICE_FILE=$SYSTEMD_SERVICE_DIR/mariadb.service
    COLUMNSTORE_CONFIG=$CONFIG_DIR/columnstore.cnf

    if [[ $ASAN = true ]] ; then
        if grep -q thread_stack $COLUMNSTORE_CONFIG; then
            warn "MDB Server has thread_stack settings on $COLUMNSTORE_CONFIG check it's compatibility with ASAN"
        else
            echo "thread_stack = ${THREAD_STACK_SIZE}" >> $COLUMNSTORE_CONFIG
            message "thread_stack was set to ${THREAD_STACK_SIZE} in $COLUMNSTORE_CONFIG"
        fi

        if grep -q ASAN $MDB_SERVICE_FILE; then
            warn "MDB Server has ASAN options in $MDB_SERVICE_FILE, check it's compatibility"
        else
            echo Environment="'ASAN_OPTIONS=abort_on_error=1:disable_coredump=0,print_stats=false,detect_odr_violation=0,check_initialization_order=1,detect_stack_use_after_return=1,atexit=false,log_path=${REPORT_PATH}/asan.mariadb'" >> $MDB_SERVICE_FILE
            message "ASAN options were added to $MDB_SERVICE_FILE"
        fi
    fi

    if [[ $TSAN = true ]] ; then
        if grep -q TSAN $MDB_SERVICE_FILE; then
            warn "MDB Server has TSAN options in $MDB_SERVICE_FILE, check it's compatibility"
        else
            echo Environment="'TSAN_OPTIONS=abort_on_error=0,log_path=${REPORT_PATH}/tsan.mariadb'" >> $MDB_SERVICE_FILE
            message "TSAN options were added to $MDB_SERVICE_FILE"
        fi
    fi

    if [[ $UBSAN = true ]] ; then
        if grep -q UBSAN $MDB_SERVICE_FILE; then
            warn "MDB Server has UBSAN options in $MDB_SERVICE_FILE, check it's compatibility"
        else
            echo Environment="'UBSAN_OPTIONS=abort_on_error=0,print_stacktrace=true,log_path=${REPORT_PATH}/ubsan.mariadb'" >> $MDB_SERVICE_FILE
            message "UBSAN options were added to $MDB_SERVICE_FILE"
        fi
    fi
    if [[ $RECOMPILE_ONLY = false ]] ; then
        message Reloading systemd
        systemctl daemon-reload
    fi
}

make_dir()
{
    mkdir -p $1
    chown mysql:mysql $1
}

install()
{
    if [[ $RECOMPILE_ONLY = false ]] ; then
        message_split
        message "Installing MariaDB"
        disable_plugins_for_bootstrap

        make_dir $REPORT_PATH
        chmod 777 $REPORT_PATH

        check_user_and_group mysql
        check_user_and_group syslog


        make_dir $CONFIG_DIR

        echo "[client-server]
    socket=/run/mysqld/mysqld.sock" > $CONFIG_DIR/socket.cnf

        mv $INSTALL_PREFIX/lib/mysql/plugin/ha_columnstore.so /tmp/ha_columnstore_1.so || mv $INSTALL_PREFIX/lib64/mysql/plugin/ha_columnstore.so /tmp/ha_columnstore_2.so
        make_dir /var/lib/mysql

        message "Running mysql_install_db"
        sudo -u mysql mysql_install_db --rpm --user=mysql > /dev/null
        mv /tmp/ha_columnstore_1.so $INSTALL_PREFIX/lib/mysql/plugin/ha_columnstore.so || mv /tmp/ha_columnstore_2.so $INSTALL_PREFIX/lib64/mysql/plugin/ha_columnstore.so

        enable_columnstore_back

        make_dir /etc/columnstore

        cp $MDB_SOURCE_PATH/storage/columnstore/columnstore/oam/etc/Columnstore.xml /etc/columnstore/Columnstore.xml
        cp $MDB_SOURCE_PATH/storage/columnstore/columnstore/storage-manager/storagemanager.cnf /etc/columnstore/storagemanager.cnf

        cp $MDB_SOURCE_PATH/support-files/*.service /lib/systemd/system/
        cp $MDB_SOURCE_PATH/storage/columnstore/columnstore/oam/install_scripts/*.service /lib/systemd/system/

        if [[ "$OS" = 'Ubuntu' || "$OS" = 'Debian' ]]; then
            make_dir /usr/share/mysql
            make_dir /etc/mysql/
            cp $MDB_SOURCE_PATH/debian/additions/debian-start.inc.sh /usr/share/mysql/debian-start.inc.sh
            cp $MDB_SOURCE_PATH/debian/additions/debian-start /etc/mysql/debian-start
            > /etc/mysql/debian.cnf
        fi

        fix_config_files

        make_dir /etc/my.cnf.d
        if [ -d "/etc/mysql/mariadb.conf.d/" ]; then
            message "Copying configs from /etc/mysql/mariadb.conf.d/ to /etc/my.cnf.d"
            cp -rp /etc/mysql/mariadb.conf.d/* /etc/my.cnf.d
        fi

        if [ -d "/etc/mysql/conf.d/" ]; then
            message "Copying configs from /etc/mysql/conf.d/ to /etc/my.cnf.d"
            cp -rp /etc/mysql/conf.d/* /etc/my.cnf.d
        fi

        make_dir /var/lib/columnstore/data1
        make_dir /var/lib/columnstore/data1/systemFiles
        make_dir /var/lib/columnstore/data1/systemFiles/dbrm
        make_dir /run/mysqld/
        make_dir $DATA_DIR

        chmod +x $INSTALL_PREFIX/bin/mariadb*

        ldconfig

        start_storage_manager_if_needed

        message "Running columnstore-post-install"
        make_dir /var/lib/columnstore/local
        columnstore-post-install --rpmmode=install
        message "Running install_mcs_mysql"
        install_mcs_mysql.sh
    fi

    chown -R syslog:syslog /var/log/mariadb/
    chmod 777 /var/log/mariadb/
    chmod 777 /var/log/mariadb/columnstore
}


smoke()
{
    if [[ $SKIP_SMOKE = false ]] ; then
        message_split
        message "Creating test database"
        mariadb -e "create database if not exists test;"
        message "Selecting magic numbers"
        MAGIC=`mysql -N test < $MDB_SOURCE_PATH/storage/columnstore/columnstore/tests/scripts/smoke.sql`
        if [[ $MAGIC == '42' ]] ; then
            message "Great answer correct!"
        else
            warn "Smoke failed, answer is '$MAGIC'"
        fi
    fi
}


generate_svgs()
{
    if [[ $DRAW_DEPS = true ]] ; then
    message_split
    warn "Generating svgs with dependency graph to $REPORT_PATH"
        for f in $MDB_SOURCE_PATH/mariadb.dot.*;
            do dot -Tsvg -o $REPORT_PATH/`basename $f`.svg $f;
        done
    fi
}

if [[ $DO_NOT_FREEZE_REVISION = false ]] ; then
    disable_git_restore_frozen_revision
fi

select_branch

if [[ $INSTALL_DEPS = true ]] ; then
    install_deps
fi

if [[ $RESTART_SERVICES = true ]] ; then
    stop_service
fi

if [[ $NO_CLEAN = false ]] ; then
    clean_old_installation
fi

build
run_unit_tests
run_microbenchmarks_tests
install
if [[ $RESTART_SERVICES = true ]] ; then
    start_service
    smoke
    generate_svgs
fi

message_splitted "FINISHED"
