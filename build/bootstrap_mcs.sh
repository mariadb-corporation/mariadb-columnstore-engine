#!/bin/bash

# This script compiles/installs MCS from scratch and it makes some assumptions:
# - the server's source code is two directories above the MCS engine source.
# - the script is to be run under root.

set -o pipefail

export CLICOLOR_FORCE=1 #cmake output

INSTALL_PREFIX="/usr/"
DATA_DIR="/var/lib/mysql/data"
CMAKE_BIN_NAME=cmake

RPM_CONFIG_DIR="/etc/my.cnf.d"
DEB_CONFIG_DIR="/etc/mysql/mariadb.conf.d"
CONFIG_DIR=$RPM_CONFIG_DIR

SCRIPT_LOCATION=$(dirname "$0")
MDB_SOURCE_PATH=$(realpath "$SCRIPT_LOCATION"/../../../..)
COLUMSNTORE_SOURCE_PATH=$(realpath "$SCRIPT_LOCATION"/../)

DEFAULT_MARIA_BUILD_PATH=$(realpath "$MDB_SOURCE_PATH"/../BuildOf_$(basename "$MDB_SOURCE_PATH"))

BUILD_TYPE_OPTIONS=("Debug" "RelWithDebInfo")
DISTRO_OPTIONS=("ubuntu:20.04" "ubuntu:22.04" "ubuntu:24.04" "debian:11" "debian:12" "rockylinux:8" "rockylinux:9")

GCC_VERSION="11"
MDB_CMAKE_FLAGS=()

source "$SCRIPT_LOCATION"/utils.sh

echo "Arguments received: $@"

optparse.define short=A long=asan desc="Build with ASAN" variable=ASAN default=false value=true
optparse.define short=a long=build-path desc="Path for build output" variable=MARIA_BUILD_PATH default=$DEFAULT_MARIA_BUILD_PATH
optparse.define short=B long=run-microbench desc="Compile and run microbenchmarks " variable=RUN_BENCHMARKS default=false value=true
optparse.define short=c long=cloud desc="Enable cloud storage" variable=CLOUD_STORAGE_ENABLED default=false value=true
optparse.define short=C long=force-cmake-reconfig desc="Force cmake reconfigure" variable=FORCE_CMAKE_CONFIG default=false value=true
optparse.define short=d long=distro desc="Choose your OS: ${DISTRO_OPTIONS[*]}" variable=OS
optparse.define short=D long=install-deps desc="Install dependences" variable=INSTALL_DEPS default=false value=true
optparse.define short=F long=custom-cmake-flags desc="Add custom cmake flags" variable=CUSTOM_CMAKE_FLAGS
optparse.define short=f long=do-not-freeze-revision desc="Disable revision freezing, or do not set 'update none' for columnstore submodule in MDB repository" variable=DO_NOT_FREEZE_REVISION default=false value=true
optparse.define short=g long=alien desc="Turn off maintainer mode (ex. -Werror)" variable=MAINTAINER_MODE default=true value=false
optparse.define short=G long=draw-deps desc="Draw dependencies graph" variable=DRAW_DEPS default=false value=true
optparse.define short=j long=parallel desc="Number of paralles for build" variable=CPUS default=$(getconf _NPROCESSORS_ONLN)
optparse.define short=M long=skip-smoke desc="Skip final smoke test" variable=SKIP_SMOKE default=false value=true
optparse.define short=N long=ninja desc="Build with ninja" variable=USE_NINJA default=false value=true
optparse.define short=n long=no-clean-install desc="Do not perform a clean install (keep existing db files)" variable=NO_CLEAN default=false value=true
optparse.define short=o long=recompile-only variable=RECOMPILE_ONLY default=false value=true
optparse.define short=O long=static desc="Build all with static libraries" variable=STATIC_BUILD default=false value=true
optparse.define short=p long=build-packages desc="Build packages" variable=BUILD_PACKAGES default=false value=true
optparse.define short=P long=report-path desc="Path for storing reports and profiles" variable=REPORT_PATH default="/core"
optparse.define short=r long=restart-services variable=RESTART_SERVICES default=true value=false
optparse.define short=R long=gcc-toolset-for-rocky-8 variable=GCC_TOOLSET default=false value=true
optparse.define short=S long=skip-columnstore-submodules desc="Skip columnstore submodules initialization" variable=SKIP_SUBMODULES default=false value=true
optparse.define short=t long=build-type desc="Build Type: ${BUILD_TYPE_OPTIONS[*]}" variable=MCS_BUILD_TYPE
optparse.define short=T long=tsan desc="Build with TSAN" variable=TSAN default=false value=true
optparse.define short=u long=skip-unit-tests desc="Skip UnitTests" variable=SKIP_UNIT_TESTS default=false value=true
optparse.define short=U long=ubsan desc="Build with UBSAN" variable=UBSAN default=false value=true
optparse.define short=v long=verbose desc="Verbose makefile commands" variable=MAKEFILE_VERBOSE default=false value=true
optparse.define short=V long=add-branch-name-to-outdir desc="Add branch name to build output directory" variable=BRANCH_NAME_TO_OUTDIR default=false value=true
optparse.define short=W long=without-core-dumps desc="Do not produce core dumps" variable=WITHOUT_COREDUMPS default=false value=true
optparse.define short=s long=sccache desc="Build with sccache" variable=SCCACHE default=false value=true

source $(optparse.build)

message "Building MariaDB Server from $color_yellow$MDB_SOURCE_PATH$color_normal"

if [[ ! " ${BUILD_TYPE_OPTIONS[*]} " =~ " ${MCS_BUILD_TYPE} " ]]; then
    getChoice -q "Select your Build Type" -o BUILD_TYPE_OPTIONS
    MCS_BUILD_TYPE=$selectedChoice
fi

if [[ ! " ${DISTRO_OPTIONS[*]} " =~ " ${OS} " ]]; then
    echo "OS is empty, trying to detect..."
    detect_distro
fi

select_pkg_format ${OS}

if [[ "$PKG_FORMAT" == "rpm" ]]; then
    CTEST_BIN_NAME="ctest3"
else
    CTEST_BIN_NAME="ctest"
fi

install_sccache() {
    if [[ "$SCCACHE" == false ]]; then
        return
    fi

    if [[ "$(arch)" == "x86_64" ]]; then
        sccache_arch="x86_64"
    else
        sccache_arch="aarch64"
    fi

    message "getting sccache..."
    curl -L -o sccache.tar.gz \
        "https://github.com/mozilla/sccache/releases/download/v0.10.0/sccache-v0.10.0-${sccache_arch}-unknown-linux-musl.tar.gz"

    tar xzf sccache.tar.gz
    install sccache*/sccache /usr/local/bin/ && message "sccache installed"
}

install_deps() {
    if [[ $INSTALL_DEPS = false ]]; then
        return
    fi
    message_split
    prereq=""
    RPM_BUILD_DEPS="lz4 lz4-devel systemd-devel git make libaio-devel openssl-devel boost-devel bison \
      snappy-devel flex libcurl-devel libxml2-devel ncurses-devel automake libtool policycoreutils-devel \
      rpm-build lsof iproute pam-devel perl-DBI cracklib-devel expect createrepo python3 checkpolicy \
      cppunit-devel cmake3 libxcrypt-devel xz-devel zlib-devel libzstd-devel glibc-devel"

    DEB_BUILD_DEPS="build-essential automake libboost-all-dev \
      bison cmake libncurses5-dev python3 libaio-dev libsystemd-dev libpcre2-dev libperl-dev libssl-dev libxml2-dev \
      libkrb5-dev flex libpam-dev git libsnappy-dev libcurl4-openssl-dev libgtest-dev libcppunit-dev googletest \
      libjemalloc-dev liblz-dev liblzo2-dev liblzma-dev liblz4-dev libbz2-dev libbenchmark-dev libdistro-info-perl \
      graphviz devscripts ccache equivs eatmydata curl python3"

    if [[ "$OS" == *"rockylinux:8"* || "$OS" == *"rocky:8"* ]]; then
        command="dnf install -y curl 'dnf-command(config-manager)' && dnf config-manager --set-enabled powertools && \
      dnf install -y libarchive cmake  ${RPM_BUILD_DEPS}"
        if [[ $GCC_TOOLSET = false ]]; then
            command="$command && dnf group install -y \"Development Tools\""
        else
            command="$command && dnf install -y gcc-toolset-${GCC_VERSION} && . /opt/rh/gcc-toolset-${GCC_VERSION}/enable"
        fi
    elif
        [[ "$OS" == "rockylinux:9"* || "$OS" == "rocky:9"* ]]
    then
        command="dnf install -y 'dnf-command(config-manager)' && dnf config-manager --set-enabled crb && \
      dnf install -y pcre2-devel gcc gcc-c++ curl-minimal ${RPM_BUILD_DEPS}"

    elif [[ "$OS" == "debian:11"* ]] || [[ "$OS" == "debian:12"* ]] || [[ "$OS" == "ubuntu:20.04"* ]] || [[ "$OS" == "ubuntu:22.04"* ]] || [[ "$OS" == "ubuntu:24.04"* ]]; then
        command="apt-get -y update && apt-get -y install ${DEB_BUILD_DEPS}"
    else
        echo "Unsupported OS: $OS"
        exit 17
    fi

    if [[ $OS == 'ubuntu:22.04' || $OS == 'ubuntu:24.04' ]]; then
        if [ -f /.dockerenv ]; then
            change_ubuntu_mirror us
        fi
        command="${command} lto-disabled-list"
    fi

    eval "$prereq"
    message "Installing dependencies for $OS"
    retry_eval 5 "$command"
}

install_deps
install_sccache

cd $COLUMSNTORE_SOURCE_PATH
COLUMNSTORE_BRANCH=$(git rev-parse --abbrev-ref HEAD)
message "Columnstore will be built from $color_yellow$COLUMNSTORE_BRANCH$color_cyan branch"

cd $MDB_SOURCE_PATH
MARIADB_BRANCH=$(git rev-parse --abbrev-ref HEAD)
message "MariaDB will be built from $color_yellow$MARIADB_BRANCH$color_cyan branch"
cd - >/dev/null

if [[ ${BRANCH_NAME_TO_OUTDIR} = true ]]; then
    MARIA_BUILD_PATH="${MARIA_BUILD_PATH}_${MARIADB_BRANCH}_${COLUMNSTORE_BRANCH}"
fi

disable_git_restore_frozen_revision() {
    if [[ $DO_NOT_FREEZE_REVISION = true ]]; then
        return
    fi
    cd $MDB_SOURCE_PATH
    git config submodule.storage/columnstore/columnstore.update none
    cd - >/dev/null
}

DEP_GRAPH_PATH="$MARIA_BUILD_PATH/dependency_graph/mariadb.dot"

stop_service() {
    if [[ $RESTART_SERVICES = false || $RECOMPILE_ONLY = true ]]; then
        return
    fi

    if [ "$EUID" -ne 0 ]; then
        error "Please run script as root to be able to manage services"
        exit 1
    fi

    message_split
    message "Stopping MariaDB services"
    systemctl stop mariadb
    systemctl stop mariadb-columnstore
    systemctl stop mcs-storagemanager
}

check_service() {
    if systemctl is-active --quiet "$1"; then
        message "$1 $color_normal[$color_green OK $color_normal]"
    else
        message "$1 $color_normal[$color_red Fail $color_normal]"
        service "$1" status
    fi
}

start_service() {
    if [[ $RESTART_SERVICES = false || $RECOMPILE_ONLY = true ]]; then
        return
    fi

    if [ "$EUID" -ne 0 ]; then
        error "Please run script as root to be able to manage services"
        exit 1
    fi

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

start_storage_manager_if_needed() {
    if [[ $CLOUD_STORAGE_ENABLED = false ]]; then
        return
    fi

    export MCS_USE_S3_STORAGE=1
    message_split
    message "Starting Storage Manager service"
    systemctl start mcs-storagemanager
    check_service mcs-storagemanager
}

clean_old_installation() {
    if [[ $NO_CLEAN = true || $RECOMPILE_ONLY = true ]]; then
        return
    fi

    if [ "$EUID" -ne 0 ]; then
        error "Please run script as root to be able to clean installations"
        exit 1
    fi
    message_split
    message "Cleaning old installation"
    rm -rf /var/lib/columnstore/data1/*
    rm -rf /var/lib/columnstore/data/
    rm -rf /var/lib/columnstore/local/
    rm -rf /var/lib/columnstore/storagemanager/*
    rm -rf /var/log/mariadb/columnstore/*
    rm -rf /tmp/*
    rm -rf "$REPORT_PATH"
    rm -rf /var/lib/mysql
    rm -rf /var/run/mysqld
    rm -rf $DATA_DIR
    rm -rf $CONFIG_DIR
}

modify_packaging() {
    echo "Modifying_packaging..."
    cd $MDB_SOURCE_PATH

    # Bypass of debian version list check in autobake
    if [[ $PKG_FORMAT == "deb" ]]; then
        sed -i 's|.*-d storage/columnstore.*|elif [[ -d storage/columnstore/columnstore/debian ]]|' debian/autobake-deb.sh
    fi

    # patch to avoid fakeroot, which is using LD_PRELOAD for libfakeroot.so
    # and eamtmydata which is using LD_PRELOAD for libeatmydata.so and this
    # breaks intermediate build binaries to fail with "ASan runtime does not come first in initial library list
    if [[ $PKG_FORMAT == "deb" && $ASAN = true ]]; then
        sed -i 's|BUILDPACKAGE_DPKGCMD+=( "fakeroot" "--" )|echo "fakeroot was disabled for ASAN build"|' debian/autobake-deb.sh
        sed -i 's|BUILDPACKAGE_DPKGCMD+=("eatmydata")|echo "eatmydata was disabled for ASAN build"|' debian/autobake-deb.sh
    fi

    #disable LTO for 22.04 for now
    if [[ $OS == 'ubuntu:22.04' || $OS == 'ubuntu:24.04' ]]; then
        for i in mariadb-plugin-columnstore mariadb-server mariadb-server-core mariadb mariadb-10.6; do
            echo "$i any" >>/usr/share/lto-disabled-list/lto-disabled-list
        done &&
            grep mariadb /usr/share/lto-disabled-list/lto-disabled-list
    fi

    if [[ $PKG_FORMAT == "deb" ]]; then
        apt-cache madison liburing-dev | grep liburing-dev || {
            sed 's/liburing-dev/libaio-dev/g' -i debian/control &&
                sed '/-DIGNORE_AIO_CHECK=YES/d' -i debian/rules &&
                sed '/-DWITH_URING=yes/d' -i debian/rules
        }
        apt-cache madison libpmem-dev | grep 'libpmem-dev' || {
            sed '/libpmem-dev/d' -i debian/control &&
                sed '/-DWITH_PMEM/d' -i debian/rules
        }
        sed '/libfmt-dev/d' -i debian/control

        # Remove Debian build flags that could prevent ColumnStore from building
        sed -i '/-DPLUGIN_COLUMNSTORE=NO/d' debian/rules

        # Disable dh_missing strict check for missing files
        sed -i 's/--fail-missing/--list-missing/' debian/rules

        # Tweak debian packaging stuff
        for i in mariadb-plugin libmariadbd; do
            sed -i "/Package: $i.*/,/^$/d" debian/control
        done

        sed -i 's/Depends: galera.*/Depends:/' debian/control

        for i in galera wsrep ha_sphinx embedded; do
            sed -i "/$i/d" debian/*.install
        done
    fi
}

construct_cmake_flags() {
    MDB_CMAKE_FLAGS=(
        -DBUILD_CONFIG=mysql_release
        -DCMAKE_BUILD_TYPE=$MCS_BUILD_TYPE
        -DCMAKE_EXPORT_COMPILE_COMMANDS=1
        -DCMAKE_INSTALL_PREFIX:PATH=$INSTALL_PREFIX
        -DMYSQL_MAINTAINER_MODE=NO
        -DPLUGIN_COLUMNSTORE=YES
        -DPLUGIN_CONNECT=NO
        -DPLUGIN_GSSAPI=NO
        -DPLUGIN_MROONGA=NO
        -DPLUGIN_OQGRAPH=NO
        -DPLUGIN_ROCKSDB=NO
        -DPLUGIN_SPHINX=NO
        -DPLUGIN_SPIDER=NO
        -DPLUGIN_TOKUDB=NO
        -DWITH_EMBEDDED_SERVER=NO
        -DWITH_SSL=system
        -DWITH_SYSTEMD=yes
        -DWITH_WSREP=NO
    )

    if [[ $BUILD_PACKAGES = true ]]; then
        MDB_CMAKE_FLAGS+=(-DCOLUMNSTORE_PACKAGES_BUILD=YES)
        message "Building packages for Columnstore"
    fi

    if [[ $MAINTAINER_MODE = true ]]; then
        MDB_CMAKE_FLAGS+=(-DCOLUMNSTORE_MAINTAINER=YES)
        message "Columnstore maintainer mode on"
    else
        warn "Maintainer mode is disabled, be careful, alien"
    fi

    if [[ $SKIP_UNIT_TESTS = true ]]; then
        warn "Unittests are not build"
    else
        MDB_CMAKE_FLAGS+=(-DWITH_UNITTESTS=YES)
        message "Buiding with unittests"
    fi

    if [[ $DRAW_DEPS = true ]]; then
        warn "Generating dependendies graph to mariadb.dot"
        MDB_CMAKE_FLAGS+=(--graphviz=$DEP_GRAPH_PATH)
    fi

    if [[ $USE_NINJA = true ]]; then
        warn "Using Ninja instead of Makefiles"
        MDB_CMAKE_FLAGS+=(-GNinja)
    fi

    if [[ $STATIC_BUILD = true ]]; then
        warn "Building all with static linkage"
        MDB_CMAKE_FLAGS+=(-DCOLUMNSTORE_STATIC_LIBRARIES:BOOL=ON)
    fi

    if [[ $ASAN = true ]]; then
        warn "Building with Address Sanitizer "
        MDB_CMAKE_FLAGS+=(-DWITH_ASAN=ON -DWITH_COLUMNSTORE_ASAN=ON -DWITH_COLUMNSTORE_REPORT_PATH=${REPORT_PATH})
    fi

    if [[ $TSAN = true ]]; then
        warn "Building with Thread Sanitizer"
        MDB_CMAKE_FLAGS+=(-DWITH_TSAN=ON -DWITH_COLUMNSTORE_REPORT_PATH=${REPORT_PATH})
        message "Setting vm.mmap_rnd_bits=30 for TSAN support"
        sysctl vm.mmap_rnd_bits=30
    fi

    if [[ $UBSAN = true ]]; then
        warn "Building with UB Sanitizer"
        MDB_CMAKE_FLAGS+=(-DWITH_UBSAN=ON -DWITH_COLUMNSTORE_REPORT_PATH=${REPORT_PATH})
    fi

    if [[ $WITHOUT_COREDUMPS = true ]]; then
        warn "Cores are not dumped"
    else
        warn "Building with CoreDumps"
        MDB_CMAKE_FLAGS+=(-DWITH_COREDUMPS=ON)

        if [ -f /.dockerenv ]; then
            warn "Build is executed in Docker, core dumps saving path /proc/sys/kernel/core_pattern will not be configured!"
        else
            warn "/proc/sys/kernel/core_pattern changed to ${REPORT_PATH}/core_%e.%p"
            echo "${REPORT_PATH}/core_%e.%p" >/proc/sys/kernel/core_pattern
        fi
    fi

    if [[ $MAKEFILE_VERBOSE = true ]]; then
        warn "Verbosing Makefile Commands"
        MDB_CMAKE_FLAGS+=(-DCMAKE_VERBOSE_MAKEFILE:BOOL=ON)
    fi

    if [[ $SCCACHE = true ]]; then
        warn "Use sccache"
        MDB_CMAKE_FLAGS+=(-DCMAKE_C_COMPILER_LAUNCHER=sccache -DCMAKE_CXX_COMPILER_LAUNCHER=sccache)
    fi

    if [[ $RUN_BENCHMARKS = true ]]; then
        if [[ $MCS_BUILD_TYPE = 'Debug' ]]; then
            error "Benchmarks will not be build in run in Debug build Mode"
            MDB_CMAKE_FLAGS+=(-DWITH_MICROBENCHMARKS=NO)
            RUN_BENCHMARKS=false
        elif [[ $OS != *"ubuntu"* && $OS != *"debian"* ]]; then
            error "Benchmarks are now avaliable only at Ubuntu or Debian"
            MAKE_FLAGS="${MDB_CMAKE_FLAGS[@]} -DWITH_MICROBENCHMARKS=NO"
            RUN_BENCHMARKS=false
        else
            message "Compile with microbenchmarks"
            MDB_CMAKE_FLAGS+=(-DWITH_MICROBENCHMARKS=YES)
        fi
    else
        MDB_CMAKE_FLAGS+=(-DWITH_MICROBENCHMARKS=NO)
        message "Buiding without microbenchmarks"
    fi

    if [[ "$OS" == *"rocky"* ]]; then
        OS_VERSION=${OS//[^0-9]/}
        MDB_CMAKE_FLAGS+=(-DRPM=rockylinux${OS_VERSION})
    elif [[ "$OS" == "debian:11" ]]; then
        CODENAME="bullseye"
    elif [[ "$OS" == "debian:12" ]]; then
        CODENAME="bookworm"
    elif [[ "$OS" == "ubuntu:20.04" ]]; then
        CODENAME="focal"
    elif [[ "$OS" == "ubuntu:22.04" ]]; then
        CODENAME="jammy"
    elif [[ "$OS" == "ubuntu:24.04" ]]; then
        CODENAME="noble"
    else
        echo "Unsupported OS: $OS"
        exit 17
    fi

    if [[ -n "$CODENAME" ]]; then
        MDB_CMAKE_FLAGS+=(-DDEB=${CODENAME})
    fi

    if [[ "$NO_CLEAN" == true ]]; then
        message "Skipping Columnstore.etc config installation"
        MDB_CMAKE_FLAGS+=(-DSKIP_CONFIG_INSTALLATION=ON)
    fi

    MDB_CMAKE_FLAGS+=($CUSTOM_CMAKE_FLAGS)

    message "Building with flags"
    newline_array "${MDB_CMAKE_FLAGS[@]}"
}

init_submodules() {
    if [[ $SKIP_SUBMODULES = true ]]; then
        warn "Skipping initialization of columnstore submodules"
    else
        message "Initialization of columnstore submodules"
        cd $MDB_SOURCE_PATH
        git submodule update --init --recursive
        cd - >/dev/null
    fi
}

check_errorcode() {
    if [ $? -ne 0 ]; then
        message_split
        error "!!!! BUILD FAILED !!!!"
        message_split
        exit 1
    fi
    cd - >/dev/null
}

generate_svgs() {
    if [[ $DRAW_DEPS = true ]]; then
        message_split
        warn "Generating svgs with dependency graph to $DEP_GRAPH_PATH"
        for f in $(ls "$DEP_GRAPH_PATH".* | grep -v ".svg"); do
            dot -Tsvg -o "$f.svg" "$f"
        done
    fi
}

build_package() {
    cd $MDB_SOURCE_PATH

    if [[ $PKG_FORMAT == "rpm" ]]; then
        command="cmake ${MDB_CMAKE_FLAGS[@]} && make -j\$(nproc) package"
    else
        export DEBIAN_FRONTEND="noninteractive"
        export DEB_BUILD_OPTIONS="parallel=$(nproc)"
        export DH_BUILD_DDEBS="1"
        export BUILDPACKAGE_FLAGS="-b"
        command="mk-build-deps debian/control -t 'apt-get -y -o Debug::pkgProblemResolver=yes --no-install-recommends' -r -i && \
       CMAKEFLAGS=\"${MDB_CMAKE_FLAGS[@]}\" debian/autobake-deb.sh"
    fi

    echo "Building a package for $OS"
    echo "Build command: $command"
    eval "$command"

    check_errorcode
}

check_debian_install_file() {
    message "checking debian/mariadb-plugin-columnstore.install"
    message_split
    python3 $COLUMSNTORE_SOURCE_PATH/build/debian_install_file_compare.py \
        ${COLUMSNTORE_SOURCE_PATH}/debian/mariadb-plugin-columnstore.install \
        $MARIA_BUILD_PATH/mariadb-plugin-columnstore.install.generated
}

build_binary() {
    MARIA_BUILD_PATH=$(realpath "$MARIA_BUILD_PATH")
    message_split
    message "Building sources in $color_yellow$MCS_BUILD_TYPE$color_cyan mode"
    message "Compiled artifacts will be written to $color_yellow$MARIA_BUILD_PATH$color_cyan"
    mkdir -p "$MARIA_BUILD_PATH"

    cd $MDB_SOURCE_PATH

    if [[ $FORCE_CMAKE_CONFIG = true ]]; then
        warn "Erasing cmake cache"
        rm -f "$MARIA_BUILD_PATH/CMakeCache.txt"
        rm -rf "$MARIA_BUILD_PATH/CMakeFiles"
    fi

    message "Configuring cmake silently"
    ${CMAKE_BIN_NAME} "${MDB_CMAKE_FLAGS[@]}" -S"$MDB_SOURCE_PATH" -B"$MARIA_BUILD_PATH" | spinner
    message_split
    # check_debian_install_file // will be uncommented later
    generate_svgs

    ${CMAKE_BIN_NAME} --build "$MARIA_BUILD_PATH" -j "$CPUS" | onelinearizator
    check_errorcode

    message "Adding symbol link to compile_commands.json to the source root"
    ln -sf "$MARIA_BUILD_PATH/compile_commands.json" "$MDB_SOURCE_PATH"
}

install_binary() {
    if [[ $RECOMPILE_ONLY = true ]]; then
        warn "No binary installation done"
        return
    fi

    message "Installing silently"
    cd $MDB_SOURCE_PATH
    ${CMAKE_BIN_NAME} --install "$MARIA_BUILD_PATH" | spinner 30
    check_errorcode
}

check_user_and_group() {
    user=$1
    if [ -z "$(grep "$user" /etc/passwd)" ]; then
        message "Adding user $user into /etc/passwd"
        useradd -r -U "$user" -d /var/lib/mysql
    fi

    if [ -z "$(grep "$user" /etc/group)" ]; then
        GroupID=$(awk -F: '{uid[$3]=1}END{for(x=100; x<=999; x++) {if(uid[x] != ""){}else{print x; exit;}}}' /etc/group)
        message "Adding group $user with id $GroupID"
        groupadd -g "$GroupID" "$user"
    fi
}

run_unit_tests() {
    message_split
    if [[ $SKIP_UNIT_TESTS = true ]]; then
        warn "Skipping unittests"
        return
    fi

    message "Running unittests"
    cd $MARIA_BUILD_PATH
    ${CTEST_BIN_NAME} . -R columnstore: -j $(nproc) --output-on-failure
    cd - >/dev/null
}

run_microbenchmarks_tests() {
    message_split
    if [[ $RUN_BENCHMARKS = false ]]; then
        warn "Skipping microbenchmarks"
        return
    fi

    message "Runnning microbenchmarks"
    cd $MARIA_BUILD_PATH
    ${CTEST_BIN_NAME} . -V -R columnstore_microbenchmarks: -j $(nproc) --progress
    cd - >/dev/null
}

disable_plugins_for_bootstrap() {
    find /etc -type f -exec sed -i 's/plugin-load-add=auth_gssapi.so//g' {} +
    find /etc -type f -exec sed -i 's/plugin-load-add=ha_columnstore.so/#plugin-load-add=ha_columnstore.so/g' {} +
}

enable_columnstore_back() {
    if [[ "$NO_CLEAN" == true ]]; then
        find /etc -type f -exec sed -i 's/#plugin-load-add=ha_columnstore.so/plugin-load-add=ha_columnstore.so/g' {} +
    else
        cp "$MDB_SOURCE_PATH"/storage/columnstore/columnstore/dbcon/mysql/columnstore.cnf $CONFIG_DIR
    fi
}

fix_config_files() {
    message Fixing config files
    THREAD_STACK_SIZE="20M"

    # while packaging we have to patch configs in the sources to get them in the packakges
    # for local builds, we patch config after installation in the systemdirs
    if [[ $BUILD_PACKAGES = true ]]; then
        MDB_SERVICE_FILE=$MDB_SOURCE_PATH/support-files/mariadb.service.in
        COLUMNSTORE_CONFIG=$COLUMSNTORE_SOURCE_PATH/dbcon/mysql/columnstore.cnf
        SANITIZERS_ABORT_ON_ERROR='0'
    else
        SYSTEMD_SERVICE_DIR="/usr/lib/systemd/system"
        MDB_SERVICE_FILE=$SYSTEMD_SERVICE_DIR/mariadb.service
        COLUMNSTORE_CONFIG=$CONFIG_DIR/columnstore.cnf
        SANITIZERS_ABORT_ON_ERROR='1'
    fi

    if [[ $ASAN = true ]]; then
        if grep -q thread_stack $COLUMNSTORE_CONFIG; then
            warn "MDB Server has thread_stack settings on $COLUMNSTORE_CONFIG check it's compatibility with ASAN"
        else
            echo "" >>$COLUMNSTORE_CONFIG
            echo "thread_stack = ${THREAD_STACK_SIZE}" >>$COLUMNSTORE_CONFIG
            message "thread_stack was set to ${THREAD_STACK_SIZE} in $COLUMNSTORE_CONFIG"
        fi

        if grep -q ASAN $MDB_SERVICE_FILE; then
            warn "MDB Server has ASAN options in $MDB_SERVICE_FILE, check it's compatibility"
        else
            echo Environment="'ASAN_OPTIONS=abort_on_error=$SANITIZERS_ABORT_ON_ERROR:disable_coredump=0,print_stats=false,detect_odr_violation=0,check_initialization_order=1,detect_stack_use_after_return=1,atexit=false,log_path=${REPORT_PATH}/asan.mariadb'" >>$MDB_SERVICE_FILE
            message "ASAN options were added to $MDB_SERVICE_FILE"
        fi
    fi

    if [[ $TSAN = true ]]; then
        if grep -q TSAN $MDB_SERVICE_FILE; then
            warn "MDB Server has TSAN options in $MDB_SERVICE_FILE, check it's compatibility"
        else
            echo Environment="'TSAN_OPTIONS=abort_on_error=$SANITIZERS_ABORT_ON_ERROR,log_path=${REPORT_PATH}/tsan.mariadb'" >>$MDB_SERVICE_FILE
            message "TSAN options were added to $MDB_SERVICE_FILE"
        fi
    fi

    if [[ $UBSAN = true ]]; then
        if grep -q UBSAN $MDB_SERVICE_FILE; then
            warn "MDB Server has UBSAN options in $MDB_SERVICE_FILE, check it's compatibility"
        else
            echo Environment="'UBSAN_OPTIONS=abort_on_error=$SANITIZERS_ABORT_ON_ERROR,print_stacktrace=true,log_path=${REPORT_PATH}/ubsan.mariadb'" >>$MDB_SERVICE_FILE
            message "UBSAN options were added to $MDB_SERVICE_FILE"
        fi
    fi
    if [[ $RECOMPILE_ONLY = false ]]; then
        message Reloading systemd
        systemctl daemon-reload
    fi
}

make_dir() {
    mkdir -p "$1"
    chown mysql:mysql "$1"
}

install() {
    if [[ $RECOMPILE_ONLY = true ]]; then
        warn "No install configuration done"
        return
    fi

    if [ "$EUID" -ne 0 ]; then
        error "Please run script as root to install MariaDb to system paths"
        exit 1
    fi

    message_split
    message "Installing MariaDB"
    disable_plugins_for_bootstrap

    make_dir "$REPORT_PATH"
    chmod 777 "$REPORT_PATH"

    check_user_and_group mysql
    check_user_and_group syslog

    make_dir $CONFIG_DIR

    echo "[client-server]
    socket=/run/mysqld/mysqld.sock" >$CONFIG_DIR/socket.cnf

    make_dir /var/lib/mysql

    message "Running mariadb-install-db"
    sudo -u mysql mariadb-install-db --rpm --user=mysql >/dev/null

    enable_columnstore_back

    make_dir /etc/columnstore

    if [[ "$NO_CLEAN" == false ]]; then
        cp "$MDB_SOURCE_PATH"/storage/columnstore/columnstore/oam/etc/Columnstore.xml /etc/columnstore/Columnstore.xml
        cp "$MDB_SOURCE_PATH"/storage/columnstore/columnstore/storage-manager/storagemanager.cnf /etc/columnstore/storagemanager.cnf
    fi

    cp "$MDB_SOURCE_PATH"/storage/columnstore/columnstore/oam/install_scripts/*.service /lib/systemd/system/

    if [[ "$OS" = *"ubuntu"* || "$OS" = *"debian"* ]]; then
        make_dir /usr/share/mysql
        make_dir /etc/mysql/
        cp "$MDB_SOURCE_PATH"/debian/additions/debian-start.inc.sh /usr/share/mysql/debian-start.inc.sh
        cp "$MDB_SOURCE_PATH"/debian/additions/debian-start /etc/mysql/debian-start
        >/etc/mysql/debian.cnf
    fi

    fix_config_files

    if [ -d "$DEBCONFIG_DIR" ]; then
        message "Copying configs from $DEBCONFIG_DIR to $CONFIG_DIR"
        cp -rp "$DEBCONFIG_DIR"/* "$CONFIG_DIR"
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

    chown -R syslog:syslog /var/log/mariadb/
    chmod 777 /var/log/mariadb/
    chmod 777 /var/log/mariadb/columnstore
}

smoke() {
    if [[ $RECOMPILE_ONLY = true || $SKIP_SMOKE = true || $RESTART_SERVICES = false ]]; then
        return
    fi

    message_split
    message "Creating test database"
    mariadb -e "create database if not exists test;"
    message "Selecting magic numbers"
    MAGIC=$(mariadb -N test <"$MDB_SOURCE_PATH"/storage/columnstore/columnstore/tests/scripts/smoke.sql)
    if [[ $MAGIC == '42' ]]; then
        message "Great answer correct!"
    else
        warn "Smoke failed, answer is '$MAGIC'"
    fi
}

if [[ $DO_NOT_FREEZE_REVISION = false ]]; then
    disable_git_restore_frozen_revision
fi

construct_cmake_flags
init_submodules

if [[ $BUILD_PACKAGES = true ]]; then
    modify_packaging
    fix_config_files
    (build_package && run_unit_tests)
    exit_code=$?

    if [[ $SCCACHE = true ]]; then
        sccache --show-adv-stats
    fi

    exit $exit_code
fi

stop_service
clean_old_installation
build_binary
install_binary
run_unit_tests
run_microbenchmarks_tests
install
start_service
smoke

message_splitted "FINISHED"
