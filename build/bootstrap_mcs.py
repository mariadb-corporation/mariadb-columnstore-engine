#!/usr/bin/env python3
import os
import sys
import subprocess
import argparse
from pathlib import Path

# Color output (simple, for now)
def message(text):
    print(f"\033[1;32m{text}\033[0m")

def warn(text):
    print(f"\033[1;33m{text}\033[0m")

def error(text):
    print(f"\033[1;31m{text}\033[0m", file=sys.stderr)
    sys.exit(1)

def message_split():
    print("\n" + "-" * 60 + "\n")

def message_splitted(text):
    message_split()
    message(text)
    message_split()

# Paths and env vars
SCRIPT_LOCATION = Path(__file__).parent.resolve()
MDB_SOURCE_PATH = (SCRIPT_LOCATION / '../../../..').resolve()
COLUMSNTORE_SOURCE_PATH = (SCRIPT_LOCATION / '../').resolve()
DEFAULT_MARIA_BUILD_PATH = (MDB_SOURCE_PATH.parent / f"BuildOf_{MDB_SOURCE_PATH.name}").resolve()

# Default values
INSTALL_PREFIX = "/usr/"
DATA_DIR = "/var/lib/mysql/data"
RPM_CONFIG_DIR = "/etc/my.cnf.d"
DEB_CONFIG_DIR = "/etc/mysql/mariadb.conf.d"
CONFIG_DIR = RPM_CONFIG_DIR
GCC_VERSION = "11"
DISTRO_OPTIONS = ["ubuntu:20.04", "ubuntu:22.04", "ubuntu:24.04", "debian:11", "debian:12", "rockylinux:8", "rockylinux:9"]

# Argument parsing
def parse_args():
    parser = argparse.ArgumentParser(description="Compile/install MCS from scratch.")
    parser.add_argument('-A', '--asan', action='store_true', help='Build with ASAN')
    parser.add_argument('-a', '--build-path', default=str(DEFAULT_MARIA_BUILD_PATH), help='Path for build output')
    parser.add_argument('-B', '--run-microbench', action='store_true', help='Compile and run microbenchmarks')
    parser.add_argument('-c', '--cloud', action='store_true', help='Enable cloud storage')
    parser.add_argument('-C', '--force-cmake-reconfig', action='store_true', help='Force cmake reconfigure')
    parser.add_argument('-d', '--distro', choices=DISTRO_OPTIONS, help='Choose your OS')
    parser.add_argument('-D', '--install-deps', action='store_true', help='Install dependencies')
    parser.add_argument('-F', '--custom-cmake-flags', help='Add custom cmake flags')
    parser.add_argument('-f', '--do-not-freeze-revision', action='store_true', help="Disable revision freezing")
    parser.add_argument('-g', '--alien', action='store_true', help='Turn off maintainer mode (ex. -Werror)')
    parser.add_argument('-G', '--draw-deps', action='store_true', help='Draw dependencies graph')
    parser.add_argument('-j', '--parallel', type=int, default=os.cpu_count(), help='Number of parallels for build')
    parser.add_argument('-M', '--skip-smoke', action='store_true', help='Skip final smoke test')
    parser.add_argument('-N', '--ninja', action='store_true', help='Build with ninja')
    parser.add_argument('-n', '--no-clean-install', action='store_true', help='Do not perform a clean install')
    return parser.parse_args()

def run(cmd, check=True, **kwargs):
    message(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, check=check, **kwargs)
    return result

def retry_eval(retries, cmd):
    # Simple retry logic for commands
    for attempt in range(retries):
        try:
            run(cmd)
            return
        except subprocess.CalledProcessError as e:
            warn(f"Attempt {attempt+1} failed: {e}")
    error(f"Command failed after {retries} attempts: {cmd}")

def change_ubuntu_mirror(region):
    warn(f"Would change Ubuntu mirror to region: {region} (stub)")

def detect_distro():
    # Stub: In bash, this would detect and set OS variable
    warn("OS detection not implemented. Please specify --distro.")
    return None

def install_deps(OS, GCC_VERSION):
    RPM_BUILD_DEPS = (
        "dnf install -y lz4 lz4-devel systemd-devel git make libaio-devel openssl-devel boost-devel bison "
        "snappy-devel flex libcurl-devel libxml2-devel ncurses-devel automake libtool policycoreutils-devel "
        "rpm-build lsof iproute pam-devel perl-DBI cracklib-devel expect createrepo python3 checkpolicy "
        "cppunit-devel cmake3 libxcrypt-devel xz-devel zlib-devel libzstd-devel glibc-devel"
    )
    DEB_BUILD_DEPS = (
        "apt-get -y update && apt-get -y install build-essential automake libboost-all-dev "
        "bison cmake libncurses5-dev python3 libaio-dev libsystemd-dev libpcre2-dev libperl-dev libssl-dev libxml2-dev "
        "libkrb5-dev flex libpam-dev git libsnappy-dev libcurl4-openssl-dev libgtest-dev libcppunit-dev googletest "
        "libjemalloc-dev liblz-dev liblzo2-dev liblzma-dev liblz4-dev libbz2-dev libbenchmark-dev libdistro-info-perl "
        "graphviz devscripts ccache equivs eatmydata curl"
    )
    if "rockylinux:8" in OS or "rocky:8" in OS:
        command = (
            f"dnf install -y curl 'dnf-command(config-manager)' && dnf config-manager --set-enabled powertools && "
            f"dnf install -y gcc-toolset-{GCC_VERSION} libarchive cmake && . /opt/rh/gcc-toolset-{GCC_VERSION}/enable && "
            f"{RPM_BUILD_DEPS}"
        )
    elif "rockylinux:9" in OS or "rocky:9" in OS:
        command = (
            "dnf install -y 'dnf-command(config-manager)' && dnf config-manager --set-enabled crb && "
            "dnf install -y pcre2-devel gcc gcc-c++ curl-minimal && "
            f"{RPM_BUILD_DEPS}"
        )
    elif OS.startswith("debian:11") or OS.startswith("debian:12") \
        or OS.startswith("ubuntu:20.04") or OS.startswith("ubuntu:22.04") or OS.startswith("ubuntu:24.04"):
        command = DEB_BUILD_DEPS
    else:
        error(f"Unsupported OS: {OS}")
    if OS in ['ubuntu:22.04', 'ubuntu:24.04']:
        if os.path.exists('/.dockerenv'):
            change_ubuntu_mirror('us')
        command += " lto-disabled-list"
    message(f"Installing dependencies for {OS}")
    retry_eval(5, command)

def construct_cmake_flags(args, env_vars):
    MDB_CMAKE_FLAGS = [
        '-DBUILD_CONFIG=mysql_release',
        f'-DCMAKE_BUILD_TYPE={env_vars["MCS_BUILD_TYPE"]}',
        '-DCMAKE_EXPORT_COMPILE_COMMANDS=1',
        f'-DCMAKE_INSTALL_PREFIX:PATH={env_vars["INSTALL_PREFIX"]}',
        '-DMYSQL_MAINTAINER_MODE=NO',
        '-DPLUGIN_COLUMNSTORE=YES',
        '-DPLUGIN_CONNECT=NO',
        '-DPLUGIN_GSSAPI=NO',
        '-DPLUGIN_MROONGA=NO',
        '-DPLUGIN_OQGRAPH=NO',
        '-DPLUGIN_ROCKSDB=NO',
        '-DPLUGIN_SPHINX=NO',
        '-DPLUGIN_SPIDER=NO',
        '-DPLUGIN_TOKUDB=NO',
        '-DWITH_EMBEDDED_SERVER=NO',
        '-DWITH_SSL=system',
        '-DWITH_SYSTEMD=yes',
        '-DWITH_WSREP=NO',
    ]
    if env_vars['MAINTAINER_MODE']:
        MDB_CMAKE_FLAGS.append('-DCOLUMNSTORE_MAINTAINER=YES')
        message("Columnstore maintainer mode on")
    else:
        warn("Maintainer mode is disabled, be careful, alien")
    if env_vars.get('SKIP_UNIT_TESTS', False):
        warn("Unittests are not built")
    else:
        MDB_CMAKE_FLAGS.append('-DWITH_UNITTESTS=YES')
        message("Building with unittests")
    if env_vars['DRAW_DEPS']:
        warn("Generating dependencies graph to mariadb.dot")
        MDB_CMAKE_FLAGS.append(f'--graphviz={env_vars["DEP_GRAPH_PATH"]}')
    if env_vars['USE_NINJA']:
        warn("Using Ninja instead of Makefiles")
        MDB_CMAKE_FLAGS.append('-GNinja')
    if env_vars.get('STATIC_BUILD', False):
        warn("Building all with static linkage")
        MDB_CMAKE_FLAGS.append('-DCOLUMNSTORE_STATIC_LIBRARIES:BOOL=ON')
    if env_vars['ASAN']:
        warn("Building with Address Sanitizer ")
        MDB_CMAKE_FLAGS.extend([
            '-DWITH_ASAN=ON',
            '-DWITH_COLUMNSTORE_ASAN=ON',
            f'-DWITH_COLUMNSTORE_REPORT_PATH={env_vars.get("REPORT_PATH", "/tmp")}'
        ])
    if env_vars.get('TSAN', False):
        warn("Building with Thread Sanitizer")
        MDB_CMAKE_FLAGS.extend([
            '-DWITH_TSAN=ON',
            f'-DWITH_COLUMNSTORE_REPORT_PATH={env_vars.get("REPORT_PATH", "/tmp")}'
        ])
        message("Setting vm.mmap_rnd_bits=30 for TSAN support")
        run('sysctl vm.mmap_rnd_bits=30')
    if env_vars.get('UBSAN', False):
        warn("Building with UB Sanitizer")
        MDB_CMAKE_FLAGS.extend([
            '-DWITH_UBSAN=ON',
            f'-DWITH_COLUMNSTORE_REPORT_PATH={env_vars.get("REPORT_PATH", "/tmp")}'
        ])
    if not env_vars.get('WITHOUT_COREDUMPS', False):
        warn("Building with CoreDumps")
        MDB_CMAKE_FLAGS.append('-DWITH_COREDUMPS=ON')
        if os.path.exists('/.dockerenv'):
            warn("Build is executed in Docker, core dumps saving path /proc/sys/kernel/core_pattern will not be configured!")
        else:
            warn(f"/proc/sys/kernel/core_pattern changed to {env_vars.get('REPORT_PATH', '/tmp')}/core_%e.%p")
            with open('/proc/sys/kernel/core_pattern', 'w') as f:
                f.write(f"{env_vars.get('REPORT_PATH', '/tmp')}/core_%e.%p\n")
    else:
        warn("Cores are not dumped")
    if env_vars.get('MAKEFILE_VERBOSE', False):
        warn("Verbosing Makefile Commands")
        MDB_CMAKE_FLAGS.append('-DCMAKE_VERBOSE_MAKEFILE:BOOL=ON')
    if env_vars.get('SCCACHE', False):
        warn("Use sccache")
        MDB_CMAKE_FLAGS.extend([
            '-DCMAKE_C_COMPILER_LAUNCHER=sccache',
            '-DCMAKE_CXX_COMPILER_LAUNCHER=sccache'
        ])
    if env_vars['RUN_BENCHMARKS']:
        if env_vars['MCS_BUILD_TYPE'] == 'Debug':
            error("Benchmarks will not be built or run in Debug build Mode")
            MDB_CMAKE_FLAGS.append('-DWITH_MICROBENCHMARKS=NO')
            env_vars['RUN_BENCHMARKS'] = False
        elif 'ubuntu' not in env_vars['OS'] and 'debian' not in env_vars['OS']:
            error("Benchmarks are now available only on Ubuntu or Debian")
            MDB_CMAKE_FLAGS.append('-DWITH_MICROBENCHMARKS=NO')
            env_vars['RUN_BENCHMARKS'] = False
        else:
            message("Compile with microbenchmarks")
            MDB_CMAKE_FLAGS.append('-DWITH_MICROBENCHMARKS=YES')
    else:
        MDB_CMAKE_FLAGS.append('-DWITH_MICROBENCHMARKS=NO')
        message("Building without microbenchmarks")
    CODENAME = None
    if 'rocky' in env_vars['OS']:
        OS_VERSION = ''.join(filter(str.isdigit, env_vars['OS']))
        MDB_CMAKE_FLAGS.append(f'-DRPM=rockylinux{OS_VERSION}')
    elif env_vars['OS'] == 'debian:11':
        CODENAME = 'bullseye'
    elif env_vars['OS'] == 'debian:12':
        CODENAME = 'bookworm'
    elif env_vars['OS'] == 'ubuntu:20.04':
        CODENAME = 'focal'
    elif env_vars['OS'] == 'ubuntu:22.04':
        CODENAME = 'jammy'
    elif env_vars['OS'] == 'ubuntu:24.04':
        CODENAME = 'noble'
    else:
        error(f"Unsupported OS: {env_vars['OS']}")
    if CODENAME:
        MDB_CMAKE_FLAGS.append(f'-DDEB={CODENAME}')
    if env_vars['CUSTOM_CMAKE_FLAGS']:
        MDB_CMAKE_FLAGS.append(env_vars['CUSTOM_CMAKE_FLAGS'])
    message("Building with flags")
    for flag in MDB_CMAKE_FLAGS:
        print(flag)
    return MDB_CMAKE_FLAGS

def init_submodules(SKIP_SUBMODULES, MDB_SOURCE_PATH):
    if SKIP_SUBMODULES:
        warn("Skipping initialization of columnstore submodules")
    else:
        message("Initialization of columnstore submodules")
        cwd = os.getcwd()
        try:
            os.chdir(MDB_SOURCE_PATH)
            run("git submodule update --init --recursive")
        finally:
            os.chdir(cwd)

def stop_service():
    message_split()
    message("Stopping MariaDB services")
    run("systemctl stop mariadb", check=False)
    run("systemctl stop mariadb-columnstore", check=False)
    run("systemctl stop mcs-storagemanager", check=False)

import shutil

def clean_old_installation(REPORT_PATH, DATA_DIR, CONFIG_DIR):
    message_split()
    message("Cleaning old installation")
    paths = [
        '/var/lib/columnstore/data1/*',
        '/var/lib/columnstore/data/',
        '/var/lib/columnstore/local/',
        '/var/lib/columnstore/storagemanager/*',
        '/var/log/mariadb/columnstore/*',
        '/tmp/*',
        REPORT_PATH,
        '/var/lib/mysql',
        '/var/run/mysqld',
        DATA_DIR,
        CONFIG_DIR
    ]
    for p in paths:
        for path in [str(x) for x in Path('/').glob(p.lstrip('/'))]:
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
                elif os.path.isfile(path):
                    os.remove(path)
            except Exception as e:
                warn(f"Could not remove {path}: {e}")

def check_debian_install_file():
    warn("check_debian_install_file stubbed")

def generate_svgs():
    warn("generate_svgs stubbed")

def check_errorcode():
    warn("check_errorcode stubbed")

def build_binary(MARIA_BUILD_PATH, MDB_SOURCE_PATH, FORCE_CMAKE_CONFIG, CMAKE_BIN_NAME, MDB_CMAKE_FLAGS, CPUS):
    MARIA_BUILD_PATH = Path(MARIA_BUILD_PATH).resolve()
    message_split()
    message(f"Building sources in mode")
    message(f"Compiled artifacts will be written to {MARIA_BUILD_PATH}")
    MARIA_BUILD_PATH.mkdir(parents=True, exist_ok=True)
    os.chdir(MDB_SOURCE_PATH)
    if FORCE_CMAKE_CONFIG:
        warn("Erasing cmake cache")
        cache = MARIA_BUILD_PATH / 'CMakeCache.txt'
        files = MARIA_BUILD_PATH / 'CMakeFiles'
        if cache.exists():
            cache.unlink()
        if files.exists():
            shutil.rmtree(files, ignore_errors=True)
    message("Configuring cmake silently")
    run(f"{CMAKE_BIN_NAME} {' '.join(MDB_CMAKE_FLAGS)} -S{MDB_SOURCE_PATH} -B{MARIA_BUILD_PATH}")
    message_split()
    check_debian_install_file()
    generate_svgs()
    run(f"{CMAKE_BIN_NAME} --build {MARIA_BUILD_PATH} -j {CPUS}")
    message("Installing silently")
    run(f"{CMAKE_BIN_NAME} --install {MARIA_BUILD_PATH}")
    check_errorcode()
    message("Adding symbol link to compile_commands.json to the source root")
    src = MARIA_BUILD_PATH / 'compile_commands.json'
    dst = Path(MDB_SOURCE_PATH) / 'compile_commands.json'
    if src.exists():
        if dst.exists():
            dst.unlink()
        dst.symlink_to(src)

def run_unit_tests(SKIP_UNIT_TESTS, MARIA_BUILD_PATH, CTEST_BIN_NAME):
    message_split()
    if SKIP_UNIT_TESTS:
        warn("Skipping unittests")
    else:
        message("Running unittests")
        cwd = os.getcwd()
        try:
            os.chdir(MARIA_BUILD_PATH)
            run(f"{CTEST_BIN_NAME} . -R columnstore: -j {os.cpu_count()} --progress --output-on-failure")
        finally:
            os.chdir(cwd)

def run_microbenchmarks_tests(RUN_BENCHMARKS, MARIA_BUILD_PATH, CTEST_BIN_NAME):
    message_split()
    if not RUN_BENCHMARKS:
        warn("Skipping microbenchmarks")
    else:
        message("Runnning microbenchmarks")
        cwd = os.getcwd()
        try:
            os.chdir(MARIA_BUILD_PATH)
            run(f"{CTEST_BIN_NAME} . -V -R columnstore_microbenchmarks: -j {os.cpu_count()} --progress")
        finally:
            os.chdir(cwd)

def make_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)

import pwd
import grp

def check_user_and_group(user_or_group):
    # Try user first, then group
    try:
        pwd.getpwnam(user_or_group)
        message(f"User '{user_or_group}' exists.")
        return True
    except KeyError:
        try:
            grp.getgrnam(user_or_group)
            message(f"Group '{user_or_group}' exists.")
            return True
        except KeyError:
            # Try to create as user, fallback to group
            try:
                run(f"groupadd {user_or_group}", check=True)
                message(f"Group '{user_or_group}' created.")
            except Exception:
                pass
            try:
                run(f"useradd -r -g {user_or_group} {user_or_group}", check=True)
                message(f"User '{user_or_group}' created.")
            except Exception as e:
                warn(f"Failed to create user/group '{user_or_group}': {e}")
                return False
    return True

def fix_config_files(MDB_SOURCE_PATH=None, CONFIG_DIR=None, OS=None):
    # Example: copy default config if not present
    import shutil
    default_cnf = Path(MDB_SOURCE_PATH or '.') / 'storage/columnstore/columnstore/oam/etc/Columnstore.xml'
    target_cnf = Path(CONFIG_DIR or '/etc/columnstore') / 'Columnstore.xml'
    try:
        if not target_cnf.exists() and default_cnf.exists():
            target_cnf.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(str(default_cnf), str(target_cnf))
            message(f"Copied {default_cnf} to {target_cnf}")
        else:
            message(f"Config file {target_cnf} already exists or source missing.")
    except Exception as e:
        warn(f"Failed to fix config files: {e}")
        return False
    return True

import psutil

def start_storage_manager_if_needed(MDB_SOURCE_PATH=None, CONFIG_DIR=None):
    # Check if storage manager process is running, start if not
    storage_manager_name = 'mcs-storagemanager'
    running = any(storage_manager_name in p.name() for p in psutil.process_iter(attrs=['name']))
    if running:
        message(f"Storage manager '{storage_manager_name}' already running.")
        return True
    try:
        run(f"systemctl start {storage_manager_name}", check=True)
        message(f"Started storage manager '{storage_manager_name}'.")
        return True
    except Exception as e:
        warn(f"Failed to start storage manager '{storage_manager_name}': {e}")
        return False

def install(RECOMPILE_ONLY, EUID, REPORT_PATH, CONFIG_DIR, MDB_SOURCE_PATH, OS, DEBCONFIG_DIR, INSTALL_PREFIX, DATA_DIR):
    if not RECOMPILE_ONLY:
        if EUID != 0:
            error("Please run script as root to install MariaDb to system paths")
            sys.exit(1)
        message_split()
        message("Installing MariaDB")
        # disable_plugins_for_bootstrap stubbed
        make_dir(REPORT_PATH)
        run(f"chmod 777 {REPORT_PATH}")
        check_user_and_group("mysql")
        check_user_and_group("syslog")
        make_dir(CONFIG_DIR)
        with open(f"{CONFIG_DIR}/socket.cnf", "w") as f:
            f.write("[client-server]\n    socket=/run/mysqld/mysqld.sock\n")
        make_dir("/var/lib/mysql")
        message("Running mysql_install_db")
        run("sudo -u mysql mysql_install_db --rpm --user=mysql > /dev/null")
        # enable_columnstore_back stubbed
        make_dir("/etc/columnstore")
        run(f"cp {MDB_SOURCE_PATH}/storage/columnstore/columnstore/oam/etc/Columnstore.xml /etc/columnstore/Columnstore.xml")
        run(f"cp {MDB_SOURCE_PATH}/storage/columnstore/columnstore/storage-manager/storagemanager.cnf /etc/columnstore/storagemanager.cnf")
        run(f"cp {MDB_SOURCE_PATH}/storage/columnstore/columnstore/oam/install_scripts/*.service /lib/systemd/system/")
        if "ubuntu" in OS or "debian" in OS:
            make_dir("/usr/share/mysql")
            make_dir("/etc/mysql/")
            run(f"cp {MDB_SOURCE_PATH}/debian/additions/debian-start.inc.sh /usr/share/mysql/debian-start.inc.sh")
            run(f"cp {MDB_SOURCE_PATH}/debian/additions/debian-start /etc/mysql/debian-start")
            Path("/etc/mysql/debian.cnf").touch()
        fix_config_files()
        if Path(DEBCONFIG_DIR).is_dir():
            message(f"Copying configs from {DEBCONFIG_DIR} to {CONFIG_DIR}")
            run(f"cp -rp {DEBCONFIG_DIR}/* {CONFIG_DIR}")
        make_dir("/var/lib/columnstore/data1")
        make_dir("/var/lib/columnstore/data1/systemFiles")
        make_dir("/var/lib/columnstore/data1/systemFiles/dbrm")
        make_dir("/run/mysqld/")
        make_dir(DATA_DIR)
        run(f"chmod +x {INSTALL_PREFIX}/bin/mariadb*")
        run("ldconfig")
        start_storage_manager_if_needed()
        message("Running columnstore-post-install")
        make_dir("/var/lib/columnstore/local")
        run("columnstore-post-install --rpmmode=install")
        message("Running install_mcs_mysql")
        run("install_mcs_mysql.sh")
    run("chown -R syslog:syslog /var/log/mariadb/")
    run("chmod 777 /var/log/mariadb/")
    run("chmod 777 /var/log/mariadb/columnstore")

import time

def check_service(service, retries=3, delay=2):
    for attempt in range(1, retries+1):
        result = subprocess.run(f"systemctl is-active {service}", shell=True, capture_output=True, text=True)
        status = result.stdout.strip()
        if status == 'active':
            message(f"Service '{service}' is active.")
            return True
        else:
            warn(f"Service '{service}' not active (attempt {attempt}/{retries}), status: {status}")
            if attempt < retries:
                time.sleep(delay)
    error(f"Service '{service}' failed to become active after {retries} attempts.")
    return False

def start_service():
    message_split()
    message("Starting MariaDB services")
    run("systemctl start mariadb-columnstore", check=False)
    run("systemctl start mariadb", check=False)
    for svc in ["mariadb", "mariadb-columnstore", "mcs-controllernode", "mcs-ddlproc", "mcs-dmlproc", "mcs-primproc", "mcs-workernode@1", "mcs-writeengineserver"]:
        check_service(svc)

def smoke(SKIP_SMOKE, MDB_SOURCE_PATH):
    if not SKIP_SMOKE:
        message_split()
        message("Creating test database")
        run("mariadb -e 'create database if not exists test;'")
        message("Selecting magic numbers")
        sql_path = Path(MDB_SOURCE_PATH) / "storage/columnstore/columnstore/tests/scripts/smoke.sql"
        result = subprocess.run(f"mysql -N test < {sql_path}", shell=True, capture_output=True, text=True)
        MAGIC = result.stdout.strip()
        if MAGIC == '42':
            message("Great answer correct!")
        else:
            warn(f"Smoke failed, answer is '{MAGIC}'")

def disable_git_restore_frozen_revision(MDB_SOURCE_PATH):
    cwd = os.getcwd()
    try:
        os.chdir(MDB_SOURCE_PATH)
        run("git config submodule.storage/columnstore/columnstore.update none")
    finally:
        os.chdir(cwd)

def modify_packaging(OS, MDB_SOURCE_PATH):
    print("Modifying packaging...")
    cwd = os.getcwd()
    try:
        os.chdir(MDB_SOURCE_PATH)
        # Stub: pkg_format detection
        pkg_format = 'deb' if 'debian' in OS or 'ubuntu' in OS else 'rpm'
        if pkg_format == "deb":
            run("sed -i 's|.*-d storage/columnstore.*|elif [[ -d storage/columnstore/columnstore/debian ]]|' debian/autobake-deb.sh")
        if OS in ['ubuntu:22.04', 'ubuntu:24.04']:
            for i in ["mariadb-plugin-columnstore", "mariadb-server", "mariadb-server-core", "mariadb", "mariadb-10.6"]:
                run(f"echo '{i} any' >> /usr/share/lto-disabled-list/lto-disabled-list")
            run("grep mariadb /usr/share/lto-disabled-list/lto-disabled-list")
        if pkg_format == "deb":
            run("apt-cache madison liburing-dev | grep liburing-dev || { sed 's/liburing-dev/libaio-dev/g' -i debian/control && sed '/-DIGNORE_AIO_CHECK=YES/d' -i debian/rules && sed '/-DWITH_URING=yes/d' -i debian/rules; }")
            run("apt-cache madison libpmem-dev | grep 'libpmem-dev' || { sed '/libpmem-dev/d' -i debian/control && sed '/-DWITH_PMEM/d' -i debian/rules; }")
            run("sed '/libfmt-dev/d' -i debian/control")
            run("sed -i '/-DPLUGIN_COLUMNSTORE=NO/d' debian/rules")
            run("sed -i 's/--fail-missing/--list-missing/' debian/rules")
            for i in ["mariadb-plugin", "libmariadbd"]:
                run(f"sed -i '/Package: {i}.*/,/^$/d' debian/control")
            run("sed -i 's/Depends: galera.*/Depends:/' debian/control")
            for i in ["galera", "wsrep", "ha_sphinx", "embedded"]:
                run(f"sed -i '/{i}/d' debian/*.install")
    finally:
        os.chdir(cwd)

def build_package(OS, MDB_SOURCE_PATH, MDB_CMAKE_FLAGS):
    cwd = os.getcwd()
    try:
        os.chdir(MDB_SOURCE_PATH)
        pkg_format = 'deb' if 'debian' in OS or 'ubuntu' in OS else 'rpm'
        if pkg_format == 'rpm':
            command = f"cmake {' '.join(MDB_CMAKE_FLAGS)} && make -j$(nproc) package"
        else:
            command = (
                "export DEBIAN_FRONTEND=noninteractive; "
                "export DEB_BUILD_OPTIONS=parallel=$(nproc); "
                "export DH_BUILD_DDEBS=1; "
                "export BUILDPACKAGE_FLAGS=-b; "
                "mk-build-deps debian/control -t 'apt-get -y -o Debug::pkgProblemResolver=yes --no-install-recommends' -r -i && "
                f'CMAKEFLAGS="{' '.join(MDB_CMAKE_FLAGS)}" debian/autobake-deb.sh'
            )
        print(f"Building a package for {OS}")
        print(f"Build command: {command}")
        run(command)
        check_errorcode()
    finally:
        os.chdir(cwd)

def main():
    args = parse_args()
    message(f"Arguments received: {sys.argv[1:]}")

    # Set up variables from args and defaults
    ASAN = args.asan
    MARIA_BUILD_PATH = Path(args.build_path).resolve()
    RUN_BENCHMARKS = args.run_microbench
    CLOUD_STORAGE_ENABLED = args.cloud
    FORCE_CMAKE_CONFIG = args.force_cmake_reconfig
    OS = args.distro
    INSTALL_DEPS = args.install_deps
    CUSTOM_CMAKE_FLAGS = args.custom_cmake_flags
    DO_NOT_FREEZE_REVISION = args.do_not_freeze_revision
    MAINTAINER_MODE = not args.alien
    DRAW_DEPS = args.draw_deps
    CPUS = args.parallel
    SKIP_SMOKE = args.skip_smoke
    USE_NINJA = args.ninja
    NO_CLEAN = args.no_clean_install
    # Reasonable defaults for unmapped variables
    BUILD_PACKAGES = False
    RESTART_SERVICES = True
    RECOMPILE_ONLY = False
    SKIP_UNIT_TESTS = False
    STATIC_BUILD = False
    TSAN = False
    UBSAN = False
    WITHOUT_COREDUMPS = False
    MAKEFILE_VERBOSE = False
    SCCACHE = False
    REPORT_PATH = "/tmp/mcs_report"
    DEBCONFIG_DIR = "/etc/mysql/mariadb.conf.d"
    CMAKE_BIN_NAME = "cmake"
    CTEST_BIN_NAME = "ctest"
    INSTALL_PREFIX = "/usr/"
    DATA_DIR = "/var/lib/mysql/data"
    CONFIG_DIR = "/etc/my.cnf.d"
    MDB_SOURCE_PATH = (Path(__file__).parent / '../../../..').resolve()
    MDB_SOURCE_PATH = str(MDB_SOURCE_PATH)
    # Compose env_vars for construct_cmake_flags
    env_vars = {
        'MCS_BUILD_TYPE': 'RelWithDebInfo',
        'INSTALL_PREFIX': INSTALL_PREFIX,
        'MAINTAINER_MODE': MAINTAINER_MODE,
        'SKIP_UNIT_TESTS': SKIP_UNIT_TESTS,
        'DRAW_DEPS': DRAW_DEPS,
        'DEP_GRAPH_PATH': str(MARIA_BUILD_PATH) + "/dependency_graph/mariadb.dot",
        'USE_NINJA': USE_NINJA,
        'STATIC_BUILD': STATIC_BUILD,
        'ASAN': ASAN,
        'TSAN': TSAN,
        'UBSAN': UBSAN,
        'WITHOUT_COREDUMPS': WITHOUT_COREDUMPS,
        'MAKEFILE_VERBOSE': MAKEFILE_VERBOSE,
        'SCCACHE': SCCACHE,
        'RUN_BENCHMARKS': RUN_BENCHMARKS,
        'OS': OS,
        'CUSTOM_CMAKE_FLAGS': CUSTOM_CMAKE_FLAGS,
        'REPORT_PATH': REPORT_PATH,
    }

    if INSTALL_DEPS:
        install_deps(OS, "11")

    if not DO_NOT_FREEZE_REVISION:
        disable_git_restore_frozen_revision(MDB_SOURCE_PATH)

    MDB_CMAKE_FLAGS = construct_cmake_flags(args, env_vars)
    init_submodules(False, MDB_SOURCE_PATH)

    if not BUILD_PACKAGES:
        stop_service()
        if not NO_CLEAN:
            clean_old_installation(REPORT_PATH, DATA_DIR, CONFIG_DIR)
        build_binary(MARIA_BUILD_PATH, MDB_SOURCE_PATH, FORCE_CMAKE_CONFIG, CMAKE_BIN_NAME, MDB_CMAKE_FLAGS, CPUS)
        run_unit_tests(SKIP_UNIT_TESTS, MARIA_BUILD_PATH, CTEST_BIN_NAME)
        run_microbenchmarks_tests(RUN_BENCHMARKS, MARIA_BUILD_PATH, CTEST_BIN_NAME)
        install(RECOMPILE_ONLY, os.geteuid(), REPORT_PATH, CONFIG_DIR, MDB_SOURCE_PATH, OS, DEBCONFIG_DIR, INSTALL_PREFIX, DATA_DIR)
        if RESTART_SERVICES:
            start_service()
            smoke(SKIP_SMOKE, MDB_SOURCE_PATH)
    else:
        modify_packaging(OS, MDB_SOURCE_PATH)
        build_package(OS, MDB_SOURCE_PATH, MDB_CMAKE_FLAGS)
    message_splitted("FINISHED")

if __name__ == "__main__":
    main()
