#!/usr/bin/env python3

# pip3 install paramiko scp python-util

"""
./run_mtr.py  --machine-name c --target 10.4-enterprise-2020-May-12-04-00-00 --reposerver mdbe-ci-repo.mariadb.net --reposerver-passfile ~/.config/mdbci/repo_server_pass --mtr-param mtr-normal-test --log-server max-tst-01.mariadb.com --log-server-user vagrant --log-path /home/vagrant/LOGS/c01
"""

import pathlib
from argparse import ArgumentParser
import os
from scp import SCPClient
import shutil
import requests
import re
import time

from mdb_buildbot_scripts.methods import versions_processing
from mdb_buildbot_scripts.operations import host
from mdb_buildbot_scripts.operations import sdk_repo_logs
from mdb_buildbot_scripts.operations import ssh_tools
from mdb_buildbot_scripts.operations import mdbci
from mdb_buildbot_scripts.constants import mariadb_tests
from mdb_buildbot_scripts.constants import cli_commands
from mdb_buildbot_scripts.constants import ci_repository
from mdb_buildbot_scripts.constants import products_names
from mdb_buildbot_scripts.constants import products_versions

DEFAULT_DATA_DIR = '/var/tmp/mtr'
MOUNTED_DATA_DIR = '/filesystem_mount'
LOG_NAME = 'logs.tar.gz'
LOG_NAME_FS = 'logs_fs.tar.gz'
DEVICE_PATHS = {
    'gcp': '/dev/disk/by-id/google-data-disk-0',
    'aws': '/dev/sdh',
}
ADDITIONAL_LOGS = {
    LOG_NAME: DEFAULT_DATA_DIR,
    'var_log_mariadb.tar.gz': '/var/log/mariadb',
    'var_log_mysql.tar.gz': '/usr/share/mysql/mysql-test/logs.tar.gz',
    'syslog.tar.gz': '/var/log/syslog',
    'messages.tar.gz': '/var/log/messages',
    'core_dumps.tar.gz': '/tmp/core*',
    LOG_NAME_FS: MOUNTED_DATA_DIR,
}

MTR = {
    'mtr-empty': [" "],

    'mtr-big-test': [" --max-test-fail=0 --big-test --retry=3 --parallel=auto "
                    "--max-save-core=1 --max-save-datadir=1  --verbose-restart "],

    'mtr-galera-test': [" --suite=galera,wsrep,galera_3nodes --max-test-fail=0 "
                       "--max-save-core=1 --max-save-datadir=1  --verbose-restart "
                       "--testcase-timeout=20 --big-test --retry=3 --parallel=1 "
                       "--max-save-core=1 --max-save-datadir=1  --verbose-restart "],

    'mtr-normal-test': [" --max-test-fail=0 --retry=3 --parallel=auto "
                       "--max-save-core=1 --max-save-datadir=1  --verbose-restart "],

    'mtr-psproto-test': [" --ps-protocol --max-test-fail=0 --retry=3 --parallel=auto "
                        "--max-save-core=1 --max-save-datadir=1  --verbose-restart "],

    'mtr-extra-test': [" --suite=funcs_1,funcs_2,stress,jp --testcase-timeout=120 "
                      "--max-save-core=1 --max-save-datadir=1  --verbose-restart "
                      "--mysqld=--open-files-limit=0 --mysqld=--log-warnings=1 --max-test-fail=0 "
                      "--retry=3 --parallel=auto --max-save-core=1 --max-save-datadir=1  --verbose-restart "],

    'mtr-engines-test': [" --suite=spider,spider/bg,engines/funcs,engines/iuds --testcase-timeout=20 "
                        "--mysqld=--open-files-limit=0 --mysqld=--log-warnings=1 --max-test-fail=0 "
                        "--retry=3 --parallel=auto --max-save-core=1 --max-save-datadir=1  --verbose-restart "],

    'mtr-spider-odbc-test': [" --suite=spider/odbc/mariadb --big-test --max-test-fail=0 "
                            "--retry=3 --parallel=1 --max-save-core=1 --max-save-datadir=1 --verbose-restart "],
    'mtr-spider-sqlite-test': [" --suite=spider/odbc/sqlite --big-test --max-test-fail=0 "
                            "--retry=3 --parallel=1 --max-save-core=1 --max-save-datadir=1 --verbose-restart "],

    'mtr-spider-odbc-oracle-test': [" --suite=spider/odbc/oracle --big-test --max-test-fail=0 "
                                   "--retry=3 --parallel=1 --max-save-core=1 --max-save-datadir=1 --verbose-restart "],

    'mtr-postgres-test': [" --suite=spider/odbc/pg --big-test --max-test-fail=0 --retry=3 --parallel=1 "
                         "--max-save-core=1 --max-save-datadir=1 --verbose-restart "],

    'mtr-rocksdb-test': [" --suite=rocksdb* --skip-test=rocksdb_hotbackup* --big-test "
                        "--max-save-core=1 --max-save-datadir=1 --verbose-restart "],

    'mtr-columnstore-test': [" --suite=columnstore/setup "
                            "--skip-test=rocksdb_hotbackup* --big-test "
                            "--max-save-core=1 --max-save-datadir=1 --verbose-restart --max-test-fail=0 ",

                            " --suite=columnstore/basic,columnstore/bugfixes,columnstore/devregression,"
                            "columnstore/autopilot,columnstore/extended,"
                            "columnstore/multinode,columnstore/oracle,columnstore/1pmonly "
                            "--skip-test=rocksdb_hotbackup* --big-test "
                            "--max-save-core=1 --max-save-datadir=1 --verbose-restart --max-test-fail=0 "],

}

GALERA_FULL_TEST_ARGS = [" --suite=galera,wsrep,galera_3nodes,galera_sr,galera_3nodes_sr "
                         "--testcase-timeout=20 --big-test --max-test-fail=0  --parallel=1"]

UTIL_PASS = 'ect436c3rf34f5#ct570c3521fCCR'
UTIL_USER = 'util'

# Please do not remove this commented code (it is possible we will need Xpand tests again
# CLUSTRIX_SETUP = [
#    'CREATE USER \'repl\'@\'%\' IDENTIFIED BY \'repl\'',
#    'GRANT ALL ON *.* TO \'repl\'@\'%\' WITH GRANT OPTION',
# ]

psql_config = """# "local" is for Unix domain socket connections only
local   all             all                                     password
local   all             postgres                                ident
# IPv4 local connections:
host    all             all             127.0.0.1/32            password
host    all             postgres        127.0.0.1/32            ident
# IPv6 local connections:
host    all             all             ::1/128                 password"""

ODBC_VERSION = "3.1.15"

odbc_config = """[postgres_mysql]
Description=test sample db
Driver=PostgreSQL
Trace=No
TraceFile=sql.log
Database=test
Servername=
UserName=mtr
Password=mtr
Port=5432
ReadOnly=No
RowVersioning=No
ShowSystemTables=No
ShowOidColumn=No
FakeOidIndex=No

[postgres_auto_test_remote]
Description=auto_test_remote
Driver=PostgreSQL
Trace=No
TraceFile=auto_test_remote.log
Database=auto_test_remote
Servername=
UserName=mtr
Password=mtr
Port=5432
ReadOnly=No
RowVersioning=No
ShowSystemTables=No
ShowOidColumn=No
FakeOidIndex=No

[postgres_auto_test_remote2]
Description=auto_test_remote2
Driver=PostgreSQL
Trace=No
TraceFile=auto_test_remote2.log
Database=auto_test_remote2
Servername=
UserName=mtr
Password=mtr
Port=5432
ReadOnly=No
RowVersioning=No
ShowSystemTables=No
ShowOidColumn=No
FakeOidIndex=No

[postgres_auto_test_remote3]
Description=auto_test_remote3
Driver=PostgreSQL
Trace=No
TraceFile=auto_test_remote3.log
Database=auto_test_remote3
Servername=
UserName=mtr
Password=mtr
Port=5432
ReadOnly=No
RowVersioning=No
ShowSystemTables=No
ShowOidColumn=No
FakeOidIndex=No"""

odbcinst_config = """[MariaDB ODBC 3.0 Driver]
Description     = ODBC for MariaDB
Driver64        = /usr/{}/libmaodbc.so
FileUsage       = 1"""

odbcinst_sqlite = """ [SQLite3]
Description = SQLite3 ODBC Driver
Driver      = /usr/local/lib/libsqlite3odbc.so
Setup       = /usr/local/lib/libsqlite3odbc.so
UsageCount  = 1"""


odbcinst_postrges = """[PostgreSQL]
Description     = ODBC for PostgreSQL
Driver64        = /usr/lib64/psqlodbca.so
FileUsage       = 1"""

odbcinst_postrges_sles12 = """[PostgreSQL]
Description     = ODBC for PostgreSQL
Driver64        = /usr/pgsql-12/lib/psqlodbc.so
FileUsage       = 1"""

postgres_install_yum = """
sudo yum install -y postgresql-odbc postgresql-server postgresql"""

postgres_install_apt = """
sudo apt install -y --force-yes odbc-postgresql postgresql postgresql-contrib"""

postgres_install_zypper15 = """
sudo zypper install -n -y psqlODBC postgresql-server postgresql"""

postgres_install_zypper12 = """
sudo zypper addrepo https://download.postgresql.org/pub/repos/zypp/repo/pgdg-sles-12.repo
sudo zypper --gpg-auto-import-keys refresh
sudo zypper install -n -y postgresql12-odbc postgresql12-server postgresql12"""

posgres_config_name_default = '/var/lib/pgsql/data/pg_hba.conf'

POSTGRES_CONFIG_NAMES = {
    'debian-stretch': '/etc/postgresql/9.6/main/pg_hba.conf',
    'debian-buster': '/etc/postgresql/11/main/pg_hba.conf',
    'debian-bullseye': '/etc/postgresql/13/main/pg_hba.conf',
    'debian-bookworm': '/etc/postgresql/15/main/pg_hba.conf',
    'ubuntu-jammy': '/etc/postgresql/14/main/pg_hba.conf',
    'ubuntu-focal': '/etc/postgresql/12/main/pg_hba.conf',
    'ubuntu-bionic': '/etc/postgresql/10/main/pg_hba.conf',
    'ubuntu-xenial': '/etc/postgresql/9.5/main/pg_hba.conf',
    'rhel-6': '/var/lib/pgsql/data/pg_hba.conf',
    'rhel-7': '/var/lib/pgsql/data/pg_hba.conf',
    'rhel-8': '/var/lib/pgsql/data/pg_hba.conf',
    'centos-6': '/var/lib/pgsql/data/pg_hba.conf',
    'centos-7': '/var/lib/pgsql/data/pg_hba.conf',
    'centos-8': '/var/lib/pgsql/data/pg_hba.conf',
    'rocky-8': '/var/lib/pgsql/data/pg_hba.conf',
    'sles-12': '/var/lib/pgsql/data/pg_hba.conf',
    'sles-15': '/var/lib/pgsql/data/pg_hba.conf',
}

postgres_config = """
sudo rm -rf /var/lib/pgsql/data
sudo /usr/bin/postgresql-setup initdb
sudo /usr/pgsql-12/bin/postgresql-12-setup initdb
sudo service postgresql restart
sudo service postgresql-12 restart
sudo su - postgres -s /bin/bash -c \"echo CREATE USER mtr WITH ENCRYPTED PASSWORD \\\'mtr\\\' | PATH=$PATH:/usr/pgsql-12/bin/ psql\"
sudo su - postgres -s /bin/bash -c \"echo CREATE DATABASE test | PATH=$PATH:/usr/pgsql-12/bin/ psql\"
sudo su - postgres -s /bin/bash -c \"echo CREATE DATABASE auto_test_remote | PATH=$PATH:/usr/pgsql-12/bin/ psql\"
sudo su - postgres -s /bin/bash -c \"echo CREATE DATABASE auto_test_remote2 | PATH=$PATH:/usr/pgsql-12/bin/ psql\"
sudo su - postgres -s /bin/bash -c \"echo CREATE DATABASE auto_test_remote3 | PATH=$PATH:/usr/pgsql-12/bin/ psql\"
sudo su - postgres -s /bin/bash -c \"echo grant all privileges on database test to mtr | PATH=$PATH:/usr/pgsql-12/bin/ psql\"
sudo su - postgres -s /bin/bash -c \"echo grant all privileges on database auto_test_remote to mtr | PATH=$PATH:/usr/pgsql-12/bin/ psql\"
sudo su - postgres -s /bin/bash -c \"echo grant all privileges on database auto_test_remote2 to mtr | PATH=$PATH:/usr/pgsql-12/bin/ psql\"
sudo su - postgres -s /bin/bash -c \"echo grant all privileges on database auto_test_remote3 to mtr | PATH=$PATH:/usr/pgsql-12/bin/ psql\"
sudo su - postgres -s /bin/bash -c \"echo ALTER DATABASE test OWNER TO mtr | PATH=$PATH:/usr/pgsql-12/bin/ psql\"
sudo su - postgres -s /bin/bash -c \"echo ALTER DATABASE auto_test_remote OWNER TO mtr | PATH=$PATH:/usr/pgsql-12/bin/ psql\"
sudo su - postgres -s /bin/bash -c \"echo ALTER DATABASE auto_test_remote2 OWNER TO mtr | PATH=$PATH:/usr/pgsql-12/bin/ psql\"
sudo su - postgres -s /bin/bash -c \"echo ALTER DATABASE auto_test_remote3 OWNER TO mtr | PATH=$PATH:/usr/pgsql-12/bin/ psql\""""

postgres13_config = """
sudo rm -rf /var/lib/pgsql/data
sudo mkdir -p /var/lib/pgsql/data
sudo chown postgres /var/lib/pgsql/data
sudo su - postgres -s /bin/bash -c \"initdb -D /var/lib/pgsql/data\"
sudo su - postgres -s /bin/bash -c \"pg_ctl -D /var/lib/pgsql/data start\"
sudo su - postgres -s /bin/bash -c \"echo CREATE USER mtr WITH ENCRYPTED PASSWORD \\\'mtr\\\' | psql\"
sudo su - postgres -s /bin/bash -c \"echo CREATE DATABASE test | psql\"
sudo su - postgres -s /bin/bash -c \"echo CREATE DATABASE auto_test_remote | psql\"
sudo su - postgres -s /bin/bash -c \"echo CREATE DATABASE auto_test_remote2 | psql\"
sudo su - postgres -s /bin/bash -c \"echo CREATE DATABASE auto_test_remote3 | psql\"
sudo su - postgres -s /bin/bash -c \"echo grant all privileges on database test to mtr | psql\"
sudo su - postgres -s /bin/bash -c \"echo grant all privileges on database auto_test_remote to mtr | psql\"
sudo su - postgres -s /bin/bash -c \"echo grant all privileges on database auto_test_remote2 to mtr | psql\"
sudo su - postgres -s /bin/bash -c \"echo grant all privileges on database auto_test_remote3 to mtr | psql\"
sudo su - postgres -s /bin/bash -c \"echo ALTER DATABASE test OWNER TO mtr | psql\"
sudo su - postgres -s /bin/bash -c \"echo ALTER DATABASE auto_test_remote OWNER TO mtr | psql\"
sudo su - postgres -s /bin/bash -c \"echo ALTER DATABASE auto_test_remote2 OWNER TO mtr | psql\"
sudo su - postgres -s /bin/bash -c \"echo ALTER DATABASE auto_test_remote3 OWNER TO mtr | psql\""""

postgres_xenial_config = """
sudo rm -rf /var/lib/pgsql/data
sudo mkdir -p /var/lib/pgsql/data
sudo chown postgres /var/lib/pgsql/data
sudo sed -i \"s/ident/trust/\" /etc/postgresql/9.5/main/pg_hba.conf
sudo su - postgres -s /bin/bash -c \"/usr/lib/postgresql/9.5/bin/initdb -D /var/lib/pgsql/data\"
sudo su - postgres -s /bin/bash -c \"/usr/lib/postgresql/9.5/bin/pg_ctl -D /var/lib/pgsql/data start\"
sudo su - postgres -s /bin/bash -c \"echo CREATE USER mtr WITH ENCRYPTED PASSWORD \\\'mtr\\\' | psql\"
sudo su - postgres -s /bin/bash -c \"echo CREATE DATABASE test | psql\"
sudo su - postgres -s /bin/bash -c \"echo CREATE DATABASE auto_test_remote | psql\"
sudo su - postgres -s /bin/bash -c \"echo CREATE DATABASE auto_test_remote2 | psql\"
sudo su - postgres -s /bin/bash -c \"echo CREATE DATABASE auto_test_remote3 | psql\"
sudo su - postgres -s /bin/bash -c \"echo grant all privileges on database test to mtr | psql\"
sudo su - postgres -s /bin/bash -c \"echo grant all privileges on database auto_test_remote to mtr | psql\"
sudo su - postgres -s /bin/bash -c \"echo grant all privileges on database auto_test_remote2 to mtr | psql\"
sudo su - postgres -s /bin/bash -c \"echo grant all privileges on database auto_test_remote3 to mtr | psql\""""

FILESYSTEM_TOOLS = {
    'btrfs': ['btrfs-progs'],
    'f2fs': ['f2fs-tools', 'linux-modules-extra-$(uname -r)'],
    'jfs': ['jfsutils', 'linux-modules-extra-$(uname -r)'],
    'xfs': ['xfsprogs'],
    'zfs': ['zfsutils-linux'],
    'reiserfs': ['reiserfsprogs', 'linux-modules-extra-$(uname -r)'],
}

FORCE_OPTION_MKFS = {
    'btrfs',
    'f2fs',
    'jfs',
    'xfs',
    'zfs',
    'reiserfs',
}

ORACLE_PACKAGES = {
    'aarch64': [
        'https://download.oracle.com/otn_software/linux/instantclient/instantclient-basic-linux-arm64.zip',
        'https://download.oracle.com/otn_software/linux/instantclient/instantclient-odbc-linux-arm64.zip',
    ],
    'x86_64': [
        'https://download.oracle.com/otn_software/linux/instantclient/instantclient-basic-linuxx64.zip',
        'https://download.oracle.com/otn_software/linux/instantclient/instantclient-odbc-linuxx64.zip',
    ],
}


COLUMNSTORE_DATA_FILE_URL = "https://cspkg.s3.amazonaws.com/mtr-test-data.tar.lz4"
#COLUMNSTORE_DATA_FILE_URL = "https://mdbe-ci-repo.mariadb.net/public/mtr-test-data.tar.lz4"


def sshMariaDB(ssh, sql, db=''):
    host.printInfo('SQL: ' + sql)
    setup_log_file = open("results/setup.log", 'w+', errors='ignore')
    r = ssh_tools.interactiveExec(ssh, 'echo \"' + sql + '\" | sudo mysql ' + db,
                                  ssh_tools.printAndSaveText, setup_log_file, get_pty=True)
    setup_log_file.close()
    return r


def sqlMariaDB(ssh, sqls, db=''):
    for sql in sqls:
        sshMariaDB(ssh, sql, db)


def writeConfig(ssh, conf, confName, delete):
    config_log_file = open("results/config.log", 'w+', errors='ignore')
    if delete:
        ssh_tools.interactiveExec(ssh, 'sudo rm -rf ' + confName,
                                  ssh_tools.printAndSaveText, config_log_file, get_pty=True)
        ssh_tools.interactiveExec(ssh, 'sudo touch ' + confName,
                                  ssh_tools.printAndSaveText, config_log_file, get_pty=True)
    for line in conf.split('\n'):
        ssh_tools.interactiveExec(ssh, 'echo ' + line + ' | sudo tee -a ' + confName,
                                  ssh_tools.printAndSaveText, config_log_file, get_pty=True)
    config_log_file.close()


def installGalera(machine, minorVersion, product, galeraProduct, galeraCIVersion):
    if galeraProduct in ['auto', 'none', 'None']:
        # Determine minor version of the server
        galeraProduct = versions_processing.determiteGalera(minorVersion, product)
    # Form default CI version of Galera: mariadb-3.x, mariadb-4.x, es-mariadb-3.x, es-mariadb-4.x
    if galeraCIVersion in ['auto', 'none', 'None']:
        galeraCIVersion = products_versions.DEFAULT_GALERA_VER[galeraProduct]
    mdbci.runMdbci('install_product', '--product=' + products_names.PRODS[galeraProduct],
                   '--product-version=' + galeraCIVersion, machine.fullName)


def installMariaDB(product, target):
    return mdbci.runMdbci('install_product', '--product=' + products_names.PRODS[product],
                          '--product-version=' + target, '--include-unsupported', machine.fullName)


def installMariaDBTest(target):
    return mdbci.runMdbci('install_product', '--product=plugin_mariadb_test',
                          '--product-version=' + target, machine.fullName)


def mountAndFormat(machine, ssh, device, filesystem, directory):
    fs_log_file = open("results/mtr.log", 'w+', errors='ignore')
    res = 0
    if len(device) > 1 and device != 'None':
        devicePath = device
    else:
        devicePath = DEVICE_PATHS.get(machine.provider, '/dev/sdb')
    if filesystem in FILESYSTEM_TOOLS.keys():
        for filesystemTool in FILESYSTEM_TOOLS[filesystem]:
            if ssh_tools.interactiveExec(ssh,
                                         f'sudo {cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]]} '
                                         f'{filesystemTool}',
                                         ssh_tools.printAndSaveText, fs_log_file) != 0:
                res = 1

    if filesystem == 'zfs':
        if ssh_tools.interactiveExec(ssh, f'sudo zpool create {directory.replace("/", "")} {devicePath}',
                                     ssh_tools.printAndSaveText, fs_log_file) != 0:
            res = 1
    else:
        if filesystem in FORCE_OPTION_MKFS:
            fOpt = '-f'
        else:
            fOpt = ''
        if ssh_tools.interactiveExec(ssh, f'sudo mkfs.{filesystem} {fOpt} -q {devicePath}',
                                     ssh_tools.printAndSaveText, fs_log_file) != 0:
            res = 1
        if ssh_tools.interactiveExec(ssh, f'sudo mkdir {directory}',
                                     ssh_tools.printAndSaveText, fs_log_file) != 0:
            res = 1
        if ssh_tools.interactiveExec(ssh, f'sudo mount {devicePath} {directory}',
                                     ssh_tools.printAndSaveText, fs_log_file) != 0:
            res = 1
    if ssh_tools.interactiveExec(ssh, f'sudo chmod a+w {directory}',
                                 ssh_tools.printAndSaveText, fs_log_file) != 0:
        res = 1
    fs_log_file.close()
    return res


def setupOracle(machine, architeture, directory='/opt/oracle/', odbcIniDir='/etc/'):
    ssh = mdbci.createSSH(machine)
    f = open('results/oracle_packages.log', 'w+', errors='ignore')
    ssh_tools.interactiveExec(ssh, f'{cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]]} wget',
                              ssh_tools.printAndSaveText, f)
    ssh_tools.interactiveExec(ssh, f'sudo mkdir -p {directory}',
                              ssh_tools.printAndSaveText, f)
    for package in ORACLE_PACKAGES[architeture]:
        fileName = pathlib.Path(package).name
        ssh_tools.interactiveExec(ssh, f'sudo wget {package} -O {directory}{fileName}',
                                  ssh_tools.printAndSaveText, f)
        ssh_tools.interactiveExec(ssh, f'sudo unzip {directory}{fileName} -d {directory}',
                                  ssh_tools.printAndSaveText, f)

    dirList = []
    ssh_tools.interactiveExec(ssh, f'ls -d {directory}/*/',
                              ssh_tools.addTextToList, dirList)
    for directory in dirList:
        if len(directory) > len(directory) and '/' in directory:
            oracleDir = os.path.dirname(directory)
            break
    ssh_tools.interactiveExec(ssh, f'sudo touch {odbcIniDir}/odbcinst.ini',
                              ssh_tools.printAndSaveText, f)

    if odbcIniDir != "/etc/":
        ssh_tools.interactiveExec(ssh,
                                  f'sudo sed -i "s|/etc/odbc|{odbcIniDir}odbc|g" {oracleDir}/odbc_update_ini.sh',
                                  ssh_tools.printAndSaveText, f)
    ssh_tools.interactiveExec(ssh,
                              f'cd {oracleDir}; sudo ./odbc_update_ini.sh / '
                              f'{oracleDir} "Oracle ODBC driver" "ora" {odbcIniDir}/odbc.ini',
                              ssh_tools.printAndSaveText, f)
    ssh_tools.interactiveExec(ssh,
                              f'sudo sh -c "echo {oracleDir} > /etc/ld.so.conf.d/oracle-instantclient.conf" '
                              f'&& sudo ldconfig',
                              ssh_tools.printAndSaveText, f)
    ssh_tools.interactiveExec(ssh,
                              f'sudo sed -i "/^UserID/d" {odbcIniDir}/odbc.ini;'
                              f'sudo sed -i "/^Password/d" {odbcIniDir}/odbc.ini;'
                              f'sudo sed -i "/^TNSNamesFile/d" {odbcIniDir}/odbc.ini;'
                              f'sudo sed -i "/^ServerName/d" {odbcIniDir}/odbc.ini;',
                              ssh_tools.printAndSaveText, f)
    ssh_tools.interactiveExec(ssh,
                              f'echo "UserID = system" | sudo tee -a {odbcIniDir}/odbc.ini;'
                              f'echo "Password = password" | sudo tee -a {odbcIniDir}/odbc.ini;'
                              f'echo "TNSNamesFile = {oracleDir}/network/admin/tnsnames.ora" |'
                              f' sudo tee -a {odbcIniDir}/odbc.ini;'
                              f'echo "ServerName = ora" | sudo tee -a {odbcIniDir}/odbc.ini;',
                              ssh_tools.printAndSaveText, f)
    ssh_tools.interactiveExec(ssh,
                              f'echo "ora = (DESCRIPTION=" | sudo tee {oracleDir}/network/admin/tnsnames.ora;'
                              f'echo "  (ADDRESS=(PROTOCOL=tcp) (HOST=localhost) (PORT=1521))" | '
                              f'sudo tee -a {oracleDir}/network/admin/tnsnames.ora;'
                              f'echo "  (CONNECT_DATA=(SERVICE_NAME=XE)))" | '
                              f'sudo tee -a {oracleDir}/network/admin/tnsnames.ora',
                              ssh_tools.printAndSaveText, f)

    ssh_tools.interactiveExec(ssh, f'{cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]]} git',
                              ssh_tools.printAndSaveText, f)
    if cli_commands.DISTRO_CLASSES[machine.platform] in ['apt']:
        addDockerOpt = ' -o "--load"'
    else:
        addDockerOpt = ''
    ssh_tools.interactiveExec(ssh, f'git clone https://github.com/oracle/docker-images.git;'
                                   f'cd docker-images/OracleDatabase/SingleInstance/dockerfiles;'
                                   f'./buildContainerImage.sh -x -v 18.4.0 {addDockerOpt}',
                              ssh_tools.printAndSaveText, f)
    dockerOutput = []
    ssh_tools.interactiveExec(ssh,
                              'docker run -d -p 1521:1521 -p 5500:5500 -e ORACLE_PWD="password" '
                              'oracle/database:18.4.0-xe',
                              ssh_tools.addTextToList, dockerOutput)

    for outputPart in dockerOutput:
        if len(outputPart) > 1:
            containerID = outputPart
            break
    host.printInfo(f'Container ID {containerID}')

    for i in range(1, 360):
        logs = []
        ssh_tools.interactiveExec(ssh, f'docker logs {containerID}',
                                  ssh_tools.addTextToList, logs)
        if 'DATABASE IS READY TO USE!' in ' '.join(logs):
            break
        time.sleep(10)

    ssh.close()
    f.close()


def installDocker(machine):
    if mdbci.runMdbci('install_product',
                      '--product=docker',
                      '--product-version=latest',
                      machine.fullName) != 0:
        host.printInfo("Docker installation error")
        return 1
    ssh_tools.sshMachine(ssh, 'sudo usermod -a -G docker mysql')
    time.sleep(10)
    return 0


parser = ArgumentParser()
parser.add_argument("--machine-name", dest="machine_name",
                    help="Name of MDBCI configuration", type=host.machineName)
parser.add_argument("--node-name", dest="node_name", help="Name of node in the MDBCI configuratiom", default="build")
parser.add_argument("--reposerver-pass", dest="reposerver_pass", help="https password")
parser.add_argument("--reposerver", dest="reposerver", help="Repository server name")
parser.add_argument("--mtr-param", dest="mtr_param", help="MTR test suite (defined in MariaDBE-CI/scripts/es-test.sh)")
parser.add_argument("--mtr-string", dest="mtr_string",
                    help="run_test.pl parameters string (if defined, --mtr-param will be ignored")
parser.add_argument("--mtr-name", dest="mtr_name",
                    help="Name of log directory (if mtr-string is defined, "
                         "this parameter will be used instead of mtr_param as dir name")
parser.add_argument("--bintar", dest="bintar",
                    help="Path to the bintar on the repository server")
parser.add_argument("--bintar-file", dest="bintar_file", help="Path to the file with path to bintar, "
                                                              "if defined - all other bintar argumets are ignored")
parser.add_argument("--target", dest="target", help="CI target")
parser.add_argument("--log-server", dest="log_server", help="name of server to put logs")
parser.add_argument("--log-server-user", dest="log_server_user", help="user name to access logs server")
parser.add_argument("--log-path", dest="log_path", help="path on logs server",
                    default=ci_repository.LOGS_BASEPATH)
parser.add_argument("--product", dest="product", help="'MariaDBEnterprise' or 'MariaDBServerCommunity'",
                    default="MariaDBEnterprise")
parser.add_argument("--galera-product", dest="galera_product", help="'MariaDBGalera3', 'MariaDBGalera4' or "
                                                                    "'MariaDBGalera3Enterprise', "
                                                                    "'MariaDBGalera4Enterprise'",
                    default="auto")
parser.add_argument("--galera-ci-version", dest="galera_ci_version", help="CI version ('target') for Galera ",
                    default="auto")

parser.add_argument("--additional-options", dest="additional_options", help="Anything to be added to MTR command line",
                    default="none")
parser.add_argument("--additional-library", dest="additional_library", help="Build with additional lib",
                    default="none")
parser.add_argument("--use-packages", dest="use_packages",
                    type=host.stringArgumentToBool, default=False,
                    help="Use MariaDB installed from packages and package mariadb-test")
parser.add_argument("--install-packages", dest="install_packages",
                    type=host.stringArgumentToBool, default=True,
                    help="Install MariaDB server packages before tests")
parser.add_argument("--architecture", help="Processor architecture (x86_64 / aarch64)", default="x86_64")
parser.add_argument("--build-number", help="Number of the build")
parser.add_argument("--connector-odbc-product", help="ODBC connector - production, staging or ci",
                    default="connector_odbc")
parser.add_argument("--connector-odbc-version", help="ODBC connector version", default="latest")

parser.add_argument("--block-device", help="Block device to mound and format", default="/dev/sdb")
parser.add_argument("--filesystem", help="Filesystem for data (if defined, block device will be mounted and formatted")
parser.add_argument("--multinode", type=host.stringArgumentToBool, default=False,
                    help="Prepare environment for multinode Columnstore MTR")

script_dir = os.path.abspath(os.path.dirname(__file__))

arguments = parser.parse_args()

mdbci.setupMdbciEnvironment()
machine = mdbci.Machine(arguments.machine_name, arguments.node_name)

ssh = ssh_tools.loadSSH()
ssh.connect(machine.ip_address, username=machine.ssh_user, key_filename=machine.ssh_key)
scp = SCPClient(ssh.get_transport())

runCommand = ssh_tools.createRunner(ssh)

if arguments.architecture == "amd64":
    architecture = "x86_64"
else:
    architecture = arguments.architecture

repository_server = arguments.reposerver

runCommand(cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]] + ' wget')
if arguments.use_packages:
    if arguments.install_packages:
        if installMariaDB(arguments.product, arguments.target) != 0:
            exit(1)
        if installMariaDBTest(arguments.target) != 0:
            exit(1)
    runCommand(cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]] + ' patch')
else:
    if not arguments.bintar_file:
        repo_url = 'https://' + arguments.reposerver_pass + '@' + repository_server + '/' + \
                   arguments.product + '/' + arguments.target

        if arguments.bintar:
            bin_tar = arguments.bintar
        else:
            r = requests.get(
                f'{repo_url}/bintar/{machine.platform}/{machine.platform_version}/{architecture}/bintar_name')
            bin_tar = f'{repo_url}/bintar/{machine.platform}/{machine.platform_version}/{architecture}/{r.content.decode("utf-8").rstrip()}'

shutil.rmtree('results', ignore_errors=True)
os.mkdir("results")

if machine.platform not in ['windows']:
    runCommand('sudo rm -rf MariaDBEnterprise')
    runCommand('mkdir MariaDBEnterprise')
    scp.put(script_dir + '/configure_core.sh', remote_path='/home/' + machine.ssh_user + '/')
    runCommand(f"sudo /home/{machine.ssh_user}/configure_core.sh")

    if not arguments.use_packages:
        runCommand('rm -rf *.tar.gz')
        if arguments.bintar_file:
            bin_tar = os.path.basename(arguments.bintar_file)
            host.printInfo("Copying bintar " + bin_tar)
            scp.put(arguments.bintar_file, remote_path='/home/' + machine.ssh_user + '/')
            versionFileName = os.path.dirname(os.path.abspath(arguments.bintar_file)) + '/VERSION'
            scp.put(versionFileName, remote_path='/home/' + machine.ssh_user + '/MariaDBEnterprise/')
            versionFile = open(versionFileName, 'r')
            versionFileContent = versionFile.read()
            minorVersion = int(re.findall("VERSION_MINOR=.*$", versionFileContent, re.MULTILINE)[0].split("=")[1])
            versionFile.close()
        else:
            host.printInfo("Downloading bintar " + bin_tar)
            result = runCommand('wget ' + bin_tar, get_pty=True)
            host.printInfo("wget status: " + str(result))
            runCommand(
                f'wget {repo_url}/bintar/{machine.platform}/{machine.platform_version}/{architecture}/VERSION -P MariaDBEnterprise')
            g = requests.get(f'{repo_url}/bintar/{machine.platform}/{machine.platform_version}/{architecture}/VERSION')
            minorVersion = int(
                re.findall("VERSION_MINOR=.*$", g.content.decode("utf-8"), re.MULTILINE)[0].split("=")[1])

        host.printInfo("unpack")
        runCommand('tar xf *.tar.gz --strip-components=1 -C MariaDBEnterprise')

        host.printInfo("Copying test script to VM")
        scp.put(script_dir + '/es-test.sh', remote_path='/home/' + machine.ssh_user + '/MariaDBEnterprise/scripts/')

        host.printInfo("Installing Galera")
        installGalera(machine, minorVersion, arguments.product, arguments.galera_product, arguments.galera_ci_version)

        runCommand('сhmod +x ~/MariaDBEnterprise/scripts/*.sh')
        pathToMTR = '~/MariaDBEnterprise/mysql-test/'
    else:
        find_test_file = open("results/find_test_file.log", 'w+', errors='ignore')
        ssh_tools.interactiveExec(ssh, 'sudo find /usr/share/ -name mysql-test-run.pl | grep -v lib',
                                  ssh_tools.printAndSaveText, find_test_file, get_pty=True)
        find_test_file.close()
        find_test_file = open("results/find_test_file.log", 'r', errors='ignore')
        test_file = find_test_file.readlines()
        find_test_file.close()
        for l in test_file:
            if 'mysql-test-run.pl' in l:
                pathToMTR = os.path.dirname(l)
                break
        host.printInfo('Path to MTR executable {}'.format(pathToMTR))
        mariadbVersion = ssh_tools.getMariaDBVersion(ssh)
        minorVersion = versions_processing.minorVersion(mariadbVersion)

    result = runCommand('[ -d {}/suite/galera_sr ]'.format(pathToMTR))
    if result == 0:
        MTR['mtr-galera-test'] = GALERA_FULL_TEST_ARGS

    # Please do not remove this commented code (it is possible we will need Xpand tests again
    #    Hack for Xpand test
    #    if arguments.mtr_param == 'mtr-xpand-test':
    #        clustrixMachine = mdbci.Machine(arguments.machine_name, 'clustrix')
    #        clustrixSSH = mdbci.createSSH(clustrixMachine)
    #        runClustrixCommand = ssh_tools.createRunner(clustrixSSH)
    #        host.printInfo('Setup Clustrix')
    #        sqlMariaDB(clustrixSSH, CLUSTRIX_SETUP)
    #        clustrixSSH.close()
    #        # runCommand("sudo sed -i '$aplugin_maturity=experimental'  MariaDBEnterprise/mysql-test/plugin/xpand/xpand/my.cnf")
    #        MTR['mtr-xpand-test'] = '--suite xpand --max-test-fail=0 ' + \
    #                                '--mysqld="--plugin-maturity=experimental" ' + \
    #                                '--mysqld="--xpand_hosts=' + \
    #                                clustrixMachine.ip_address + \
    #                                '" --mysqld="--xpand_port=3306" ' + \
    #                                '--mysqld="--plugin-load=xpand=ha_xpand.so" ' + \
    #                                '--mysqld="--xpand_username=repl" ' + \
    #                                '--mysqld="--xpand_password=repl" ' + \
    #                                '--parallel=auto'

    if arguments.mtr_string:
        mtr = [arguments.mtr_string]
        mtrDir = arguments.mtr_name
    else:
        mtr = MTR[arguments.mtr_param]
        mtrDir = arguments.mtr_param

    # for Spider ODBC test
    if arguments.mtr_param in ['mtr-spider-odbc-test', 'mtr-postgres-test',
                               'mtr-spider-odbc-oracle-test', 'mtr-spider-sqlite-test']:
        if mdbci.runMdbci('install_product',
                          '--product=' + arguments.connector_odbc_product,
                          '--product-version=' + arguments.connector_odbc_version,
                          machine.fullName) != 0:
            host.printInfo("ODBC Connector installation error")
            exit(1)

        # Do not delete this - probably it should be restored when MDBCI is able to use "-o=<repo_name>"
        # if mdbci.runMdbci('install_product',
        #                   '--product=plugin_spider',
        #                   '--product-version=' + arguments.target,
        #                   machine.fullName) != 0:
        #    host.printInfo("Spider plugin installation error")
        #    exit(1)

        if machine.platform in ['sles', 'suse', 'opensuse']:
            odbc_ini_dir = '/etc/unixODBC/'
        else:
            odbc_ini_dir = '/etc/'

        if runCommand('[ -d /usr/lib64 ]') == 0:
            conn_lib = 'lib64'
        else:
            conn_lib = 'lib'

        writeConfig(ssh, odbcinst_config.format(conn_lib), '{}odbcinst.ini'.format(odbc_ini_dir), False)

    if arguments.mtr_param in ['mtr-spider-sqlite-test']:
        #if mdbci.runMdbci('install_product --product=mdbe_build --product-version=latest',
        #                  machine.fullName) != 0:
        #    host.printInfo("MDBE build environment setup error")
        #    exit(1)
        ssh_tools.sshMachine(ssh, cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]] + ' git gcc make')
        ssh_tools.sshMachine(ssh, cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]] + ' sqlite3')
        ssh_tools.sshMachine(ssh, cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]] + ' libsqlite3-dev')
        ssh_tools.sshMachine(ssh, cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]] + ' sqlite')
        ssh_tools.sshMachine(ssh, cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]] + ' sqlite-devel')
        if cli_commands.PACKAGE_TYPE[cli_commands.DISTRO_CLASSES[machine.platform]] in 'DEB':
            ssh_tools.sshMachine(ssh,
                                 cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]] +
                                 ' unixodbc-dev')
        else:
            crb = ''
            if machine.platform in ['rocky'] and machine.platform_version in ['9']:
                crb = ' --enablerepo crb'
            ssh_tools.sshMachine(ssh,
                                 f'{cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]]} '
                                 f'unixODBC-devel {crb}')
        #ssh_tools.sshMachine(ssh, cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]] + ' libsqliteodbc')
        archCfg = ''
        if architecture in ['aarch64']:
            archCfg = ' --build=aarch64-unknown-linux-gnu'
        ssh_tools.sshMachine(ssh, 'git clone https://github.com/softace/sqliteodbc; '
                                  'cd sqliteodbc; '
                                  f'./configure {archCfg}; '
                                  'make; '
                                  'sudo make install')
        writeConfig(ssh, odbcinst_sqlite, '{}odbcinst.ini'.format(odbc_ini_dir), False)


    # Install Docker and setup Oracle stuff
    if arguments.mtr_param == 'mtr-spider-odbc-oracle-test':
        if installDocker(machine) != 0:
            exit(1)
        setupOracle(machine, architecture, directory='/opt/oracle/', odbcIniDir=odbc_ini_dir)

    # Postgress install
    if arguments.mtr_param == 'mtr-postgres-test':
        if machine.platform in ['rhel', 'centos', 'rocky']:
            ssh_tools.sshMachine(ssh, postgres_install_yum)
        if machine.platform in ['ubuntu', 'debian']:
            ssh_tools.sshMachine(ssh, postgres_install_apt)
        if machine.platform in ['sles', 'opensuse', 'suse']:
            if machine.platform_version == "12":
                ssh_tools.sshMachine(ssh, postgres_install_zypper12)
                writeConfig(ssh, odbcinst_postrges_sles12, '{}odbcinst.ini'.format(odbc_ini_dir), False)
            else:
                ssh_tools.sshMachine(ssh, postgres_install_zypper15)
                writeConfig(ssh, odbcinst_postrges, '{}odbcinst.ini'.format(odbc_ini_dir), False)
        setup_log_file = open("results/setup.log", 'w+', errors='ignore')
        ssh_tools.interactiveExec(ssh, 'sudo sed -i \"s/PostgreSQL ANSI/PostgreSQL/\" {}odbcinst.ini'.format(
            odbc_ini_dir),
                                  ssh_tools.printAndSaveText, setup_log_file, get_pty=True)
        ssh_tools.interactiveExec(ssh, 'sudo sed -i \"s/PSQL/PostgreSQL/\" {}odbcinst.ini'.format(odbc_ini_dir),
                                  ssh_tools.printAndSaveText, setup_log_file, get_pty=True)
        setup_log_file.close()
        postgres_config_cmd = postgres_config
        if (machine.platform in ['sles', 'opensuse', 'suse'] and machine.platform_version == "15") or \
                (machine.platform in ['ubuntu'] and machine.platform_version == "jammy"):
            postgres_config_cmd = postgres13_config
        if machine.platform in ['ubuntu'] and machine.platform_version == "xenial":
            postgres_config_cmd = postgres_xenial_config
        ssh_tools.sshMachine(ssh, postgres_config_cmd)
        posgres_config_name = posgres_config_name_default
        platformFull = machine.platform + '-' + machine.platform_version
        if platformFull in list(POSTGRES_CONFIG_NAMES.keys()):
            posgres_config_name = POSTGRES_CONFIG_NAMES[platformFull]
        writeConfig(ssh, psql_config, posgres_config_name, True)
        writeConfig(ssh, odbc_config, '{}odbc.ini'.format(odbc_ini_dir), True)
        runCommand('sudo service postgresql restart')
        runCommand('sudo service postgresql-12 restart')

    socketPath = ''
    testUser = 'mysql'
    # fetch data for Columnstore tests
    if arguments.mtr_param in ['mtr-columnstore-test']:
        mdbci.runMdbci('install_product', '--product=mdbe_build', '--product-version=latest', machine.fullName)
        runCommand(cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]] + ' lz4')
        runCommand(f"wget -qO- {COLUMNSTORE_DATA_FILE_URL} | lz4 -dc - | tar xf - -C .")
        runCommand("sudo mv data /")
        runCommand("sudo mkdir -p  /usr/share/mysql/mysql-test/suite/columnstore/std_data")
        runCommand("sudo mkdir -p  /usr/share/mysql-test/suite/columnstore/std_data")
        runCommand("sudo cp -r /usr/share/mysql/mysql-test/plugin/columnstore/columnstore/std_data/*  "
                   "/usr/share/mysql/mysql-test/suite/columnstore/std_data/")
        runCommand("sudo cp -r /usr/share/mysql-test/plugin/columnstore/columnstore/std_data/*  "
                   "/usr/share/mysql/mysql-test/suite/columnstore/std_data/")
        runCommand("sudo cp -r /usr/share/mysql/mysql-test/suite/columnstore/std_data/*  "
                   "/usr/share/mysql-test/suite/columnstore/std_data/")
        runCommand("sudo setenforce permissive")
        runCommand("sudo systemctl disable apparmor")
        runCommand("sudo bash -c 'sed -i /ProtectSystem/d $(systemctl "
                   "show --property FragmentPath mariadb | sed s/FragmentPath=//)'")
        if arguments.multinode:
            runCommand("sudo mariadb -e \"create database if not exists test;\"; ")
        else:
            runCommand("sudo systemctl daemon-reload; "
                       "sudo systemctl start mariadb; "
                       "sudo mariadb -e \"create database if not exists test;\"; "
                       "sudo systemctl unmask mariadb-columnstore; "
                       "sudo systemctl restart mariadb-columnstore; "
                       "sudo systemctl status mariadb-columnstore")
        sockL = []
        ssh_tools.interactiveExec(ssh, 'sudo lsof -p `pidof mariadbd` | grep "\.sock"',
                                  ssh_tools.addTextToList, sockL,
                                  get_pty=True)
        for s in sockL[0].split():
            if '.sock' in s:
                socketPath = f'--extern socket={s}'
                break
        host.printInfo(f'Socket path {socketPath}')
        testUser = 'root'

    if (not arguments.additional_options == "") and ("none" not in arguments.additional_options.lower()):
        mtr += " " + arguments.additional_options

    additional_lib_cmd = ''
    if arguments.additional_library == "msan":
        mdbci.installMSAN(ssh)
        additional_lib_cmd = ' LD_LIBRARY_PATH=/usr/local/lib MSAN=Y '

    host.printInfo("Running tests: " + " ;".join(mtr))
    mtr_log_file = open("results/mtr.log", 'w+', errors='ignore')
    esDir = '~/MariaDBEnterprise/'
    if (arguments.filesystem and len(arguments.filesystem) > 1 and arguments.filesystem
            in mariadb_tests.FILESYSTEMS_LIST):
        dataDirectory = MOUNTED_DATA_DIR
        mainLogName = LOG_NAME_FS
        if mountAndFormat(machine, ssh, arguments.block_device, arguments.filesystem, dataDirectory) != 0:
            result = 1
            host.printInfo("Filesystem mounting error")
            exit(1)
    else:
        dataDirectory = DEFAULT_DATA_DIR
        mainLogName = LOG_NAME
        ssh_tools.interactiveExec(ssh, f"sudo mkdir -p {dataDirectory}; sudo chown mysql:mysql {dataDirectory}",
                                  ssh_tools.printAndSaveText, mtr_log_file, get_pty=True)
    ssh_tools.interactiveExec(ssh, "sudo cat /etc/os-release",
                              ssh_tools.printAndSaveText, mtr_log_file, get_pty=True)
    ssh_tools.interactiveExec(ssh, "uname -a",
                              ssh_tools.printAndSaveText, mtr_log_file, get_pty=True)
    ssh_tools.interactiveExec(ssh, f"df -kT {dataDirectory}",
                              ssh_tools.printAndSaveText, mtr_log_file, get_pty=True)
    if not arguments.use_packages:
        result = ssh_tools.interactiveExec(ssh, f'sudo platform={machine.platform} '
                                                f'platform_version={machine.platform_version} '
                                                f'dataDirectory={dataDirectory} {additional_lib_cmd} '
                                                f'~/MariaDBEnterprise/scripts/es-test.sh {mtr[0]}',
                                           ssh_tools.printAndSaveText, mtr_log_file, get_pty=True)
        ssh_tools.interactiveExec(ssh, f'ls -la  ~/MariaDBEnterprise/{mariadb_tests.MTR_XML_FILE_NAME}',
                                  ssh_tools.printAndSaveText, mtr_log_file, get_pty=True)
    else:
        ssh_tools.interactiveExec(ssh, "sudo chown -R mysql:mysql {}".format(pathToMTR),
                                  ssh_tools.printAndSaveText, mtr_log_file, get_pty=True)
        ssh_tools.interactiveExec(ssh, "sudo cp /usr/share/mysql/not_supported/* /usr/lib/mysql/plugin/",
                                  ssh_tools.printAndSaveText, mtr_log_file, get_pty=True)
        ssh_tools.interactiveExec(ssh, "sudo cp /usr/share/mysql/not_supported/* /usr/lib64/mysql/plugin/",
                                  ssh_tools.printAndSaveText, mtr_log_file, get_pty=True)
        result = 0
        for m in mtr:
            r = ssh_tools.interactiveExec(ssh, f"sudo su - {testUser} -s /bin/bash -c 'mkdir -p {dataDirectory}/log; "
                                               f"export WSREP_PROVIDER=`find /usr -type f -name libgalera*smm.so`; "
                                               f"cd {pathToMTR} && perl mysql-test-run.pl {m}"
                                               f" --xml-report=/tmp/{mariadb_tests.MTR_XML_FILE_NAME}"
                                               f" --max-save-core=0 --max-save-datadir=1 "
                                               f"--force --vardir={dataDirectory} {socketPath}' ",
                                          ssh_tools.printAndSaveText, mtr_log_file, get_pty=True)
            if r != 0:
                result = 1

        if result != 0:
            for logArchive, logsLocation in ADDITIONAL_LOGS.items():
                if (ssh_tools.interactiveExec(ssh, f"sudo ls {logsLocation}", ssh_tools.printAndSaveText,
                                              mtr_log_file, get_pty=True)) == 0:
                    ssh_tools.interactiveExec(ssh, f"sudo tar czf {esDir}/{logArchive} {logsLocation}",
                                              ssh_tools.printAndSaveText, mtr_log_file, get_pty=True)
                    ssh_tools.interactiveExec(ssh, f"sudo chmod a+r {esDir}/{logArchive}",
                                              ssh_tools.printAndSaveText, mtr_log_file, get_pty=True)
        ssh_tools.interactiveExec(ssh, f"cp /tmp/{mariadb_tests.MTR_XML_FILE_NAME} " + esDir,
                                  ssh_tools.printAndSaveText, mtr_log_file, get_pty=True)

    mtr_log_file.close()
    host.printInfo('MTR run status: {result}'.format(result=result))
else:
    # Windows
    host.printInfo("Copying test script to VM")
    scp.put(script_dir + '/win_mtr.ps1')
    if arguments.mtr_string:
        mtr = [arguments.mtr_string]
        mtrDir = arguments.mtr_name
    else:
        mtr = MTR[arguments.mtr_param]
        mtrDir = arguments.mtr_param
    host.printInfo("Running tests: " + " ;".join(mtr))
    mtr_log_file = open("results/mtr.log", 'w+', errors='ignore')
    result = ssh_tools.interactiveExec(ssh,
                                       'pwsh win_mtr.ps1 ' + bin_tar + ' ' + mtr[0].replace(' ', '#'),
                                       ssh_tools.printAndSaveText, mtr_log_file, get_pty=False)
    mtr_log_file.close()
    host.printInfo('MTR run status: {result}'.format(result=result))
    esDir = 'MariaDBEnterprise/mysql-test/'
res = 0

if result != 0:
    host.printInfo('Get logs')
    if machine.platform not in ['windows']:
        errors = []
        for index, logArchive in enumerate(ADDITIONAL_LOGS.keys()):
            try:
                scp.get(remote_path=esDir + logArchive, local_path=f"results/{logArchive}")
            except:
                errors.append(f"{logArchive} not found!")
        host.printInfo('\n'.join(errors))
    else:
        try:
            scp.get(remote_path=esDir + 'logs.zip', local_path=f"results/logs.zip")
        except:
            host.printInfo('No logs! Tests PASSED!')
else:
    host.printInfo('MTR exit code is 0! Tests PASSED!')

scp.close()

host.printInfo('Get XML')
scp = SCPClient(ssh.get_transport())
try:
    scp.get(
        remote_path=esDir + mariadb_tests.MTR_XML_FILE_NAME,
        local_path=f'results/{mariadb_tests.MTR_XML_FILE_NAME}'
    )
except:
    host.printInfo('No XML yet :(')
    res = 1

ssh.close()

os.system('cat results/mtr.log | grep "Failing test(s): " > results/FAILED')


logPath = sdk_repo_logs.formServerLogPath(
    arguments.log_path, arguments.product, arguments.target,
    machine.box, mtrDir, arguments.build_number
)

host.printInfo(f"Publishing results to {logPath}")
try:
    sdk_repo_logs.publishFileToRepo('results', logPath, arguments.log_server, arguments.log_server_user)
except sdk_repo_logs.PublishError as error:
    host.printInfo(error)
    exit(1)

if res == 0 and result != 0:
    exit(2)
exit(res)
