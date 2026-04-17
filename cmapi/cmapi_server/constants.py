"""Module contains constants values for cmapi, failover and other .py files.

TODO: move main constant paths here and replace in files in next releases.
"""
import os
from dataclasses import dataclass
from enum import Enum
from typing import NamedTuple

# default MARIADB ColumnStore config path
MCS_ETC_PATH = '/etc/columnstore'
DEFAULT_MCS_CONF_PATH = os.path.join(MCS_ETC_PATH, 'Columnstore.xml')

# default Storage Manager config path
DEFAULT_SM_CONF_PATH = os.path.join(MCS_ETC_PATH, 'storagemanager.cnf')

# MCSLOGDIR (in mcs engine code) and related paths
MCS_LOG_PATH = '/var/log/mariadb/columnstore'

# MCSDATADIR (in mcs engine code) and related paths
MCS_DATA_PATH = '/var/lib/columnstore'
MCS_MODULE_FILE_PATH = os.path.join(MCS_DATA_PATH, 'local/module')
EM_PATH_SUFFIX = 'data1/systemFiles/dbrm'
MCS_EM_PATH = os.path.join(MCS_DATA_PATH, EM_PATH_SUFFIX)
MCS_BRM_CURRENT_PATH = os.path.join(MCS_EM_PATH, 'BRM_saves_current')
S3_BRM_CURRENT_PATH = os.path.join(EM_PATH_SUFFIX, 'BRM_saves_current')

# keys file for CEJ password encryption\decryption
# (CrossEngineSupport section in Columnstore.xml)
MCS_SECRETS_FILENAME = '.secrets'
MCS_SECRETS_FILE_PATH = os.path.join(MCS_DATA_PATH, MCS_SECRETS_FILENAME)

# CMAPI SERVER
CMAPI_CONFIG_FILENAME = 'cmapi_server.conf'
CMAPI_ROOT_PATH = os.path.dirname(__file__)
PROJECT_PATH = os.path.dirname(CMAPI_ROOT_PATH)
# path to VERSION file
VERSION_PATH = os.path.join(PROJECT_PATH, 'VERSION')
CMAPI_LOG_CONF_PATH =  os.path.join(CMAPI_ROOT_PATH, 'cmapi_logger.conf')
# path to CMAPI default config
CMAPI_DEFAULT_CONF_PATH = os.path.join(CMAPI_ROOT_PATH, CMAPI_CONFIG_FILENAME)
# CMAPI config path
CMAPI_CONF_PATH = os.path.join(MCS_ETC_PATH, CMAPI_CONFIG_FILENAME)

# TOTP secret key
SECRET_KEY = 'MCSIsTheBestEver'  # not just a random string! (base32)

DEFAULT_FAILOVER_SAMPLING_INTERVAL_SECS = 30


# network constants
# according to https://www.ibm.com/docs/en/storage-sentinel/1.1.2?topic=installation-map-your-local-host-loopback-address

LOCALHOST_IPS = {
    '127.0.0.1',
    '::1',
}
LOCALHOST_HOSTNAMES = {
    'localhost', 'localhost.localdomain',
    'localhost4', 'localhost4.localdomain4',
    'localhost6', 'localhost6.localdomain6',
}
LOCALHOSTS = LOCALHOST_IPS.union(LOCALHOST_HOSTNAMES)

CMAPI_INSTALL_PATH = '/usr/share/columnstore/cmapi/'
CMAPI_PYTHON_BIN = os.path.join(CMAPI_INSTALL_PATH, 'python/bin/python3')
CMAPI_PYTHON_DEPS_PATH = os.path.join(CMAPI_INSTALL_PATH, 'deps')
CMAPI_PYTHON_BINARY_DEPS_PATH = os.path.join(CMAPI_PYTHON_DEPS_PATH, 'bin')
CMAPI_SERVER_INSTALL_PATH = os.path.join(CMAPI_INSTALL_PATH, 'cmapi_server')
CMAPI_SINGLE_NODE_XML = os.path.join(CMAPI_SERVER_INSTALL_PATH, 'SingleNode.xml')

# default self-signed certificate paths and validity period
CMAPI_CERT_PATH = os.path.join(CMAPI_SERVER_INSTALL_PATH, 'self-signed.crt')
CMAPI_KEY_PATH = os.path.join(CMAPI_SERVER_INSTALL_PATH, 'self-signed.key')
CERT_DAYS = 365

class MCSProgs(str, Enum):
    STORAGE_MANAGER = 'StorageManager'
    WORKER_NODE = 'workernode'
    CONTROLLER_NODE = 'controllernode'
    PRIM_PROC = 'PrimProc'
    WRITE_ENGINE_SERVER = 'WriteEngineServer'
    DML_PROC = 'DMLProc'
    DDL_PROC = 'DDLProc'

# constants for dispatchers
class ProgInfo(NamedTuple):
    """NamedTuple for some additional info about handling mcs processes."""
    stop_priority: int  # priority for building stop sequence
    service_name: str  # systemd service name
    subcommand: str  # subcommand for process run in docker container
    only_primary: bool  # use this process only on primary
    delay: int = 0  # delay after process start in docker container

# mcs-loadbrm and mcs-savebrm are dependencies for workernode and resolved
# on top level of process handling
# mcs-storagemanager starts conditionally inside mcs-loadbrm, but should be
# stopped using cmapi
ALL_MCS_PROGS: dict[MCSProgs, ProgInfo] = {
    # workernode starts on primary and non primary node with 1 or 2 added
    # to subcommand (DBRM_Worker1 - on primary, DBRM_Worker2 - non primary)
    MCSProgs.STORAGE_MANAGER: ProgInfo(15, 'mcs-storagemanager', '', False, 1),
    MCSProgs.WORKER_NODE: ProgInfo(13, 'mcs-workernode', 'DBRM_Worker{}', False, 1),
    MCSProgs.CONTROLLER_NODE: ProgInfo(11, 'mcs-controllernode', 'fg', True),
    MCSProgs.PRIM_PROC: ProgInfo(5, 'mcs-primproc', '', False, 1),
    MCSProgs.WRITE_ENGINE_SERVER: ProgInfo(7, 'mcs-writeengineserver', '', False, 3),
    MCSProgs.DML_PROC: ProgInfo(3, 'mcs-dmlproc', '', False),
    MCSProgs.DDL_PROC: ProgInfo(1, 'mcs-ddlproc', '', False),
}

# constants for docker container dispatcher
MCS_INSTALL_BIN = '/usr/bin'
IFLAG = os.path.join(MCS_ETC_PATH, 'container-initialized')
LIBJEMALLOC_DEFAULT_PATH = os.path.join(MCS_DATA_PATH, 'libjemalloc.so.2')

# BRM shmem lock inspection/reset tool
SHMEM_LOCKS_PATH = os.path.join(MCS_INSTALL_BIN, 'mcs-shmem-locks')
# mcs and cmapi constanst shared
MCS_BACKUP_MANAGER_SH = os.path.join(MCS_INSTALL_BIN, 'mcs_backup_manager.sh')

# client constants
CMAPI_PORT = 8640  #TODO: use it in all places
CURRENT_NODE_CMAPI_URL = f'https://localhost:{CMAPI_PORT}'
REQUEST_TIMEOUT: float = 30.0

DMLPROC_SHUTDOWN_TIMEOUT: float = 300.0  # 5 minutes, should be less then LONG_REQUEST_TIMEOUT
LONG_REQUEST_TIMEOUT: float = 400.0  # should be less than TRANSACTION_TIMEOUT
TRANSACTION_TIMEOUT: float = 600.0  # 10 minutes

# API version
_version = '0.4.0'

# constants for packages and repositories
DEB_DISTROS = ('ubuntu', 'debian')
RPM_DISTROS = ('centos', 'rhel', 'rocky', 'almalinux')
SUPPORTED_DISTROS = DEB_DISTROS + RPM_DISTROS
SUPPORTED_ARCHITECTURES = ('x86_64', 'amd64', 'aarch64', 'arm64')


class PkgType(str, Enum):
    DEB = 'deb'
    RPM = 'rpm'


def get_pkg_type(os_name: str) -> PkgType:
    """Return the package type for a given OS distribution name."""
    if os_name in DEB_DISTROS:
        return PkgType.DEB
    if os_name in RPM_DISTROS:
        return PkgType.RPM
    raise ValueError(f'Unsupported OS type: {os_name}')


@dataclass(frozen=True)
class MultiDistroNamer:
    rhel: str
    deb: str

MDB_SERVER_PACKAGE_NAME = MultiDistroNamer(
    rhel='MariaDB-server',
    deb='mariadb-server'
)
MDB_CS_PACKAGE_NAME = MultiDistroNamer(
    rhel='MariaDB-columnstore-engine',
    deb='mariadb-plugin-columnstore'
)
CMAPI_PACKAGE_NAME = MultiDistroNamer(
    rhel='MariaDB-columnstore-cmapi',
    deb='mariadb-columnstore-cmapi'
)
ES_REPO = MultiDistroNamer(
    rhel=(
'''[mariadb-es-main]
name = MariaDB Enterprise Server
baseurl = https://dlm.mariadb.com/repo/{token}/mariadb-enterprise-server/{mdb_version}/rpm/rhel/{os_major_version}/{arch}
gpgkey = {gpg_key_url}
gpgcheck = 1
enabled = 1
module_hotfixes = 1
'''
    ),
    deb='deb [arch=amd64,arm64] https://dlm.mariadb.com/repo/{token}/mariadb-enterprise-server/{mdb_version}/deb {os_version} main'
)
ES_REPO_PRIORITY_PREFS = '''
Package: *
Pin: origin dlm.mariadb.com
Pin-Priority: 1700
'''
ES_VERIFY_URL = MultiDistroNamer(
    rhel='https://dlm.mariadb.com/repo/{token}/mariadb-enterprise-server/{mdb_version}/rpm/rhel/{os_major_version}/{arch}/repodata/repomd.xml',
    deb='https://dlm.mariadb.com/repo/{token}/mariadb-enterprise-server/{mdb_version}/deb/dists/{os_version}/Release'
)
MDB_GPG_KEY_URL = 'https://supplychain.mariadb.com/MariaDB-Enterprise-GPG-KEY'
ES_TOKEN_VERIFY_URL = 'https://dlm.mariadb.com/browse/{token}/mariadb_enterprise_server/'
MDB_LATEST_RELEASES_URL = 'https://dlm.mariadb.com/rest/releases/mariadb_enterprise_server/'
PKG_GET_VER_CMD = MultiDistroNamer(
    rhel="rpm -q --queryformat '%{{VERSION}}' {package_name}",
    deb="dpkg-query -f '${{Version}}' -W {package_name}"
)

class ClusterModeEnum(str, Enum):
    READONLY = 'readonly'
    READWRITE = 'readwrite'


# Constants for upgrade agent
UPGRADE_AGENT_PORT = 8619
UPGRADE_AGENT_START_TIMEOUT = 30  # seconds to wait for agent to start
UPGRADE_AGENT_DEFAULT_EXEC_TIMEOUT = 30  # seconds to wait for command execution
UPGRADE_AGENT_SERVER_TIMEOUT = 3600  # 1 hour default upgrade agent server lifetime
UPGRADE_AGENT_MODULE = 'cmapi_server.managers.upgrade.upgrade_agent'
UPGRADE_DIR = '/tmp/mcs_install_es'
UPGRADE_BACKUP_DIR = os.path.join(UPGRADE_DIR, 'backup')
UPGRADE_STALE_REPOS_DIR = os.path.join(UPGRADE_BACKUP_DIR, 'stale_repos')
UPGRADE_LOG_FILEPATH = os.path.join(UPGRADE_DIR, 'mcs_cli_install_es.log')
MDB_COLUMNSTORE_RHEL_CNF_PATH = '/etc/my.cnf.d/columnstore.cnf'
MDB_COLUMNSTORE_DEB_CNF_PATH = '/etc/mysql/mariadb.conf.d/columnstore.cnf'
# Config options that may not be supported in older MariaDB versions
UNSUPPORTED_MARIADB_CLI_OPTIONS = ['quick', 'quick-max-column-width']
CMAPI_SYSTEMD_SERVICE_NAME = 'mariadb-columnstore-cmapi'
