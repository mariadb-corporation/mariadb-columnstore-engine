"""Module contains constants values for cmapi, failover and other .py files.

TODO: move main constant paths here and replace in files in next releases.
"""
import os
from typing import NamedTuple


# default MARIADB ColumnStore config path
MCS_ETC_PATH = '/etc/columnstore'
DEFAULT_MCS_CONF_PATH = os.path.join(MCS_ETC_PATH, 'Columnstore.xml')

# default Storage Manager config path
DEFAULT_SM_CONF_PATH = os.path.join(MCS_ETC_PATH, 'storagemanager.cnf')

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


# network constants
# according to https://www.ibm.com/docs/en/storage-sentinel/1.1.2?topic=installation-map-your-local-host-loopback-address
LOCALHOSTS = (
    '127.0.0.1',
    'localhost', 'localhost.localdomain',
    'localhost4', 'localhost4.localdomain4',
    '::1',
    'localhost6', 'localhost6.localdomain6',
)

CMAPI_INSTALL_PATH = '/usr/share/columnstore/cmapi/'
CMAPI_PYTHON_BIN = os.path.join(CMAPI_INSTALL_PATH, "python/bin/python3")
CMAPI_PYTHON_DEPS_PATH = os.path.join(CMAPI_INSTALL_PATH, "deps")
CMAPI_PYTHON_BINARY_DEPS_PATH = os.path.join(CMAPI_PYTHON_DEPS_PATH, "bin")
CMAPI_SINGLE_NODE_XML = os.path.join(
    CMAPI_INSTALL_PATH, 'cmapi_server/SingleNode.xml'
)

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
ALL_MCS_PROGS = {
    # workernode starts on primary and non primary node with 1 or 2 added
    # to subcommand (DBRM_Worker1 - on primary, DBRM_Worker2 - non primary)
    'StorageManager': ProgInfo(15, 'mcs-storagemanager', '', False, 1),
    'workernode': ProgInfo(13, 'mcs-workernode', 'DBRM_Worker{}', False, 1),
    'controllernode': ProgInfo(11, 'mcs-controllernode', 'fg', True),
    'PrimProc': ProgInfo(5, 'mcs-primproc', '', False, 1),
    'ExeMgr': ProgInfo(9, 'mcs-exemgr', '', False, 1),
    'WriteEngineServer': ProgInfo(7, 'mcs-writeengineserver', '', False, 3),
    'DMLProc': ProgInfo(3, 'mcs-dmlproc', '', False),
    'DDLProc': ProgInfo(1, 'mcs-ddlproc', '', False),
}

# constants for docker container dispatcher
MCS_INSTALL_BIN = '/usr/bin'
IFLAG = os.path.join(MCS_ETC_PATH, 'container-initialized')
LIBJEMALLOC_DEFAULT_PATH = os.path.join(MCS_DATA_PATH, 'libjemalloc.so.2')
MCS_LOG_PATH = '/var/log/mariadb/columnstore'


# client constants
CMAPI_PORT = 8640  #TODO: use it in all places
CURRENT_NODE_CMAPI_URL = f'https://localhost:{CMAPI_PORT}'
REQUEST_TIMEOUT: float = 30.0
TRANSACTION_TIMEOUT: float = 300.0  # 5 minutes