
# To be moved to the library to share between Columnstore and BuildBot repos

import argparse
import datetime
import os
import resource
import subprocess
import sys
import paramiko
import select
from scp import SCPClient
import tempfile
import logging
import shutil
import pathlib

configurationRootPath = pathlib.Path(__file__).parent.parent.parent
sys.path.append(str(configurationRootPath))
from constants.build_parameters import products_versions

MDBCI_VM_PATH = os.path.expanduser('~/vms/')
logging.basicConfig(
    datefmt='%d-%b-%y %H:%M:%S',
    format='%(asctime)s: %(message)s',
    level=logging.INFO,
)

ES_PRODUCT_STAGING = 'MariaDBEnterpriseStaging'
ES_PRODUCT_PRESTAGING = 'MariaDBEnterprisePrestaging'


ES_VERSIONS = {
    '10.2.38': '10.2.38-12',
    '10.3.29': '10.3.29-12',
    '10.4.19': '10.4.19-12',
    '10.5.10': '10.5.10-7',

    '10.2.40': '10.2.40-13',
    '10.3.31': '10.3.31-13',
    '10.4.21': '10.4.21-13',
    '10.5.12': '10.5.12-8',
    '10.6.4': '10.6.4-1',

    '10.2.41': '10.2.41-14',
    '10.3.32': '10.3.32-14',
    '10.4.22': '10.4.22-14',
    '10.5.13': '10.5.13-9',
    '10.6.5': '10.6.5-2',

    '10.2.43': '10.2.43-15',
    '10.3.34': '10.3.34-15',
    '10.4.24': '10.4.24-15',
    '10.5.15': '10.5.15-10',
    '10.6.7': '10.6.7-3',

    '10.3.35': '10.3.35-16',
    '10.4.25': '10.4.25-16',
    '10.5.16': '10.5.16-11',
    '10.6.8': '10.6.8-4',

    '10.3.36': '10.3.36-17',
    '10.4.26': '10.4.26-17',
    '10.5.17': '10.5.17-12',
    '10.6.9': '10.6.9-5',

    '10.3.37': '10.3.37-18',
    '10.4.27': '10.4.27-18',
    '10.5.18': '10.5.18-13',
    '10.6.11': '10.6.11-6',

    '10.3.38': '10.3.38-19',
    '10.4.28': '10.4.28-19',
    '10.5.19': '10.5.19-14',
    '10.6.12': '10.6.12-7',

    '10.3.39': '10.3.39-20',
    '10.4.30': '10.4.30-20',
    '10.5.21': '10.5.21-15',
    '10.6.14': '10.6.14-9',


    '10.4.31': '10.4.31-21',
    '10.5.22': '10.5.22-16',
    '10.6.15': '10.6.15-10',

    '23.06.0': "23.06.0",

    '23.07.0': "23.07.0",
    '23.07.1': "23.07.1",

    '23.08.0': "23.08.0",

    '10.4.32': '10.4.32-22',
    '10.5.23': '10.5.23-17',
    '10.6.16': '10.6.16-11',

    '10.4.33': '10.4.33-23',
    '10.5.24': '10.5.24-18',
    '10.6.17': '10.6.17-12',

    '23.08.1': "23.08.1",
}

ES_VERIONS_LISTS = {
    '10.2': [
        '10.2.38',
        '10.2.40',
        '10.2.41',
        '10.2.43',
    ],
    '10.3': [
        '10.3.29',
        '10.3.31',
        '10.3.32',
        '10.3.34',
        '10.3.35',
        '10.3.36',
        '10.3.37',
        '10.3.38',
        '10.3.39',
    ],
    '10.4': [
        '10.4.19',
        '10.4.21',
        '10.4.22',
        '10.4.24',
        '10.4.25',
        '10.4.26',
        '10.4.27',
        '10.4.28',
        '10.4.30',
        '10.4.31',
        '10.4.32',
        '10.4.33',
    ],
    '10.5': [
        '10.5.10',
        '10.5.12',
        '10.5.13',
        '10.5.15',
        '10.5.16',
        '10.5.17',
        '10.5.18',
        '10.5.19',
        '10.5.21',
        '10.5.22',
        '10.5.23',
        '10.5.24',
    ],
    '10.6': [
        '10.6.4',
        '10.6.5',
        '10.6.7',
        '10.6.8',
        '10.6.9',
        '10.6.11',
        '10.6.12',
        '10.6.14',
        '10.6.15',
        '10.6.16',
        '10.6.17',
    ],
    '23.07': [
        '23.07.0',
        '23.07.1',
    ],
    '23.08': [
        '23.08.0',
        '23.08.1',
    ]
}

CS_VERSIONS = {
    '10.2.40': '10.2.40',
    '10.3.31': '10.3.31',
    '10.4.21': '10.4.21',
    '10.5.12': '10.5.12',
    '10.6.4': '10.6.4',

    '10.3.32': '10.3.32',
    '10.4.22': '10.4.22',
    '10.5.13': '10.5.13',
    '10.6.5': '10.6.5',
    '10.7.1': '10.7.1',
    '10.2.42': '10.2.42',
    '10.3.34': '10.3.34',
    '10.4.24': '10.4.24',
    '10.5.15': '10.5.15',
    '10.6.7': '10.6.7',
    '10.7.3': '10.7.3',
    '10.8.2': '10.8.2',

    '10.2.44': '10.2.44',
    '10.3.35': '10.3.35',
    '10.4.25': '10.4.25',
    '10.5.16': '10.5.16',
    '10.6.8': '10.6.8',
    '10.7.4': '10.7.4',
    '10.8.3': '10.8.3',
    '10.9.1': '10.9.1',

    '10.3.36': '10.3.36',
    '10.4.26': '10.4.26',
    '10.5.17': '10.5.17',
    '10.6.9': '10.6.9',
    '10.7.5': '10.7.5',
    '10.8.4': '10.8.4',
    '10.9.2': '10.9.2',

    '10.3.37': '10.3.37',
    '10.4.27': '10.4.27',
    '10.5.18': '10.5.18',
    '10.6.10': '10.6.10',
    '10.7.6': '10.7.6',
    '10.8.5': '10.8.5',
    '10.9.3': '10.9.3',

    '10.6.11': '10.6.11',
    '10.7.7': '10.7.7',
    '10.8.6': '10.8.6',
    '10.9.4': '10.9.4',

    '10.10.1': '10.10.1',
    '10.10.2': '10.10.2',

    '10.11.1': '10.11.1',
    '10.11.2': '10.11.2',

    '11.0.0': '11.0.0',

    '10.3.38': '10.3.38',
    '10.4.28': '10.4.28',
    '10.5.19': '10.5.19',
    '10.6.12': '10.6.12',
    '10.7.8': '10.7.8',
    '10.8.7': '10.8.7',
    '10.9.5': '10.9.5',
    '10.10.3': '10.10.3',

    '10.3.39': '10.3.39',
    '10.4.29': '10.4.29',
    '10.5.20': '10.5.20',
    '10.6.13': '10.6.13',
    '10.8.8': '10.8.8',
    '10.9.6': '10.9.6',
    '10.10.4': '10.10.4',
    '11.0.1': '11.0.1',
    '11.0.2': '11.0.2',

    '10.3.40': '10.3.40',
    '10.4.30': '10.4.30',
    '10.5.21': '10.5.21',
    '10.6.14': '10.6.14',
    '10.9.7': '10.9.7',
    '10.10.5': '10.10.5',
    '10.11.4': '10.11.4',

    '11.0.3': '11.0.3',
    '11.1.1': '11.1.1',
    '11.1.2': '11.1.2',

    '10.4.31': '10.4.31',
    '10.5.22': '10.5.22',
    '10.6.15': '10.6.15',
    '10.9.8': '10.9.8',
    '10.10.6': '10.10.6',
    '10.11.5': '10.11.5',

    '10.4.32': '10.4.32',
    '10.5.23': '10.5.23',
    '10.6.16': '10.6.16',
    '10.9.9': '10.9.9',
    '10.10.7': '10.10.7',
    '10.11.6': '10.11.6',

    '11.1.3': '11.1.3',
    '11.2.1': '11.2.1',
    '11.2.2': '11.2.2',

    '11.0.4': '11.0.4',

    '10.5.24': '10.5.24',
    '10.6.17': '10.6.17',
    '10.11.7': '10.11.7',
    '11.0.5': '11.0.5',
    '11.1.4': '11.1.4',
    '11.2.3': '11.2.3',
    '11.3.1': '11.3.1',
    '11.3.2': '11.3.2',

    '11.4.0': '11.4.0',
    '11.4.1': '11.4.1',

    '10.4.34': '10.4.34',
    '10.5.25': '10.5.25',
    '10.6.18': '10.6.18',
    '10.11.8': '10.11.8',
    '11.0.6': '11.0.6',
    '11.1.5': '11.1.5',
    '11.2.4': '11.2.4',

    '11.3.3': '11.3.3',
    '11.4.2': '11.4.2',
}

CS_VERIONS_LISTS = {
    '10.2': [ # EOL
        '10.2.38',
        '10.2.39',
        '10.2.40',
        '10.2.42',
        '10.2.44',
    ],
    '10.3': [ # EOL
        '10.3.29',
        '10.3.30',
        '10.3.31',
        '10.3.32',
        '10.3.34',
        '10.3.35',
        '10.3.36',
        '10.3.37',
        '10.3.38',
        '10.3.39',
        '10.3.40',
    ],
    '10.4': [
        '10.4.19',
        '10.4.20',
        '10.4.21',
        '10.4.22',
        '10.4.24',
        '10.4.25',
        '10.4.26',
        '10.4.27',
        '10.4.28',
        '10.4.29',
        '10.4.30',
        '10.4.31',
        '10.4.32',
        '10.4.33',
        '10.4.34',
    ],
    '10.5': [
        '10.5.10',
        '10.5.11',
        '10.5.12',
        '10.5.13',
        '10.5.15',
        '10.5.16',
        '10.5.17',
        '10.5.18',
        '10.5.19',
        '10.5.20',
        '10.5.21',
        '10.5.22',
        '10.5.23',
        '10.5.24',
    ],
    '10.6': [
        '10.6.1',
        '10.6.2',
        '10.6.3',
        '10.6.4',
        '10.6.5',
        '10.6.7',
        '10.6.8',
        '10.6.9',
        '10.6.10',
        '10.6.11',
        '10.6.12',
        '10.6.13',
        '10.6.14',
        '10.6.15',
        '10.6.16',
        '10.6.17',
        '10.6.18',
    ],
    '10.7': [ # EOL
        '10.7.1',
        '10.7.3',
        '10.7.4',
        '10.7.5',
        '10.7.6',
        '10.7.7',
        '10.7.8',
        '10.7.9',
    ],
    '10.8': [ # EOL
        '10.8.2',
        '10.8.3',
        '10.8.4',
        '10.8.5',
        '10.8.6',
        '10.8.7',
        '10.8.8',
    ],
    '10.9': [ # EOL
        '10.9.1',
        '10.9.2',
        '10.9.3',
        '10.9.4',
        '10.9.5',
        '10.9.6',
        '10.9.7',
        '10.9.8',
        '10.9.9',
    ],
    '10.10': [ # EOL
        '10.10.1',
        '10.10.2',
        '10.10.3',
        '10.10.4',
        '10.10.5',
        '10.10.6',
        '10.10.7',
        '10.10.8',
    ],
    '10.11': [
        '10.11.1',
        '10.11.2',
        '10.11.3',
        '10.11.4',
        '10.11.5',
        '10.11.6',
        '10.11.7',
        '10.11.8',
    ],
    '11.0': [
        '11.0.0',
        '11.0.1',
        '11.0.2',
        '11.0.3',
        '11.0.4',
        '11.0.5',
        '11.0.6',
    ],
    '11.1': [
        '11.1.1',
        '11.1.2',
        '11.1.3',
        '11.1.4',
        '11.1.5',
    ],
    '11.2': [
        '11.2.1',
        '11.2.2',
        '11.2.3',
        '11.2.4',
    ],
    '11.3': [
        '11.3.1',
        '11.3.2',
        '11.3.3',
    ],
    '11.4': [
        '11.4.0',
        '11.4.1',
        '11.4.2',
    ],
}

VERSIONS_WITH_MYSQL_CLIENT = ['10.0', '10.1', '10.2', '10.3']
ES_MAIN_BRANCHES = [
    #'10.2-enterprise',
    #'10.3-enterprise',
    '10.4-enterprise',
    '10.5-enterprise',
    '10.6-enterprise',
    #'10.7-enterprise',
    #'10.8-enterprise',
    #'10.11-enterprise',
    '23.06-enterprise',
]

VERSIONS_IN_DISTROS = {
    "centos": {
        "7": "5.5",
        "8": "10.3",
        "9": "10.5",
    },
    "rocky": {
        "8": "10.3",
        "9": "10.5",
    },
    "rhel": {
        "7": "5.5",
        "8": "10.3",
        "9": "10.5",
    },
    "debian": {
        "stretch": "10.1",
        "buster": "10.3",
        "bullseye": "10.5",
        "bookworm": "10.11",
    },
    "ubuntu": {
        "xenial": "10.0",
        "bionic": "10.1",
        "focal": "10.3",
        "jammy": "10.5",
    },
    "sles": {
        "12": "10.1",
        "15": "10.5",
    },
    "suse": {
        "15": "10.5",
    },
}

PRODS = {
    'MariaDBEnterprise': 'mdbe_ci',
    ES_PRODUCT_STAGING: 'mdbe_staging',
    ES_PRODUCT_PRESTAGING: 'mdbe_prestaging',
    'MariaDBServerCommunity': 'mariadb_ci',
    'MariaDBServerCommunityStaging': 'mariadb_staging',
    'MariaDBEnterpriseProduction': 'mdbe',
    'MariaDBServerCommunityProduction': 'mariadb',
    'MariaDBGalera3': 'galera_3_community',
    'MariaDBGalera4': 'galera_4_community',
    'MariaDBGalera3Enterprise': 'galera_3_enterprise',
    'MariaDBGalera4Enterprise': 'galera_4_enterprise',
    'Maxscale': 'maxscale_ci',
    'MaxscaleProduction': 'maxscale',
    'MaxscaleStaging': 'maxscale_staging',
    'MaxscaleEnterprise': 'maxscale_enterprise_ci',
    'MySQL': 'mysql',
    'Clustrix': 'clustrix',
    'ConnectorC': 'connector_c_ci',
    'ConnectorCpp': 'connector_cpp_ci',
    'ConnectorODBC': 'connector_odbc_ci',
}

ENTERPRISE_PRODS = [
    'MariaDBEnterprise',
    'MariaDBEnterpriseProduction',
    ES_PRODUCT_STAGING,
    'MariaDBGalera3Enterprise',
    'MariaDBGalera4Enterprise',
    ES_PRODUCT_PRESTAGING,
]

MAXSCALE_PRODS = [
    'Maxscale',
    'MaxscaleProduction',
    'MaxscaleStaging',
    'MaxscaleEnterprise',
]

SERVER_PRODS = [
    'MariaDBEnterprise',
    ES_PRODUCT_STAGING,
    ES_PRODUCT_PRESTAGING,
    'MariaDBServerCommunity',
    'MariaDBServerCommunityStaging',
    'MariaDBEnterpriseProduction',
    'MariaDBServerCommunityProduction',
]

MAXSCALE_BACKEND_PRODS = [
    'MariaDBServerCommunityProduction',
    'MariaDBEnterpriseProduction',
    'MariaDBServerCommunity',
    'MariaDBEnterprise',
    'MariaDBServerCommunityStaging',
    ES_PRODUCT_STAGING,
    'MySQL',
]

PRODUCTION_PRODS = [
    'MaxscaleProduction',
    ES_PRODUCT_STAGING,
    'MariaDBEnterpriseProduction',
    'MariaDBServerCommunityProduction',
]

PRODUCTION_PRODS_DICT = {
    'MariaDBEnterprise': 'MariaDBEnterpriseProduction',
    'MariaDBEnterpriseProduction': 'MariaDBEnterpriseProduction',
    ES_PRODUCT_STAGING: 'MariaDBEnterpriseProduction',
    ES_PRODUCT_PRESTAGING: 'MariaDBEnterpriseProduction',
    'MariaDBServerCommunity': 'MariaDBServerCommunityProduction',
    'MariaDBServerCommunityProduction': 'MariaDBServerCommunityProduction',
    'MariaDBServerCommunityStaging': 'MariaDBServerCommunityProduction',
    'MaxscaleProduction': 'MaxscaleProduction',
    'MaxscaleStaging': 'MaxscaleProduction',
    'Maxscale': 'MaxscaleProduction',
}

STAGING_PRODS = [
    'MaxscaleStaging',
    ES_PRODUCT_STAGING,
    'MariaDBServerCommunityStaging',
]

ODBC_CONNECTOR_PRODUCTS = [
    'connector_odbc',
    'connector_odbc_staging',
    'connector_odbc_ci',
]

XPAND_PRODUCTS = [
    'xpand',
    'xpand_staging',
]

DEFAULT_GALERA_VER = {
    'MariaDBGalera3': 'mariadb-3.x-latest',
    'MariaDBGalera4': 'mariadb-4.x-latest',
    'MariaDBGalera3Enterprise': 'es-mariadb-3.x-latest',
    'MariaDBGalera4Enterprise': 'es-mariadb-4.x-latest',
}

DEBIAN_PLATFORMS = [
    'ubuntu',
    'debian',
]


APT = 'export  DEBIAN_FRONTEND=noninteractive; sudo -E apt-get -q -o Dpkg::Options::=--force-confold ' \
       '-o Dpkg::Options::=--force-confdef -y --force-yes install '
YUM = 'sudo yum install -y '
ZYPPER = 'sudo zypper -n install '

CMDS = {
    'apt': APT,
    'yum': YUM,
    'zypper': ZYPPER,
}

PACKAGE_TYPE = {
    'apt': 'DEB',
    'yum': 'RPM',
    'zypper': 'RPM',
    'windows': 'MSI',
}

LIST_PACKAGES_CMDS = {
    'DEB': "dpkg --get-selections | grep -v \"deinstall\" | cut -f1 | cut -f1 -d\":\" ",
    'RPM': "rpm -qa --qf \"%{NAME}\\n\""
}

RM_CMDS = {
    'apt': APT.replace('install', 'remove'),
    'yum': YUM.replace('install', 'remove'),
    'zypper': ZYPPER.replace('install', 'remove'),
}

DISTRO_CLASSES = {
    'ubuntu': 'apt',
    'debian': 'apt',
    'rhel': 'yum',
    'centos': 'yum',
    'rocky': 'yum',
    'suse': 'zypper',
    'opensuse': 'zypper',
    'sles': 'zypper',
    'windows': 'windows',
}

MARIADB_PACKET_FILE = {
    'ubuntu': '/etc/apt/sources.list.d/mariadb.list',
    'debian': '/etc/apt/sources.list.d/mariadb.list',
    'rhel': '/etc/yum.repos.d/mariadb.repo',
    'centos': '/etc/yum.repos.d/mariadb.repo',
    'rocky': '/etc/yum.repos.d/mariadb.repo',
    'suse': '/etc/zypp/repos.d/mariadb.repo',
    'opensuse': '/etc/zypp/repos.d/mariadb.repo',
    'sles': '/etc/zypp/repos.d/mariadb.repo',
}

UPDATE_REPO = {
    'apt': APT.replace('install', 'update'),
    'yum': '',
    'zypper': ZYPPER.replace('install', 'refresh'),
}

MARIADB_ALL_VERSIONS = CS_VERIONS_LISTS.keys()

SHARED_STORAGES = [
    "nostorage",
    "gluster",
]

SSH_OPT_ARRAY = ['-o', 'UserKnownHostsFile=/dev/null',
                 '-o', 'StrictHostKeyChecking=no',
                 '-o', 'ConnectTimeout=120']
SSH_OPT = ' '.join(SSH_OPT_ARRAY)


# Definitions for the Docker product creation

MAXSCALE_CI_DOCKER_PRODUCT_NAME = 'skysql-maxscale-dev'
ES_CI_DOCKER_PRODUCT_NAME = 'enterprise-server'
ES_JENKINS_PRODUCT_NAME = 'es-server-test'
ES_DOCKER_PRODUCT_NAME = 'enterprise-server'
CS_CI_DOCKER_PRODUCT_NAME = 'cs-server'

CI_DOCKER_REGISTRY = 'https://maxscale-docker-registry.mariadb.net'

DOCKER_REGISTRY = 'https://docker.mariadb.com'
ES_TEST_DOCKER_REGISTRY = 'https://gcr.io/downloads-234321'
CI_DOCKER_REGISTRY_MAXSCALE = 'https://registry.hub.docker.com/mariadb'
#CI_DOCKER_REGISTRY_MAXSCALE = ES_TEST_DOCKER_REGISTRY
CI_DOCKER_REGISTRY_MAXSCALE_USER = 'timofeyturenko'

ES_TEST_DOCKER_DIRS = [
    'es-server-test',
    'maxscale-ci',
]
DOCKER_HUB = 'https://hub.docker.com'

CI_DOCKER_USER_NAME = 'ci.servers'

DOCKER_DISTRO = "ubuntu"
DOCKER_DISTRO_VER = "focal"
DOCKER_DISTRO_VER_10_2 = "bionic"

# Location of the CI repository
CI_REPO_HOST = "mdbe-ci-repo-us"
CI_REPO_USER = "timofey_turenko_mariadb_com"
CI_REPO_PATH = "/srv/repository/"

# Filesystrems for MTRa
FILESYSTEMS_LIST = [
    '',
    'ext2',
    'ext3',
    'ext4',
    'btrfs',
    'f2fs',
    'jfs',
    'xfs',
    'zfs',
    'reiserfs',
]

DEFAULT_IMAGE_FILESYSTEMS_TEST = 'ubuntu_focal_gcp'


MDBCI_TEST_DEFAULT_CLUSTRIX_BOX = "centos_7"

MDBCI_TEST_PASSED = 'PASSED'
MDBCI_TEST_FAILED = 'FAILED'
MDBCI_TEST_SKIPPED = 'SKIPPED'

MDBCI_TEST_PLATFORM = 'platform'
MDBCI_TEST_PLATFORM_VERSION = 'platform_version'
MDBCI_TEST_VERSIONS = 'versions'
MDBCI_TEST_ONLY_PLATFORMS = 'only_platforms'
MDBCI_TEST_EXCLUSIONS = 'exclusions'
MDBCI_TEST_VERSION_CMD = 'versionCmd'


MDBCI_TEST_EMPTY = {
    "build": {
        "hostname": "default",
    }
}

MDBCI_TEST_PLATFORMS = [
    'rhel',
    'centos',
    'rocky_linux',
    'debian',
    'ubuntu',
    'sles',
    'opensuse',
]

MDBCI_TEST_PLATFORM_VERSIONS = {
    'rhel': ['7', '8', '9'],
    'centos': ['7', '8'],
    'rocky_linux': ['8', '9'],
    'debian' : [
        'stretch',
        'buster',
        'bullseye',
    ],
    'ubuntu': [
        'focal',
        'jammy',
    ],
    'sles': ['12', '15'],
    'opensuse': ['12', '15'],
}

MDBCI_TEST_SERVER_PLATFORMS = []
for platform in MDBCI_TEST_PLATFORM_VERSIONS.keys():
    for version in MDBCI_TEST_PLATFORM_VERSIONS[platform]:
        MDBCI_TEST_SERVER_PLATFORMS.append(
            {
                MDBCI_TEST_PLATFORM: platform,
                MDBCI_TEST_PLATFORM_VERSION: version,
            }
        )

MDBCI_TEST_PROVIDERS = [
    'libvirt',
    'aws',
    'gcp',
    'digitalocean',
]

MDBCI_TEST_SERVER_EXLUSIONS = {
            '10.2': [
                {
                    MDBCI_TEST_PLATFORM: 'ubuntu',
                    MDBCI_TEST_PLATFORM_VERSION: 'focal'
                },
                {
                    MDBCI_TEST_PLATFORM: 'debian',
                    MDBCI_TEST_PLATFORM_VERSION: 'buster'
                },
                {
                    MDBCI_TEST_PLATFORM: 'debian',
                    MDBCI_TEST_PLATFORM_VERSION: 'bullseye',
                },
            ],
            '10.3': [
                {
                    MDBCI_TEST_PLATFORM: 'debian',
                    MDBCI_TEST_PLATFORM_VERSION: 'bullseye',
                }
            ],
            '10.4': [
                {
                    MDBCI_TEST_PLATFORM: 'debian',
                    MDBCI_TEST_PLATFORM_VERSION: 'bullseye',
                },
            ],
        }

MDBCI_TEST_PRODUCTS = {
    'empty': {},
    'mdbe': {
        MDBCI_TEST_VERSIONS: list(products_versions.MDBE_RELEASED_VERSIONS.keys()),
        MDBCI_TEST_VERSION_CMD: 'sudo service mariadb restart; echo "select @@version" | sudo mysql',
        MDBCI_TEST_ONLY_PLATFORMS: MDBCI_TEST_SERVER_PLATFORMS,
        MDBCI_TEST_EXCLUSIONS: MDBCI_TEST_SERVER_EXLUSIONS,
    },
    'mariadb': {
        MDBCI_TEST_VERSIONS: list(products_versions.MARIADB_RELEASED_VERSIONS.keys()),
        MDBCI_TEST_VERSION_CMD: 'sudo service mariadb restart; echo "select @@version" | sudo mysql',
        MDBCI_TEST_ONLY_PLATFORMS: MDBCI_TEST_SERVER_PLATFORMS,
        MDBCI_TEST_EXCLUSIONS: MDBCI_TEST_SERVER_EXLUSIONS,
    },
    'maxscale': {
        MDBCI_TEST_VERSIONS: list(products_versions.MAXSCALE_RELEASED_VERSIONS.keys()),
        MDBCI_TEST_VERSION_CMD: 'maxscale --version-full',
        MDBCI_TEST_ONLY_PLATFORMS: MDBCI_TEST_SERVER_PLATFORMS,
    },
    'clustrix': {
        MDBCI_TEST_VERSIONS: ['latest'],
        MDBCI_TEST_ONLY_PLATFORMS: [
            {
                MDBCI_TEST_PLATFORM: 'centos',
                MDBCI_TEST_PLATFORM_VERSION: '7',
            },
            {
                MDBCI_TEST_PLATFORM: 'rhel',
                MDBCI_TEST_PLATFORM_VERSION: '7',
            },
        ]
    },
    'mdbe_build': {},
    'docker': {MDBCI_TEST_VERSIONS: ['latest'], },
    'connector_odbc': {MDBCI_TEST_VERSIONS: ['latest'], },
    'caching_tools': {},
    'debugging_tools': {},
    'extra_package_management': {},
    'security_tools': {},
    'java': {MDBCI_TEST_VERSIONS: ['latest'], },
    'python': {},
    'sysbench': {},
    'core_dump': {},
    'kerberos': {},
    'kerberos_server': {},
}


class PublishError(Exception):
    pass


def printInfo(info, end=None):
    print(info, end=end, flush=True)


def setupMdbciEnvironment(commandsFile=None):
    """
    Methods modifies the environment, so the VM managed by the MDBCI
    will always be in `~/vms/` directory.

    Also adds the `~/mdbci/` to the executable search path.
    This method should be called before calling runMdbci
    """
    os.environ['MDBCI_VM_PATH'] = MDBCI_VM_PATH
    mdbciDirectory = os.path.expanduser('~/mdbci')
    originalPath = os.environ['PATH']
    newPath = originalPath + os.pathsep + mdbciDirectory
    if mdbciDirectory not in originalPath:
        os.environ['PATH'] = newPath
    if commandsFile:
        print(f'export MDBCI_VM_PATH={MDBCI_VM_PATH}', file=commandsFile)
        print(f'export PATH={newPath}', file=commandsFile)


def printCommandInfo(command, commandFile=None, logCommand = True, prefix="~Run^command"):
    if logCommand:
        logging.info(f'{prefix}: {command}')
        if commandFile:
            print(command, file=commandFile)


def runMdbci(*arguments, commandsFile=None):
    """
    Run MDBCI with the specified arguments. Put all output to standard output streams
    :param arguments: the list of strings to pass as arguments to MDBCI
    :return: exit code of the MDBCI
    """
    all_commands = ['mdbci']
    all_commands.extend(arguments)
    printCommandInfo(' '.join(all_commands), commandFile=commandsFile)
    process = subprocess.run(
        all_commands, stdout=sys.stdout, stderr=sys.stderr
    )
    return process.returncode


def getMdbciInfo(*cmd):
    all_commands = ['mdbci', '--silent']
    all_commands.extend(cmd)
    process = subprocess.Popen(all_commands,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    output, _ = process.communicate()
    return output.decode('utf-8').splitlines()[0]


def getMdbciInfoMultiline(*cmd):
    all_commands = ['mdbci', '--silent']
    all_commands.extend(cmd)
    process = subprocess.Popen(all_commands,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    output, _ = process.communicate()
    return output.decode('utf-8').splitlines()


def dig(structure, *keys, default=None):
    """
    Dig a hierarchical structure for a value. If in some point it was not found,
    return the default value.
    :param structure: nested structure of dicts and lists to get data from
    :param keys: a list of indexes to fetch data on every step
    :param default: the default value that will be returned
    :return: the dug value or default one
    """
    current = structure
    try:
        for key in keys:
            if type(current) in [dict, list]:
                current = current[key]
            else:
                return default
    except (KeyError, IndexError):
        return default
    return current


def createRunner(connection, commandsFile=None):
    """Creates a function that is able to run commands on specific runner"""

    def runRemoteCommand(command, **kwargs):
        printCommandInfo(command, commandFile=commandsFile)
        _, output, errors = connection.exec_command(command, **kwargs)

        commandOutput = output.read()
        if commandOutput:
            printCommandInfo(commandOutput, commandFile=commandsFile, prefix="Output")
        commandErrors = errors.read()
        if commandErrors:
            printCommandInfo(commandErrors, commandFile=commandsFile, prefix="Errors")

        return output.channel.recv_exit_status()

    return runRemoteCommand


def skipAllText(_text, _outStream):
    pass


def closeCorrectly(channel, commandFile, logCommand):
    channel.shutdown_read()
    try:
        channel.close()
    except EOFError:
        # There is a race that when shutdown_read has been called and when
        # you try to close the connection, the socket is already closed.
        # Such errors should be ignored.
        # This issue on GitHub: https://github.com/paramiko/paramiko/issues/2227
        printCommandInfo("Ignoring EOFError on close", commandFile, logCommand=logCommand, prefix="Warning")


def interactiveExec(connection, cmd, consumer=skipAllText, outfile=None, get_pty=False, commandFile=None, readStdoutTimeout=1,
                    echoCommand=False, logCommand = True, inactivityTimeout=900):
    printCommandInfo(cmd, commandFile, logCommand=logCommand)
    if echoCommand:
        printInfo(cmd)
    con_stdin, con_stdout, con_stderr = connection.exec_command(cmd, get_pty=get_pty, bufsize=1)
    channel = con_stdout.channel

    con_stdin.close()
    channel.shutdown_write()

    stdout = (channel.recv, channel.in_buffer)
    stderr = (channel.recv_stderr, channel.in_stderr_buffer)

    def send_converted_text(input_type):
        stream, buffer = input_type
        buffer_len = len(buffer)
        if buffer_len == 0:
            return 1
        data = stream(buffer_len)
        text = data.decode('utf-8', 'replace')
        if text != '':
            consumer(text, outfile)
        return 0

    send_converted_text(stdout)  # Pass all text from read buffer
    send_converted_text(stderr)
    stdoutTime = datetime.datetime.now()
    inactivityDelta = datetime.timedelta(seconds=inactivityTimeout)
    while not channel.closed:
        read_ready_queue, _, _ = select.select([channel], [], [], readStdoutTimeout)
        hasOutput = False
        for ready_channel in read_ready_queue:
            if ready_channel.recv_ready():  # Pass stdout to the consumer
                if send_converted_text(stdout) == 0:
                    stdoutTime = datetime.datetime.now()
                hasOutput = True
            if ready_channel.recv_stderr_ready():  # Pass stderr to the consumer
                if send_converted_text(stderr) == 0:
                    stdoutTime = datetime.datetime.now()
                hasOutput = True
        if (channel.exit_status_ready() and not channel.recv_stderr_ready() and
                not channel.recv_ready() and not hasOutput):
            closeCorrectly(channel, commandFile, logCommand)
            break
        if (datetime.datetime.now() - stdoutTime) > inactivityDelta:
            printCommandInfo(f"Timeout {inactivityTimeout}", commandFile, logCommand=logCommand, prefix="Error")
            closeCorrectly(channel, commandFile, logCommand)
            break
    send_converted_text(stdout)  # Pass all residual text
    send_converted_text(stderr)
    con_stdout.close()
    con_stderr.close()

    return channel.recv_exit_status()


def runSshCommand(machine, cmd, consumer, outfile, getPty=False,
                  commandFile=None, timeout=1):
    """
    Establish SSH connection, run single command and close ssh connection.

    Accepts all arguments from the interactiveExec except for the ssh
     connection object.
    """
    sshConnection = createSSH(machine)
    result = interactiveExec(
        sshConnection,
        cmd,
        consumer,
        outfile,
        getPty,
        commandFile,
        timeout,
    )
    sshConnection.close()
    return result


def printAndSaveText(text, outfile):
    outfile.write(text)
    print(text, end='', flush=True)


def addTextToList(text, myList):
    myList.append(text)


def printText(text, _):
    printInfo(text)


def getResultsFromLogServer(log_path, log_server, log_server_user, local_path):
    ssh = loadSSH(withSystemKeys=True)
    ssh.connect(log_server, username=log_server_user)
    scp = SCPClient(ssh.get_transport())
    scp.get(local_path=local_path, remote_path=log_path, recursive=True)
    logDir = log_path.split('/')[-1]
    if logDir == '':
        logDir = log_path.split('/')[-2]
    os.system(f'mv {local_path}/{logDir}/* {local_path}/ ; rm -rf {local_path}/{logDir}')


def formServerLogPath(
        baseDir,
        product,
        target,
        image,
        buildName,
        buildNumber,
):
    """Method constructs the path to the logs of the build"""
    return os.path.join(baseDir, product, target, image, buildName, buildNumber)


def publishFileToRepo(filePath, log_path, log_server, log_server_user, isAllTogether=False,
                      binary_tarbal_path='', removeOldInfo=True):
    """Copy directory content or single file to repository"""
    ssh = loadSSH(withSystemKeys=True)
    ssh.connect(log_server, username=log_server_user)
    runCommand = createRunner(ssh)

    if removeOldInfo:
        runCommand('rm -rf ' + log_path)
    runCommand('mkdir -p ' + log_path)

    if runCommand(f'[ -d "{log_path}" ]') != 0:
        raise PublishError("Error creating logs directory on repo!")
    if not os.path.exists(filePath):
        raise PublishError(f"File '{filePath}' do not exist!")

    scp = SCPClient(ssh.get_transport())
    scp.put(filePath, remote_path=log_path, recursive=True)

    fileName = pathlib.Path(filePath).name
    if runCommand(f'[ -d "{log_path}/{fileName}" ]') == 0:
        runCommand(f'mv {log_path}/{fileName}/* {log_path}/')
        runCommand(f'rm -rf {log_path}/{fileName}')
    runCommand(f'chmod 777 -R {log_path}/*')
    if isAllTogether:
        runCommand('mkdir -p ' + binary_tarbal_path)
        runCommand('mv ' + log_path + '/*.tar.gz ' + binary_tarbal_path + '/')
    ssh.close()


def createSSH(machine):
    ssh = loadSSH(muteOutput=True)
    ssh.connect(machine.ip_address, username=machine.ssh_user, key_filename=machine.ssh_key)
    return ssh


def sshMariaDB(ssh, sql, build_log_file, db=''):
    """Executes SQL on the remote node"""
    printInfo('SQL: ' + sql)
    return interactiveExec(ssh, 'echo \"' + sql + '\" | sudo mysql ' + db,
                           printAndSaveText, build_log_file, get_pty=True)


def sqlMariaDB(ssh, sqls, build_log_file, db=''):
    """Executes several SQLs on the remove node"""
    for sql in sqls:
        sshMariaDB(ssh, sql, build_log_file, db)


def sshMachine(ssh, cmd):
    config_log_file = open('results/cmd.log', 'w+', errors='ignore')
    res = 0
    for line in cmd.split('\n'):
        if interactiveExec(ssh, line, printAndSaveText, config_log_file, get_pty=True) != 0:
            res = 1
    config_log_file.close()
    return res


def getServerStartCmd():
    return 'sudo systemctl start mariadb'


def getClientCmd(version, windows_extension=False):
    client = 'mariadb'
    if version and majorVersion(version) in VERSIONS_WITH_MYSQL_CLIENT:
        client = 'mysql'

    if windows_extension and client == 'mysql':
        client = 'mysql.exe'

    return client


def extractVersion(versionReplyStrings):
    nonEmptyStrings = list(filter(lambda line: len(line.strip()) > 0, versionReplyStrings))
    linesWithVersions = list(filter(lambda line: line[1].find('version') >= 0, enumerate(nonEmptyStrings)))
    if len(linesWithVersions) == 0:
        return ''
    versionLineIndex = linesWithVersions[0][0] + 1  # index of the line with version string
    versionParts = nonEmptyStrings[versionLineIndex].split('-')
    if len(versionParts) == 0:
        return ''
    return versionParts[0]


def readMariaDBVersionFromFile(logFilePath):
    logFile = open(logFilePath, 'r', errors='ignore')
    verReply = logFile.readlines()
    ver = extractVersion(verReply)
    logFile.close()
    return ver


def getMariaDBVersion(ssh, logFilePath=None, commandFile=None, startServer=True):
    tempFile = None
    if logFilePath is None:
        tempFile, logFilePath = tempfile.mkstemp()
    build_log_file = open(logFilePath, 'w+', errors='ignore')
    if startServer:
        printInfo('Starting server')
        interactiveExec(ssh, getServerStartCmd(),
                        printAndSaveText, build_log_file, get_pty=True, commandFile=commandFile)
    result = interactiveExec(ssh,
                             'which mariadb',
                             printAndSaveText, build_log_file, get_pty=True, commandFile=commandFile)
    if result == 0:
        clientCMD = 'mariadb'
    else:
        clientCMD = 'mysql'
    result = interactiveExec(ssh,
                             f'echo select @@version | sudo {clientCMD}',
                             printAndSaveText, build_log_file, get_pty=True, commandFile=commandFile)
    if result != 0:
        printInfo("'select @@version' FAILED")
    build_log_file.close()
    ver = readMariaDBVersionFromFile(logFilePath)
    if tempFile:
        os.close(tempFile)
        os.remove(logFilePath)
    return ver


def getMariaDBClientVersion(ssh):
    verTxt = []
    interactiveExec(ssh, 'mysql --version', addTextToList, verTxt)
    for line in verTxt:
        if '-MariaDB' in line:
            return line.split('-MariaDB')[0].split(' ')[-1]
            break
    return ''


CS_REPO_SETUP_URL = 'https://r.mariadb.com/downloads/mariadb_repo_setup'
ES_REPO_SETUP_URL = 'https://dlm.mariadb.com/enterprise-release-helpers/mariadb_es_repo_setup'
ES_REPO_SETUP_URL_CHECKSUM = 'https://dlm.mariadb.com/enterprise-release-helpers/checksums/sha256sums.txt'
CS_REPO_SETUP_FILE = 'mariadb_repo_setup'
ES_REPO_SETUP_FILE = 'mariadb_es_repo_setup'


def getVersionForSetupRepo(target, product):
    if product in ENTERPRISE_PRODS:
        versionsTable = ES_VERSIONS
    else:
        versionsTable = CS_VERSIONS
    if len(target.split('.')) == 3 and target in versionsTable.keys():
        return versionsTable[target]
    else:
        return target


def runSetupRepoScript(machine_name, product, target, token, logFile, includeUnsupported=False, commandsFile=None):
    if product not in PRODUCTION_PRODS and not includeUnsupported:
        printInfo('Repo setup script can configure only production repos')
        return 1
    if product in SERVER_PRODS:
        verOpt = ' --mariadb-server-version=' + getVersionForSetupRepo(target, product) + ' --skip-maxscale --skip-tools'
    if product in MAXSCALE_PRODS:
        verOpt = ' --mariadb-maxscale-version=' + getVersionForSetupRepo(target, product) + ' --skip-server --skip-tools'
    machine = Machine(machine_name, 'build')
    ssh = loadSSH()
    ssh.connect(machine.ip_address, username=machine.ssh_user, key_filename=machine.ssh_key)
    build_log_file = open(logFile, 'w+', errors='ignore')
    printInfo('Downloading repo setup script')
    if product not in ENTERPRISE_PRODS:
        repoSetupUrl = CS_REPO_SETUP_URL
        repoSetupUrlChecksum = ''
        repoSetupFile = CS_REPO_SETUP_FILE
        tokenOpt = ''
    else:
        if product in [ES_PRODUCT_STAGING]:
            repoSetupUrl = f'https://dlm.mariadb.com/{token}/enterprise-release-helpers-staging/mariadb_es_repo_setup'
            repoSetupUrlChecksum = f'https://dlm.mariadb.com/{token}/enterprise-release-helpers-staging/checksums/sha256sums.txt'
        else:
            repoSetupUrl = ES_REPO_SETUP_URL
            repoSetupUrlChecksum = ES_REPO_SETUP_URL_CHECKSUM
        repoSetupFile = ES_REPO_SETUP_FILE
        tokenOpt = ' --token=' + token

    interactiveExec(ssh, CMDS[DISTRO_CLASSES[machine.platform]] + ' curl',
                    printAndSaveText, build_log_file, get_pty=True, commandFile=commandsFile)
    if DISTRO_CLASSES[machine.platform] in ['apt']:
        interactiveExec(ssh, CMDS[DISTRO_CLASSES[machine.platform]] + ' apt-transport-https',
                        printAndSaveText, build_log_file, get_pty=True, commandFile=commandsFile)
    result = interactiveExec(ssh, 'curl -sSLO ' + repoSetupUrl,
                             printAndSaveText, build_log_file, get_pty=True, commandFile=commandsFile)
    if len(repoSetupUrlChecksum) != 0:
        result = interactiveExec(ssh, f'curl -LsS {repoSetupUrlChecksum} | grep {repoSetupFile} | sha256sum -c',
                                 printAndSaveText, build_log_file, get_pty=True, commandFile=commandsFile)
    if result != 0 and product in [ES_PRODUCT_STAGING]:
        interactiveExec(ssh, 'curl -sSLO ' + ES_REPO_SETUP_URL,
                        printAndSaveText, build_log_file, get_pty=True, commandFile=commandsFile)
        result = interactiveExec(ssh, f'curl -LsS {ES_REPO_SETUP_URL_CHECKSUM} | sha256sum -c',
                                 printAndSaveText, build_log_file, get_pty=True, commandFile=commandsFile)
    interactiveExec(ssh, 'chmod +x ' + repoSetupFile,
                    printAndSaveText, build_log_file, get_pty=True, commandFile=commandsFile)
    build_log_file.close()
    if result != 0:
        printInfo('Repo setup script download error')
        return result
    build_log_file = open(logFile, 'w+', errors='ignore')
    printInfo('Downloading repo setup script')
    include = ''
    if includeUnsupported:
        include = f' --include-unsupported'
    ignoreVersionOpt = ''
    if product in STAGING_PRODS:
        ignoreVersionOpt = ' --skip-verify '
    result = interactiveExec(ssh, 'sudo ./' + repoSetupFile + verOpt + tokenOpt + ignoreVersionOpt + ' --apply'
                             + include,
                             printAndSaveText, build_log_file, get_pty=True, commandFile=commandsFile)
    build_log_file.close()
    if result != 0:
        printInfo('Repo setup script executiom error')
        return result
    if includeUnsupported:
        finishFile(ssh, logFile, machine.platform, product)
    if product == ES_PRODUCT_STAGING:
        generateForStaging(ssh, logFile, machine.platform)
    return 0


def generateMariaDbVersion(fullVersion):
    for version in MARIADB_ALL_VERSIONS:
        if version in fullVersion:
            return version
    return '10.5'


def finishFile(ssh, logFile, platform, product):
    build_log_file = open(logFile, 'w+', errors='ignore')
    if product == ES_PRODUCT_STAGING:
        interactiveExec(ssh, 'sudo sed -i "s/unsupported/unsupported-staging/" ' + MARIADB_PACKET_FILE[platform],
                        printAndSaveText, build_log_file, get_pty=True)
    build_log_file.close()


def generateForStaging(ssh, logFile, platform):
    build_log_file = open(logFile, 'w+', errors='ignore')
    interactiveExec(ssh, 'sudo sed -i "s/enterprise-server/enterprise-staging/" ' + MARIADB_PACKET_FILE[platform],
                    printAndSaveText, build_log_file, get_pty=True)
    interactiveExec(ssh, UPDATE_REPO[DISTRO_CLASSES[platform]],
                    printAndSaveText, build_log_file, get_pty=True)
    build_log_file.close()


def majorVersion(ver):
    """
    Return 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8
    """
    verSplitted = ver.split('.')
    if len(verSplitted) > 2:
        return verSplitted[0] + '.' + verSplitted[1]
    else:
        return ver


def minorVersion(ver):
    """
    Return 2, 3, 4, 5, 6, ... for 10.2, 10.3, 10.4, 10.5, 10.6, ...
    """
    verSplitted = ver.split('.')
    if len(verSplitted) >= 2:
        return verSplitted[1]
    else:
        return ver


def majorVersionForDistro(platform, platformVersion):
    """
    Get a major version of MariaDB Server provided by a specified platform
    :param platform: name of the platform
    :param platformVersion: release of the platform
    :return: version of the server in the distributive
    """
    return dig(VERSIONS_IN_DISTROS, platform, platformVersion, default="10.0")


def getPreviousMinorVersion(version, product):
    if product in ENTERPRISE_PRODS:
        versionList = ES_VERIONS_LISTS.get(majorVersion(version), [])
        versions = ES_VERSIONS
    else:
        versionList = CS_VERIONS_LISTS.get(majorVersion(version), [])
        versions = CS_VERSIONS
    if version in versionList:
        i = versionList.index(version)
        if i > 0:
            return versions[versionList[i-1]]
        else:
            return getPreviousMajorVersion(version, product)
    return majorVersion(version)


def getPreviousMajorVersion(version, product):
    if product in ENTERPRISE_PRODS:
        versionList = list(ES_VERIONS_LISTS.keys())
    else:
        versionList = list(CS_VERIONS_LISTS.keys())

    majorVer = majorVersion(version)
    if not majorVer in versionList:
        return majorVer
    i = versionList.index(majorVer)
    if i > 0:
        return versionList[i - 1]
    else:
        return majorVer


class Machine:
    def __init__(self, configName, nodeName):
        self.fullName = configName + '/' + nodeName
        self.ip_address = getMdbciInfo('show', 'network', self.fullName)
        self.ssh_key = getMdbciInfo('show', 'keyfile', self.fullName)
        self.ssh_user = getMdbciInfo('ssh', '--command', 'whoami', self.fullName).split('\\')[-1]
        self.box = getMdbciInfo('show', 'box', self.fullName)
        self.platform = getMdbciInfo('show', 'boxinfo', '--box-name={}'.format(self.box), '--field', 'platform',
                                     self.fullName)
        self.platform_version = getMdbciInfo('show', 'boxinfo', '--box-name={}'.format(self.box),
                                             '--field', 'platform_version', self.fullName)
        self.architecture = getMdbciInfo('show', 'boxinfo', '--box-name={}'.format(self.box),
                                         '--silent', '--field', 'architecture')
        self.provider = getMdbciInfo('show', 'boxinfo', '--box-name={}'.format(self.box),
                                         '--silent', '--field', 'provider')


def stringArgumentToBool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0', 'none'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def determiteGalera(minorVersion, product):
    if minorVersion > 3:
        galeraVersion = 4
    else:
        galeraVersion = 3
    # Form Galera product name
    galeraProduct = 'MariaDBGalera' + str(galeraVersion)
    if product in ENTERPRISE_PRODS:
        galeraProduct += 'Enterprise'
    return galeraProduct


def installMSAN(ssh):
    build_log_file = open('results/msan_install.log', 'w+', errors='ignore')
    interactiveExec(ssh,
                    'DEBIAN_FRONTEND="noninteractive" sudo apt-get update && '
                    'sudo apt-get -y install ca-certificates && '
                    'sudo apt-get install -y bison cmake git pkg-config wget nettle-dev libc6-dev '
                    'clang-10 clang++-10 libc++-10-dev libc++abi-10-dev',
                    printAndSaveText, build_log_file, get_pty=True)

    interactiveExec(ssh,
                    'DEBIAN_FRONTEND="noninteractive" sudo apt-get remove gcc gcc-9 -y && sudo apt-get autoremove -y',
                    printAndSaveText, build_log_file, get_pty=True)
    interactiveExec(ssh,
                    'sudo rm -rf /usr/src; sudo mkdir -p /usr/src; sudo chmod a+rwx /usr/src; cd /usr/src; '
                    'git clone --depth 1 https://github.com/llvm/llvm-project.git -b llvmorg-10.0.0',
                    printAndSaveText, build_log_file, get_pty=True)
    interactiveExec(ssh,
                    'cd /usr/src; mkdir libcxx_msan && cd libcxx_msan && '
                    'CC=clang-10 CXX=clang++-10 cmake ../llvm-project/llvm -DCMAKE_BUILD_TYPE=Release '
                    '-DLLVM_ENABLE_PROJECTS="libcxx;libcxxabi" '
                    '-DLLVM_USE_SANITIZER=MemoryWithOrigins '
                    '-DCMAKE_INSTALL_PREFIX=/usr/local && '
                    'cmake --build . --parallel `nproc` -- cxx cxxabi && sudo make install-cxx install-cxxabi && '
                    'ldconfig',
                    printAndSaveText, build_log_file, get_pty=True)
    interactiveExec(ssh,
                    'cd /usr/src; wget https://www.gnupg.org/ftp/gcrypt/gnutls/v3.6/gnutls-3.6.14.tar.xz && '
                    'tar xf gnutls-3.6.14.tar.xz && cd gnutls-3.6.14 && '
                    'CC=clang-10 CXX=clang++-10 ./configure CFLAGS="-fno-omit-frame-pointer -O0 -g -fsanitize=memory" '
                    'LDFLAGS="-fsanitize=memory" --with-included-libtasn1 '
                    '--with-included-unistring --without-p11-kit && make -j`nproc` && sudo make install',
                    printAndSaveText, build_log_file, get_pty=True)
    build_log_file.close()


def loadSSH(withSystemKeys=False, muteOutput=False):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    logger = paramiko.util.logging.getLogger()
    if muteOutput:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.INFO)

    if withSystemKeys:
        ssh.load_system_host_keys()
    return ssh


def runBashCommand(cmd):
    printCommandInfo(cmd)
    return os.system(cmd)


def generateSshFromMachineName(machineName, withMachine=False):
    setupMdbciEnvironment()
    machine = Machine(machineName, 'build')
    ssh = createSSH(machine)
    if withMachine:
        return ssh, machine
    else:
        return ssh


def prepareResultsDirectory():
    shutil.rmtree('results', ignore_errors=True)
    os.mkdir('results')


def createCommandsLogFile(name, directory, additionalFileName=None, machineName=None):
    if directory is None:
        return None
    os.makedirs(directory, exist_ok=True)
    if additionalFileName is None:
        return open(f'{directory}/{name}', 'a')
    else:
        if os.path.isfile(f'{directory}/{additionalFileName}'):
            return open(f'{directory}/{additionalFileName}')
        else:
            mainFile = open(f'{directory}/{name}', 'a')
            print(f"ssh -F {os.environ['MDBCI_VM_PATH']}{machineName}_ssh_config build < {additionalFileName}", file=mainFile)
            mainFile.close()
            return open(f'{directory}/{additionalFileName}', 'a')


def limitMemory(maxScriptMemory=1024 * 1024 * 1024 * 4):
    """
    Return decorator that set maximum amount of memory that script can use.
    Default maximum memory - 4GB.
    """
    def limitMemoryDecorator(function):
        def _wrapper(*args, **kwargs):
            _, hard = resource.getrlimit(resource.RLIMIT_AS)
            resource.setrlimit(resource.RLIMIT_AS, (maxScriptMemory, hard))

            try:
                return function(*args, **kwargs)
            except MemoryError:
                printInfo(f"Out of memory! Limit: {maxScriptMemory} bytes.")
                exit(2)

        return _wrapper

    return limitMemoryDecorator


def runLocalCommand(cmd):
    printInfo('RUN: {}'.format(cmd))
    proc = subprocess.Popen(cmd,
                            shell=True,
                            stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    outStr = ''
    errStr = ''
    if out:
        outStr = out.decode('utf-8').rstrip("\n")
    if err:
        errStr = err.decode('utf-8').rstrip("\n")
    res = proc.returncode
    printInfo(outStr)
    if res != 0:
        printInfo('ERROR: {}'.format(errStr))
    return outStr, errStr, res


def compareVersions(version1: str, version2: str) -> int:
    """
    :return: 0 if versions are equal, 1 if version1 > version2,
    -1 if version1 < version2
    """
    try:
        versionSplitted1 = version1.split('.')
        versionSplitted1 = list(map(int, versionSplitted1))
        versionSplitted2 = version2.split('.')
        versionSplitted2 = list(map(int, versionSplitted2))

        minLen = min(len(versionSplitted1), len(versionSplitted2))
        for i in range(minLen):
            if versionSplitted1[i] > versionSplitted2[i]:
                return 1
            elif versionSplitted1[i] < versionSplitted2[i]:
                return -1
        return 0
    except ValueError:
        raise ValueError(f"Incorrect version format error when "
                         f"comparing '{version1}' and '{version2}'!")
