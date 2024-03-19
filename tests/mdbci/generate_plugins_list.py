#!/usr/bin/env python3

# To be moved to the library to share between Columnstore and BuildBot repos

import re
import sys
from argparse import ArgumentParser
import json
import common
import pathlib

configurationRootPath = pathlib.Path(__file__).parent.parent.parent
sys.path.append(str(configurationRootPath))
from constants.build_parameters import products_versions

ALL_VERSIONS = "0-"

ALL_RPMS = {
    "centos_7": ALL_VERSIONS + ", distro",
    "rocky_8": ALL_VERSIONS + ", distro",
    "rocky_9": ALL_VERSIONS + ", distro",
    "rhel_7": ALL_VERSIONS + ", distro",
    "rhel_8": ALL_VERSIONS + ", distro",
    "rhel_9": ALL_VERSIONS + ", distro",
    "sles_12": ALL_VERSIONS + ", distro",
    "sles_15": ALL_VERSIONS + ", distro",
    "suse_15": ALL_VERSIONS + ", distro",
}

ALL_RPMS_WITHOUT_DISTRO = {
    "centos_7": ALL_VERSIONS,
    "rocky_8": ALL_VERSIONS,
    "rocky_9": ALL_VERSIONS,
    "rhel_7": ALL_VERSIONS,
    "rhel_8": ALL_VERSIONS,
    "rhel_9": ALL_VERSIONS,
    "sles_12": ALL_VERSIONS,
    "sles_15": ALL_VERSIONS,
    "suse_15": ALL_VERSIONS,
}

ALL_DEBS_WITHOUT_DISTRO = {
    "debian_stretch": ALL_VERSIONS,
    "debian_buster": ALL_VERSIONS,
    "debian_bullseye": ALL_VERSIONS,
    "debian_bookworm": ALL_VERSIONS,
    "ubuntu_xenial": ALL_VERSIONS,
    "ubuntu_bionic": ALL_VERSIONS,
    "ubuntu_focal": ALL_VERSIONS,
    "ubuntu_jammy": ALL_VERSIONS,
}

ALL_DEBS = {
    "debian_stretch": ALL_VERSIONS + ", distro",
    "debian_buster": ALL_VERSIONS + ", distro",
    "debian_bullseye": ALL_VERSIONS + ", distro",
    "ubuntu_xenial": ALL_VERSIONS + ", distro",
    "ubuntu_bionic": ALL_VERSIONS + ", distro",
    "ubuntu_focal": ALL_VERSIONS + ", distro",
    "ubuntu_jammy": ALL_VERSIONS + ", distro",
}

PLATFORMS_SIMPLE = {
    "centos_7": "10.2-, 11.0-, 23-",
    "rocky_8": "10.2-, 11.0-, 23-, distro",
    "rocky_9": "10.2-, 11.0-, 23-, distro",
    "rhel_7": "10.2-, 11.0-, 23-",
    "rhel_8": "10.2-, 11.0-, 23-, distro",
    "rhel_9": "10.2-, 11.0-, 23-, distro",
    "debian_stretch": "10.2-, 11.0-, 23-",
    "debian_buster": "10.2-, 11.0-, 23-, distro",
    "debian_bullseye": "10.2-, 11.0-, 23-, distro",
    "debian_bookworm": "10.2-, 11.0-, 23-, distro",
    "ubuntu_xenial": "10.2-, 11.0-, 23-",
    "ubuntu_bionic": "10.2-, -10.3.39, -10.4.30, -10.5.21, -10.6.14, 23-",
    "ubuntu_focal": "10.2-, 11.0-, 23-, distro",
    "ubuntu_jammy": "10.2-, 11.0-, 23-, distro",
    "sles_12": "10.2-, 11.0-, 23-",
    "sles_15": "10.2-, 11.0-, 23-, distro",
    "suse_15": "10.2-, 11.0-, 23-, distro",
}

PLATFORMS_WITH_EXCLUDE_BY_DISTRO = {
    "centos_7": "10.2-, 23-",
    "rocky_8": "10.2-, 23-",
    "rocky_9": "10.2-, 23-, distro",
    "rhel_7": "10.2-, 23-",
    "rhel_8": "10.2-, 23-",
    "rhel_9": "10.2-, 23-, distro",
    "debian_stretch": "10.2-, 23-",
    "debian_buster": "10.2-, 23-, distro",
    "debian_bullseye": "10.2-, 23-, distro",
    "debian_bookworm": "10.2-, 23-, distro",
    "ubuntu_xenial": "10.2-, 23-",
    "ubuntu_bionic": "10.2-, 23-",
    "ubuntu_focal": "10.2-, 23-, distro",
    "ubuntu_jammy": "10.2-, 23-, distro",
    "sles_12": "10.2-, 23-",
    "sles_15": "10.2-, 23-, distro",
    "suse_15": "10.2-, 23-, distro",
}

PLATFORMS_COLUMNSTORE_ENTERPRISE_AARCH64 = {
    "rhel_8": "10.6.9-, 23-",
    "rhel_9": "10.6.9-, 23-",
    "rocky_8": "10.6.9-, 23-",
    "rocky_9": "10.6.9-, 23-",
    "ubuntu_jammy": "10.6.9-, 23-",
    "ubuntu_focal": "10.6.9-, 23-",
    "debian_bullseye": "10.6.9-, 23-",
    #"debian_bookworm": "23-",
}

PLATFORMS_COLUMNSTORE_ENTERPRISE_AMD64 = {
    "rhel_7": "10.5, 10.6, 23-",
    "rhel_8": "10.5, 10.6, 23-",
    "rhel_9": "10.6.9-, 23-",
    "rocky_8": "10.5, 10.6, 23-",
    "rocky_9": "10.6.9-, 23-",
    "ubuntu_bionic": "-10.5.16, -10.6.8",
    "ubuntu_jammy": "10.6.9-, 23-",
    "ubuntu_focal": "10.5, 10.6, 23-",
    "debian_buster": "10.5, -10.6.8",
    "debian_bullseye": "10.6.9-, 23-",
    #"debian_bookworm": "23-",
}

PLATFORMS_COLUMNSTORE_COMMUNITY_AARCH64 = {
    "centos_7": "10.5, 10.7",
    "rocky_8": "10.5, 10.7",
    "rocky_9": "10.7",
    "rhel_7": "10.5, 10.7",
    "rhel_8": "10.7",
    "rhel_9": "10.7",
    "debian_buster": "10.7",
    "ubuntu_focal": "10.7",
    "ubuntu_jammy": "distro",
    "suse_15": "10.5, 10.7, distro",
}

PLATFORMS_COLUMNSTORE_COMMUNITY_AMD64 = {
    "centos_7": "10.5",
    "rocky_8": "10.6, 23-",
    "rocky_9": "10.6, 23-",
    "rhel_7": "10.5",
    "rhel_8": "10.6, 23-",
    "rhel_9": "10.6, 23-",
    "debian_buster": "10.7",
    "debian_bullseye": "10.6, 23-",
    "ubuntu_xenial": "10.5,",
    "ubuntu_focal": "10.6, 23-",
    "ubuntu_jammy": "10.5, distro",
    "suse_15": "10.5, distro",
}

PLATFORMS_GSSAPI_CLIENT = {
    "debian_stretch": "10.2-, 11.0-, 23-",
    "debian_buster": "10.2-, 11.0-, 23-, distro",
    "debian_bullseye": "10.2-, 11.0-, 23-, distro",
    "debian_bookworm": "10.2-, 11.0-, 23-, distro",
    "ubuntu_xenial": "10.2-, 11.0-, 23-",
    "ubuntu_bionic": "10.2-, 11.0-, 23-",
    "ubuntu_focal": "10.2-, 11.0-, 23-, distro",
    "ubuntu_jammy": "10.2-, 11.0-, 23-, distro",
    "suse_15": "10.2-, 11.0-, 23-, distro",
}

PLATFORMS_SPIDER_ES = {
    "debian_stretch": "10.2-",
    "debian_buster": "10.2-, 23-, distro",
    "debian_bullseye": "10.2-, 23-, distro",
    "debian_bookworm": "10.2-, 23-, distro",
    "ubuntu_xenial": "10.2-, 23-",
    "ubuntu_bionic": "10.2-, 23-",
    "ubuntu_focal": "10.2-, 23-, distro",
    "ubuntu_jammy": "10.2-, 23-, distro",
    "centos_7": "10.4-, 23-, distro",
    "rocky_8": "10.4-, 23-, distro",
    "rocky_9": "10.4-, 23-, distro",
    "rhel_7": "10.4-, 23-, distro",
    "rhel_8": "10.4-, 23-, distro",
    "rhel_9": "10.4-, 23-, distro",
    "suse_15": "10.4-, 23-, distro",
    "sles_12": "10.4-, 23-, distro",
    "sles_15": "10.4-, 23-, distro",
}

PLATFORMS_SPIDER_CS = {
    "debian_stretch": "10.2-",
    "debian_buster": "10.2-, 11.0-, 23-, distro",
    "debian_bullseye": "10.2-, 11.0-, 23-, distro",
    "debian_bookworm": "10.2-, 11.0-, 23-, distro",
    "ubuntu_xenial": "10.2-",
    "ubuntu_bionic": "10.2-, 11.0-, 23-",
    "ubuntu_focal": "10.2-, 11.0-, 23-, distro",
    "ubuntu_jammy": "10.2-, 11.0-, 23-, distro",
}

PLATFORMS_S3 = {
    "centos_7": "10.5-, 11.0-, 23-",
    "rocky_8": "10.5-, 11.0-, 23-",
    "rocky_9": "10.5-, 11.0-, 23-, distro",
    "rhel_7": "10.5-, 11.0-, 23-",
    "rhel_8": "10.5-, 11.0-, 23-",
    "rhel_9": "10.5-, 11.0-, 23-, distro",
    "debian_stretch": "10.5-, 11.0-, 23-",
    "debian_buster": "10.5-, 11.0-, 23-",
    "debian_bullseye": "10.5-, 11.0-, 23-, distro",
    "debian_bookworm": "10.5-, 11.0-, 23-, distro",
    "ubuntu_xenial": "10.5-, 11.0-, 23-",
    "ubuntu_bionic": "10.5-, 11.0-, 23-",
    "ubuntu_focal": "10.5-, 11.0-, 23-",
    "ubuntu_jammy": "10.5-, 11.0-, 23-, distro",
    "sles_12": "10.5-, 11.0-, 23-",
    "sles_15": "10.5-, 11.0-, 23-, distro",
    "suse_15": "10.5-, 11.0-, 23-, distro",
}

PLATFORMS_XPAND = {
    "centos_7": "10.5, 10.6, 10.7, 10.8, 23-",
    "rocky_8": "10.5, 10.6, 10.7, 10.8, 23-",
    "rocky_9": "10.5, 10.6, 10.7, 10.8, 23-, distro",
    "rhel_7": "10.5, 10.6, 10.7, 10.8, 23-",
    "rhel_8": "10.5, 10.6, 10.7, 10.8, 23-",
    "rhel_9": "10.5, 10.6, 10.7, 10.8, 23-, distro",
    "debian_stretch": "10.5, 10.6, 10.7, 10.8, 23-",
    "debian_buster": "10.5, 10.6, 10.7, 10.8, 23-",
    "debian_bullseye": "10.5, 10.6, 10.7, 10.8, 23-, distro",
    "debian_bookworm": "10.5, 10.6, 10.7, 10.8, 23-, distro",
    "ubuntu_xenial": "10.5, 10.6, 10.7, 10.8, 23-",
    "ubuntu_bionic": "10.5, 10.6, 10.7, 10.8, 23-",
    "ubuntu_focal": "10.5, 10.6, 10.7, 10.8, 23-",
    "ubuntu_jammy": "10.5, 10.6, 10.7, 10.8, 23-, distro",
    "sles_12": "10.5, 10.6, 10.7, 10.8, 23-",
    "sles_15": "10.5, 10.6, 10.7, 10.8, 23-, distro",
    "suse_15": "10.5, 10.6, 10.7, 10.8, 23-, distro",
}

PLUGINS_ENTERPRISE = {
    "columnstore": {
        "aarch64": PLATFORMS_COLUMNSTORE_ENTERPRISE_AARCH64,
        "amd64": PLATFORMS_COLUMNSTORE_ENTERPRISE_AMD64
    },
    "cracklib_password_check": {
        "aarch64": PLATFORMS_WITH_EXCLUDE_BY_DISTRO,
        "amd64": PLATFORMS_WITH_EXCLUDE_BY_DISTRO
    },
    "gssapi_client": {
        "aarch64": PLATFORMS_GSSAPI_CLIENT,
        "amd64": PLATFORMS_GSSAPI_CLIENT
    },
    "gssapi_server": {
        "aarch64": PLATFORMS_SIMPLE,
        "amd64": PLATFORMS_SIMPLE
    },
    "rocksdb": {
        "aarch64": PLATFORMS_WITH_EXCLUDE_BY_DISTRO,
        "amd64": PLATFORMS_WITH_EXCLUDE_BY_DISTRO
    },
    "spider": {
        "aarch64": PLATFORMS_SPIDER_ES,
        "amd64": PLATFORMS_SPIDER_ES
    },
    "backup": {
        "aarch64": PLATFORMS_SIMPLE,
        "amd64": PLATFORMS_SIMPLE
    },
    "s3": {
        "aarch64": PLATFORMS_S3,
        "amd64": PLATFORMS_S3
    },
    # "xpand": {
    #     "aarch64": PLATFORMS_XPAND,
    #     "amd64": PLATFORMS_XPAND
    # },
}

PLUGINS_COMMUNITY = {
    "columnstore": {
        "aarch64": PLATFORMS_COLUMNSTORE_COMMUNITY_AARCH64,
        "amd64": PLATFORMS_COLUMNSTORE_COMMUNITY_AMD64
    },
    "cracklib_password_check": {
        "aarch64": PLATFORMS_WITH_EXCLUDE_BY_DISTRO,
        "amd64": PLATFORMS_WITH_EXCLUDE_BY_DISTRO
    },
    "gssapi_client": {
        "aarch64": PLATFORMS_GSSAPI_CLIENT,
        "amd64": PLATFORMS_GSSAPI_CLIENT
    },
    "gssapi_server": {
        "aarch64": PLATFORMS_SIMPLE,
        "amd64": PLATFORMS_SIMPLE
    },
    "mariadb_test": {
        "aarch64": PLATFORMS_SIMPLE,
        "amd64": PLATFORMS_SIMPLE
    },
    "rocksdb": {
        "aarch64": PLATFORMS_WITH_EXCLUDE_BY_DISTRO,
        "amd64": PLATFORMS_WITH_EXCLUDE_BY_DISTRO
    },
    "spider": {
        "aarch64": PLATFORMS_SPIDER_CS,
        "amd64": PLATFORMS_SPIDER_CS
    },
    "backup": {
        "aarch64": PLATFORMS_SIMPLE,
        "amd64": PLATFORMS_SIMPLE
    },
    "s3": {
        "aarch64": PLATFORMS_S3,
        "amd64": PLATFORMS_S3
    },
}

PLUGINS_EXTEND = {
    "mariadb_client": {
        "centos_7": ALL_VERSIONS,
        "rocky_8": ALL_VERSIONS,
        "rocky_9": ALL_VERSIONS + ", distro",
        "rhel_7": ALL_VERSIONS,
        "rhel_8": ALL_VERSIONS,
        "rhel_9": ALL_VERSIONS + ", distro",
        "debian_stretch": ALL_VERSIONS + ", distro",
        "debian_buster": ALL_VERSIONS + ", distro",
        "debian_bullseye": ALL_VERSIONS + ", distro",
        "debian_bookworm": ALL_VERSIONS + ", distro",
        "ubuntu_xenial": ALL_VERSIONS + ", distro",
        "ubuntu_bionic": ALL_VERSIONS + ", distro",
        "ubuntu_focal": ALL_VERSIONS + ", distro",
        "ubuntu_jammy": ALL_VERSIONS + ", distro",
        "sles_12": ALL_VERSIONS,
        "sles_15": ALL_VERSIONS,
        "suse_15": ALL_VERSIONS + ", distro",
    },
    "mariadb_client_debuginfo": ALL_RPMS_WITHOUT_DISTRO,
    "mariadb_common": {
        "centos_7": ALL_VERSIONS,
        "rocky_8": ALL_VERSIONS + ", distro",
        "rocky_9": ALL_VERSIONS + ", distro",
        "rhel_7": ALL_VERSIONS,
        "rhel_8": ALL_VERSIONS + ", distro",
        "rhel_9": ALL_VERSIONS + ", distro",
        "debian_stretch": ALL_VERSIONS + ", distro",
        "debian_buster": ALL_VERSIONS + ", distro",
        "debian_bullseye": ALL_VERSIONS + ", distro",
        "debian_bookworm": ALL_VERSIONS + ", distro",
        "ubuntu_xenial": ALL_VERSIONS + ", distro",
        "ubuntu_bionic": ALL_VERSIONS + ", distro",
        "ubuntu_focal": ALL_VERSIONS + "-, distro",
        "ubuntu_jammy": ALL_VERSIONS + ", distro",
        "sles_12": ALL_VERSIONS,
        "sles_15": ALL_VERSIONS,
        "suse_15": ALL_VERSIONS + ", distro",
    },
    "mariadb_common_debuginfo": ALL_RPMS_WITHOUT_DISTRO,
    "mariadb_devel": ALL_RPMS_WITHOUT_DISTRO,
    "mariadb_devel_debuginfo": ALL_RPMS_WITHOUT_DISTRO,
    "mariadb_server": {
        "centos_7": ALL_VERSIONS + ", distro",
        "rocky_8": ALL_VERSIONS + ", distro",
        "rocky_9": ALL_VERSIONS + ", distro",
        "rhel_7": ALL_VERSIONS + ", distro",
        "rhel_8": ALL_VERSIONS + ", distro",
        "rhel_9": ALL_VERSIONS + ", distro",
        "debian_stretch": ALL_VERSIONS + ", distro",
        "debian_buster": ALL_VERSIONS + ", distro",
        "debian_bullseye": ALL_VERSIONS + ", distro",
        "debian_bookworm": ALL_VERSIONS + ", distro",
        "ubuntu_xenial": ALL_VERSIONS + ", distro",
        "ubuntu_bionic": ALL_VERSIONS + ", distro",
        "ubuntu_focal": ALL_VERSIONS + ", distro",
        "ubuntu_jammy": ALL_VERSIONS + ", distro",
        "sles_12": ALL_VERSIONS,
        "sles_15": ALL_VERSIONS,
        "suse_15": ALL_VERSIONS + ", distro",
    },
    "mariadb_server_debuginfo": ALL_RPMS_WITHOUT_DISTRO,
    "mariadb_shared": {
        "centos_7": ALL_VERSIONS,
        "rocky_8": ALL_VERSIONS,
        "rocky_9": ALL_VERSIONS + ", distro",
        "rhel_7": ALL_VERSIONS,
        "rhel_8": ALL_VERSIONS,
        "rhel_9": ALL_VERSIONS + ", distro",
        "sles_12": ALL_VERSIONS,
        "sles_15": ALL_VERSIONS,
        "suse_15": ALL_VERSIONS + ", distro",
    },
    "mariadb_shared_debuginfo": ALL_RPMS_WITHOUT_DISTRO,
    "libmariadb_dev": ALL_DEBS_WITHOUT_DISTRO,
    "libmariadb3": ALL_DEBS_WITHOUT_DISTRO,
    "libmariadbclient18": ALL_DEBS_WITHOUT_DISTRO,
    "libmysqlclient18": ALL_DEBS_WITHOUT_DISTRO,
    "mysql_common": ALL_DEBS,
}

PLUGINS_EXTEND_COMMUNITY = {
    "libmariadbd_dev": ALL_DEBS_WITHOUT_DISTRO,
    "libmariadbd19": ALL_DEBS_WITHOUT_DISTRO,
    "mariadb_test_data": ALL_DEBS,
}

PLUGINS_UNSUPPORTED = [
    "connect",
    "mariadb_test"
]

ADD_FOR_DISTRO_MARIADB_BY_PLATFORM = {
    "mariadb": ["rhel_7", "rhel_8", "centos_7",
                "rocky_8", "sles_12", "sles_15"],
    "mariadb-tools": ["sles_12", "sles_15"],
}

PLUGINS_ALIAS = {
    "columnstore": {
        "apt": ["mariadb-plugin-columnstore", "mariadb-columnstore-cmapi"],
        "yum": ["MariaDB-columnstore-engine", "MariaDB-columnstore-cmapi"],
        "zypper": ["MariaDB-columnstore-engine", "MariaDB-columnstore-cmapi"],
    },
    "cracklib_password_check": {
        "apt": ["mariadb-plugin-cracklib-password-check"],
        "yum": ["MariaDB-cracklib-password-check"],
        "zypper": ["MariaDB-cracklib-password-check"],
    },
    "gssapi_client": {
        "apt": ["mariadb-plugin-gssapi-client"],
        "yum": [""],
        "zypper": [""],
    },
    "gssapi_server": {
        "apt": ["mariadb-plugin-gssapi-server"],
        "yum": ["MariaDB-gssapi-server"],
        "zypper": ["MariaDB-gssapi-server"],
    },
    "mariadb_test": {
        "apt": ["mariadb-test"],
        "yum": ["MariaDB-test"],
        "zypper": ["MariaDB-test"],
    },
    "rocksdb": {
        "apt": ["mariadb-plugin-rocksdb"],
        "yum": ["MariaDB-rocksdb-engine"],
        "zypper": ["MariaDB-rocksdb-engine"],
    },
    "spider": {
        "apt": ["mariadb-plugin-spider"],
        "yum": ["MariaDB-spider-engine"],
        "zypper": ["MariaDB-spider-engine"],
    },
    "backup": {
        "apt": ["mariadb-backup"],
        "yum": ["MariaDB-backup"],
        "zypper": ["MariaDB-backup"],
    },
    "s3": {
        "apt": ["mariadb-plugin-s3"],
        "yum": ["MariaDB-s3-engine"],
        "zypper": ["MariaDB-s3-engine"],
    },
    # "xpand": {
    #     "apt": ["mariadb-plugin-xpand"],
    #     "yum": ["MariaDB-xpand-engine"],
    #     "zypper": ["MariaDB-xpand-engine"],
    # },
    "mariadb_client": {
        "apt": ["mariadb-client"],
        "yum": ["MariaDB-client"],
        "zypper": ["MariaDB-client"]
    },
    "mariadb_client_debuginfo": {
        "yum": ["MariaDB-client-debuginfo"],
        "zypper": ["MariaDB-client-debuginfo"]
    },
    "mariadb_common": {
        "apt": ["mariadb-common"],
        "yum": ["MariaDB-common"],
        "zypper": ["MariaDB-common"]
    },
    "mariadb_common_debuginfo": {
        "yum": ["MariaDB-common-debuginfo"],
        "zypper": ["MariaDB-common"]
    },
    "mariadb_devel": {
        "yum": ["MariaDB-devel"],
        "zypper": ["MariaDB-devel"]
    },
    "mariadb_devel_debuginfo": {
        "yum": ["MariaDB-devel-debuginfo"],
        "zypper": ["MariaDB-devel-debuginfo"]
    },
    "mariadb_server": {
        "apt": ["mariadb-server"],
        "yum": ["MariaDB-server"],
        "zypper": ["MariaDB-server"]
    },
    "mariadb_server_debuginfo": {
        "yum": ["MariaDB-server-debuginfo"],
        "zypper": ["MariaDB-server-debuginfo"]
    },
    "mariadb_shared": {
        "yum": ["MariaDB-shared"],
        "zypper": ["MariaDB-shared"]
    },
    "mariadb_shared_debuginfo": {
        "yum": ["MariaDB-shared-debuginfo"],
        "zypper": ["MariaDB-shared-debuginfo"]
    },
    "libmariadb_dev": {"apt": ["libmariadb-dev"]},
    "libmariadb3": {"apt": ["libmariadb3"]},
    "libmariadbclient18": {"apt": ["libmariadbclient18"]},
    "libmysqlclient18": {"apt": ["libmysqlclient18"]},
    "mysql_common": {"apt": ["mysql-common"]},
    "libmariadbd_dev": {"apt": ["libmariadbd-dev"]},
    "libmariadbd19": {"apt": ["libmariadbd19"]},
    "mariadb_test_data": {"apt": ["mariadb-test-data"]},
    "connect": {
        "apt": ["mariadb-plugin-connect"],
        "yum": ["MariaDB-connect-engine"],
        "zypper": ["MariaDB-connect-engine"],
    },
}

ARCHITECTURE_ALLIAS = {
    "amd64": "amd64",
    "x86_64": "amd64",
    "intel64": "amd64",
    "aarch64": "aarch64",
    "arm64": "aarch64"
}


def parseArguments():
    parser = ArgumentParser(description="Install test")
    parser.add_argument("--product", help="'MariaDBEnterprise' or 'MariaDBServerCommunity'",
                        default="MariaDBEnterprise")
    parser.add_argument("--target-version", help="Full version of the server")
    parser.add_argument("--architecture", help="Processor architecture (aarch64 / amd64)", default="amd64")
    parser.add_argument("--platform", help="Platform name")
    parser.add_argument("--platform-version", help="Platform version")
    parser.add_argument("--use-mdbci-names", help="Use MDBCI plugins names (True / False)",
                        type=common.stringArgumentToBool, default=True)
    parser.add_argument("--include-unsupported", help="Include unsupported repositories",
                        type=common.stringArgumentToBool, default=False)

    return parser.parse_args()


def versionsEqual(mariadbVersion, version, deep=-1):
    deep = len(version) if (deep == -1) else deep
    for i in range(deep):
        if version[i] != mariadbVersion[i]:
            return False
    return True


def isVersionAppropriate(mariadbVersion, versions):
    if mariadbVersion == "distro":
        return "distro" in versions
    res = False
    for version in versions.split(', '):
        try:
            versionSplitted = version.replace("-", "").split('.')
            versionSplitted = list(map(int, versionSplitted))
            mariadbVersionSplitted = mariadbVersion.split('.')
            mariadbVersionSplitted = list(map(int, mariadbVersionSplitted))
            versionOrder = len(versionSplitted)
            if version.startswith("-"):
                res = mariadbVersionSplitted[versionOrder - 1] <= versionSplitted[versionOrder - 1] and \
                      versionsEqual(mariadbVersionSplitted, versionSplitted, versionOrder - 1)
            elif version.endswith("-"):
                res = mariadbVersionSplitted[versionOrder - 1] >= versionSplitted[versionOrder - 1] and \
                      versionsEqual(mariadbVersionSplitted, versionSplitted, versionOrder - 1)
            else:
                res = versionsEqual(mariadbVersionSplitted, versionSplitted)
        except ValueError:
            pass
        if res:
            return res
    return res


def removePrerelease(version):
    index = version.find("-")
    if index != -1:
        return version[:index]
    return version


def formatVersion(mariadbVersion):
    versionSplitted = mariadbVersion.split('.')
    if len(versionSplitted) == 1:
        return mariadbVersion + ".0.0"
    elif len(versionSplitted) == 2:
        return mariadbVersion + ".0"
    return mariadbVersion


def targetVersionForOld(version, product):
    if not re.match(r"\d+(\d+\.)*", version):
        common.printInfo("Cannot generate plugins list: "
                         "incorrect version format or "
                         "CI old version")
        exit(1)
    if re.match(r"\d+\.\d+$", version):
        try:
            if product == "MariaDBEnterpriseProduction":
                return products_versions.MDBE_RELEASED_VERSIONS[version]["targetVersion"]
            elif product == "MariaDBServerCommunityProduction":
                return products_versions.MARIADB_RELEASED_VERSIONS[version]["targetVersion"]
            elif "Enterprise" in product:
                return products_versions.MDBE_CURRENT_VERSIONS[version]["targetVersion"]
            else:
                return products_versions.MARIADB_CURRENT_VERSIONS[version]["targetVersion"]
        except KeyError:
            common.printInfo("Can't find target version for old version in dictionaries!")
            exit(1)
    return version


def distroPackageName(packageName, platform):
    if platform in ['centos', 'rhel', 'sles', 'rocky']:
        return packageName.lower()
    else:
        return packageName


def generatePackageName(plugin, distroClass, mariaVersion):
    pluginAliases = PLUGINS_ALIAS[plugin].get(distroClass, plugin)
    if plugin == "backup" and isVersionAppropriate(mariaVersion, "10.2") and distroClass == "apt":
        for i in range(len(pluginAliases)):
            pluginAliases[i] = f"{pluginAliases[i]}-10.2"

    return pluginAliases


def generateStandartPlugins(pluginsEs, pluginsCs, product, platform, mariadbVersion, platformFull, architecture):
    pluginsConfig = pluginsEs if "Enterprise" in product else pluginsCs
    if platform == "windows":
        return []
    else:
        plugins = []
        for pluginName, versions in pluginsConfig.items():
            if isVersionAppropriate(mariadbVersion, versions[architecture].get(platformFull, "")):
                plugins.append(pluginName)
        return plugins


def generateExtendPlugins(pluginsExtend, pluginsExtendCs, product, mariadbVersion, platformFull):
    pluginsConfig = pluginsExtend if "Enterprise" in product else {**pluginsExtend, **pluginsExtendCs}
    packages = []
    for pluginName, versions in pluginsConfig.items():
        if isVersionAppropriate(mariadbVersion, versions.get(platformFull, "")):
            packages.append(pluginName)
    return packages


def generateUnsupportedPlugins(pluginsUnsupported):
    plugins = []
    for pluginName in pluginsUnsupported:
        plugins.append(pluginName)
    return plugins


def addForDistro(pluginsForDistro, platformFull):
    plugins = []
    for pluginName, platforms in pluginsForDistro.items():
        if platformFull in platforms:
            plugins.append(pluginName)
    return plugins


def generateAllPlugins(args, pluginsEs, pluginsCs, pluginsExtend,
                       pluginsExtendCs, pluginsUnsupported, pluginsForDistro):
    if args.platform == "windows":
        print([])
        exit(0)
    platformFull = f"{args.platform}_{args.platform_version}"
    isDistro = (args.target_version == "distro")
    if isDistro:
        mariadbVersion = args.target_version
    else:
        mariadbVersion = removePrerelease(args.target_version)
        mariadbVersion = targetVersionForOld(mariadbVersion, args.product)
        mariadbVersion = formatVersion(mariadbVersion)
    architecture = ARCHITECTURE_ALLIAS[args.architecture]
    plugins = generateStandartPlugins(
        pluginsEs=pluginsEs,
        pluginsCs=pluginsCs,
        product=args.product,
        platform=args.platform,
        mariadbVersion=mariadbVersion,
        platformFull=platformFull,
        architecture=architecture,
    )
    if args.use_mdbci_names:
        packages = [f"mdbe_plugin_{plugin}" for plugin in plugins]
    else:
        plugins += generateExtendPlugins(
            pluginsExtend=pluginsExtend,
            pluginsExtendCs=pluginsExtendCs,
            product=args.product,
            mariadbVersion=mariadbVersion,
            platformFull=platformFull,
        )
        if args.include_unsupported:
            plugins += generateUnsupportedPlugins(pluginsUnsupported)
        packages = []
        for plugin in plugins:
            packages.extend(generatePackageName(plugin, common.DISTRO_CLASSES[args.platform], mariadbVersion))
    if isDistro:
        packages = [distroPackageName(package, args.platform)
                    for package in packages]
        packages += addForDistro(pluginsForDistro, platformFull)
    return packages


def main():
    args = parseArguments()
    print(json.dumps(generateAllPlugins(
        args,
        pluginsEs=PLUGINS_ENTERPRISE,
        pluginsCs=PLUGINS_COMMUNITY,
        pluginsExtend=PLUGINS_EXTEND,
        pluginsExtendCs=PLUGINS_EXTEND_COMMUNITY,
        pluginsUnsupported=PLUGINS_UNSUPPORTED,
        pluginsForDistro=ADD_FOR_DISTRO_MARIADB_BY_PLATFORM
    )))


if __name__ == "__main__":
    main()
