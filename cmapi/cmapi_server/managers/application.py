import logging
import platform
from typing import Optional, Tuple, Dict

import distro

from cmapi_server.constants import (
    MDB_CS_PACKAGE_NAME, MDB_SERVER_PACKAGE_NAME, PKG_GET_VER_CMD,
    SUPPORTED_DISTROS, SUPPORTED_ARCHITECTURES, VERSION_PATH, MultiDistroNamer
)
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.process_dispatchers.base import BaseDispatcher


class AppManager:
    started: bool = False
    version: Optional[str] = None
    git_revision: Optional[str] = None

    @classmethod
    def get_version(cls) -> str:
        if cls.version:
            return cls.version
        version, revision = cls._read_version_file()
        cls.version = version
        cls.git_revision = revision
        return cls.version

    @classmethod
    def get_git_revision(cls) -> Optional[str]:
        if cls.git_revision is not None:
            return cls.git_revision
        _, revision = cls._read_version_file()
        cls.git_revision = revision
        return cls.git_revision

    @classmethod
    def _read_version_file(cls) -> Tuple[str, Optional[str]]:
        """Read structured values from VERSION file.

        Returns tuple: (semantic_version, git_revision or None)
        """
        values: Dict[str, str] = {}
        try:
            with open(VERSION_PATH, encoding='utf-8') as version_file:
                for line in version_file.read().splitlines():
                    if not line or '=' not in line:
                        continue
                    key, val = line.strip().split('=', 1)
                    values[key.strip()] = val.strip()
        except Exception:
            logging.exception("Failed to read VERSION file")
            return 'Undefined', None

        # Release (build) part is optional
        release = values.get('CMAPI_VERSION_RELEASE')
        revision = values.get('CMAPI_GIT_REVISION')

        required_keys = (
            'CMAPI_VERSION_MAJOR',
            'CMAPI_VERSION_MINOR',
            'CMAPI_VERSION_PATCH',
        )
        if not all(k in values and values[k] for k in required_keys):
            logging.error("Couldn't detect version from VERSION file!")
            return 'Undefined', revision

        version = '.'.join([
            values['CMAPI_VERSION_MAJOR'],
            values['CMAPI_VERSION_MINOR'],
            values['CMAPI_VERSION_PATCH'],
        ])
        if release:
            version = f"{version}.{release}"
        return version, revision

    @classmethod
    def get_architecture(cls) -> str:
        """Get system architecture.

        :return: system architecture
        :rtype: str
        """
        arch = platform.machine().lower()
        if arch not in SUPPORTED_ARCHITECTURES:
            message = f'Unsupported architecture: {arch}'
            logging.error(message)
            raise CMAPIBasicError(message)
        if arch in ['x86_64', 'amd64']:
            arch = 'x86_64'
        elif arch in ['aarch64', 'arm64']:
            arch = 'aarch64'
        return arch

    @classmethod
    def get_distro_info(cls) -> tuple[str, str]:
        """Get OS name and version.

        :return: OS name and version
        :rtype: tuple[str, str]
        """
        platform_name = platform.system().lower()
        if platform_name == 'linux':
            distro_name = distro.id().lower()
            distro_version = distro.version().lower()
            if distro_name == 'debian':
                if distro_version.startswith('11'):
                    distro_version = 'bullseye'
                elif distro_version.startswith('12'):
                    distro_version = 'bookworm'
                else:
                    message = (
                        f'Unsupported Debian version: {distro_version}. '
                        'Supported versions are 11 (bullseye) and 12 '
                        '(bookworm).'
                    )
                    logging.error(message)
                    raise CMAPIBasicError(message)
            if distro_name == 'ubuntu':
                if distro_version.startswith('20.04'):
                    distro_version = 'focal'
                elif distro_version.startswith('22.04'):
                    distro_version = 'jammy'
                elif distro_version.startswith('24.04'):
                    distro_version = 'noble'
                else:
                    message = (
                        f'Unsupported Ubuntu version: {distro_version}. '
                        'Supported versions are 20.04 (focal), 22.04 (jammy), '
                        'and 24.04 (noble).'
                    )
                    logging.error(message)
                    raise CMAPIBasicError(message)
            if distro_name not in SUPPORTED_DISTROS:
                message = (
                    f'Unsupported Linux distribution: {distro_name}. '
                )
                logging.error(message)
                raise CMAPIBasicError(message)
        else:
            message = f'Unsupported platform: {platform_name}'
            logging.error(message)
            raise CMAPIBasicError(message)
        return distro_name, distro_version

    @classmethod
    def get_installed_pkg_ver(cls, pkg_namer: MultiDistroNamer) -> str:
        """Get package version with given package name.

        :param pkg_namer: object that contains package name for several ditros
        :type pkg_namer: MultiDistroNamer
        :raises CMAPIBasicError: if failed getting version
        :return: package version
        :rtype: str
        """
        distro_name, _ = cls.get_distro_info()
        cmd: str = ''
        package_name: str = ''
        if distro_name in ['ubuntu', 'debian']:
            package_name = pkg_namer.deb
            cmd = PKG_GET_VER_CMD.deb.format(package_name=package_name)
        elif distro_name in ['centos', 'rhel', 'rocky', 'almalinux']:
            package_name = pkg_namer.rhel
            cmd = PKG_GET_VER_CMD.rhel.format(package_name=package_name)
        success, result_raw = BaseDispatcher.exec_command(cmd)
        if not success:
            message = (
                f'Failed to get {package_name} version with result: '
                f'{result_raw}'
            )
            logging.error(message)
            raise CMAPIBasicError(message)
        version_clean = result_raw
        if distro_name in ['ubuntu', 'debian']:
            # remove prefix before : (epoch)
            result_raw = result_raw.split(':', 1)[1]
            # remove suffix after first '+'
            version_clean = result_raw.split('+', 1)[0]
        return version_clean

    @classmethod
    def get_mdb_version(cls) -> str:
        """Get MDB version.

        :return: MDB version
        :rtype: str
        """
        return cls.get_installed_pkg_ver(MDB_SERVER_PACKAGE_NAME)

    @classmethod
    def get_columnstore_version(cls) -> str:
        """Get Columnstore version.

        :return: Columnstore version
        :rtype: str
        """
        return cls.get_installed_pkg_ver(MDB_CS_PACKAGE_NAME)
