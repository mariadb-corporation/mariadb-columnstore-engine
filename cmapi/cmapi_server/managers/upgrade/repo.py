import logging
import os
import re
import subprocess

import requests

from cmapi_server.constants import (
    ES_REPO, ES_REPO_PRIORITY_PREFS, ES_TOKEN_VERIFY_URL, ES_VERIFY_URL, MDB_GPG_KEY_URL,
    MDB_LATEST_RELEASES_URL, MDB_LATEST_TESTED_MAJOR, REQUEST_TIMEOUT,
)
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.managers.upgrade.utils import ComparableVersion
from cmapi_server.process_dispatchers.base import BaseDispatcher


class MariaDBESRepoManager:
    def __init__(
            self, token: str, arch: str, os_type: str, os_version: str,
            mariadb_version: str = 'latest',
    ):
        self.token = token
        self.arch = arch
        self.os_type = os_type
        self.os_version = os_version
        if mariadb_version == 'latest':
            self.mariadb_version = self.get_latest_tested_mdb_version()
        else:
            self.mariadb_version = mariadb_version

    def _import_mariadb_keyring(self):
        """
        Download and place the MariaDB keyring into /etc/apt/trusted.gpg.d.
        """
        key_url = 'https://supplychain.mariadb.com/mariadb-keyring-2019.gpg'
        keyring_path = '/etc/apt/trusted.gpg.d/mariadb-keyring-2019.gpg'

        try:
            # Download the keyring file
            response = requests.get(key_url)
            response.raise_for_status()

            # Write the keyring file to the specified path
            with open(keyring_path, 'wb') as key_file:
                key_file.write(response.content)

            # Set permissions to 644
            os.chmod(keyring_path, 0o644)
            logging.debug(
                f'Keyring successfully downloaded and placed at {keyring_path}'
            )
        except requests.RequestException as exc:
            raise CMAPIBasicError(
                f'Failed to download keyring from {key_url}: {exc}'
            )
        except OSError as exc:
            raise CMAPIBasicError(
                f'Failed to write keyring to {keyring_path}: {exc}'
            )

    def check_mdb_version_exists(self):
        """Check if passed MDB version exists in the repo.

        :raises CMAPIBasicError: unsupported OS type
        :raises CMAPIBasicError: wrong MDB version passed
        :raises CMAPIBasicError: some other request/response errors
        """
        verify_url: str = ''
        if self.os_type in ['ubuntu', 'debian']:
            # get only two first numbers from version to build repo link
            verify_url = ES_VERIFY_URL.deb.format(
                token=self.token,
                mdb_version=self.mariadb_version,
                os_version=self.os_version
            )
        elif self.os_type in ['centos', 'rhel', 'rocky']:
            verify_url = ES_VERIFY_URL.rhel.format(
                token=self.token,
                mdb_version=self.mariadb_version,
                os_major_version=self.os_version.split('.', maxsplit=1)[0],
                arch=self.arch
            )
        else:
            raise CMAPIBasicError(f'Unsupported OS type: {self.os_type}')
        try:
            # Download the keyring file
            response = requests.get(verify_url, timeout=REQUEST_TIMEOUT)
            if response.status_code in (403, 404):
                raise CMAPIBasicError(
                    'MariaDB Enterprise Server version '
                    f'{self.mariadb_version} is not working for your OS '
                    'version or OS type.\nPlease verify that it is correct.\n '
                    'Not all releases of MariaDB are available on all '
                    'distributions.'
                )
            elif response.ok:
                logging.debug(
                    'MariaDB Enterprise Server version '
                    f'{self.mariadb_version} is valid.'
                )
            else:
                response.raise_for_status()
        except requests.RequestException:
            raise CMAPIBasicError(
                'Failed to check MDB version exists from '
                f'{verify_url}'
            )

    @staticmethod
    def verify_token(token: str):
        """Verify ES token.

        :param token: es token to verify
        :type token: str
        :raises CMAPIBasicError: Invalid token format
        :raises CMAPIBasicError: Invalid token
        :raises CMAPIBasicError: Other request errors
        """
        # Check token format UUID
        valid_format = re.fullmatch(
            r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
            token
        )
        if not valid_format:
            err_message = (
                f'Invalid token format: "{token}". The token should be '
                'of the form ########-####-####-####-############'
            )
            raise CMAPIBasicError(err_message)

        verify_url = ES_TOKEN_VERIFY_URL.format(token=token)

        try:
            response = requests.head(
                verify_url, allow_redirects=True, timeout=REQUEST_TIMEOUT
            )
            if response.status_code in (403, 404):
                raise CMAPIBasicError(
                    'Invalid token. Please verify that it is correct.'
                )
            elif response.ok:
                logging.debug('MariaDB ES Token is valid.')
            else:
                response.raise_for_status()
        except requests.RequestException:
            raise CMAPIBasicError(
                'Problem encountered while trying to verify ES token.'
            )

    @classmethod
    def get_latest_tested_mdb_version(cls) -> str:
        """Get latest tested MDB version from repo.

        :raises CMAPIBasicError: no latest version matched with latest tested
        :raises CMAPIBasicError: if request error
        :return: latest MDB version matched with latest tested major
        :rtype: str
        """
        try:
            # Download the keyring file
            response = requests.get(
                MDB_LATEST_RELEASES_URL, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            latest_version_nums = [
                ver
                for ver in response.text.split(' ')
                if MDB_LATEST_TESTED_MAJOR in ver
            ]
            if not latest_version_nums:
                raise CMAPIBasicError(
                    'Failed to get latest MDB version number matched latest '
                    f'tested major {MDB_LATEST_TESTED_MAJOR}'
            )
            latest_version_num = sorted(latest_version_nums, reverse=True)[0]
            logging.debug(
                'Succesfully got latest MBD version number: '
                f'{latest_version_num}'
            )
        except requests.RequestException as exc:
            raise CMAPIBasicError(
                'Failed to get latest MDB version numbers from '
                f'{MDB_LATEST_RELEASES_URL}'
            )
        return latest_version_num

    @classmethod
    def get_ver_of(cls, package_name: str, os_type: str,) -> str:
        """Get version of package in a repo.

        :param package_name: name of package to get version
        :type package_name: str
        :param os_type: os name
        :type os_type: str
        :raises CMAPIBasicError: if os type isn't supported
        :raises CMAPIBasicError: if failed getting package info
        :raises CMAPIBasicError: if couldn't find any package with given name
        :return: latest available package version
        :rtype: str
        """
        latest_version: str = ''
        cmd: str = ''
        if os_type in ['ubuntu', 'debian']:
            cmd = f'apt show {package_name}'
        elif os_type in ['centos', 'rhel', 'rocky']:
            cmd = f'yum info --showduplicates --available {package_name}'
        else:
            raise CMAPIBasicError(f'Unsupported OS type: {os_type}')

        success, result = BaseDispatcher.exec_command(cmd)
        if not success:
            message = (
                f'Failed to get {package_name} package information using '
                f'command {cmd} with result: {result}'
            )
            logging.error(message)
            raise CMAPIBasicError(message)

        matches = re.findall(r'\bVersion\s*:\s+(\S+)', result)
        if matches:
            latest_version = max(matches, key=ComparableVersion)
        else:
            raise CMAPIBasicError(
                'Could not find any version for package with name '
                f'{package_name}'
            )
        return latest_version

    def check_repo(self):
        """Check repo installed and have needed version of MDB.

        :raises CMAPIBasicError: if unsupported os type detected
        :raises CMAPIBasicError: could not find package matching the version
        """
        pkg_ver: str = ''
        mdb_pkg_mgr_version = self.get_ver_of('mariadb-server', self.os_type)
        if self.os_type in ['ubuntu', 'debian']:
            # for deb packages it's just a part of version
            # eg: 10.6.22.18 and 1:10.6.22.18+maria~ubu2204
            pkg_ver = self.mariadb_version.replace('-', '.')
        elif self.os_type in ['centos', 'rhel', 'rocky']:
            # in rhel distros it's full version
            pkg_ver = self.mariadb_version.replace('-', '_')
        else:
            raise CMAPIBasicError(f'Unsupported OS type: {self.os_type}')
        if pkg_ver not in mdb_pkg_mgr_version:
            raise CMAPIBasicError(
                'Could not find mariadb-server package matched with version '
                f'{pkg_ver}'
            )

    def setup_repo(self):
        """Set up the MariaDB Enterprise repository based on OS type."""
        self.check_mdb_version_exists()
        if self.os_type in ['ubuntu', 'debian']:
            # get only two first numbers from version to build repo link
            repo_data = ES_REPO.deb.format(
                token=self.token,
                mdb_version=self.mariadb_version,
                os_version=self.os_version
            )
            repo_file = '/etc/apt/sources.list.d/mariadb.list'
            with open(repo_file, 'w', encoding='utf-8') as f:
                f.write(repo_data)
            # Set permissions to 640
            os.chmod(repo_file, 0o640)

            pref_file = '/etc/apt/preferences'
            with open(pref_file, 'w', encoding='utf-8') as f:
                f.write(ES_REPO_PRIORITY_PREFS)

            self._import_mariadb_keyring()
            subprocess.run(['apt-get', 'update'], check=True)
        elif self.os_type in ['centos', 'rhel', 'rocky']:
            repo_data = ES_REPO.rhel.format(
                token=self.token,
                mdb_version=self.mariadb_version,
                os_major_version=self.os_version.split('.', maxsplit=1)[0],
                arch=self.arch,
                gpg_key_url=MDB_GPG_KEY_URL
            )
            repo_file = '/etc/yum.repos.d/mariadb.repo'
            with open(repo_file, 'w', encoding='utf-8') as f:
                f.write(repo_data)
            subprocess.run(['rpm', '--import', MDB_GPG_KEY_URL], check=True)
        else:
            raise CMAPIBasicError(f'Unsupported OS type: {self.os_type}')
        self.check_repo()
