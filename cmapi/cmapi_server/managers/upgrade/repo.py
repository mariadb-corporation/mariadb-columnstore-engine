import glob
import logging
import os
import re
import shlex
import shutil
import subprocess

import requests

from cmapi_server.constants import (
    ES_REPO, ES_REPO_PRIORITY_PREFS,
    ES_TOKEN_VERIFY_URL,
    ES_VERIFY_URL,
    MDB_GPG_KEY_URL,
    MDB_LATEST_RELEASES_URL,
    REQUEST_TIMEOUT,
    UPGRADE_STALE_REPOS_DIR,
    PkgType,
    get_pkg_type,
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
        try:
            self.pkg_type = get_pkg_type(os_type)
        except ValueError as exc:
            raise CMAPIBasicError(str(exc)) from exc
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
        if self.pkg_type == PkgType.DEB:
            # get only two first numbers from version to build repo link
            verify_url = ES_VERIFY_URL.deb.format(
                token=self.token,
                mdb_version=self.mariadb_version,
                os_version=self.os_version
            )
        elif self.pkg_type == PkgType.RPM:
            verify_url = ES_VERIFY_URL.rhel.format(
                token=self.token,
                mdb_version=self.mariadb_version,
                os_major_version=self.os_version.split('.', maxsplit=1)[0],
                arch=self.arch
            )
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
    def get_latest_tested_mdb_version(cls, mdb_ver_prefix: str = '') -> str:
        """Get latest tested MDB version from repo.

        When *mdb_ver_prefix* is given (e.g. ``"10"``, ``"10.6"``,
        ``"11.4"``), only versions whose dot/dash-separated numeric parts
        start with the same components are considered.  When omitted the
        overall latest version is returned.

        :param mdb_ver_prefix: optional major (or major.minor) version
            prefix to filter by (e.g. ``"10"``, ``"10.6"``, ``"11"``).
        :raises CMAPIBasicError: no versions found
        :raises CMAPIBasicError: if request error
        :return: latest available MDB version (optionally within prefix)
        """
        try:
            response = requests.get(
                MDB_LATEST_RELEASES_URL, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            all_versions = [
                ver.strip()
                for ver in response.text.split(' ')
                if ver.strip()
            ]
            if not all_versions:
                raise CMAPIBasicError(
                    'Failed to get any MDB version numbers from '
                    f'{MDB_LATEST_RELEASES_URL}'
                )

            latest_version_num = cls.select_latest_version(
                all_versions, mdb_ver_prefix
            )

            logging.debug(
                f'Successfully got latest MDB version number: {latest_version_num}'
            )
        except requests.RequestException as exc:
            raise CMAPIBasicError(
                'Failed to get latest MDB version numbers from '
                f'{MDB_LATEST_RELEASES_URL}'
            )
        return latest_version_num

    @staticmethod
    def select_latest_version(
        all_versions: list[str], mdb_ver_prefix: str = '',
    ) -> str:
        """Select the latest version from a list, optionally filtered by prefix.

        When *mdb_ver_prefix* is given (e.g. ``"10"``, ``"10.6"``,
        ``"11.4"``), only versions whose numeric components start with
        the same prefix are considered.

        :param all_versions: non-empty list of version strings
        :param mdb_ver_prefix: optional major (or major.minor) prefix
        :raises CMAPIBasicError: invalid prefix format
        :raises CMAPIBasicError: no versions match the prefix
        :return: the latest version string (within prefix if given)
        """
        if mdb_ver_prefix:
            prefix_parts = re.split(r'[.\-_]', mdb_ver_prefix)
            try:
                prefix_nums = [int(p.lstrip('0') or '0') for p in prefix_parts]
            except ValueError:
                raise CMAPIBasicError(f'Invalid version prefix: "{mdb_ver_prefix}"')

            matched_versions = [
                ver for ver in all_versions
                if ComparableVersion(ver).version_nums[:len(prefix_nums)] == prefix_nums
            ]
            if not matched_versions:
                raise CMAPIBasicError(
                    f'No MDB version matched prefix "{mdb_ver_prefix}"'
                )
            return max(matched_versions, key=ComparableVersion)
        else:
            return max(all_versions, key=ComparableVersion)

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
        try:
            pkg_type = get_pkg_type(os_type)
        except ValueError as exc:
            raise CMAPIBasicError(str(exc)) from exc
        if pkg_type == PkgType.DEB:
            cmd = f'apt show {package_name}'
        elif pkg_type == PkgType.RPM:
            cmd = f'yum info --showduplicates --available {package_name}'

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
        if self.pkg_type == PkgType.DEB:
            # for deb packages it's just a part of version
            # eg: 10.6.22.18 and 1:10.6.22.18+maria~ubu2204
            pkg_ver = self.mariadb_version.replace('-', '.')
            mdb_pkg_mgr_version = self.get_ver_of(
                'mariadb-server', self.os_type
            )
            if pkg_ver not in mdb_pkg_mgr_version:
                raise CMAPIBasicError(
                    'Could not find mariadb-server package matched with '
                    f'version {pkg_ver}'
                )
        elif self.pkg_type == PkgType.RPM:
            # yum may list multiple versions from different repos;
            # check all of them, not just the latest.
            pkg_ver = self.mariadb_version.replace('-', '_')
            cmd = 'yum info --showduplicates --available mariadb-server'
            success, result = BaseDispatcher.exec_command(cmd)
            if not success:
                raise CMAPIBasicError(
                    'Failed to get mariadb-server package information using '
                    f'command {cmd} with result: {result}'
                )
            versions = re.findall(r'\bVersion\s*:\s+(\S+)', result)
            if not any(pkg_ver in v for v in versions):
                raise CMAPIBasicError(
                    'Could not find mariadb-server package matched with '
                    f'version {pkg_ver}'
                )

    @staticmethod
    def _cleanup_stale_deb_repos(
        target_repo_file: str,
        es_url_marker: str = 'dlm.mariadb.com',
    ):
        """Move stale deb repo files providing MariaDB packages to backup.

        Queries ``apt-cache madison`` to discover which repositories
        provide MariaDB packages.  Source files in
        ``/etc/apt/sources.list.d`` (other than *target_repo_file*)
        whose hostnames appear in the results (excluding the ES
        repository identified by *es_url_marker*) are moved into
        :data:`UPGRADE_STALE_REPOS_DIR`.
        """
        repo_dir = '/etc/apt/sources.list.d'
        target = os.path.abspath(target_repo_file)

        # Collect hostnames of non-ES repos that provide MariaDB pkgs
        stale_hostnames: set = set()
        success, result = BaseDispatcher.exec_command(
            'apt-cache madison mariadb-server 2>/dev/null'
        )
        if not success:
            logging.warning(
                'Failed to query apt-cache madison for mariadb-server; '
                'skipping stale-repo cleanup'
            )
            return
        for line in result.splitlines():
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 3:
                continue
            source = parts[2].strip()
            url = source.split()[0] if source else ''
            if not url.startswith('http'):
                continue
            if es_url_marker in url:
                continue
            # Extract hostname from URL
            host = url.split('://')[1].split('/')[0].split(':')[0]
            if host:
                stale_hostnames.add(host)

        if not stale_hostnames:
            return

        # Find and move .list files containing stale repo hostnames
        for repo_path in glob.glob(os.path.join(repo_dir, '*.list')):
            if os.path.abspath(repo_path) == target:
                continue
            try:
                with open(repo_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            except OSError:
                continue
            if any(host in content for host in stale_hostnames):
                # Skip system repo files managed by dpkg packages
                owned, owner = BaseDispatcher.exec_command(
                    f'dpkg -S {shlex.quote(repo_path)}'
                )
                if owned and 'no path found' not in owner.lower():
                    logging.info(
                        'Skipping system repo file (owned by %s): %s',
                        owner.strip(), repo_path
                    )
                    continue
                os.makedirs(UPGRADE_STALE_REPOS_DIR, exist_ok=True)
                backup_path = os.path.join(
                    UPGRADE_STALE_REPOS_DIR, os.path.basename(repo_path)
                )
                logging.info(
                    'Moving stale MariaDB repo file: %s -> %s',
                    repo_path, backup_path
                )
                try:
                    shutil.move(repo_path, backup_path)
                except OSError as exc:
                    logging.warning(
                        'Failed to move %s: %s', repo_path, exc
                    )

    @staticmethod
    def _cleanup_stale_rhel_repos(
        target_repo_file: str,
        es_repo_id: str = 'mariadb-es-main',
    ):
        """Move RHEL repo files providing conflicting MariaDB packages.

        Queries ``yum list available`` to discover which repositories
        provide MariaDB packages.  Any ``.repo`` file (other than
        *target_repo_file*) whose section matches a discovered repo id
        (excluding *es_repo_id*) is moved into
        :data:`UPGRADE_STALE_REPOS_DIR`.
        """
        repo_dir = '/etc/yum.repos.d'
        target = os.path.abspath(target_repo_file)

        # Build map: repo section name -> .repo file path
        section_to_file: dict = {}
        for repo_path in glob.glob(os.path.join(repo_dir, '*.repo')):
            if os.path.abspath(repo_path) == target:
                continue
            try:
                with open(repo_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            except OSError:
                continue
            for section in re.findall(
                r'^\[([^\]]+)\]', content, re.MULTILINE
            ):
                section_to_file[section] = repo_path

        # Ask yum which repos provide MariaDB packages
        success, result = BaseDispatcher.exec_command(
            "yum list available 'MariaDB-*' 2>/dev/null"
        )
        if not success:
            logging.warning(
                'Failed to query available MariaDB packages; '
                'skipping stale-repo cleanup'
            )
            return

        # Parse output – lines after "Available Packages" look like:
        #   MariaDB-server.x86_64   10.6.26_22-1.el9   drone
        stale_files: set = set()
        in_packages = False
        for line in result.splitlines():
            if 'Available Packages' in line:
                in_packages = True
                continue
            if not in_packages:
                continue
            parts = line.split()
            if len(parts) >= 3 and '.' in parts[0]:
                repo_id = parts[-1]
                if repo_id != es_repo_id and repo_id in section_to_file:
                    stale_files.add(section_to_file[repo_id])

        for repo_path in stale_files:
            # Skip system repo files managed by RPM packages (e.g.
            # appstream in rocky.repo) — only remove custom/CI repos.
            owned, owner = BaseDispatcher.exec_command(
                f'rpm -qf {shlex.quote(repo_path)}'
            )
            if owned and 'not owned' not in owner.lower():
                logging.info(
                    'Skipping system repo file (owned by %s): %s',
                    owner.strip(), repo_path
                )
                continue
            os.makedirs(UPGRADE_STALE_REPOS_DIR, exist_ok=True)
            backup_path = os.path.join(
                UPGRADE_STALE_REPOS_DIR, os.path.basename(repo_path)
            )
            logging.info(
                'Moving stale MariaDB repo file: %s -> %s',
                repo_path, backup_path
            )
            try:
                shutil.move(repo_path, backup_path)
            except OSError as exc:
                logging.warning(
                    'Failed to move %s: %s', repo_path, exc
                )

    def setup_repo(self):
        """Set up the MariaDB Enterprise repository based on OS type."""
        self.check_mdb_version_exists()
        if self.pkg_type == PkgType.DEB:
            repo_file = '/etc/apt/sources.list.d/mariadb.list'
            self._cleanup_stale_deb_repos(repo_file)
            # get only two first numbers from version to build repo link
            repo_data = ES_REPO.deb.format(
                token=self.token,
                mdb_version=self.mariadb_version,
                os_version=self.os_version
            )
            with open(repo_file, 'w', encoding='utf-8') as f:
                f.write(repo_data)
            # Set permissions to 640
            os.chmod(repo_file, 0o640)

            pref_file = '/etc/apt/preferences'
            with open(pref_file, 'w', encoding='utf-8') as f:
                f.write(ES_REPO_PRIORITY_PREFS)

            self._import_mariadb_keyring()
            subprocess.run(['apt-get', 'update'], check=True)
        elif self.pkg_type == PkgType.RPM:
            repo_file = '/etc/yum.repos.d/mariadb.repo'
            # Remove stale repos before installing the new one
            self._cleanup_stale_rhel_repos(repo_file)
            repo_data = ES_REPO.rhel.format(
                token=self.token,
                mdb_version=self.mariadb_version,
                os_major_version=self.os_version.split('.', maxsplit=1)[0],
                arch=self.arch,
                gpg_key_url=MDB_GPG_KEY_URL
            )
            with open(repo_file, 'w', encoding='utf-8') as f:
                f.write(repo_data)
            subprocess.run(['rpm', '--import', MDB_GPG_KEY_URL], check=True)
            subprocess.run(['yum', 'clean', 'all'], check=True)
        self.check_repo()
