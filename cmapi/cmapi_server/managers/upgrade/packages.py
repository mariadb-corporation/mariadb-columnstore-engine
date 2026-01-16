import logging
import os
import re

from cmapi_server.constants import (
    MDB_CS_PACKAGE_NAME,
    MDB_SERVER_PACKAGE_NAME,
    MDB_COLUMNSTORE_DEB_CNF_PATH,
    MDB_COLUMNSTORE_RHEL_CNF_PATH,
    PkgType,
    get_pkg_type,
)
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.process_dispatchers.base import BaseDispatcher


class PackagesManager:
    """
    This class is responsible for managing the installation of packages.
    It provides methods to install, uninstall packages.
    """
    def __init__(self, os_name: str, mdb_version: str, mcs_version: str):
        self.os_name = os_name
        self.mdb_version = mdb_version
        self.mcs_version = mcs_version
        self.pkg_manager: str = ''
        try:
            self.pkg_type = get_pkg_type(os_name)
        except ValueError as exc:
            raise CMAPIBasicError(str(exc)) from exc
        if self.pkg_type == PkgType.DEB:
            # Prefer apt-get in scripts for stability and noninteractive support
            self.pkg_manager = 'apt-get'
            self.mdb_pkg_name = MDB_SERVER_PACKAGE_NAME.deb
            self.mcs_pkg_name = MDB_CS_PACKAGE_NAME.deb
        elif self.pkg_type == PkgType.RPM:
            self.pkg_manager = 'yum'
            self.mdb_pkg_name = MDB_SERVER_PACKAGE_NAME.rhel
            self.mcs_pkg_name = MDB_CS_PACKAGE_NAME.rhel

    def install_package(self, package_name: str, version: str = 'latest', dry_run: bool = False):
        """Install a package by its name.

        :param package_name: the name of the package to install.
        :param version: version to install
        :param dry_run: if True, simulate the transaction without making changes
        """
        logging.debug(
            '%s package %s (version=%s) ',
            'Simulating installation of' if dry_run else 'Installing',
            package_name, version,
        )
        extra_args = ''
        dry_run_flag = ''
        conf_opt = ''
        env_vars = None
        package = package_name
        if self.pkg_type == PkgType.DEB:
            if version != 'latest':
                package = f'{package_name}={version}'
                # On Debian/Ubuntu, mariadb-server is a metapackage that
                # depends on mariadb-server-{major.minor} at an exact
                # version.  When downgrading, the versioned binary package
                # may still be installed at the old (higher) version.
                # Include it explicitly so apt can plan the downgrade of
                # the whole dependency chain in a single transaction.
                if package_name == MDB_SERVER_PACKAGE_NAME.deb:
                    major_minor = self._extract_deb_major_minor(version)
                    # The versioned binary package (mariadb-server-{major.minor})
                    # only exists for the 10.x series.  Starting with 11.x,
                    # MariaDB ships a single mariadb-server package.
                    if major_minor and major_minor.startswith('10.'):
                        versioned_pkg = (
                            f'mariadb-server-{major_minor}={version}'
                        )
                        package = f'{package} {versioned_pkg}'
                # Allow downgrades explicitly when version is pinned
                extra_args = '--allow-downgrades'
            # Noninteractive mode and ALWAYS keep current configs
            env_vars = {'DEBIAN_FRONTEND': 'noninteractive'}
            conf_opt = '-o Dpkg::Options::=--force-confold'
            dry_run_flag = '-s' if dry_run else ''
        elif self.pkg_type == PkgType.RPM:
            if version != 'latest':
                package = f'{package_name}-{version}'
            # For yum, use --assumeno as a safe preview of the transaction
            # but it exits non-zero; prefer tsflags=test for a clean dry-run
            dry_run_flag = '--setopt=tsflags=test' if dry_run else ''

        cmd = (
            f"{self.pkg_manager} install -y {dry_run_flag} "
            f"{conf_opt} {extra_args} {package}"
        ).strip()
        success, result = BaseDispatcher.exec_command(cmd, env=env_vars)
        if not success:
            message = (
                f'Failed to install {package} using command {cmd} with '
                f'result: {result}'
            )
            logging.error(message)
            raise CMAPIBasicError(message)

    def remove_package(self, package_name: str, *, dry_run: bool = False) -> bool:
        """Uninstall a package by its name.

        :param package_name: The name of the package to remove.
        :param dry_run: if True, simulate the transaction without making changes
        """
        env_vars = None
        dry_flag = ''
        if self.pkg_type == PkgType.DEB:
            env_vars = {'DEBIAN_FRONTEND': 'noninteractive'}
            dry_flag = '-s' if dry_run else ''
        elif self.pkg_type == PkgType.RPM:
            # use tsflags=test to simulate with zero exit code
            dry_flag = '--setopt=tsflags=test' if dry_run else ''
        cmd = f'{self.pkg_manager} remove -y {dry_flag} {package_name}'.strip()
        logging.debug(
            f'{"Removing" if not dry_run else "Simulating removal of"} package {package_name} '
            f'with command: {cmd} and env: {env_vars}'
        )
        success, result = BaseDispatcher.exec_command(cmd, env=env_vars)
        if not success:
            message = (
                f'Failed to remove {package_name} using command {cmd} with '
                f'result: {result}'
            )
            logging.error(message)
            raise CMAPIBasicError(message)

    def clean_dependencies(self, dry_run: bool = False):
        """Clean up any orphaned dependencies after package removals.

        :param dry_run: if True, simulate the transaction without making changes
        """
        if self.pkg_type == PkgType.DEB:
            logging.debug(
                '%s orphaned dependencies.',
                'Simulating cleanup of' if dry_run else 'Cleaning up',
            )
            cmd = 'apt-get autoremove -y'
            if dry_run:
                cmd = f'{cmd} -s'
        elif self.pkg_type == PkgType.RPM:
            logging.debug(
                'Autoremoving dependencies not tested on all RHEL-based distros, so skip for now.'
            )
            return
            # # yum doesn't have a direct autoremove equivalent, but we can simulate with tsflags=test
            # cmd = 'yum autoremove -y'
            # if dry_run:
            #     cmd = f'{cmd} --setopt=tsflags=test'

        success, result = BaseDispatcher.exec_command(cmd)
        if not success:
            message = (
                f'Failed to clean dependencies using command {cmd} with '
                f'result: {result}'
            )
            logging.error(message)
            raise CMAPIBasicError(message)

    def upgrade_mdb_and_mcs(self, *, precheck: bool = True):
        """Remove packages and then install newer or older versions.

        The function can perform a dry-run of all steps first. If any of the
        simulated transactions fail, the actual removal/installation will not
        be executed.

        :param precheck: when True, simulate remove/install before real actions
        """
        # Simulate removals - fail fast if not possible
        if precheck:
            self.remove_package(self.mcs_pkg_name, dry_run=True)
        self.remove_package(self.mcs_pkg_name)
        self._backup_columnstore_cnf()

        if precheck:
            self.remove_package(self.mdb_pkg_name, dry_run=True)
        self.remove_package(self.mdb_pkg_name)

        if precheck:
            # Simulate cleaning up any orphaned dependencies
            self.clean_dependencies(dry_run=True)

        # Clean up any orphaned dependencies
        self.clean_dependencies()

        if precheck:
            # Simulate mdb install
            self.install_package(self.mdb_pkg_name, self.mdb_version, dry_run=True)
        # install mdb
        self.install_package(self.mdb_pkg_name, self.mdb_version)

        if precheck:
            # Simulate mcs install
            self.install_package(self.mcs_pkg_name, self.mcs_version, dry_run=True)
        # install mcs
        self.install_package(self.mcs_pkg_name, self.mcs_version)

    def _backup_columnstore_cnf(self):
        """Rename columnstore.cnf to .bak if it still exists after removal.

        Older packages lack the rename logic in their postrm script.
        Without this, ``--force-confold`` keeps the old conffile and the
        new package never installs a fresh ``columnstore.cnf``.
        Possibly it could break the mariadb installation if the old config
        contain some columnstore columnstore values without `loose` prefix.
        """
        cnf_path: str = ''
        if self.pkg_type == PkgType.DEB:
            cnf_path = MDB_COLUMNSTORE_DEB_CNF_PATH
        elif self.pkg_type == PkgType.RPM:
            cnf_path = MDB_COLUMNSTORE_RHEL_CNF_PATH

        if os.path.exists(cnf_path):
            bak_path = cnf_path + '.bak'
            os.rename(cnf_path, bak_path)
            logging.info('Renamed %s to %s', cnf_path, bak_path)

    @staticmethod
    def _extract_deb_major_minor(version: str) -> str:
        """Extract major.minor from a Debian package version string.

        E.g. '1:10.6.12.7+maria~ubu2204' -> '10.6'
        """
        # Strip optional epoch (e.g. '1:')
        ver = version.split(':', 1)[-1]
        match = re.match(r'(\d+\.\d+)', ver)
        return match.group(1) if match else ''

    @classmethod
    def kick_cmapi_upgrade(cls, cmapi_version: str):
        """Starts the one-shot cmapi_updater.service.

        :param cmapi_version: target CMAPI version to install
        :type cmapi_version: str
        """
        with open('/tmp/cmapi_updater.conf', 'w+', encoding='utf-8') as file:
            file.write(f'CMAPI_VERSION={cmapi_version}')
        cmd = 'systemctl start cmapi_updater.service'
        success, result = BaseDispatcher.exec_command(cmd, daemonize=True)
        # Note: this likely never reports an error in practice, but we still check.
        if not success:
            message = (
                f'Failed to start cmapi_updater.serice using command {cmd} '
                f'with result: {result}'
            )
            logging.error(message)
            raise CMAPIBasicError(message)
        logging.info('Started cmapi_updater.service to upgrade CMAPI.')
