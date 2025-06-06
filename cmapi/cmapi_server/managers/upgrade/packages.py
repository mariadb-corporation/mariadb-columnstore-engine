import logging

from cmapi_server.constants import (
    MDB_CS_PACKAGE_NAME, MDB_SERVER_PACKAGE_NAME
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
        if self.os_name in ['ubuntu', 'debian']:
            # Prefer apt-get in scripts for stability and noninteractive support
            self.pkg_manager = 'apt-get'
            self.mdb_pkg_name = MDB_SERVER_PACKAGE_NAME.deb
            self.mcs_pkg_name = MDB_CS_PACKAGE_NAME.deb
        elif self.os_name in ['centos', 'rhel', 'rocky']:
            self.pkg_manager = 'yum'
            self.mdb_pkg_name = MDB_SERVER_PACKAGE_NAME.rhel
            self.mcs_pkg_name = MDB_CS_PACKAGE_NAME.rhel
        else:
            raise CMAPIBasicError(f'Unsupported OS type: {self.os_name}')

    def install_package(self, package_name: str, version: str = 'latest', dry_run: bool = False):
        """Install a package by its name.

        :param package_name: the name of the package to install.
        :param version: version to install
        :param dry_run: if True, simulate the transaction without making changes
        """
        extra_args = ''
        dry_run_flag = ''
        conf_opt = ''
        env_vars = None
        package = package_name
        if self.os_name in ['ubuntu', 'debian']:
            if version != 'latest':
                package = f'{package_name}={version}'
                # Allow downgrades explicitly when version is pinned
                extra_args = '--allow-downgrades'
            # Noninteractive mode and ALWAYS keep current configs
            env_vars = {'DEBIAN_FRONTEND': 'noninteractive'}
            conf_opt = '-o Dpkg::Options::=--force-confold'
            dry_run_flag = '-s' if dry_run else ''
        elif self.os_name in ['centos', 'rhel', 'rocky']:
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
        if self.os_name in ['ubuntu', 'debian']:
            env_vars = {'DEBIAN_FRONTEND': 'noninteractive'}
            dry_flag = '-s' if dry_run else ''
        elif self.os_name in ['centos', 'rhel', 'rocky']:
            # use tsflags=test to simulate with zero exit code
            dry_flag = '--setopt=tsflags=test' if dry_run else ''

        cmd = f'{self.pkg_manager} remove -y {dry_flag} {package_name}'.strip()
        success, result = BaseDispatcher.exec_command(cmd, env=env_vars)
        if not success:
            message = (
                f'Failed to remove {package_name} using command {cmd} with '
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
        if precheck:
            # 1) Simulate removals; fail fast if not possible
            self.remove_package(self.mcs_pkg_name, dry_run=True)
            self.remove_package(self.mdb_pkg_name, dry_run=True)

        # 2) Perform actual removals
        self.remove_package(self.mcs_pkg_name)
        self.remove_package(self.mdb_pkg_name)

        if precheck:
            # 3) Now that packages are removed, simulate installs to validate deps
            self.install_package(self.mdb_pkg_name, self.mdb_version, dry_run=True)
            self.install_package(self.mcs_pkg_name, self.mcs_version, dry_run=True)

        # 4) Perform actual installs
        self.install_package(self.mdb_pkg_name, self.mdb_version)
        self.install_package(self.mcs_pkg_name, self.mcs_version)

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
