import logging
import glob
import shutil
import os
from datetime import datetime

from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.process_dispatchers.base import BaseDispatcher
from cmapi_server.constants import MCS_BACKUP_MANAGER_SH


class BackupRestoreManager:
    @classmethod
    def backup_dbrm(cls, args: dict) -> None:
        """Make DBRM backup.

        :raises CMAPIBasicError: If dbrm backup is unsuccesfull.
        """
        cmd: str = (
            f'{MCS_BACKUP_MANAGER_SH} dbrm_backup '
            f'{" ".join([f"{k} {v}" if v else k for k,v in args.items()])}'
        )
        success, cmd_output = BaseDispatcher.exec_command(cmd)
        if not success:
            err_message: str = ''
            if not cmd_output:
                err_message = f'Can\'t start DBRM backup using cmd: "{cmd}"'
                logging.error(err_message, exc_info=True)
            else:
                logging.error(err_message)
            raise CMAPIBasicError(err_message)


class PreUpgradeBackupRestoreManager(BackupRestoreManager):
    @classmethod
    def backup_dbrm(cls) -> None:  #pylint: disable=arguments-differ
        """PreUpgrade dbrm backup.

        :raises CMAPIBasicError: If dbrm backup is unsuccesfull.
        """
        args = {
            '-r': '9999',
            '-nb': 'preupgrade_dbrm_backup',
            '--quiet': '',
            '--skip-locks': ''
        }
        super().backup_dbrm(args)


    @classmethod
    def _copy_files(cls, source_pattern: str, destination_dir: str):
        """Copy files using pattern path to a given destination.

        :param source_pattern: source pattern could be just a str path
        :type source_pattern: str
        :param destination_dir: destination dir
        :type destination_dir: str
        """
        files = glob.glob(source_pattern)
        if not files:
            logging.warning(f'No files matched: {source_pattern}')
            return

        for file_path in files:
            try:
                logging.debug(f'Copying: {file_path} -> {destination_dir}')
                shutil.copy(file_path, destination_dir)
            except Exception:
                logging.warning(
                    f'Failed to copy {file_path}',
                    exc_info=True
                )

    @classmethod
    def backup_configs(cls, distro_name: str):
        """Backup config files.

        :param distro_name: node distro name
        :type distro_name: str
        """
        timestamp = datetime.now().strftime('%m-%d-%Y-%H%M')
        pre_upgrade_config_directory = (
            f'/tmp/preupgrade-configurations-{timestamp}'
        )
        os.makedirs(pre_upgrade_config_directory, exist_ok=True)
        cls._copy_files(
            '/etc/columnstore/Columnstore.xml', pre_upgrade_config_directory
        )
        cls._copy_files(
            '/etc/columnstore/storagemanager.cnf', pre_upgrade_config_directory
        )
        cls._copy_files(
            '/etc/columnstore/cmapi_server.conf', pre_upgrade_config_directory
        )

        if distro_name in ['centos', 'rhel', 'rocky', 'almalinux']:
            cls._copy_files(
                '/etc/my.cnf.d/server.cnf', pre_upgrade_config_directory
            )
        elif distro_name in ['ubuntu', 'debian']:
            cls._copy_files(
                '/etc/mysql/mariadb.conf.d/*server.cnf',
                pre_upgrade_config_directory
            )
        else:
            logging.error(f'Unknown distro: {distro_name}')

    @staticmethod
    def restore_dbrm():
        #TODO: implement restore logic
        pass

    @staticmethod
    def restore_configs():
        pass
