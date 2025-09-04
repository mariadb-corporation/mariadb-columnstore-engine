"""
Module contains non-systemd/container process dispatcher class implementation.
"""

import logging
import os.path
import re
from pathlib import Path
from time import sleep

import psutil

from cmapi_server.constants import (
    IFLAG, LIBJEMALLOC_DEFAULT_PATH, MCS_INSTALL_BIN, ALL_MCS_PROGS,
)
from cmapi_server.process_dispatchers import utils as dispatcher_utils
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.process_dispatchers.base import BaseDispatcher


class ContainerDispatcher(BaseDispatcher):
    """Manipulates processes in docker container.

    It's possible to use in any OS/container environment in cases when
    we don't want to use systemd or don't have it.
    """
    libjemalloc_path = None

    @staticmethod
    def _set_iflag():
        """Create IFLAG file.

        Means Columnstore container init finished.
        """
        Path(IFLAG).touch()

    @classmethod
    def _get_proc_object(cls, name: str) -> psutil.Process:
        """Getting psutil Process object by service name.

        :param name: process name
        :type name: str
        :raises psutil.NoSuchProcess: if no process with such name presented
        :return: Process object with specified name
        :rtype: psutil.Process

        ...TODO: add types-psutil to requirements for mypy checks
        """
        for proc in psutil.process_iter(['pid', 'name', 'username']):
            if proc.name().lower() == name.lower():
                return proc
        raise psutil.NoSuchProcess(pid=None, name=name)

    @classmethod
    def get_libjemalloc_path(cls) -> str:
        """Get libjemalloc.so path.

        :raises CMAPIBasicError: raises if ldconfig execution returned non zero
        :raises FileNotFoundError: if no libjemalloc.so.2 found
        :return: libjemalloc.so.2 path
        :rtype: str
        """
        logger = logging.getLogger('container_sh')
        if cls.libjemalloc_path:
            return cls.libjemalloc_path
        # pylint: disable=line-too-long
        # for reference: https://github.com/pyinstaller/pyinstaller/blob/f29b577df4e1659cf65aacb797034763308fd298/PyInstaller/depend/utils.py#L304

        splitlines_count = 1
        pattern = re.compile(r'^\s+(\S+)(\s.*)? => (\S+)')
        success, result = cls.exec_command('ldconfig -p')
        if not success:
            raise CMAPIBasicError('Failed executing ldconfig.')

        text = result.strip().splitlines()[splitlines_count:]

        for line in text:
            # this assumes library names do not contain whitespace
            p_match = pattern.match(line)
            # Sanitize away any abnormal lines of output.
            if p_match is None:
                continue

            lib_path = p_match.groups()[-1]
            lib_name = p_match.group(1)
            if 'libjemalloc' in lib_name:
                # use the first entry
                # TODO: do we need path or name here?
                # $(ldconfig -p | grep -m1 libjemalloc | awk '{print $1}')
                cls.libjemalloc_path = lib_path
                break

        if not cls.libjemalloc_path:
            if not os.path.exists(LIBJEMALLOC_DEFAULT_PATH):
                logger.error('No libjemalloc.so.2 found.')
                raise FileNotFoundError
            cls.libjemalloc_path = LIBJEMALLOC_DEFAULT_PATH

        return cls.libjemalloc_path

    @classmethod
    def is_service_running(cls, service: str, use_sudo: bool = True) -> bool:
        """Check if mcs process is running.

        :param service: service name
        :type service: str
        :param use_sudo: interface requirement, unused here, defaults to True
        :type use_sudo: bool, optional
        :return: True if service is running, otherwise False
        :rtype: bool

        ..Note:
            Not working with multiple services at a time.
        """
        try:
            cls._get_proc_object(service)
        except psutil.NoSuchProcess:
            return False
        return True

    @staticmethod
    def _make_cmd(service: str) -> str:
        """Make shell command by service name.

        :param service: service name
        :type service: str
        :return: command with arguments if needed
        :rtype: str
        """
        service_info = ALL_MCS_PROGS[service]
        command = os.path.join(MCS_INSTALL_BIN, service)

        if service_info.subcommand:
            subcommand = service_info.subcommand
            command = f'{command} {subcommand}'

        return command

    @classmethod
    def start(
        cls, service: str, is_primary: bool, use_sudo: bool = True
    ) -> bool:
        """Start process in docker container.

        :param service: process name
        :type service: str
        :param is_primary: is node primary or not
        :type is_primary: bool, optional
        :param use_sudo: interface required, unused here, defaults to True
        :type use_sudo: bool, optional
        :return: True if service started successfully
        :rtype: bool
        """
        logger = logging.getLogger('container_sh')
        if cls.is_service_running(service):
            return True

        logger.debug(f'Starting {service}')
        env_vars = {"LD_PRELOAD": cls.get_libjemalloc_path()}
        command = cls._make_cmd(service)

        if service == 'workernode':
            # workernode starts on primary and non primary node with 1 or 2
            # added to the end of argument:
            # DBRM_Worker1 - on primary, DBRM_Worker2 - non primary
            command = command.format(1 if is_primary else 2)

            # start mcs-loadbrm.py before workernode
            logger.debug('Waiting to load BRM.')
            loadbrm_path = os.path.join(MCS_INSTALL_BIN, 'mcs-loadbrm.py')
            loadbrm_logpath = cls._create_mcs_process_logfile(
                'mcs-loadbrm.log'
            )
            with open(loadbrm_logpath, 'a', encoding='utf-8') as loadbrm_logfh:
                success, _ = cls.exec_command(
                    f'{loadbrm_path} no', stdout=loadbrm_logfh, env=env_vars
                )
            if not success:
                logger.error('Error while loading BRM.')
            else:
                logger.debug('Successfully loaded BRM.')

        service_log_path = cls._create_mcs_process_logfile(
            f'{service.lower()}.log'
        )
        success, _ = cls.exec_command(
            command, daemonize=True,
            stdout=open(service_log_path, 'a', encoding='utf-8'),
            env=env_vars
        )
        # TODO: any other way to detect service finished its initialisation?
        sleep(ALL_MCS_PROGS[service].delay)
        logger.debug(f'Started "{service}".')

        if is_primary and service == 'DDLProc':
            cls._run_dbbuilder()

        return cls.is_service_running(service)

    @classmethod
    def stop(
        cls, service: str, is_primary: bool, use_sudo: bool = True
    ) -> bool:
        """Stop process in docker container.

        :param service: process name
        :type service: str
        :param is_primary: is node primary or not
        :type is_primary: bool, optional
        :param use_sudo: interface required, unused here, defaults to True
        :type use_sudo: bool, optional
        :return: True if service started successfully
        :rtype: bool
        """
        logger = logging.getLogger('container_sh')
        if not cls.is_service_running(service):
            return True

        logger.debug(f'Stopping {service}')
        service_proc = cls._get_proc_object(service)

        if service == 'workernode':
            # Run pre-stop lock reset before saving BRM
            # These stale locks can occur if the controllernode couldn't stop correctly
            #  and they cause mcs-savebrm.py to hang
            dispatcher_utils.release_shmem_locks(logger)

            # start mcs-savebrm.py before stoping workernode
            logger.debug('Waiting to save BRM.')
            savebrm_path = os.path.join(MCS_INSTALL_BIN, 'mcs-savebrm.py')
            savebrm_logpath = cls._create_mcs_process_logfile(
                'mcs-savebrm.log'
            )
            with open(savebrm_logpath, 'a', encoding='utf-8') as savebrm_logfh:
                success, _ = cls.exec_command(
                    savebrm_path, stdout=savebrm_logfh
                )
            if not success:
                logger.error('Error while saving BRM.')
            else:
                logger.debug('Successfully saved BRM.')

            logger.debug('Start clearing SHM.')
            clearshm_path = os.path.join(MCS_INSTALL_BIN, 'clearShm')
            success, _ = cls.exec_command(clearshm_path)
            if not success:
                logger.error('Error while clearing SHM.')
            else:
                logger.debug('Successfully cleared SHM.')

        service_proc.terminate()
        # timeout got from old container.sh
        # TODO: this is still not enough for controllernode process
        #       it should be always stop by SIGKILL, need to investigate.
        timeout = 3
        if service == 'StorageManager':
            timeout = 300  # 5 minutes
        logger.debug(f'Waiting to gracefully stop "{service}".')
        # This function will return as soon as all processes terminate
        # or when timeout (seconds) occurs.
        gone, alive = psutil.wait_procs([service_proc], timeout=timeout)
        if alive:
            logger.debug(
                f'{service} not terminated with SIGTERM, sending SIGKILL.'
            )
            # only one process could be in a list
            alive[0].kill()
            gone, alive = psutil.wait_procs([service_proc], timeout=timeout)
            if gone:
                logger.debug(f'Successfully killed "{service}".')
            else:
                logger.warning(
                    f'Service "{service}" still alive after sending "kill -9" '
                    f'and waiting {timeout} seconds.'
                )
        else:
            logger.debug(f'Gracefully stopped "{service}".')

        return not cls.is_service_running(service)

    @classmethod
    def restart(
        cls, service: str, is_primary: bool, use_sudo: bool = True
    ) -> bool:
        """Restart process in docker container.

        :param service: process name
        :type service: str
        :param is_primary: is node primary or not
        :type is_primary: bool, optional
        :param use_sudo: interface required, unused here, defaults to True
        :type use_sudo: bool, optional
        :return: True if service started successfully
        :rtype: bool

        ...TODO: for next releases. Additional error handling.
        """
        stop_success = True
        if cls.is_service_running(service):
            # TODO: retry?
            stop_success = cls.stop(service, is_primary, use_sudo)
        start_success = cls.start(service, is_primary, use_sudo)

        return stop_success and start_success
