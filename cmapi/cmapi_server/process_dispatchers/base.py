"""Module contains base process dispatcher class implementation.

Formally this is must have interface for subclasses.
"""

import logging
import os
import shlex
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, TextIO, Tuple

from cmapi_server.constants import MCS_INSTALL_BIN, MCS_LOG_PATH


class BaseDispatcher:
    """Class with base interfaces for dispatchers."""

    @staticmethod
    def _create_mcs_process_logfile(filename: str) -> str:
        """Create log file by name.

        :param filename: log filename
        :type filename: str
        :return: full path of created log file
        :rtype: str
        """
        log_fullpath = os.path.join(MCS_LOG_PATH, filename)
        Path(log_fullpath).touch(mode=666)
        return log_fullpath

    @staticmethod
    def exec_command(
        command: str, daemonize: bool = False, silent: bool = False,
        stdout: TextIO = subprocess.PIPE, env: Optional[Dict] = None
    ) -> Tuple[bool, str]:
        """Run command using subprocess.

        :param command: command to run
        :type command: str
        :param daemonize: run command in detached mode, defaults to False
        :type daemonize: bool, optional
        :param silent: prevent error logs on non-zero exit status,
                       defaults to False
        :type silent: bool, optional
        :param stdout: stdout argument for Popen, defaults to subprocess.STDOUT
        :type stdout: TextIO, optional
        :param env: environment argument for Popen, defaults to None
        :type env: Optional[Dict], optional
        :return: tuple with success status and output string from subprocess,
                 if there are multiple lines in output they should be splitted
        :rtype: Tuple[bool, str]
        """
        output: str = ''
        result: Tuple = (False, output)
        try:
            proc = subprocess.Popen(
                shlex.split(command),
                stdout=stdout,
                stderr=subprocess.STDOUT,
                start_new_session=daemonize,
                env=env,
                encoding='utf-8'
            )
        except Exception:
            logging.error(f'Failed on run command "{command}".', exc_info=True)
            # TODO: cmapi have to close with exception here
            #       to stop docker container?
            # raise
            return result
        if daemonize:
            # remove Popen object. optionally gc.collect could be invoked.
            # this is made to prevent eventually spawning duplicated "defunct"
            # (zombie) python parented processes. This could happened
            # previously after cluster restart. It didn't affects cluster
            # condition, only makes "mcs cluster status" command output
            # confusing and ugly.
            del proc
            result = (True, output)
        else:
            logging.debug('Waiting for command to finish.')
            stdout_str, _ = proc.communicate()
            returncode = proc.wait()
            if stdout_str is not None:
                # output guaranteed to be empty string not None
                output = stdout_str
            result = (True, output)
            if returncode != 0:
                if not silent:
                    logging.error(
                        f'Calling "{command}" finished with return code: '
                        f'"{returncode}" and stderr+stdout "{output}".'
                    )
                result = (False, output)
        return result

    @classmethod
    def _run_dbbuilder(cls, use_su=False) -> None:
        # attempt to run dbbuilder on primary node
        # e.g., s3 was setup after columnstore install
        logging.info('Attempt to run dbbuilder on primary node')
        dbbuilder_path = os.path.join(MCS_INSTALL_BIN, 'dbbuilder')
        dbbuilder_arg = '7'
        dbb_command = f'{dbbuilder_path} {dbbuilder_arg}'
        if use_su:
            # TODO: move mysql user to constants
            dbb_command = f'su -s /bin/sh -c "{dbb_command}" mysql'
        dbb_log_path = cls._create_mcs_process_logfile('dbbuilder.log')
        with open(dbb_log_path, 'a', encoding='utf-8') as dbb_log_fh:
            dbb_start_time = datetime.now().strftime('%d/%b/%Y %H:%M:%S')
            dbb_log_fh.write(f'-----Started at {dbb_start_time}.-----\n')
            # TODO: error handling?
            #       check if exist for next releases?
            success, _ = cls.exec_command(dbb_command, stdout=dbb_log_fh)
            dbb_log_fh.write('-----Finished run.-----\n\n')

    @classmethod
    def init(cls):
        """Method for dispatcher initialisation."""
        pass

    @classmethod
    def is_service_running(cls, service: str, use_sudo: bool) -> bool:
        """Check if systemd proceess/service is running."""
        raise NotImplementedError

    @classmethod
    def start(cls, service: str, is_primary: bool, use_sudo: bool) -> bool:
        """Start process/service."""
        raise NotImplementedError

    @classmethod
    def stop(cls, service: str, is_primary: bool, use_sudo: bool) -> bool:
        """Stop process/service."""
        raise NotImplementedError

    @classmethod
    def restart(cls, service: str, is_primary: bool, use_sudo: bool) -> bool:
        """Restart process/service."""
        raise NotImplementedError

    @classmethod
    def reload(cls, service: str, is_primary: bool, use_sudo: bool) -> bool:
        """Reload process/service."""
        raise NotImplementedError
