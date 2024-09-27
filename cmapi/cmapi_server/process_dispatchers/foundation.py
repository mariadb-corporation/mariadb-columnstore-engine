"""
Module with Foundation DB process disptcher. First try.
TODO: make some kind of SystemD base dispatcher + add container dispatcher.
"""

import logging
import re
from typing import Union, Tuple

from cmapi_server.process_dispatchers.base import BaseDispatcher


class FDBDispatcher(BaseDispatcher):
    """Manipulates with systemd FDB service."""
    systemctl_version: int = 219  #  7 version

    @classmethod
    def _systemctl_call(
        cls, command: str, service: str, use_sudo: bool = True,
        return_output=False, *args, **kwargs
    ) -> Union[Tuple[bool, str], bool]:
        """Run "systemctl" with arguments.

        :param command: command for systemctl
        :type command: str
        :param service: systemd service name
        :type service: str
        :param use_sudo: use sudo or not, defaults to True
        :type use_sudo: bool, optional
        :return: return status of operation, True if success, otherwise False
        :rtype: Union[Tuple[bool, str], bool]
        """
        cmd = f'systemctl {command} {service}'
        if use_sudo:
            cmd = f'sudo {cmd}'
        logging.debug(f'Call "{command}" on service "{service}" with "{cmd}".')
        success, output = cls.exec_command(cmd, *args, **kwargs)
        if return_output:
            return success, output
        return success

    @classmethod
    def init(cls):
        cmd = 'systemctl --version'
        success, output = cls.exec_command(cmd)
        if success:
            # raw result will be like
            # "systemd 239 (245.4-4ubuntu3.17)\n <string with compile flags>"
            cls.systemctl_version = int(
                re.search(r'systemd (\d+)', output).group(1)
            )
            logging.info(f'Detected {cls.systemctl_version} SYSTEMD version.')
        else:
            logging.error('Couldn\'t detect SYSTEMD version')

    @classmethod
    def is_service_running(cls, service: str, use_sudo: bool = True) -> bool:
        """Check if systemd service is running.

        :param service: service name
        :type service: str
        :param use_sudo: use sudo or not, defaults to True
        :type use_sudo: bool, optional
        :return: True if service is running, otherwise False
        :rtype: bool

        ..Note:
            Not working with multiple services at a time.
        """
        logging.debug(f'Checking "{service}" is running.')
        # TODO: remove conditions below when we'll drop CentOS 7 support
        cmd = 'show -p ActiveState --value'
        if cls.systemctl_version < 230:  # not supported --value in old version
            cmd = 'show -p ActiveState'
        _, output = cls._systemctl_call(
            cmd,
            service, use_sudo, return_output=True
        )
        service_state = output.strip()
        if cls.systemctl_version < 230:  # result like 'ActiveState=active'
            service_state = service_state.split('=')[1]
        logging.debug(f'Service "{service}" is in "{service_state}" state')
        # interpret non "active" state as not running service
        if service_state == 'active':
            return True
        # output could be inactive, activating or even empty if
        # command execution was unsuccessfull
        return False

    @classmethod
    def start(
        cls, use_sudo: bool = True
    ) -> bool:
        """Start FDB systemd service.

        :param use_sudo: use sudo or not, defaults to True
        :type use_sudo: bool, optional
        :return: True if service started successfully
        :rtype: bool
        """
        service_name = 'foundationdb'

        if cls.is_service_running(service_name, use_sudo):
            return True

        logging.debug(f'Starting "{service_name}".')
        if not cls._systemctl_call('start', service_name, use_sudo):
            logging.error(f'Failed while starting "{service_name}".')
            return False

        logging.debug(f'Successfully started {service_name}.')
        return cls.is_service_running(service_name, use_sudo)

    @classmethod
    def stop(
        cls, use_sudo: bool = True
    ) -> bool:
        """Stop FDB systemd service.

        :param use_sudo: use sudo or not, defaults to True
        :type use_sudo: bool, optional
        :return: True if service stopped successfully
        :rtype: bool
        """
        service_name = 'foundationdb'

        logging.debug(f'Stopping "{service_name}".')
        if not cls._systemctl_call('stop', service_name, use_sudo):
            logging.error(f'Failed while stopping "{service_name}".')
            return False

        return not cls.is_service_running(service_name, use_sudo)

    @classmethod
    def restart(
        cls, use_sudo: bool = True
    ) -> bool:
        """Restart systemd service.

        :param use_sudo: use sudo or not, defaults to True
        :type use_sudo: bool, optional
        :return: True if service restarted successfully
        :rtype: bool
        """
        service_name = 'foundationdb'

        logging.debug(f'Restarting "{service_name}".')
        if not cls._systemctl_call('restart', service_name, use_sudo):
            logging.error(f'Failed while restarting "{service_name}".')
            return False

        return cls.is_service_running(service_name, use_sudo)
