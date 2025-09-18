"""Module contains systemd process dispatcher class implementation."""

import logging
import re
from typing import Union, Tuple

from cmapi_server.process_dispatchers.base import BaseDispatcher
from cmapi_server.process_dispatchers.locks import release_shmem_locks


class SystemdDispatcher(BaseDispatcher):
    """Manipulates with systemd services."""
    systemctl_version: int = 219  #CentOS 7 version

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
        :type service: str, optional
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

    @staticmethod
    def _workernode_get_service_name(is_primary: bool) -> str:
        """Get proper workernode service name based on primary status.

        :param is_primary: is node where we running primary?
        :type is_primary: bool
        :return: correct workernode service name
        :rtype: str
        """
        service = 'mcs-workernode'
        return f'{service}@1.service' if is_primary else f'{service}@2.service'

    @classmethod
    def _workernode_enable(cls, enable: bool, use_sudo: bool = True) -> None:
        """Enable workernode service.

        :param enable: enable or disable
        :type enable: bool
        :param use_sudo: use sudo or not, defaults to True
        :type use_sudo: bool, optional
        """
        sub_cmd = 'enable' if enable else 'disable'
        service = 'mcs-workernode@1.service'

        if not cls._systemctl_call(sub_cmd, service, use_sudo):
            # enabling\disabling service is not critical, just log failure
            logging.warning(f'Failed to {sub_cmd} {service}')

    @classmethod
    def start(
        cls, service: str, is_primary: bool = True, use_sudo: bool = True
    ) -> bool:
        """Start systemd service.

        :param service: service name
        :type service: str, optional
        :param is_primary: is node primary or not
        :type is_primary: bool, optional
        :param use_sudo: use sudo or not, defaults to True
        :type use_sudo: bool, optional
        :return: True if service started successfully
        :rtype: bool
        """
        service_name = service
        if service_name == 'mcs-workernode':
            service_name = cls._workernode_get_service_name(is_primary)
            if is_primary:
                cls._workernode_enable(True, use_sudo)

        if cls.is_service_running(service_name, use_sudo):
            return True

        logging.debug(f'Starting "{service_name}".')
        if not cls._systemctl_call('start', service_name, use_sudo):
            logging.error(f'Failed while starting "{service_name}".')
            return False

        if is_primary and service == 'mcs-ddlproc':
            cls._run_dbbuilder(use_su=True)

        logging.debug(f'Successfully started {service_name}.')
        return cls.is_service_running(service_name, use_sudo)

    @classmethod
    def stop(
        cls, service: str, is_primary: bool = True, use_sudo: bool = True
    ) -> bool:
        """Stop systemd service.

        :param service: service name
        :type service: str, optional
        :param is_primary: is node primary or not
        :type is_primary: bool, optional
        :param use_sudo: use sudo or not, defaults to True
        :type use_sudo: bool, optional
        :return: True if service stopped successfully
        :rtype: bool
        """
        service_name = service
        if service_name == 'mcs-workernode':
            # Run pre-stop lock reset before saving BRM
            # These stale locks can occur if the controllernode couldn't stop correctly
            #  and they cause mcs-savebrm.py to hang
            release_shmem_locks(logging.getLogger(__name__))

            service_name = f'{service_name}@1.service {service_name}@2.service'
            cls._workernode_enable(False, use_sudo)

        logging.debug(f'Stopping "{service_name}".')
        if not cls._systemctl_call('stop', service_name, use_sudo):
            logging.error(f'Failed while stopping "{service_name}".')
            return False

        return not cls.is_service_running(service, use_sudo)

    @classmethod
    def restart(
        cls, service: str, is_primary: bool = True, use_sudo: bool = True
    ) -> bool:
        """Restart systemd service.

        :param service: service name
        :type service: str, optional
        :param is_primary: is node primary or not, defaults to True
        :type is_primary: bool, optional
        :param use_sudo: use sudo or not, defaults to True
        :type use_sudo: bool, optional
        :return: True if service restarted successfully
        :rtype: bool
        """
        service_name = service
        if service_name == 'mcs-workernode':
            service_name = cls._workernode_get_service_name(is_primary)

        logging.debug(f'Restarting "{service_name}".')
        if not cls._systemctl_call('restart', service_name, use_sudo):
            logging.error(f'Failed while restarting "{service_name}".')
            return False

        return cls.is_service_running(service, use_sudo)

    @classmethod
    def reload(
        cls, service: str, is_primary: bool = True, use_sudo: bool=True
    ) -> bool:
        """Reload systemd service.

        :param service: service name, defaults to 'Unknown_service'
        :type service: str, optional
        :param is_primary: is node primary or not, defaults to True
        :type is_primary: bool, optional
        :param use_sudo: use sudo or not, defaults to True
        :type use_sudo: bool, optional
        :return: True if service reloaded successfully
        :rtype: bool

        ..NOTE: For next releases. It should become important when we teach
                MCS to add/remove nodes w/o whole cluster restart.
                Additional error handling?
        """
        service_name = service
        if service_name == 'mcs-workernode':
            service_name = cls._workernode_get_service_name(is_primary)

        logging.debug(f'Reloading "{service_name}".')
        if not cls._systemctl_call('reload', service_name, use_sudo):
            logging.error(f'Failed while reloading "{service_name}".')
            return False

        return not cls.is_service_running(service, use_sudo)
