from __future__ import annotations

import logging
import os.path
import socket
from time import sleep

import psutil

from cmapi_server.constants import ALL_MCS_PROGS, MCS_INSTALL_BIN, MCSProgs, ProgInfo
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.process_dispatchers.base import BaseDispatcher
from cmapi_server.process_dispatchers.container import ContainerDispatcher
from cmapi_server.process_dispatchers.systemd import SystemdDispatcher
from mcs_node_control.models.dbrm import DBRM
from mcs_node_control.models.dbrm_socket import SOCK_TIMEOUT
from mcs_node_control.models.misc import get_workernodes
from mcs_node_control.models.process import Process

PROCESS_DISPATCHERS: dict[str, type[BaseDispatcher]] = {
    'systemd': SystemdDispatcher,
    # could be used in docker containers and OSes w/o systemd
    'container': ContainerDispatcher,
}
PRIMARY_PROGS = ('controllernode', 'DMLProc', 'DDLProc')


class MCSProcessManager:
    """Class to run process operations.

    e.g. re/-start or stop systemd services, run executable.
    """
    CONTROLLER_MAX_RETRY = 30
    mcs_progs: dict[str, ProgInfo] = {}
    mcs_version_info = None
    dispatcher_name = None
    process_dispatcher: BaseDispatcher = None

    @classmethod
    def _get_prog_name(cls, name: str) -> str:
        """Get proper service name for systemd or non-systemd installations.

        :param name: service name
        :type name: str
        :return: correct service name
        :rtype: str
        """
        if cls.dispatcher_name == 'systemd':
            prog = MCSProgs(name)
            return ALL_MCS_PROGS[prog].service_name
        return name

    @classmethod
    def _get_sorted_progs(
        cls, is_primary: bool, reverse: bool = False, is_read_replica: bool = False
    ) -> dict:
        """Get sorted services dict.

        :param is_primary: is primary node or not
        :type is_primary: bool
        :param reverse: reverse sort order, defaults to False
        :type reverse: bool, optional
        :return: dict with sorted services in correct start/stop order
        :rtype: dict
        """
        unsorted_progs: dict
        if is_primary:
            unsorted_progs = cls.mcs_progs
        else:
            unsorted_progs = {
                prog_name: prog_info
                for prog_name, prog_info in cls.mcs_progs.items()
                if prog_name not in PRIMARY_PROGS
            }

        if is_read_replica:
            logging.debug('Node is a read replica, skipping WriteEngine')
            unsorted_progs.pop(
                MCSProgs.WRITE_ENGINE_SERVER, None
            )

        if reverse:
            # stop sequence builds using stop_priority property
            return dict(
                sorted(
                    unsorted_progs.items(),
                    key=lambda item: item[1].stop_priority,
                )
            )
        # start up sequence is a dict default sequence
        return unsorted_progs

    @classmethod
    def _detect_processes(cls) -> None:
        """Detect existing mcs services. Depends on MCS version."""
        if cls.mcs_progs:
            logging.warning('Mcs ProcessHandler already detected processes.')

        for prog, prog_info in ALL_MCS_PROGS.items():
            prog_name = prog.value
            if os.path.exists(os.path.join(MCS_INSTALL_BIN, prog_name)):
                cls.mcs_progs[prog_name] = prog_info

    @classmethod
    def detect(cls, dispatcher_name: str, dispatcher_path: str = None) -> None:
        """Detect mcs version info and installed processes.

        :param dispatcher_name: process dispatcher name
        :type dispatcher_name: str
        :param dispatcher_path: path to custom dispatcher,
            for next releases, defaults to None
        :type dispatcher_path: str, optional
        :raises CMAPIBasicError: if custom dispatcher path doesn't exists
        :raises CMAPIBasicError: Not implemented custom dispatcher error
        """
        cls._detect_processes()
        # detect mcs version info by processes
        if len(cls.mcs_progs) == 8:
            cls.mcs_version_info = '6.4.x and lower'
        elif len(cls.mcs_progs) == 7 and 'ExeMgr' not in cls.mcs_progs:
            cls.mcs_version_info = '22.08.x and higher'
        else:
            cls.mcs_version_info = 'Undefined'
            logging.warning(
                'MCS version haven\'t been detected properly.'
                'Please try to update your CMAPI version or contact support.'
            )
        logging.info(
            f'Detected {len(cls.mcs_progs)} MCS services.'
            f'MCS version is {cls.mcs_version_info}'
        )
        # TODO: For next releases. Do we really need custom dispatchers?
        if dispatcher_name not in PROCESS_DISPATCHERS:
            logging.warning(
                f'Custom process dispatcher with name "{dispatcher_name}" '
                f'and path "{dispatcher_path}" used.'
            )
            if not dispatcher_path or not os.path.exists(dispatcher_path):
                err_msg = 'Wrong dispatcher path in cmapi_config file.'
                logging.error(err_msg)
                raise CMAPIBasicError(err_msg)
            cls.dispatcher_name = 'custom'
            raise CMAPIBasicError('Custom dispatchers yet not implemented!')

        cls.dispatcher_name = dispatcher_name
        cls.process_dispatcher = PROCESS_DISPATCHERS[dispatcher_name]
        cls.process_dispatcher.init()

    @classmethod
    def _wait_for_workernodes(cls) -> bool:
        """Wait for workernodes processes.

        Waiting for all workernodes to come up before starting
        controllernode on a primary.

        :return: True on success
        :rtype: bool
        """
        logging.debug(
            'Waiting for all workernodes to come up before starting '
            'controllernode on a primary.'
        )
        workernodes = get_workernodes()
        attempts = cls.CONTROLLER_MAX_RETRY
        while attempts > 0 and len(workernodes) > 0:
            logging.debug(f'Waiting for "{list(workernodes)}"....{attempts}')
            # creating a separated list with workernode names
            # for safe deleting items from source dict
            for name in list(workernodes):
                try:
                    sock = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM
                    )
                    sock.settimeout(SOCK_TIMEOUT)
                    sock.connect(
                        (
                            workernodes[name]['IPAddr'],
                            workernodes[name]['Port']
                        )
                    )
                except (ConnectionRefusedError, socket.timeout):
                    logging.debug(
                        f'"{name}" {workernodes[name]["IPAddr"]}:'
                        f'{workernodes[name]["Port"]} not started yet.'
                    )
                else:
                    # delete started workernode from workernodes dict
                    del workernodes[name]
                finally:
                    sock.close()
            attempts -= 1

        if workernodes:
            logging.error(
                f'Some workernodes: "{workernodes}" are not reachable after '
                f'{cls.CONTROLLER_MAX_RETRY} attempts to connect with '
                f'{SOCK_TIMEOUT} seconds timeout.'
                'Starting mcs-controllernode anyway.'
            )
            return False
        return True

    @classmethod
    def _wait_for_controllernode(cls) -> bool:
        """Waiting for controllernode to come up on a primary.

        :return: True on success
        :rtype: bool
        """
        logging.debug(
            'Waiting for controllernode to come up before starting '
            'ddlproc/dmlproc on non-primary nodes.'
        )
        attempts = cls.CONTROLLER_MAX_RETRY
        success = False
        while attempts > 0:
            try:
                with DBRM():
                    # check connection
                    success = True
            except (OSError, ConnectionRefusedError, RuntimeError):
                logging.info(
                    'Cannot establish connection to controllernode.'
                    f'Controller node still not started. Waiting...{attempts}'
                )
            else:
                break
            attempts -= 1

        if not success:
            logging.error(
                'Controllernode is not reachable after '
                f'{cls.CONTROLLER_MAX_RETRY} attempts to connect with '
                f'{SOCK_TIMEOUT} seconds timeout.'
                'Starting mcs-dmlproc/mcs-ddlproc anyway.'
            )
            return False
        return True

    @classmethod
    def _wait_for_DMLProc_stop(cls, timeout: int = 10) -> bool:
        """Waiting DMLProc process to stop.

        :param timeout: timeout to wait, defaults to 10
        :type timeout: int, optional
        :return: True on success
        :rtype: bool
        """
        logging.info(f'Waiting for DMLProc to stop in {timeout} seconds')
        dmlproc_stopped = False
        while timeout > 0:
            logging.info(
                f'Waiting for DMLProc to stop. Seconds left {timeout}.'
            )
            if not Process.check_process_alive('DMLProc'):
                logging.info('DMLProc gracefully stopped by DBRM command.')
                dmlproc_stopped = True
                break
            sleep(1)
            timeout -= 1
        else:
            logging.error(
                f'DMLProc did not stopped gracefully by DBRM command within '
                f'{timeout} seconds. Will be stopped directly.'
            )
        return dmlproc_stopped

    @classmethod
    def noop(cls, *args, **kwargs):
        """No operation. TODO: looks like useless."""
        cls.process_dispatcher.noop()

    @classmethod
    def gracefully_stop_dmlproc(cls) -> None:
        """Gracefully stop DMLProc using DBRM commands."""
        logging.info(
            'Trying to gracefully stop DMLProc using DBRM commands.'
        )
        try:
            with DBRM() as dbrm:
                dbrm.set_system_state(
                    ['SS_ROLLBACK', 'SS_SHUTDOWN_PENDING']
                )
        except (ConnectionRefusedError, RuntimeError):
            logging.error(
                'Cannot set SS_ROLLBACK and SS_SHUTDOWN_PENDING via DBRM, '
                'graceful auto stop of DMLProc failed. '
                'Try a regular stop method.'
            )
            raise

    @classmethod
    def is_service_running(cls, name: str, use_sudo: bool = True) -> bool:
        """Check if MCS process is running.

        :param name: mcs process name
        :type name: str
        :param use_sudo: use sudo or not, defaults to True
        :type use_sudo: bool, optional
        :return: True if mcs process is running, otherwise False
        :rtype: bool
        """
        return cls.process_dispatcher.is_service_running(
            cls._get_prog_name(name), use_sudo
        )


    @classmethod
    def start(cls, name: str, is_primary: bool, use_sudo: bool) -> bool:
        """Start mcs process.

        :param name: mcs process name
        :type name: str
        :param is_primary: is node primary or not
        :type is_primary: bool
        :param use_sudo: use sudo or not
        :type use_sudo: bool
        :return: True if process started successfully
        :rtype: bool
        """
        return cls.process_dispatcher.start(
            cls._get_prog_name(name), is_primary, use_sudo
        )

    @classmethod
    def stop(
        cls, name: str, is_primary: bool, use_sudo: bool, timeout: int = 10
    ) -> bool:
        """Stop mcs process.

        :param name: mcs process name
        :type name: str
        :param is_primary: is node primary or not
        :type is_primary: bool
        :param use_sudo: use sudo or not
        :type use_sudo: bool
        :param timeout: timeout for DMLProc gracefully stop using DBRM, seconds
        :type timeout: int
        :return: True if process stopped successfully
        :rtype: bool
        """
        # TODO: do we need here force stop DMLProc as a method argument?

        if is_primary and name == 'DMLProc':
            try:
                cls.gracefully_stop_dmlproc()
            except (ConnectionRefusedError, RuntimeError):
                # stop DMLProc using regular signals or systemd
                return cls.process_dispatcher.stop(
                    cls._get_prog_name(name), is_primary, use_sudo
                )
            # DMLProc gracefully stopped using DBRM commands otherwise
            # continue with a regular stop method
            if cls._wait_for_DMLProc_stop(timeout):
                return True
        return cls.process_dispatcher.stop(
            cls._get_prog_name(name), is_primary, use_sudo
        )

    @classmethod
    def restart(cls, name: str, is_primary: bool, use_sudo: bool) -> bool:
        """Restart mcs process.

        :param name: mcs process name
        :type name: str
        :param is_primary: is node primary or not
        :type is_primary: bool
        :param use_sudo: use sudo or not
        :type use_sudo: bool
        :return: True if process restarted successfully
        :rtype: bool
        """
        return cls.process_dispatcher.restart(
            cls._get_prog_name(name), is_primary, use_sudo
        )

    @classmethod
    def get_running_mcs_procs(cls) -> list[dict]:
        """Search for mcs processes.

        The method returns PIDs of MCS services in both container or systemd
        environments.

        :return: list of dicts with name and pid of mcs process
        :rtype: list[dict]
        """
        return [
            {'name': proc.name(), 'pid': proc.pid}
            for proc in psutil.process_iter(['pid', 'name'])
            if proc.name() in cls.mcs_progs
        ]

    @classmethod
    def is_node_processes_ok(
        cls, is_primary: bool, node_stopped: bool
    ) -> bool:
        """Check if needed processes exists or not.

        :param is_primary: is node primary or not
        :type is_primary: bool
        :param node_stopped: is node stopped or started
        :type node_stopped: bool
        :return: True if there are expected value of processes, else False
        :rtype: bool

        ..NOTE: For next releases. Now only used in tests.
        """
        running_procs = cls.get_running_mcs_procs()
        if node_stopped:
            return len(running_procs) == 0
        node_progs = cls._get_sorted_progs(is_primary)
        return set(node_progs) == set(p['name'] for p in running_procs)

    @classmethod
    def start_node(
        cls,
        is_primary: bool,
        use_sudo: bool = True,
        is_read_replica: bool = False,
    ) -> None:
        """Start mcs node processes.

        :param is_primary: is node primary or not, defaults to True
        :type is_primary: bool
        :param use_sudo: use sudo or not, defaults to True
        :type use_sudo: bool, optional
        :param is_read_replica: if true, doesn't start WriteEngine
        :type is_read_replica: bool, optional
        :raises CMAPIBasicError: immediately if one mcs process not started
        """
        for prog_name in cls._get_sorted_progs(
            is_primary=is_primary,
            is_read_replica=is_read_replica,
        ):
            if (
                    cls.dispatcher_name == 'systemd'
                    and prog_name == MCSProgs.STORAGE_MANAGER
            ):
                # TODO: MCOL-5458
                logging.info(
                    f'Skip starting {prog_name} with systemd dispatcher.'
                )
                continue
            # TODO: additional error handling
            if prog_name == MCSProgs.CONTROLLER_NODE:
                cls._wait_for_workernodes()
            if prog_name in (MCSProgs.DML_PROC, MCSProgs.DDL_PROC):
                cls._wait_for_controllernode()
            if not cls.start(prog_name, is_primary, use_sudo):
                logging.error(f'Process "{prog_name}" not started properly.')
                raise CMAPIBasicError(f'Error while starting "{prog_name}".')

    @classmethod
    def stop_node(
        cls,
        is_primary: bool,
        use_sudo: bool = True,
        timeout: int = 10,
    ):
        """Stop mcs node processes.

        :param is_primary: is node primary or not, defaults to True
        :type is_primary: bool
        :param use_sudo: use sudo or not, defaults to True
        :type use_sudo: bool, optional
        :param timeout: timeout for DMLProc gracefully stop using DBRM, seconds
        :type timeout: int
        :raises CMAPIBasicError: immediately if one mcs process not stopped
        """
        # Every time try to stop all processes no matter primary it or slave,
        # so use full available list of processes. Otherwise, it could cause
        # undefined behaviour when primary gone and then recovers (failover
        # triggered 2 times).
        for prog_name in cls._get_sorted_progs(is_primary=True, reverse=True):
            if not cls.stop(prog_name, is_primary, use_sudo):
                logging.error(f'Process "{prog_name}" not stopped properly.')
                raise CMAPIBasicError(f'Error while stopping "{prog_name}"')

    @classmethod
    def restart_node(cls, is_primary: bool, use_sudo: bool, is_read_replica: bool = False):
        """TODO: For next releases."""
        if cls.get_running_mcs_procs():
            cls.stop_node(is_primary, use_sudo)
        cls.start_node(is_primary, use_sudo, is_read_replica)
