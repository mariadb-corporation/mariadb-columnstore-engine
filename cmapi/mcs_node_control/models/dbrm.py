import logging
import socket

from cmapi_server.constants import DEFAULT_MCS_CONF_PATH
from mcs_node_control.models.dbrm_socket import (
    DBRM_COMMAND_BYTES, DEFAULT_HOST, DEFAULT_PORT, DBRMSocketHandler
)
from mcs_node_control.models.node_config import NodeConfig
from mcs_node_control.models.process import Process


# TODO: why we need bitwise shift here? May be constant values?
SYSTEM_STATE_FLAGS = {
    "SS_READY":            1 << 0,  # 1
    "SS_SUSPENDED":        1 << 1,  # 2
    "SS_SUSPEND_PENDING":  1 << 2,  # 4
    "SS_SHUTDOWN_PENDING": 1 << 3,  # 8
    "SS_ROLLBACK":         1 << 4,  # 16
    "SS_FORCE":            1 << 5,  # 32
    "SS_QUERY_READY":      1 << 6,  # 64
}


module_logger = logging.getLogger()


class DBRM:
    """Class DBRM commands"""
    def __init__(
        self, root=None, config_filename: str = DEFAULT_MCS_CONF_PATH
    ):
        self.dbrm_socket = DBRMSocketHandler()
        self.root = root
        self.config_filename = config_filename

    def connect(self):
        node_config = NodeConfig()
        root = self.root or node_config.get_current_config_root(
            self.config_filename
        )
        master_conn_info = node_config.get_dbrm_conn_info(root)
        if master_conn_info is None:
            module_logger.warning(
                 'DBRB.connect: No DBRM info in the Columnstore.xml.'
            )
        dbrm_host = master_conn_info['IPAddr'] or DEFAULT_HOST
        dbrm_port = int(master_conn_info['Port']) or DEFAULT_PORT
        self.dbrm_socket.connect(dbrm_host, dbrm_port)

    def close(self):
        self.dbrm_socket.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_type:
            return False
        return True

    def _send_command(self, command_name, command_value=None):
        if command_name not in DBRM_COMMAND_BYTES:
            module_logger.warning(
                f'DBRM._send_command: Wrong command requested {command_name}'
            )
            return None

        module_logger.info(
            f'DBRM._send_command: Command {command_name} '
            f'was requested with value {command_value}'
        )

        self.dbrm_socket.send(command_name, command_value)
        response_value_bytes = self.dbrm_socket.receive()

        if command_name == 'readonly':
            reply = int.from_bytes(response_value_bytes, 'little')
        else:
            # get first byte, it's an error message
            err = int.from_bytes(response_value_bytes[:1], 'little')

            if err != 0:
                module_logger.warning(
                    f'DBRM._send_command: Command {command_name} '
                    'returned error on server'
                )
                raise RuntimeError(
                    f'Controller Node replied error with code {err} '
                    f'for command {command_name}'
                )

            if len(response_value_bytes) < 2:
                return None

            reply = int.from_bytes(response_value_bytes[1:], 'little')
        return reply

    def get_system_state(self):
        state = self._send_command('get_system_state')
        return [
            flag_name for flag_name, flag_value in SYSTEM_STATE_FLAGS.items()
            # TODO: looks like weird logic? Not readable.
            if flag_value & state
        ]

    def _edit_system_state(self, states: list, command: str):
        state = 0
        # TODO: why we need this? States type is list.
        #       May be str without loop inside is more appropriate here.
        if isinstance(states, str):
            states = (states,)

        for state_name in states:
            if state_name not in SYSTEM_STATE_FLAGS:
                module_logger.warning(
                    f'DBRM.{command}: Wrong system state requested: '
                    f'{state_name}'
                )
                continue
            # TODO: For that case it's same with simple addition?
            #       So why we need bitwise OR?
            state |= SYSTEM_STATE_FLAGS[state_name]

        self._send_command(command, state)

    def set_system_state(self, states: list):
        self._edit_system_state(states, 'set_system_state')

    def clear_system_state(self, states: list):
        self._edit_system_state(states, 'clear_system_state')

    @staticmethod
    def get_dbrm_status():
        """Reads DBRM status

        DBRM Block Resolution Manager operates in two modes:
            - master
            - slave

        This method returns the mode of this DBRM node
        looking for controllernode process running.

        :return: mode of this DBRM node
        :rtype: string
        """
        if Process.check_process_alive('controllernode'):
            return 'master'
        return 'slave'

    def _get_cluster_mode(self):
        """Get DBRM cluster mode for internal usage.

        Returns real DBRM cluster mode from socket response.
        """
        # state can be 1(readonly) or 0(readwrite) or exception raised
        state = self._send_command('readonly')
        if state == 1:
            return 'readonly'
        elif state == 0:
            return 'readwrite'

    def get_cluster_mode(self):
        """Get DBRM cluster mode for external usage.

        There are some kind of weird logic.
        It's requested from management.
        TODO: Here we can cause a logic error.
              E.g. set non master node to "readwrite" and
                we got a "readonly" in return value.

        :return: DBRM cluster mode
        :rtype: str
        """
        real_mode = self._get_cluster_mode()
        if self.get_dbrm_status() == 'master':
            return real_mode
        else:
            return 'readonly'

    def set_cluster_mode(self, mode):
        """Set cluster mode requested

        Connects to the DBRM master's socket and
        send a command to set cluster mode.

        :rtype: str :error or cluster mode set
        """

        if mode == 'readonly':
            command = 'set_readonly'
        elif mode == 'readwrite':
            command = 'set_readwrite'
        else:
            return ''

        _ = self._send_command(command)

        return self.get_cluster_mode()


def set_cluster_mode(
    mode: str, root=None, config_filename: str = DEFAULT_MCS_CONF_PATH
):
    """Set cluster mode requested

    Connects to the DBRM master's socket and send a command to
    set cluster mode.

    :rtype: str :error or cluster mode set
    """
    try:
        with DBRM(root, config_filename) as dbrm:
            return dbrm.set_cluster_mode(mode)
    except (ConnectionRefusedError, RuntimeError, socket.error):
        module_logger.warning(
            'Cannot establish DBRM connection.', exc_info=True
        )
        return 'readonly'
