import json
import logging
import re
import socket
from os import replace
from typing import Tuple, Optional

from cmapi_server.constants import (
    FDB_CONFIG_PATH, FDB_CLUSTER_CONFIG_PATH,
)
from cmapi_server.process_dispatchers.foundation import FDBDispatcher
from cmapi_server.exceptions import CMAPIBasicError


class FDBHandler:

    @staticmethod
    def read_config(filename:str) -> str:
        """Read config file.

        :param filename: filename
        :type filename: str
        :return: config string
        :rtype: str
        """
        with open(filename, encoding='utf-8') as fdb_file:
            fdb_cl_conf = fdb_file.read()
        return fdb_cl_conf

    @staticmethod
    def read_fdb_config() -> str:
        """Read FoundationDB config file

        :return: FoundationDB config file data
        :rtype: str
        """
        return FDBHandler.read_config(FDB_CONFIG_PATH)

    @staticmethod
    def read_fdb_cluster_config() -> str:
        """Read FoundationDB cluster config.

        :return: FoundationDB cluster config file data
        :rtype: str
        """
        return FDBHandler.read_config(FDB_CLUSTER_CONFIG_PATH)

    @staticmethod
    def write_config(filename: str, data: str) -> None:
        """Write config data to file.

        :param filename: filename to write
        :type filename: str
        :param data: data to write
        :type data: str
        """
        # atomic replacement
        tmp_filename = 'config.cmapi.tmp'
        with open(tmp_filename, 'w', encoding='utf-8') as fdb_file:
            fdb_file.write(data)
        replace(tmp_filename, filename)

    @staticmethod
    def write_fdb_config(data: str) -> None:
        """Write data to FoundationDB config file.

        :param data: data to write into FoundationDB config file
        :type data: str
        """
        FDBHandler.write_config(FDB_CONFIG_PATH, data)

    @staticmethod
    def write_fdb_cluster_config(data: str) -> None:
        """Write data to FoundationDB cluster config file.

        :param data: data to write into FoundationDB cluster config file
        :type data: str
        """
        FDBHandler.write_config(FDB_CLUSTER_CONFIG_PATH, data)

    @staticmethod
    def get_node_ipaddress() -> str:
        """Get FoundationDB node ip adress.

        :return: FoundationDB node ip address
        :rtype: str
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('www.foundationdb.org', 80))
            return s.getsockname()[0]
        except Exception:
            logging.error(
                'Could not determine node IP address.',
                exc_info=True
            )
            # TODO: try to handle it? For eg switch to internal logic w\o FDB.
            raise

    @staticmethod
    def make_public(make_tls: bool = False) -> Tuple[str, bool]:
        """Make FoundationDB node externally accessed.

        This method is a rewrited make_public.py from original FoundationDB
        repo.

        :param make_tls: use TLS, defaults to False
        :type make_tls: bool, optional
        :raises CMAPIBasicError: if FoundationDB cluster file is invalid
        :raises CMAPIBasicError: if modified and node address is not 127.0.0.1
        :return: ip adress and use_tls flag
        :rtype: Tuple[str, bool]
        """
        ip_addr = FDBHandler.get_node_ipaddress()
        fdb_cluster_conf = FDBHandler.read_fdb_cluster_config()

        cluster_str = None
        for line in fdb_cluster_conf.split('\n'):
            line = line.strip()
            if len(line) > 0:
                if cluster_str is not None:
                    # TODO: try to handle it?
                    raise CMAPIBasicError('FDB cluster file is not valid')
                cluster_str = line

        if cluster_str is None:
            raise CMAPIBasicError('FDB cluster file is not valid')

        if not re.match(
            '^[a-zA-Z0-9_]*:[a-zA-Z0-9]*@([0-9\\.]*:[0-9]*(:tls)?,)*[0-9\\.]*:[0-9]*(:tls)?$',
            cluster_str
        ):
            raise CMAPIBasicError('FDB cluster file is not valid')

        if not re.match(
            '^.*@(127\\.0\\.0\\.1:[0-9]*(:tls)?,)*127\\.0\\.0\\.1:[0-9]*(:tls)?$',
            cluster_str
        ):
            raise CMAPIBasicError(
                'Cannot modify FDB cluster file whose coordinators are not at '
                'address 127.0.0.1'
            )

        cluster_str.replace('127.0.0.1', ip_addr)

        if make_tls:
            cluster_str = re.sub('([0-9]),', '\\1:tls,', cluster_str)
            if not cluster_str.endswith(':tls'):
                cluster_str += ':tls'

        FDBHandler.write_fdb_cluster_config(cluster_str)

        return ip_addr, cluster_str.count(':tls') != 0

    @staticmethod
    def get_status() -> dict:
        """Get FoundationDB status in json format.

        :return: dict with all FoundationDB status details
        :rtype: dict
        """
        cmd = f'fdbcli --exec "status json"'
        success, output = FDBDispatcher.exec_command(cmd)
        config_dict = json.load(output)
        return config_dict

    @staticmethod
    def get_machines_count() -> int:
        """Get machines in FoundationDB cluster count.

        :return: machines count
        :rtype: int
        """
        return len(FDBHandler.get_status()['cluster']['machines'])

    @staticmethod
    def change_cluster_redundancy(mode: str) -> bool:
        """Change FoundationDB cluster redundancy mode,

        :param mode: FoundationDB cluster redundancy mode
        :type mode: str
        :return: True if success
        :rtype: bool
        """
        if mode not in ('single', 'double', 'triple', 'three_data_hall'):
            logging.error(
                f'FDB cluster redundancy mode is wrong: {mode}. Keep old.'
            )
            return
        cmd = f'fdbcli --exec "configure {mode}"'
        success, _ = FDBDispatcher.exec_command(cmd)

        return success

    @staticmethod
    def set_coordinators(
        nodes_ips: Optional[list] = None, auto: bool = True
    ) -> bool:
        """Set FDB cluster coordinators.

        It sets coordinators ips or `auto`. If `auto` used it will add all
        available nodes to coordinators, so if one coordinators is down,
        cluster still stay healthy.

        :return: True if success
        :rtype: bool
        """
        if not nodes_ips and not auto:
            # do nothing
            logging.warning(
                'No IP address provided to set coordinators, '
                'and auto is False. Nothing to do'
            )
            return
        elif coordinators_string:
            coordinators_with_port = [
                f'{addr}:4500'
                for addr in nodes_ips
            ]
            coordinators_string = ', '.join(coordinators_with_port)
        elif not nodes_ips and auto:
            coordinators_string = 'auto'
        cmd = 'fdbcli --exec "coordinators {coordinators_string}"'
        success, _ = FDBDispatcher.exec_command(cmd)
        return success

    @staticmethod
    def include_all_nodes() -> bool:
        """Invoke command 'include all' in fdbcli.

        Command includes all available machines in a cluster. Mandatory if node
        added after it was removed from a cluster

        :return: True if success
        :rtype: bool
        """
        cmd = 'fdbcli --exec "include all"'
        success, _ = FDBDispatcher.exec_command(cmd)
        return success

    @staticmethod
    def exclude_node() -> bool:
        """Exclude current machine from FoundationDB cluster.

        Method invokes command 'exclude <IP>'

        :return: True if success
        :rtype: bool
        """
        ip_addr = FDBHandler.get_node_ipaddress()
        cmd = f'fdbcli --exec "exclude {ip_addr}"'
        success, _ = FDBDispatcher.exec_command(cmd)
        return success

    @staticmethod
    def add_to_cluster(cluster_config_data: str):
        """Add current machine to FoundationDB cluster using cluster conf data.

        :param cluster_config_data: FoundationDb cluster config data
        :type cluster_config_data: str
        """
        FDBDispatcher.start()
        FDBHandler.write_fdb_cluster_config(cluster_config_data)
        FDBDispatcher().restart()
        new_nodes_count = FDBHandler.get_machines_count() + 1
        if 5 > new_nodes_count >= 3:
            FDBHandler.change_cluster_redundancy('double')
        elif new_nodes_count >= 5:
            FDBHandler.change_cluster_redundancy('triple')
        elif new_nodes_count < 3:
            FDBHandler.change_cluster_redundancy('single')
        # TODO: add error handler
        FDBHandler.include_all_nodes()
        FDBHandler.set_coordinators(auto=True)

    @staticmethod
    def remove_from_cluster():
        """Remove current machine from FoundationDB cluster."""
        new_nodes_count = FDBHandler.get_machines_count() - 1
        if 5 > new_nodes_count >= 3:
            FDBHandler.change_cluster_redundancy('double')
        elif new_nodes_count >= 5:
            FDBHandler.change_cluster_redundancy('triple')
        elif new_nodes_count < 3:
            FDBHandler.change_cluster_redundancy('single')
        # this operation could take a while depending on data size stored in
        # FDB. May be it could be replaced with the same command with `failed`
        # flag. Data loss? TODO: Have to be tested.
        FDBHandler.exclude_node()
        # exclude node from coordinators
        FDBHandler.set_coordinators(auto=True)
        # TODO: set single node cluster file
        FDBDispatcher.restart()
