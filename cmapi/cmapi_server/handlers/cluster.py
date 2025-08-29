"""Module contains Cluster business logic functions."""
import logging
from datetime import datetime
from enum import Enum
from typing import Optional

from mcs_node_control.models.misc import get_dbrm_master
from mcs_node_control.models.node_config import NodeConfig

from cmapi_server.constants import (
    CMAPI_CONF_PATH,
    DEFAULT_MCS_CONF_PATH,
)
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.helpers import (
    broadcast_new_config,
    get_active_nodes,
    get_config_parser,
    get_current_key,
    get_dbroots,
    get_version,
    update_revision_and_manager,
)
from cmapi_server.node_manipulation import (
    add_dbroot,
    add_node,
    remove_node,
    switch_node_maintenance,
)
from tracing.traced_session import get_traced_session


class ClusterAction(Enum):
    START = 'start'
    STOP = 'stop'


def toggle_cluster_state(
        action: ClusterAction, config: str) -> dict:
    """Toggle the state of the cluster (start or stop).

    :param action: The cluster action to perform.
                   (ClusterAction.START or ClusterAction.STOP).
    :type action: ClusterAction
    :param config: The path to the MariaDB Columnstore configuration file.
    :type config: str
    """
    if action == ClusterAction.START:
        maintainance_flag = False
    elif action == ClusterAction.STOP:
        maintainance_flag = True
    else:
        raise ValueError(
            'Invalid action. Use ClusterAction.START or ClusterAction.STOP.'
        )

    switch_node_maintenance(maintainance_flag)
    update_revision_and_manager()
    broadcast_new_config(config, distribute_secrets=True)


class ClusterHandler:
    """Class for handling MCS Cluster operations."""

    @staticmethod
    def status(config: str = DEFAULT_MCS_CONF_PATH) -> dict:
        """Method to get MCS Cluster status information

        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :raises CMAPIBasicError: if catch some exception while getting status
                                 from each node separately
        :return: status result
        :rtype: dict
        """
        logger: logging.Logger = logging.getLogger('cmapi_server')
        logger.debug('Cluster status command called. Getting status.')

        response = {'timestamp': str(datetime.now())}
        active_nodes = get_active_nodes(config)
        cmapi_cfg_parser = get_config_parser(CMAPI_CONF_PATH)
        api_key = get_current_key(cmapi_cfg_parser)
        headers = {'x-api-key': api_key}
        num_nodes = 0

        for node in active_nodes:
            url = f'https://{node}:8640/cmapi/{get_version()}/node/status'
            try:
                r = get_traced_session().request('GET', url, verify=False, headers=headers)
                r.raise_for_status()
                r_json = r.json()
                if len(r_json.get('services', 0)) == 0:
                    r_json['dbrm_mode'] = 'offline'
                    r_json['cluster_mode'] = 'offline'

                response[f'{str(node)}'] = r_json
                num_nodes += 1
            except Exception as err:
                raise CMAPIBasicError(
                    f'Got an error retrieving status from node {node}'
                ) from err

        response['num_nodes'] = num_nodes
        logger.debug('Successfully finished getting cluster status.')
        return response

    @staticmethod
    def start(config: str = DEFAULT_MCS_CONF_PATH) -> dict:
        """Method to start MCS Cluster.

        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :raises CMAPIBasicError: if no nodes in the cluster
        :return: start timestamp
        :rtype: dict
        """
        logger: logging.Logger = logging.getLogger('cmapi_server')
        logger.info('Cluster start command called. Starting the cluster.')
        operation_start_time = str(datetime.now())
        toggle_cluster_state(ClusterAction.START, config)
        logger.info('Successfully finished cluster start.')
        return {'timestamp': operation_start_time}

    @staticmethod
    def shutdown(
        config: str = DEFAULT_MCS_CONF_PATH, timeout: Optional[int] = None
    ) -> dict:
        """Method to stop the MCS Cluster.

        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :param timeout: timeout in seconds to gracefully stop DMLProc,
                        defaults to None
        :type timeout: Optional[int], optional
        :raises CMAPIBasicError: if no nodes in the cluster
        :return: start timestamp
        :rtype: dict
        """
        logger: logging.Logger = logging.getLogger('cmapi_server')
        logger.debug(
            'Cluster shutdown command called. Shutting down the cluster.'
        )
        operation_start_time = str(datetime.now())
        toggle_cluster_state(ClusterAction.STOP, config)
        logger.debug('Successfully finished shutting down the cluster.')
        return {'timestamp': operation_start_time}

    @staticmethod
    def add_node(node: str, config: str = DEFAULT_MCS_CONF_PATH) -> dict:
        """Method to add node to MCS CLuster.

        :param node: node IP or name or FQDN
        :type node: str
        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :raises CMAPIBasicError: on exception while starting transaction
        :raises CMAPIBasicError: if transaction start isn't successful
        :raises CMAPIBasicError: on exception while adding node
        :raises CMAPIBasicError: on exception while distributing new config
        :raises CMAPIBasicError: on unsuccessful distibuting config file
        :raises CMAPIBasicError: on exception while committing transaction
        :return: result of adding node
        :rtype: dict
        """
        logger: logging.Logger = logging.getLogger('cmapi_server')
        logger.debug(f'Cluster add node command called. Adding node {node}.')

        response = {'timestamp': str(datetime.now())}

        try:
            add_node(
                node, input_config_filename=config,
                output_config_filename=config
            )
            if not get_dbroots(node, config):
                add_dbroot(
                    host=node, input_config_filename=config,
                    output_config_filename=config
                )
        except Exception as err:
            raise CMAPIBasicError('Error while adding node.') from err

        response['node_id'] = node
        update_revision_and_manager(
            input_config_filename=config, output_config_filename=config
        )
        broadcast_new_config(config, distribute_secrets=True)
        logger.debug(f'Successfully finished adding node {node}.')
        return response

    @staticmethod
    def remove_node(node: str, config: str = DEFAULT_MCS_CONF_PATH) -> dict:
        """Method to remove node from MCS CLuster.

        :param node: node IP or name or FQDN
        :type node: str
        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :raises CMAPIBasicError: on exception while starting transaction
        :raises CMAPIBasicError: if transaction start isn't successful
        :raises CMAPIBasicError: on exception while removing node
        :raises CMAPIBasicError: on exception while distributing new config
        :raises CMAPIBasicError: on unsuccessful distibuting config file
        :raises CMAPIBasicError: on exception while committing transaction
        :return: result of node removing
        :rtype: dict
        """
        #TODO: This method will be moved to transaction manager in next release
        #      Due to specific use of txn_nodes inside.
        logger: logging.Logger = logging.getLogger('cmapi_server')
        logger.debug(
            f'Cluster remove node command called. Removing node {node}.'
        )
        response = {'timestamp': str(datetime.now())}

        try:
            remove_node(
                node, input_config_filename=config,
                output_config_filename=config
            )
        except Exception as err:
            raise CMAPIBasicError('Error while removing node.') from err

        response['node_id'] = node
        active_nodes = get_active_nodes(config)
        if len(active_nodes) > 0:
            update_revision_and_manager(
                input_config_filename=config, output_config_filename=config
            )
            broadcast_new_config(config, nodes=active_nodes)
        logger.debug(f'Successfully finished removing node {node}.')
        return response

    @staticmethod
    def set_mode(
        mode: str, timeout: int = 60, config: str = DEFAULT_MCS_CONF_PATH,
    ) -> dict:
        """Method to set MCS CLuster mode.

        :param mode: cluster mode to set, can be only "readonly" or "readwrite"
        :type mode: str
        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :raises CMAPIBasicError: if no master found in the cluster
        :raises CMAPIBasicError: on exception while starting transaction
        :raises CMAPIBasicError: if transaction start isn't successful
        :raises CMAPIBasicError: on exception while adding node
        :raises CMAPIBasicError: on exception while distributing new config
        :raises CMAPIBasicError: on unsuccessful distibuting config file
        :raises CMAPIBasicError: on exception while committing transaction
        :return: result of adding node
        :rtype: dict
        """
        logger: logging.Logger = logging.getLogger('cmapi_server')
        logger.debug(
            f'Cluster mode set command called. Setting mode to {mode}.'
        )

        response = {'timestamp': str(datetime.now())}
        cmapi_cfg_parser = get_config_parser(CMAPI_CONF_PATH)
        api_key = get_current_key(cmapi_cfg_parser)
        headers = {'x-api-key': api_key}

        master = None
        if len(get_active_nodes(config)) != 0:
            master = get_dbrm_master(config)

        if master is None:
            raise CMAPIBasicError('No master found in the cluster.')
        else:
            master = master['IPAddr']
            payload = {'cluster_mode': mode}
            url = f'https://{master}:8640/cmapi/{get_version()}/node/config'

        nc = NodeConfig()
        root = nc.get_current_config_root(config_filename=config)
        payload['manager'] = root.find('./ClusterManager').text
        payload['revision'] = root.find('./ConfigRevision').text
        payload['timeout'] = timeout
        payload['cluster_mode'] = mode

        try:
            r = get_traced_session().request('PUT', url, headers=headers, json=payload, verify=False)
            r.raise_for_status()
            response['cluster-mode'] = mode
        except Exception as err:
            raise CMAPIBasicError(
                f'Error while setting cluster mode to {mode}'
            ) from err

        logger.debug(f'Successfully set cluster mode to {mode}.')
        return response

    @staticmethod
    def set_api_key(
        api_key: str, verification_key: str,
        config: str = DEFAULT_MCS_CONF_PATH,
    ) -> dict:
        """Method to set API key for each CMAPI node in cluster.

        :param api_key: API key to set
        :type api_key: str
        :param verification_key: TOTP key to verify
        :type verification_key: str
        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :raises CMAPIBasicError: if catch some exception while setting API key
                                 to each node
        :return: status result
        :rtype: dict
        """
        logger: logging.Logger = logging.getLogger('cmapi_server')
        logger.debug('Cluster set API key command called.')

        active_nodes = get_active_nodes(config)
        body = {
            'api_key': api_key,
            'verification_key': verification_key
        }
        response = {}
        # only for changing response object below
        active_nodes_count = len(active_nodes)

        if not active_nodes:
            # set api key in configuration file on this node
            logger.debug(
                'No active nodes found, set API key into current CMAPI conf.'
            )
            active_nodes.append('localhost')

        for node in active_nodes:
            logger.debug(f'Setting new api key to "{node}".')
            url = f'https://{node}:8640/cmapi/{get_version()}/node/apikey-set'
            try:
                resp = get_traced_session().request('PUT', url, verify=False, json=body, headers={})
                resp.raise_for_status()
                r_json = resp.json()
                if active_nodes_count > 0:
                    response[str(node)] = r_json
            except Exception as err:
                raise CMAPIBasicError(
                    f'Got an error setting API key to "{node}".'
                ) from err
            logger.debug(f'Successfully set new api key to "{node}".')

        response['timestamp'] = str(datetime.now())
        logger.debug(
            'Successfully finished setting new API key to all nodes.'
        )
        return response

    @staticmethod
    def set_log_level(
        level: str, config: str = DEFAULT_MCS_CONF_PATH,
        logger: logging.Logger = logging.getLogger('cmapi_server')
    ) -> dict:
        """Method to set level for loggers on each CMAPI node in cluster.

        :param level: logging level, including custom
        :type level: str
        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :param logger: logger, defaults to logging.getLogger('cmapi_server')
        :type logger: logging.Logger, optional
        :return: status result
        :rtype: dict
        """
        logger.debug('Cluster set new logging level called.')

        active_nodes = get_active_nodes(config)
        body = {'level': level}
        response = {}
        # only for changing response object below
        active_nodes_count = len(active_nodes)

        if not active_nodes:
            # set api key in configuration file on this node
            logger.debug(
                'No active nodes found, set log level onluy for current node.'
            )
            active_nodes.append('localhost')

        for node in active_nodes:
            logger.debug(f'Setting new log level to "{node}".')
            url = f'https://{node}:8640/cmapi/{get_version()}/node/log-level'
            try:
                resp = get_traced_session().request('PUT', url, verify=False, json=body, headers={})
                resp.raise_for_status()
                r_json = resp.json()
                if active_nodes_count > 0:
                    response[str(node)] = r_json
            except Exception as err:
                raise CMAPIBasicError(
                    f'Got an error setting log level to "{node}".'
                ) from err
            logger.debug(f'Successfully set new log level to "{node}".')

        response['timestamp'] = str(datetime.now())
        logger.debug(
            'Successfully finished setting new log level to all nodes.'
        )
        return response
