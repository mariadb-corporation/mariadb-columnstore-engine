"""Module contains Cluster business logic functions."""
import logging
from datetime import datetime

import requests

from cmapi_server.constants import (
    CMAPI_CONF_PATH, DEFAULT_MCS_CONF_PATH,
)
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.helpers import (
    broadcast_new_config, commit_transaction, get_active_nodes, get_dbroots,
    get_config_parser, get_current_key, get_id, get_version, start_transaction,
    rollback_transaction, update_revision_and_manager,
)
from cmapi_server.node_manipulation import (
    add_node, add_dbroot, remove_node, switch_node_maintenance,
)
from mcs_node_control.models.misc import get_dbrm_master
from mcs_node_control.models.node_config import NodeConfig


class ClusterHandler():
    """Class for handling MCS Cluster operations."""

    @staticmethod
    def status(
        config: str = DEFAULT_MCS_CONF_PATH,
        logger: logging.Logger = logging.getLogger('cmapi_server')
    ) -> dict:
        """Method to get MCS Cluster status information

        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :param logger: logger, defaults to logging.getLogger('cmapi_server')
        :type logger: logging.Logger, optional
        :raises CMAPIBasicError: if catch some exception while getting status
                                 from each node separately
        :return: status result
        :rtype: dict
        """
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
                r = requests.get(url, verify=False, headers=headers)
                r.raise_for_status()
                r_json = r.json()
                if len(r_json.get('services', 0)) == 0:
                    r_json['dbrm_mode'] = 'offline'

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
    def start(
        config: str = DEFAULT_MCS_CONF_PATH,
        logger: logging.Logger = logging.getLogger('cmapi_server')
    ) -> dict:
        """Method to start MCS Cluster.

        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :param logger: logger, defaults to logging.getLogger('cmapi_server')
        :type logger: logging.Logger, optional
        :raises CMAPIBasicError: on exception while starting transaction
        :raises CMAPIBasicError: if transaction start isn't successful
        :raises CMAPIBasicError: if no nodes in the cluster
        :raises CMAPIBasicError: on exception while distributing new config
        :raises CMAPIBasicError: on unsuccessful distibuting config file
        :raises CMAPIBasicError: on exception while committing transaction
        :return: start timestamp
        :rtype: dict
        """
        logger.debug('Cluster start command called. Starting the cluster.')
        start_time = str(datetime.now())
        transaction_id = get_id()

        try:
            suceeded, transaction_id, successes = start_transaction(
                cs_config_filename=config, id=transaction_id
            )
        except Exception as err:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError(
                'Error while starting the transaction.'
            ) from err
        if not suceeded:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError('Starting transaction isn\'t successful.')

        if suceeded and len(successes) == 0:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError('There are no nodes in the cluster.')

        switch_node_maintenance(False)
        update_revision_and_manager()

        # TODO: move this from multiple places to one, eg to helpers
        try:
            broadcast_successful = broadcast_new_config(config)
        except Exception as err:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError(
                'Error while distributing config file.'
            ) from err

        if not broadcast_successful:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError('Config distribution isn\'t successful.')

        try:
            commit_transaction(transaction_id, cs_config_filename=config)
        except Exception as err:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError(
                'Error while committing transaction.'
            ) from err

        logger.debug('Successfully finished cluster start.')
        return {'timestamp': start_time}

    @staticmethod
    def shutdown(
        config: str = DEFAULT_MCS_CONF_PATH,
        logger: logging.Logger = logging.getLogger('cmapi_server')
    ) -> dict:
        """Method to stop the MCS Cluster.

        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :param logger: logger, defaults to logging.getLogger('cmapi_server')
        :type logger: logging.Logger, optional
        :raises CMAPIBasicError: if no nodes in the cluster
        :return: start timestamp
        :rtype: dict
        """
        logger.debug(
            'Cluster shutdown command called. Shutting down the cluster.'
        )

        start_time = str(datetime.now())
        transaction_id = get_id()

        try:
            suceeded, transaction_id, successes = start_transaction(
                cs_config_filename=config, id=transaction_id
            )
        except Exception as err:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError(
                'Error while starting the transaction.'
            ) from err
        if not suceeded:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError('Starting transaction isn\'t successful.')

        if suceeded and len(successes) == 0:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError('There are no nodes in the cluster.')

        switch_node_maintenance(True)
        update_revision_and_manager()

        # TODO: move this from multiple places to one, eg to helpers
        try:
            broadcast_successful = broadcast_new_config(config)
        except Exception as err:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError(
                'Error while distributing config file.'
            ) from err

        if not broadcast_successful:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError('Config distribution isn\'t successful.')

        try:
            commit_transaction(transaction_id, cs_config_filename=config)
        except Exception as err:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError(
                'Error while committing transaction.'
            ) from err

        logger.debug('Successfully finished shutting down the cluster.')
        return {'timestamp': start_time}

    @staticmethod
    def add_node(
        node: str, config: str = DEFAULT_MCS_CONF_PATH,
        logger: logging.Logger = logging.getLogger('cmapi_server')
    ) -> dict:
        """Method to add node to MCS CLuster.

        :param node: node IP or name or FQDN
        :type node: str
        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :param logger: logger, defaults to logging.getLogger('cmapi_server')
        :type logger: logging.Logger, optional
        :raises CMAPIBasicError: on exception while starting transaction
        :raises CMAPIBasicError: if transaction start isn't successful
        :raises CMAPIBasicError: on exception while adding node
        :raises CMAPIBasicError: on exception while distributing new config
        :raises CMAPIBasicError: on unsuccessful distibuting config file
        :raises CMAPIBasicError: on exception while committing transaction
        :return: result of adding node
        :rtype: dict
        """
        logger.debug(f'Cluster add node command called. Adding node {node}.')

        response = {'timestamp': str(datetime.now())}
        transaction_id = get_id()

        try:
            suceeded, transaction_id, successes = start_transaction(
                cs_config_filename=config, extra_nodes=[node],
                id=transaction_id
            )
        except Exception as err:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError(
                'Error while starting the transaction.'
            ) from err
        if not suceeded:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError('Starting transaction isn\'t successful.')

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
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError('Error while adding node.') from err

        response['node_id'] = node
        update_revision_and_manager(
            input_config_filename=config, output_config_filename=config
        )

        try:
            broadcast_successful = broadcast_new_config(config)
        except Exception as err:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError(
                'Error while distributing config file.'
            ) from err

        if not broadcast_successful:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError('Config distribution isn\'t successful.')

        try:
            commit_transaction(transaction_id, cs_config_filename=config)
        except Exception as err:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError(
                'Error while committing transaction.'
            ) from err

        logger.debug(f'Successfully finished adding node {node}.')
        return response

    @staticmethod
    def remove_node(
        node: str, config: str = DEFAULT_MCS_CONF_PATH,
        logger: logging.Logger = logging.getLogger('cmapi_server')
    ) -> dict:
        """Method to remove node from MCS CLuster.

        :param node: node IP or name or FQDN
        :type node: str
        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :param logger: logger, defaults to logging.getLogger('cmapi_server')
        :type logger: logging.Logger, optional
        :raises CMAPIBasicError: on exception while starting transaction
        :raises CMAPIBasicError: if transaction start isn't successful
        :raises CMAPIBasicError: on exception while removing node
        :raises CMAPIBasicError: on exception while distributing new config
        :raises CMAPIBasicError: on unsuccessful distibuting config file
        :raises CMAPIBasicError: on exception while committing transaction
        :return: result of node removing
        :rtype: dict
        """
        logger.debug(
            f'Cluster remove node command called. Removing node {node}.'
        )
        response = {'timestamp': str(datetime.now())}
        transaction_id = get_id()

        try:
            suceeded, transaction_id, txn_nodes = start_transaction(
                cs_config_filename=config, remove_nodes=[node],
                id=transaction_id
            )
        except Exception as err:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError(
                'Error while starting the transaction.'
            ) from err
        if not suceeded:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError('Starting transaction isn\'t successful.')

        try:
            remove_node(
                node, input_config_filename=config,
                output_config_filename=config
            )
        except Exception as err:
            rollback_transaction(
                transaction_id, nodes=txn_nodes, cs_config_filename=config
            )
            raise CMAPIBasicError('Error while removing node.') from err

        response['node_id'] = node
        if len(txn_nodes) > 0:
            update_revision_and_manager(
                input_config_filename=config, output_config_filename=config
            )
            try:
                broadcast_successful = broadcast_new_config(
                    config, nodes=txn_nodes
                )
            except Exception as err:
                rollback_transaction(
                    transaction_id, nodes=txn_nodes, cs_config_filename=config
                )
                raise CMAPIBasicError(
                    'Error while distributing config file.'
                ) from err
            if not broadcast_successful:
                rollback_transaction(
                    transaction_id, nodes=txn_nodes, cs_config_filename=config
                )
                raise CMAPIBasicError('Config distribution isn\'t successful.')

        try:
            commit_transaction(transaction_id, cs_config_filename=config)
        except Exception as err:
            rollback_transaction(
                transaction_id, nodes=txn_nodes, cs_config_filename=config
            )
            raise CMAPIBasicError(
                'Error while committing transaction.'
            ) from err

        logger.debug(f'Successfully finished removing node {node}.')
        return response

    @staticmethod
    def set_mode(
        mode: str, timeout:int = 60, config: str = DEFAULT_MCS_CONF_PATH,
        logger: logging.Logger = logging.getLogger('cmapi_server')
    ) -> dict:
        """Method to set MCS CLuster mode.

        :param mode: cluster mode to set, can be only "readonly" or "readwrite"
        :type mode: str
        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :param logger: logger, defaults to logging.getLogger('cmapi_server')
        :type logger: logging.Logger, optional
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
        logger.debug(
            f'Cluster mode set command called. Setting mode to {mode}.'
        )

        response = {'timestamp': str(datetime.now())}
        cmapi_cfg_parser = get_config_parser(CMAPI_CONF_PATH)
        api_key = get_current_key(cmapi_cfg_parser)
        headers = {'x-api-key': api_key}
        transaction_id = get_id()

        master = None
        if len(get_active_nodes(config)) != 0:
            master = get_dbrm_master(config)

        if master is None:
            raise CMAPIBasicError('No master found in the cluster.')
        else:
            master = master['IPAddr']
            payload = {'cluster_mode': mode}
            url = f'https://{master}:8640/cmapi/{get_version()}/node/config'

        try:
            suceeded, transaction_id, successes = start_transaction(
                cs_config_filename=config, id=transaction_id
            )
        except Exception as err:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError(
                'Error while starting the transaction.'
            ) from err
        if not suceeded:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError('Starting transaction isn\'t successful.')

        nc = NodeConfig()
        root = nc.get_current_config_root(config_filename=config)
        payload['manager'] = root.find('./ClusterManager').text
        payload['revision'] = root.find('./ConfigRevision').text
        payload['timeout'] = timeout
        payload['cluster_mode'] = mode

        try:
            r = requests.put(url, headers=headers, json=payload, verify=False)
            r.raise_for_status()
            response['cluster-mode'] = mode
        except Exception as err:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError(
                f'Error while setting cluster mode to {mode}'
            ) from err

        try:
            commit_transaction(transaction_id, cs_config_filename=config)
        except Exception as err:
            rollback_transaction(transaction_id, cs_config_filename=config)
            raise CMAPIBasicError(
                'Error while committing transaction.'
            ) from err

        logger.debug(f'Successfully set cluster mode to {mode}.')
        return response

    @staticmethod
    def set_api_key(
        api_key: str, verification_key: str,
        config: str = DEFAULT_MCS_CONF_PATH,
        logger: logging.Logger = logging.getLogger('cmapi_server')
    ) -> dict:
        """Method to set API key for each CMAPI node in cluster.

        :param api_key: API key to set
        :type api_key: str
        :param verification_key: TOTP key to verify
        :type verification_key: str
        :param config: columnstore xml config file path,
                       defaults to DEFAULT_MCS_CONF_PATH
        :type config: str, optional
        :param logger: logger, defaults to logging.getLogger('cmapi_server')
        :type logger: logging.Logger, optional
        :raises CMAPIBasicError: if catch some exception while setting API key
                                 to each node
        :return: status result
        :rtype: dict
        """
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
                resp = requests.put(url, verify=False, json=body)
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
                resp = requests.put(url, verify=False, json=body)
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
