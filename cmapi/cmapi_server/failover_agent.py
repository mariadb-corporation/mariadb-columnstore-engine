'''
This class implements the interface used by the failover module to notify
the cluster of events like node-up / node-down, etc.
'''

import logging
import time

import requests

from cmapi_server import helpers, node_manipulation
from cmapi_server.constants import DEFAULT_MCS_CONF_PATH
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.managers.process import MCSProcessManager
from failover.agent_comm import AgentBase
from mcs_node_control.models.node_config import NodeConfig


# Bug in pylint https://github.com/PyCQA/pylint/issues/4584
requests.packages.urllib3.disable_warnings()  # pylint: disable=no-member
logger = logging.getLogger('failover_agent')


class FailoverAgent(AgentBase):

    def activateNodes(
        self, nodes, input_config_filename=DEFAULT_MCS_CONF_PATH,
        output_config_filename=None, test_mode=False
    ):
        logger.info(f'FA.activateNodes():  activating nodes: {nodes}')
        new_node_count = 0
        for node in nodes:
            try:
                logger.info(f'FA.activateNodes(): adding node {node}')
                node_manipulation.add_node(
                    node, input_config_filename, output_config_filename
                )
                new_node_count += 1
            except Exception:
                logger.error(f'FA.activateNodes(): failed to add node {node}')
                raise
        return new_node_count

    def deactivateNodes(
        self, nodes, input_config_filename=DEFAULT_MCS_CONF_PATH,
        output_config_filename=None, test_mode=False
    ):
        logger.info(f'FA.deactivateNodes():  deactivating nodes: {nodes}')

        removed_node_count = 0
        for node in nodes:
            try:
                logger.info(f'FA.deactivateNodes(): deactivating node {node}')
                node_manipulation.remove_node(
                    node, input_config_filename, output_config_filename,
                    deactivate_only=True, test_mode=test_mode
                )
                removed_node_count += 1
            except Exception as err:
                logger.error(
                    f'FA.deactivateNodes(): failed to deactivate node {node}, '
                    f'got {str(err)}'
                )
                raise
        return removed_node_count


    # the 'hack' parameter is a placeholder.  When run by agent_comm, this function gets a first parameter
    # of ().  When that is the input_config_filename, that's bad.  Need to fix.
    def movePrimaryNode(self, hack, input_config_filename = None, output_config_filename = None, test_mode = False):
        logger.info(f"FA.movePrimaryNode(): moving primary node functionality")

        # to save a little typing in testing
        kwargs = {
            "cs_config_filename": input_config_filename,
            "input_config_filename" : input_config_filename,
            "output_config_filename" : output_config_filename,
            "test_mode" : test_mode
        }

        try:
            node_manipulation.move_primary_node(**kwargs)
        except Exception as e:
            logger.error(f"FA.movePrimaryNode(): failed to move primary node, got {str(e)}")
            raise

    def enterStandbyMode(self, test_mode = False):
        nc = NodeConfig()
        node_name = nc.get_module_net_address(nc.get_current_config_root())
        logger.info(
            f'FA.enterStandbyMode(): shutting down node "{node_name}"'
        )

        # this gets retried by the caller on error
        try:
            # TODO: remove test_mode condition and add mock for testing
            if not test_mode:
                MCSProcessManager.stop_node(is_primary=nc.is_primary_node())
            logger.info(
                'FA.enterStandbyMode(): successfully stopped node.'
            )
        except CMAPIBasicError as err:
            logger.error(
                'FA.enterStandbyMode(): caught error while stopping node.'
                f'{err.message}'
            )


    def raiseAlarm(self, msg):
        logger.critical(msg)


    # The start/commit/rollback transaction fcns use the active list to decide which
    # nodes to send to; when we're adding a node the new node isn't in the active list yet
    # extra_nodes gives us add'l hostnames/addrs to send the transaction to.
    # Likewise for removing a node.  Presumably that node is not reachable, so must be
    # removed from the list to send to.
    def startTransaction(self, extra_nodes = [], remove_nodes = []):
        got_txn = False
        count = 0
        while not got_txn:
            msg = None
            try:
                (got_txn, txn_id, nodes) = helpers.start_transaction(
                    extra_nodes=extra_nodes, remove_nodes=remove_nodes
                )
            except Exception as e:
                got_txn = False
                msg = (
                    f'FA.start_transaction(): attempt #{count+1}, '
                    f'failed to get a transaction, got {str(e)}'
                )

            if not got_txn:
                if msg is None:
                    msg = (
                        f'FA.start_transaction(): attempt #{count+1}, '
                        'failed to get a transaction'
                    )
                if count < 5:
                    logger.warning(msg)
                else:
                    logger.error(msg)
                time.sleep(1)
            count += 1
        logger.info(f'FA.startTransaction(): started transaction {txn_id}')
        return (txn_id, nodes)


    # These shouldn't throw for now
    def commitTransaction(self, txn_id, nodes, **kwargs):
        try:
            helpers.update_revision_and_manager()
            # broadcacting new config invokes node restart
            helpers.broadcast_new_config(nodes=nodes)
            helpers.commit_transaction(txn_id, nodes=nodes)
        except Exception:
            logger.error(
                (
                    'FA.commitTransaction(): failed to commit transaciton '
                    f'{txn_id}'
                ),
                exc_info=True
            )
        else:
            logger.info(
                f'FA.commitTransaction(): committed transaction {txn_id}'
            )


    def rollbackTransaction(self, txn_id, nodes):
        try:
            helpers.rollback_transaction(txn_id, nodes = nodes)
        except Exception:
            logger.error(
                (
                    'FA.rollbackTransaction(): failed to rollback transaction '
                    f'{txn_id}. Got unrecognised error.'
                ),
                exc_info=True
            )
        else:
            logger.info(
                f'FA.rollbackTransaction(): rolled back transaction {txn_id})'
            )
