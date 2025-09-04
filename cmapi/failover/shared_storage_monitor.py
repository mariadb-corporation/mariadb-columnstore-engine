import logging
import time
import threading
from pathlib import Path

from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.constants import REQUEST_TIMEOUT
from cmapi_server.controllers.api_clients import ClusterControllerClient
from cmapi_server.helpers import broadcast_stateful_config
from cmapi_server.managers.application import (
    AppStatefulConfig, StatefulConfigModel, StatefulFlagsModel, StatefulVersionModel,
)
from cmapi_server.node_manipulation import get_dbroots_paths
from mcs_node_control.models.node_config import NodeConfig


class SharedStorageMonitor:

    def __init__(self, check_interval: int = 300):
        self._die = False
        self._logger = logging.getLogger('shared_storage_monitor')
        self._runner = None
        self._node_config = NodeConfig()
        self._cluster_api_client = ClusterControllerClient(request_timeout=REQUEST_TIMEOUT)
        self.check_interval = check_interval
        self.last_check_time = 0

    def __del__(self):
        self.stop()

    def start(self):
        self._die = False
        self._runner = threading.Thread(target=self.monitor, name='SharedStorageMonitor')
        self._runner.start()

    def stop(self):
        self._die = True
        self._runner.join()

    def monitor(self):
        self._logger.info('Starting shared storage monitor.')
        while not self._die:
            try:
                self._logger.info('Shared storage monitor running check.')
                self._monitor()
            except Exception:
                self._logger.error('Shared storage monitor caught an exception.', exc_info=True)
            if not self._die:
                self._logger.info(
                    'Shared storage monitor finished check, sleeping '
                    f'{self.check_interval} seconds.'
                )
                time.sleep(self.check_interval)
        self._logger.info('Shared storage monitor exited normally')

    def _check_shared_storage(self) -> bool:
        try:
            response = self._cluster_api_client.check_shared_storage()
        except CMAPIBasicError as err:
            self._logger.error(f'Error while calling cluster shared storage check: {err.message}')
            return False
        except Exception:
            self._logger.error(
                'Unexpected error while calling cluster shared storage check.',
                exc_info=True
            )
            return False
        shared_storage_on = response.get('shared_storage', None)
        if shared_storage_on is None:
            self._logger.error(
                'Shared storage check response does not contain "shared_storage" key.'
            )
            shared_storage_on = False
        active_nodes_count = int(response.get('active_nodes_count', 0))
        if active_nodes_count < 2:
            logging.debug(
                'Less than 2 nodes in cluster, no need to change flag of shared storage.'
            )
            return False
        else:
            return shared_storage_on

    def _check_listed_dbroots_exist(self):
        c_root = self._node_config.get_current_config_root()
        dbroots = get_dbroots_paths(c_root)
        if not dbroots:
            self._logger.error('No DBRoots found, cannot check shared storage.')
            return False
        for dbroot in dbroots:
            if not Path(dbroot).exists():
                self._logger.error(f'DBRoot {dbroot} listed in xml config does not exist.')
                return False
        return True

    def _monitor(self):
        dbroots_available: bool = False
        shared_storage_on: bool = False
        if not self._node_config.is_primary_node():
            self._logger.debug('This node is not primary, skipping shared storage check.')
            return
        dbroots_available = self._check_listed_dbroots_exist()
        if not dbroots_available:
            self._logger.info('DBRoots are not available, no need to api check shared storage.')
            shared_storage_on = False
        else:
            shared_storage_on = self._check_shared_storage()

        current_stateful_config: StatefulConfigModel = AppStatefulConfig.get_config_copy()
        current_shared_storage_on: bool = current_stateful_config.flags.shared_storage_on
        if not current_shared_storage_on != shared_storage_on:
            self._logger.debug(f'Shared storage state is unchanged: {current_shared_storage_on}')
        else:
            self._logger.info(
                f'Shared storage state changed from {current_shared_storage_on} '
                f'to {shared_storage_on}. Updating stateful config.'
            )
            new_stateful_config = StatefulConfigModel(
                version=current_stateful_config.version.next_seq(),
                flags=StatefulFlagsModel(shared_storage_on=shared_storage_on)
            )
            new_stateful_config_dict = new_stateful_config.model_dump(mode='json')
            broadcast_stateful_config(stateful_config_dict=new_stateful_config_dict)
