import logging
import time
import threading
from pathlib import Path
from typing import Optional, List

from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.constants import REQUEST_TIMEOUT
from cmapi_server.controllers.api_clients import ClusterControllerClient
from cmapi_server.helpers import broadcast_stateful_config, get_active_nodes
from cmapi_server.managers.application import (
    AppStatefulConfig, StatefulConfigModel, StatefulFlagsModel,
)
from cmapi_server.node_manipulation import get_dbroots_paths
from mcs_node_control.models.node_config import NodeConfig
from .heartbeat_history import HBHistory


class SharedStorageMonitor:

    def __init__(self, check_interval: int = 5, hb_history: Optional[HBHistory] = None):
        self._die = False
        self._logger = logging.getLogger('shared_storage_monitor')
        self._runner = None
        self._node_config = NodeConfig()
        self._cluster_api_client = ClusterControllerClient(request_timeout=REQUEST_TIMEOUT)
        self.check_interval = check_interval
        self.last_check_time = 0
        # Track nodes that were unreachable during the last check to avoid
        # flipping the shared storage flag based on partial visibility.
        self._last_unreachable_nodes: set[str] = set()
        self._hb_history = hb_history

    def __del__(self):
        try:
            if getattr(self, '_runner', None):
                self.stop()
        except Exception:   # pylint: disable=broad-except
            pass

    def start(self):
        self._die = False
        if self._runner and self._runner.is_alive():
            self._logger.debug('Shared storage monitor already running.')
            return
        self._runner = threading.Thread(target=self.monitor, name='SharedStorageMonitor', daemon=True)
        self._runner.start()

    def stop(self):
        self._die = True
        if self._runner and threading.current_thread() is not self._runner:
            self._runner.join(timeout=self.check_interval + 2)
            if self._runner.is_alive():
                self._logger.warning('Shared storage monitor thread did not exit promptly.')

    def monitor(self):
        self._logger.info('Starting shared storage monitor.')
        while not self._die:
            try:
                self._logger.info('Shared storage monitor running check.')
                self._monitor()
            except Exception:  # pylint: disable=broad-except
                self._logger.error('Shared storage monitor caught an exception.', exc_info=True)
            if not self._die:
                self._logger.info(
                    'Shared storage monitor finished check, sleeping '
                    f'{self.check_interval} seconds.'
                )
                time.sleep(self.check_interval)
        self._logger.info('Shared storage monitor exited normally')

    def _retrieve_unstable_nodes(self) -> List[str]:
        """Skip nodes whose latest stable heartbeat sample is NoResponse.

        We only consider the most recent finalized sample (currentTick - lateWindow),
        avoiding partial/in-flight samples. If that value is NoResponse, we
        temporarily exclude the node from the shared-storage check for this cycle.
        """
        if not self._hb_history:
            return []
        try:
            active_nodes = get_active_nodes()
        except Exception:  # pylint: disable=broad-except
            # If we cannot load active nodes, be safe and skip none.
            return []

        unstable_nodes: list[str] = []
        # We only need one stable sample; ask for 1 recent finalized value.
        lookback = 1
        for node in active_nodes:
            # Use GoodResponse as default to avoid over-skipping brand new nodes.
            hist = self._hb_history.getNodeHistory(node, lookback, HBHistory.GoodResponse)
            if not hist:
                continue
            if hist[-1] == HBHistory.NoResponse:
                unstable_nodes.append(node)
        return unstable_nodes

    def _check_shared_storage(self) -> Optional[bool]:
        extra_payload = {}
        # Compute skip list based on recent heartbeat instability (if available)
        skip_nodes = self._retrieve_unstable_nodes()
        if skip_nodes:
            self._logger.debug(
                f'Shared storage check will skip unstable nodes (HB drop): {sorted(skip_nodes)}'
            )
            extra_payload['skip_nodes'] = skip_nodes
        try:
            response = self._cluster_api_client.check_shared_storage(extra=extra_payload)
        except CMAPIBasicError as err:
            self._logger.error(f'Error while calling cluster shared storage check: {err.message}')
            return None
        except Exception:  # pylint: disable=broad-except
            self._logger.error(
                'Unexpected error while calling cluster shared storage check.',
                exc_info=True
            )
            return None
        shared_storage_on = response.get('shared_storage', None)
        if shared_storage_on is None:
            self._logger.error(
                'Shared storage check response does not contain "shared_storage" key.'
            )
            shared_storage_on = False
        active_nodes_count = int(response.get('active_nodes_count', 0))
        if active_nodes_count < 2:
            # we can't reliably detect shared storage state with less than 2 nodes
            # in cluster, so we do not change the flag in this case
            logging.debug(
                'Less than 2 nodes in cluster, no need to change flag of shared storage.'
            )
            return None

        # If some nodes were unreachable during the check, treat the result as
        # inconclusive and do not update the stateful flag. This avoids flipping
        # shared_storage_off when a node is simply down/unreachable.
        nodes_errors = response.get('nodes_errors') or {}
        if nodes_errors:
            self._last_unreachable_nodes = set(nodes_errors.keys())
            self._logger.warning(
                'Shared storage check has unreachable nodes; deferring decision. '
                f'Nodes: {sorted(self._last_unreachable_nodes)}'
            )
            return None

        # No unreachable nodes; clear any previously tracked ones.
        if self._last_unreachable_nodes:
            self._logger.info(
                'Previously unreachable nodes are now reachable; clearing state.'
            )
            self._last_unreachable_nodes.clear()

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
        shared_storage_on_result = None
        if not self._node_config.is_primary_node():
            self._logger.debug('This node is not primary, skipping shared storage check.')
            return
        dbroots_available = self._check_listed_dbroots_exist()
        if not dbroots_available:
            self._logger.info('DBRoots are not available, no need to api check shared storage.')
            shared_storage_on_result = False
        else:
            shared_storage_on_result = self._check_shared_storage()

        current_stateful_config: StatefulConfigModel = AppStatefulConfig.get_config_copy()
        current_shared_storage_on: bool = current_stateful_config.flags.shared_storage_on
        # If result is None, the check was inconclusive (e.g., some nodes unreachable);
        # keep the current flag unchanged and exit.
        if shared_storage_on_result is None:
            self._logger.debug(
                f'Shared storage check inconclusive; Keeping current state: {current_shared_storage_on}'
            )
            return

        if not current_shared_storage_on != bool(shared_storage_on_result):
            self._logger.debug(f'Shared storage state is unchanged: {current_shared_storage_on}')
        else:
            self._logger.info(
                f'Shared storage state changed from {current_shared_storage_on} '
                f'to {bool(shared_storage_on_result)}. Updating stateful config.'
            )
            new_stateful_config = StatefulConfigModel(
                version=current_stateful_config.version.next_seq(),
                flags=StatefulFlagsModel(shared_storage_on=bool(shared_storage_on_result))
            )
            new_stateful_config_dict = new_stateful_config.model_dump(mode='json')
            broadcast_stateful_config(stateful_config_dict=new_stateful_config_dict)
