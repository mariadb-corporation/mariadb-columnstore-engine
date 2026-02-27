import unittest
from unittest.mock import MagicMock

# pylint: disable=protected-access

from failover.shared_storage_monitor import SharedStorageMonitor
from failover.heartbeat_history import HBHistory


class SharedStorageMonitorTestCase(unittest.TestCase):

    def setUp(self):
        self.hb = HBHistory(tickWindow=10)
        self.monitor = SharedStorageMonitor(check_interval=0, hb_history=self.hb)
        self.monitor._cluster_api_client = MagicMock()

    def test_check_shared_storage_returns_none_when_nodes_unreachable(self):
        self.monitor._cluster_api_client.check_shared_storage.return_value = {
            'shared_storage': False,
            'active_nodes_count': 3,
            'nodes_responses': {
                'node-1': {'success': False, 'error': 'timeout'},
            },
            'nodes_errors': {'node-1': 'timeout'},
        }

        result = self.monitor._check_shared_storage()

        self.assertIsNone(result)
        self.assertEqual({'node-1'}, self.monitor._last_unreachable_nodes)

    def test_check_shared_storage_recovers_after_unreachable_nodes(self):
        # First call reports unreachable nodes
        self.monitor._cluster_api_client.check_shared_storage.return_value = {
            'shared_storage': False,
            'active_nodes_count': 3,
            'nodes_responses': {
                'node-1': {'success': False, 'error': 'timeout'},
            },
            'nodes_errors': {'node-1': 'timeout'},
        }
        first_result = self.monitor._check_shared_storage()
        self.assertIsNone(first_result)
        self.assertEqual({'node-1'}, self.monitor._last_unreachable_nodes)

        # Second call indicates all nodes reachable and shared storage ok
        self.monitor._cluster_api_client.check_shared_storage.return_value = {
            'shared_storage': True,
            'active_nodes_count': 3,
            'nodes_responses': {
                'node-1': {'success': True},
            },
            'nodes_errors': {},
        }
        second_result = self.monitor._check_shared_storage()

        self.assertTrue(second_result)
        self.assertFalse(self.monitor._last_unreachable_nodes)

    def test_skips_nodes_with_recent_good_to_no_transition(self):
        # Instead of relying on environment get_active_nodes, patch the helper
        # to return a deterministic skip list derived from HB signal.
        self.monitor._retrieve_unstable_nodes = MagicMock(return_value=['node-2'])

        # Mock API client to verify payload contains skip_nodes
        self.monitor._cluster_api_client.check_shared_storage.return_value = {
            'shared_storage': True,
            'active_nodes_count': 2,
            'nodes_responses': {
                'node-1': {'success': True},
            },
            'nodes_errors': {},
        }

        _ = self.monitor._check_shared_storage()

        # We expect check_shared_storage to be called with extra payload
        calls = self.monitor._cluster_api_client.check_shared_storage.mock_calls
        # Find latest call with kwargs
        self.assertTrue(calls, 'check_shared_storage was not called')
        last_call = calls[-1]
        # last_call is call(extra={...})
        kwargs = last_call.kwargs
        self.assertIn('extra', kwargs)
        self.assertIn('skip_nodes', kwargs['extra'])
        self.assertEqual(['node-2'], kwargs['extra']['skip_nodes'])


if __name__ == '__main__':
    unittest.main()
