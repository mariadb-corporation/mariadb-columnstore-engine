import unittest
from unittest.mock import MagicMock, patch

from cmapi_server.exceptions import CMAPIBasicError
from mcs_cluster_tool import install_es_helpers as h


class TestIsMissingEndpointError(unittest.TestCase):
    """Tests for install_es_helpers.is_missing_endpoint_error."""

    def test_404_is_missing_endpoint(self):
        exc = CMAPIBasicError(
            'API client got an HTTPError exception in request to '
            'https://node1:8640/... with code 404 and error: Not Found'
        )
        self.assertTrue(h.is_missing_endpoint_error(exc))

    def test_501_is_missing_endpoint(self):
        exc = CMAPIBasicError('... with code 501 and error: Not Implemented')
        self.assertTrue(h.is_missing_endpoint_error(exc))

    def test_422_is_not_missing_endpoint(self):
        exc = CMAPIBasicError('... with code 422 and error: some handler error')
        self.assertFalse(h.is_missing_endpoint_error(exc))

    def test_connection_error_is_not_missing_endpoint(self):
        exc = CMAPIBasicError('API client could not connect to https://node1')
        self.assertFalse(h.is_missing_endpoint_error(exc))


class TestBuildNodeClient(unittest.TestCase):
    """Tests for install_es_helpers.build_node_client."""

    def test_localhost_uses_default_base_url(self):
        with patch.object(h, 'NodeControllerClient') as ncc:
            for name in ('localhost', '127.0.0.1'):
                h.build_node_client(name)
                ncc.assert_called_with(request_timeout=None)

    def test_remote_node_uses_https_base_url(self):
        with patch.object(h, 'NodeControllerClient') as ncc:
            h.build_node_client('node2', request_timeout=5)
            _, kwargs = ncc.call_args
            self.assertEqual(kwargs['request_timeout'], 5)
            self.assertTrue(kwargs['base_url'].startswith('https://node2:'))


class TestRunNodeStepOnCluster(unittest.TestCase):
    """Tests for install_es_helpers.run_node_step_on_cluster."""

    def setUp(self):
        self.progress = MagicMock()

    def test_all_nodes_succeed(self):
        with patch.object(h, 'build_node_client') as bnc:
            client = MagicMock()
            client.preupgrade_backup.return_value = {'ok': True}
            bnc.return_value = client
            results, errors = h.run_node_step_on_cluster(
                ['n1', 'n2'], 'preupgrade_backup', 'Backup',
                self.progress, 'tid',
            )
        self.assertEqual(set(results), {'n1', 'n2'})
        self.assertEqual(errors, {})

    def test_fail_fast_stops_on_first_error(self):
        with patch.object(h, 'build_node_client') as bnc:
            client = MagicMock()
            client.preupgrade_backup.side_effect = [
                {'ok': True}, CMAPIBasicError('... with code 404 ...'),
            ]
            bnc.return_value = client
            results, errors = h.run_node_step_on_cluster(
                ['n1', 'n2', 'n3'], 'preupgrade_backup', 'Backup',
                self.progress, 'tid',
            )
        self.assertEqual(list(results), ['n1'])
        self.assertEqual(list(errors), ['n2'])
        # third node must not have been touched
        self.assertNotIn('n3', results)
        self.assertNotIn('n3', errors)

    def test_no_fail_fast_runs_all_nodes(self):
        with patch.object(h, 'build_node_client') as bnc:
            client = MagicMock()
            client.preupgrade_backup.side_effect = [
                CMAPIBasicError('boom'), {'ok': True},
            ]
            bnc.return_value = client
            results, errors = h.run_node_step_on_cluster(
                ['n1', 'n2'], 'preupgrade_backup', 'Backup',
                self.progress, 'tid', fail_fast=False,
            )
        self.assertEqual(list(errors), ['n1'])
        self.assertEqual(list(results), ['n2'])

    def test_method_kwargs_are_forwarded(self):
        with patch.object(h, 'build_node_client') as bnc:
            client = MagicMock()
            client.upgrade_mdb_mcs.return_value = {'ok': True}
            bnc.return_value = client
            h.run_node_step_on_cluster(
                ['n1'], 'upgrade_mdb_mcs', 'Upgrading',
                self.progress, 'tid',
                mariadb_version='11.4.10-8', columnstore_version='23.10.5',
            )
            client.upgrade_mdb_mcs.assert_called_once_with(
                mariadb_version='11.4.10-8', columnstore_version='23.10.5',
            )


if __name__ == '__main__':
    unittest.main()
