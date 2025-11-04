import json
from contextlib import ExitStack
from unittest.mock import Mock, patch

import requests
from lxml import etree

from cmapi_server.constants import _version
from cmapi_server.test.unittest_global import TEST_API_KEY, BaseServerTestCase

requests.packages.urllib3.disable_warnings()


class TestCmapiConfigEndpoint(BaseServerTestCase):
    def setUp(self):
        super().setUp()
        self._stack = ExitStack()
        self.addCleanup(self._stack.close)

        # Mock broadcast_new_config, TransactionManager and config path
        self.broadcast_mock = Mock()
        self._stack.enter_context(patch('cmapi_server.helpers.broadcast_new_config', new=self.broadcast_mock))
        self._stack.enter_context(patch('cmapi_server.controllers.endpoints.DEFAULT_MCS_CONF_PATH', new=self.mcs_config_filename))
        self._stack.enter_context(patch('cmapi_server.constants.DEFAULT_MCS_CONF_PATH', new=self.mcs_config_filename))

        class _FakeTxn:
            def __enter__(self):
                self.success_txn_nodes = ['n1', 'n2']
                return self

            def __exit__(self, *a):
                return False

        self._stack.enter_context(patch('cmapi_server.controllers.endpoints.TransactionManager', new=lambda: _FakeTxn()))

    def test_sampling_interval_written(self):
        r = self._request_patch(
            {
                'failover_sampling_interval_seconds': 45,
            }
        )
        self.assertEqual(r.status_code, 200)

        tree = etree.parse(str(self.mcs_config_filename))
        self.assertEqual(tree.findtext('./CMAPIConfig/FailoverSamplingIntervalSeconds'), '45')

    def test_interval_bounds_rejected(self):
        r = self._request_patch({'failover_sampling_interval_seconds': 0})
        self.assertEqual(r.status_code, 422)
        body = r.json()
        self.assertIn('error', body)

    def test_broadcast_called(self):
        r = self._request_patch({'failover_sampling_interval_seconds': 30})
        self.assertEqual(r.status_code, 200)
        self.broadcast_mock.assert_called_once()
        self.assertEqual(self.broadcast_mock.call_args.kwargs.get('nodes'), ['n1', 'n2'])

    def _request_patch(self, payload: dict):
        url = f'https://localhost:8640/cmapi/{_version}/cmapi_config'
        headers = {'x-api-key': TEST_API_KEY, 'Content-Type': 'application/json'}
        return requests.patch(url, verify=False, headers=headers, data=json.dumps(payload))
