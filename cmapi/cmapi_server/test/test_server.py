import logging
from datetime import datetime
from pathlib import Path

import requests

from cmapi_server.controllers.dispatcher import _version
from cmapi_server.test.unittest_global import BaseServerTestCase
from mcs_node_control.models.dbrm import DBRM


logging.basicConfig(level='DEBUG')
requests.urllib3.disable_warnings()


class ConfigTestCase(BaseServerTestCase):
    URL = f'https://localhost:8640/cmapi/{_version}/node/config'

    def test_config(self):
        for msg, headers, status_code in self.TEST_PARAMS:
            with self.subTest(
                msg=msg, headers=headers, status_code=status_code
            ):
                r = requests.get(self.URL, verify=False, headers=headers)
                self.assertEqual(r.status_code, status_code)


class StatusTestCase(BaseServerTestCase):
    URL = f'https://localhost:8640/cmapi/{_version}/node/status'

    def test_status(self):
        for msg, headers, status_code in self.TEST_PARAMS:
            with self.subTest(
                msg=msg, headers=headers, status_code=status_code
            ):
                r = requests.get(self.URL, verify=False, headers=headers)
                self.assertEqual(r.status_code, status_code)


class BeginTestCase(BaseServerTestCase):
    URL = f'https://localhost:8640/cmapi/{_version}/node/begin'

    def test_wrong_content_type(self):
        r = requests.put(self.URL, verify=False, headers=self.HEADERS)
        self.assertEqual(r.status_code, 415)

    def test_no_timeout(self):
        body = {'id': 42}
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 422)
        self.assertEqual(r.json(), {'error': 'id or timeout is not set.'})

    def test_no_auth(self):
        body = {'id': 42, 'timeout': 300}
        r = requests.put(
            self.URL, verify=False, headers=self.NO_AUTH_HEADERS, json=body
        )
        self.assertEqual(r.status_code, 401)

    def test_ok(self):
        txn_id_local = 42
        txn_timeout = 300
        txn_timeout_local = 300 + int(datetime.now().timestamp())
        body = {'id': txn_id_local, 'timeout': txn_timeout}
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 200)
        txn_section = self.app.config.get('txn', None)
        self.assertTrue(txn_section is not None)
        txn_id = txn_section.get('id', None)
        txn_timeout = txn_section.get('timeout', None)
        txn_manager_address = txn_section.get('manager_address', None)
        txn_config_changed = txn_section.get('config_changed', None)
        txn = [txn_id, txn_timeout, txn_manager_address, txn_config_changed]
        self.assertTrue(None not in txn)
        self.assertTrue(txn_id == txn_id_local)
        self.assertTrue(txn_timeout - txn_timeout_local <= 2)

    def test_multiple_begin(self):
        txn_id_local = 42
        txn_timeout = 300
        body = {'id': txn_id_local, 'timeout': txn_timeout}
        _ = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 422)
        self.assertEqual(
            r.json(), {'error': 'There is an active operation.'}
        )


class CommitTestCase(BaseServerTestCase):
    URL = f'https://localhost:8640/cmapi/{_version}/node/commit'

    def test_wrong_content_type(self):
        r = requests.put(self.URL, verify=False, headers=self.HEADERS)
        self.assertEqual(r.status_code, 415)

    def test_no_operation(self):
        body = {'id': 42}
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 422)
        self.assertEqual(r.json(), {'error': 'No operation to commit.'})

    def test_begin_and_commit(self):
        txn_timeout = 300
        txn_id = 42
        body = {'id': txn_id, 'timeout': txn_timeout}
        r = requests.put(
            BeginTestCase.URL, verify=False, headers=self.HEADERS, json=body
        )
        txn_section = self.app.config.get('txn', None)
        self.assertTrue(txn_section is not None)
        self.assertEqual(r.status_code, 200)
        body = {'id': 42}
        r = requests.put(self.URL, verify=False, headers=self.HEADERS, json=body)
        self.assertEqual(r.status_code, 200)
        txn_id = txn_section.get('id', None)
        txn_timeout = txn_section.get('timeout', None)
        txn_manager_address = txn_section.get('manager_address', None)
        txn_config_changed = txn_section.get('config_changed', None)
        self.assertTrue(txn_id == 0)
        self.assertEqual(txn_timeout, 0)
        self.assertEqual(txn_manager_address, '')
        self.assertFalse(txn_config_changed)

    def test_multiple_commit(self):
        body = {'id': 42}
        _ = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 422)


class RollbackTestCase(BaseServerTestCase):
    URL = f"https://localhost:8640/cmapi/{_version}/node/rollback"

    def test_wrong_content_type(self):
        r = requests.put(self.URL, verify=False, headers=self.HEADERS)
        self.assertEqual(r.status_code, 415)

    def test_no_operation(self):
        body = {'id': 42}
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 422)
        self.assertEqual(r.json(), {'error': 'No operation to rollback.'})

    def test_begin_and_rollback(self):
        txn_timeout = 300
        txn_id = 42
        body = {'id': txn_id, 'timeout': txn_timeout}
        r = requests.put(
            BeginTestCase.URL, verify=False, headers=self.HEADERS, json=body
        )
        txn_section = self.app.config.get('txn', None)
        self.assertTrue(txn_section is not None)
        self.assertEqual(r.status_code, 200)
        body = {'id': 42}
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 200)
        txn_id = txn_section.get('id', None)
        txn_timeout = txn_section.get('timeout', None)
        txn_manager_address = txn_section.get('manager_address', None)
        txn_config_changed = txn_section.get('config_changed', None)
        self.assertTrue(txn_id == 0)
        self.assertEqual(txn_timeout, 0)
        self.assertEqual(txn_manager_address, '')
        self.assertFalse(txn_config_changed)

    def test_no_operation_again(self):
        body = {'id': 42}
        _ = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 422)


class ConfigPutTestCase(BaseServerTestCase):
    URL = ConfigTestCase.URL

    def setUp(self):
        if 'skip_setUp' not in self.shortDescription():
            body = {'id': 42, 'timeout': 42}
            _ = requests.put(
                BeginTestCase.URL,
                verify=False, headers=self.HEADERS, json=body
            )
        return super().setUp()

    def tearDown(self):
        body = {'id': 42}
        _ = requests.put(
            RollbackTestCase.URL, verify=False, headers=self.HEADERS, json=body
        )
        return super().tearDownClass()

    def test_wrong_content_type(self):
        """Test wrong Content-Type."""
        r = requests.put(self.URL, verify=False, headers=self.HEADERS)
        self.assertEqual(r.status_code, 415)

    def test_no_active_operation(self):
        """Test no active operation. skip_setUp"""
        body = {
            'revision': 42,
            'manager': '1.1.1.1',
            'timeout': 42,
            'config': "<Columnstore>...</Columnstore>",
            'mcs_config_filename': self.mcs_config_filename
        }

        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 422)
        self.assertEqual(
            r.json(), {'error': 'PUT /config called outside of an operation.'}
        )

    def test_no_mandatory_attributes(self):
        """Test no mandatory attributes. skip_setUp"""
        body = {'id': 42, 'timeout': 42}
        r = requests.put(
            BeginTestCase.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 200)
        body = {
            'revision': 42,
            'timeout': 42,
            'config': "<Columnstore>...</Columnstore>",
            'mcs_config_filename': self.mcs_config_filename
        }
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 422)
        self.assertEqual(
            r.json(), {'error': 'Mandatory attribute is missing.'}
        )
        body = {
            'manager': '1.1.1.1',
            'revision': 42,
            'config': "<Columnstore>...</Columnstore>",
            'mcs_config_filename': self.mcs_config_filename
        }
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 422)
        self.assertEqual(
            r.json(), {'error': 'Mandatory attribute is missing.'}
        )
        body = {
            'manager': '1.1.1.1',
            'revision': 42,
            'timeout': 42,
            'mcs_config_filename': self.mcs_config_filename
        }
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 422)
        self.assertEqual(
            r.json(), {'error': 'Mandatory attribute is missing.'}
        )

    def test_no_auth(self):
        """Test no auth."""
        body = {
            'revision': 42,
            'manager': '1.1.1.1',
            'timeout': 42,
            'config': "<Columnstore>...</Columnstore>",
            'mcs_config_filename': self.mcs_config_filename
        }
        r = requests.put(
            self.URL, verify=False, headers=self.NO_AUTH_HEADERS, json=body
        )
        self.assertEqual(r.status_code, 401)

    def test_send_rollback(self):
        """Test send rollback."""
        body = {'id': 42}
        r = requests.put(
            RollbackTestCase.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 200)

    def test_wrong_cluster_mode(self):
        """Test wrong cluster mode."""
        body = {
            'revision': 42,
            'manager': '1.1.1.1',
            'timeout': 42,
            'cluster_mode': 'somemode',
            'mcs_config_filename': self.mcs_config_filename
        }
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 422)
        self.assertTrue(
            "Error occured setting cluster" in r.content.decode('ASCII')
        )

    def test_set_mode(self):
        """Test set mode."""
        mode = 'readwrite'
        body = {
            'revision': 42,
            'manager': '1.1.1.1',
            'timeout': 42,
            'cluster_mode': mode,
            'mcs_config_filename': self.mcs_config_filename
        }
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        # DBRM controller must be up and running
        self.assertEqual(r.status_code, 200)
        r = requests.get(
            StatusTestCase.URL, verify=False, headers=self.HEADERS
        )
        self.assertEqual(r.status_code, 200)

        fake_mode = mode
        with DBRM() as dbrm:
            if dbrm.get_dbrm_status() != 'master':
                fake_mode = 'readonly'
            self.assertEqual(r.json()['cluster_mode'], fake_mode)
            self.assertEqual(dbrm._get_cluster_mode(), mode)

    def test_apply_config(self):
        """Test apply config."""
        body = {'id': 42, 'timeout': 42}
        _ = requests.put(
            BeginTestCase.URL,
            verify=False, headers=self.HEADERS, json=body
        )
        config_file = Path(self.mcs_config_filename)
        config = config_file.read_text()
        body = {
            'revision': 42,
            'manager': '1.1.1.1',
            'timeout': 15,
            'config': config,
            'mcs_config_filename': self.mcs_config_filename
        }
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=body
        )
        self.assertEqual(r.status_code, 200)
        txn_section = self.app.config.get('txn', None)
        self.assertTrue(txn_section is not None)
        txn_config_changed = txn_section.get('config_changed', None)
        self.assertEqual(txn_config_changed, True)
        r = requests.get(
            ConfigTestCase.URL, verify=False, headers=self.HEADERS
        )
        self.assertEqual(r.status_code, 200)
        # commenting this out until we get global config
        # self.assertEqual(r.json()['config'], config)


class PrimaryTestCase(BaseServerTestCase):
    URL = f'https://localhost:8640/cmapi/{_version}/node/primary'

    def test_is_primary(self):
        r = requests.get(self.URL, verify=False)
        self.assertEqual(r.status_code, 200)
