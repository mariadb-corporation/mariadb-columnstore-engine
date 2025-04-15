import logging
import os
import socket
import subprocess
from shutil import copyfile

import requests

from cmapi_server.controllers.dispatcher import _version
from cmapi_server.managers.process import MCSProcessManager
from cmapi_server.test.unittest_global import (
    BaseServerTestCase, MCS_CONFIG_FILEPATH, COPY_MCS_CONFIG_FILEPATH,
    TEST_MCS_CONFIG_FILEPATH,
)


logging.basicConfig(level='DEBUG')
requests.urllib3.disable_warnings()


class BaseClusterTestCase(BaseServerTestCase):

    @classmethod
    def setUpClass(cls) -> None:
        copyfile(MCS_CONFIG_FILEPATH, COPY_MCS_CONFIG_FILEPATH)
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        copyfile(COPY_MCS_CONFIG_FILEPATH, MCS_CONFIG_FILEPATH)
        os.remove(os.path.abspath(COPY_MCS_CONFIG_FILEPATH))
        MCSProcessManager.stop_node(is_primary=True)
        MCSProcessManager.start_node(is_primary=True)
        return super().tearDownClass()

    def setUp(self) -> None:
        copyfile(TEST_MCS_CONFIG_FILEPATH, MCS_CONFIG_FILEPATH)
        MCSProcessManager.stop_node(is_primary=True)
        MCSProcessManager.start_node(is_primary=True)
        return super().setUp()


class ClusterStartTestCase(BaseClusterTestCase):
    URL = f'https://localhost:8640/cmapi/{_version}/cluster/start'

    def test_endpoint_with_no_api_key(self):
        r = requests.put(
            self.URL, verify=False, headers=self.NO_AUTH_HEADERS,
            json={}
        )
        self.assertEqual(r.status_code, 401)

    def test_endpoint_with_no_nodes_in_cluster(self):
        r = requests.put(
            self.URL, verify=False, headers=self.HEADERS,
            json={}
        )
        error = r.json()['error']
        self.assertEqual(r.status_code, 422)
        self.assertEqual(error, 'There are no nodes in the cluster.')

    def test_start_after_adding_a_node(self):
        payload = {'node': socket.gethostname()}
        resp = requests.post(
            ClusterAddNodeTestCase.URL, verify=False, headers=self.HEADERS,
            json=payload
        )
        self.assertEqual(resp.status_code, 200)

        payload = {'node': None}
        resp = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=payload
            )
        self.assertEqual(resp.status_code, 200)

        # test_columnstore_started
        controllernode = subprocess.check_output(['pgrep', 'controllernode'])
        self.assertIsNotNone(controllernode)


class ClusterShutdownTestCase(BaseClusterTestCase):
    URL = f'https://localhost:8640/cmapi/{_version}/cluster/shutdown'

    def test_endpoint_with_no_api_key(self):
        r = requests.put(
            self.URL, verify=False, headers=self.NO_AUTH_HEADERS,
            json={}
        )
        self.assertEqual(r.status_code, 401)

    def test_endpoint_with_no_nodes_in_cluster(self):
        resp = requests.put(self.URL, verify=False, headers=self.HEADERS,
            json={}
        )
        error = resp.json()['error']
        self.assertEqual(resp.status_code, 422)
        self.assertEqual(error, 'There are no nodes in the cluster.')

    def test_add_node_and_shutdown(self):
        payload = {'node': socket.gethostname()}
        resp = requests.post(
            ClusterAddNodeTestCase.URL, verify=False, headers=self.HEADERS,
            json=payload
        )
        self.assertEqual(resp.status_code, 200)

        # note: POST node starts up node
        try:
            controllernode = subprocess.check_output(
                ['pgrep', 'controllernode']
            )
        except Exception as e:
            controllernode = None
        self.assertIsNotNone(controllernode)

        payload = {'timeout': 60}
        resp = requests.put(
            self.URL, verify=False, headers=self.HEADERS,
            json=payload
        )
        self.assertEqual(resp.status_code, 200)

        # Check columnstore stopped
        try:
            controllernode = subprocess.check_output(
                ['pgrep', 'controllernode']
            )
        except Exception as e:
            controllernode = None
        self.assertIsNone(controllernode)


class ClusterModesetTestCase(BaseClusterTestCase):
    URL = f'https://localhost:8640/cmapi/{_version}/cluster/mode-set'

    def test_endpoint_with_no_api_key(self):
        resp = requests.put(
            self.URL, verify=False, headers=self.NO_AUTH_HEADERS,
            json={}
        )
        self.assertEqual(resp.status_code, 401)

    def test_endpoint_with_no_nodes_in_cluster(self):
        resp = requests.put(
            self.URL, verify=False, headers=self.HEADERS,
            json={}
        )
        error = resp.json()['error']
        self.assertEqual(resp.status_code, 422)
        self.assertEqual(error, 'There are no nodes in the cluster.')

    def test_add_node_and_set_readonly(self):
        payload = {'node': socket.gethostname()}
        resp = requests.post(
            ClusterAddNodeTestCase.URL, verify=False, headers=self.HEADERS,
            json=payload
        )
        self.assertEqual(resp.status_code, 200)

        payload = {'mode': 'readonly'}
        resp = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=payload
        )
        self.assertEqual(resp.status_code, 200)

        # return readwrite mode back
        payload = {'mode': 'readwrite'}
        resp = requests.put(
            self.URL, verify=False, headers=self.HEADERS, json=payload
        )
        self.assertEqual(resp.status_code, 200)

class ClusterAddNodeTestCase(BaseClusterTestCase):
    URL = f'https://localhost:8640/cmapi/{_version}/cluster/node'

    def test_endpoint_with_no_apikey(self):
        resp = requests.post(
            self.URL, verify=False, headers=self.NO_AUTH_HEADERS,
            json={}
        )
        self.assertEqual(resp.status_code, 401)

    def test_endpoint_with_missing_node_parameter(self):
        resp = requests.put(
            self.URL, verify=False, headers=self.HEADERS,
            json={}
        )
        error = resp.json()['error']
        self.assertEqual(resp.status_code, 422)
        self.assertEqual(error, 'missing node argument')

    def test_endpoint(self):
        payload = {'node': socket.gethostname()}
        resp = requests.put(
            self.URL, verify=False, headers=self.HEADERS,
            json=payload
        )
        self.assertEqual(resp.status_code, 200)

        # Check Columntore started
        controllernode = subprocess.check_output(
            ['pgrep', 'controllernode'])
        self.assertIsNotNone(controllernode)


class ClusterRemoveNodeTestCase(BaseClusterTestCase):
    URL = ClusterAddNodeTestCase.URL

    def test_endpoint_with_no_apikey(self):
        resp = requests.delete(
            self.URL, verify=False, headers=self.NO_AUTH_HEADERS,
            json={}
        )
        self.assertEqual(resp.status_code, 401)

    def test_endpoint_with_missing_node_parameter(self):
        resp = requests.delete(
            self.URL, verify=False, headers=self.HEADERS,
            json={}
        )
        error = resp.json()['error']
        self.assertEqual(resp.status_code, 422)
        self.assertEqual(error, 'missing node argument')

    def test_add_node_and_remove(self):
        payload = {'node': socket.gethostname()}
        resp = requests.post(
            ClusterAddNodeTestCase.URL, verify=False, headers=self.HEADERS,
            json=payload
        )
        self.assertEqual(resp.status_code, 200)

        resp = requests.delete(
            self.URL, verify=False, headers=self.HEADERS, json=payload
        )
        self.assertEqual(resp.status_code, 200)
