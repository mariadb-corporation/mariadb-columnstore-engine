import logging
import os
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta
from shutil import copyfile
from tempfile import TemporaryDirectory

import cherrypy
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes

from cmapi_server import helpers
from cmapi_server.constants import CMAPI_CONF_PATH
from cmapi_server.controllers.dispatcher import dispatcher, jsonify_error
from cmapi_server.managers.process import MCSProcessManager
from cmapi_server.managers.certificate import CertificateManager


TEST_API_KEY = 'somekey123'
MCS_CONFIG_FILEPATH = '/etc/columnstore/Columnstore.xml'
COPY_MCS_CONFIG_FILEPATH = './cmapi_server/test/original_Columnstore.xml'
TEST_MCS_CONFIG_FILEPATH = './cmapi_server/test/CS-config-test.xml'
# TODO:
#       - rename after fix in all places
#       - fix path to abs
mcs_config_filename = './cmapi_server/test/CS-config-test.xml'
tmp_mcs_config_filename = './cmapi_server/test/tmp.xml'
cmapi_config_filename = './cmapi_server/cmapi_server.conf'
tmp_cmapi_config_filename = './cmapi_server/test/tmp.conf'
# constants for process dispatchers
DDL_SERVICE = 'mcs-ddlproc'
CONTROLLERNODE_SERVICE = 'mcs-controllernode.service'
UNKNOWN_SERVICE = 'unknown_service'
SYSTEMCTL = 'systemctl'


logging.basicConfig(level=logging.DEBUG)


def run_detect_processes():
    cfg_parser = helpers.get_config_parser(CMAPI_CONF_PATH)
    d_name, d_path = helpers.get_dispatcher_name_and_path(cfg_parser)
    MCSProcessManager.detect(d_name, d_path)


@contextmanager
def run_server():
    CertificateManager.create_self_signed_certificate_if_not_exist()

    cherrypy.engine.start()
    cherrypy.engine.wait(cherrypy.engine.states.STARTED)
    run_detect_processes()  #TODO: Move cause slow down each test for 5s
    yield

    cherrypy.engine.exit()
    cherrypy.engine.block()


class BaseServerTestCase(unittest.TestCase):
    HEADERS = {'x-api-key': TEST_API_KEY}
    NO_AUTH_HEADERS = {'x-api-key': None}
    TEST_PARAMS = (
        ('auth ok', HEADERS, 200),
        ('no auth', NO_AUTH_HEADERS, 401)
    )

    def run(self, result=None):
        with TemporaryDirectory() as tmp_dir:
            self.tmp_dir = tmp_dir
            self.cmapi_config_filename = os.path.join(
                tmp_dir, 'tmp_cmapi_config.conf'
            )
            self.mcs_config_filename = os.path.join(
                tmp_dir, 'tmp_mcs_config.xml'
            )
            copyfile(cmapi_config_filename, self.cmapi_config_filename)
            copyfile(TEST_MCS_CONFIG_FILEPATH, self.mcs_config_filename)
            self.app = cherrypy.tree.mount(
                root=None, config=self.cmapi_config_filename
            )
            self.app.config.update({
                '/': {
                    'request.dispatch': dispatcher,
                    'error_page.default': jsonify_error,
                },
                'config': {
                    'path': self.cmapi_config_filename,
                },
                'Authentication' : self.HEADERS
            })
            cherrypy.config.update(self.cmapi_config_filename)

            with run_server():
                return super().run(result=result)


class BaseNodeManipTestCase(unittest.TestCase):
    NEW_NODE_NAME = 'mysql.com'  # something that has a DNS entry everywhere

    def setUp(self):
        self.tmp_files = []
        copyfile(TEST_MCS_CONFIG_FILEPATH, tmp_mcs_config_filename)

    def tearDown(self):
        for tmp_file in self.tmp_files:
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
        if os.path.exists(tmp_mcs_config_filename):
            os.remove(tmp_mcs_config_filename)


class BaseProcessDispatcherCase(unittest.TestCase):
    node_started = None

    @classmethod
    def setUpClass(cls) -> None:
        run_detect_processes()
        cls.node_started = MCSProcessManager.get_running_mcs_procs() != 0
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        if (MCSProcessManager.get_running_mcs_procs() !=0) == cls.node_started:
            return super().tearDownClass()
        if cls.node_started:
            MCSProcessManager.start_node(is_primary=True, use_sudo=False)
        else:
            MCSProcessManager.stop_node(is_primary=True, use_sudo=False)
        return super().tearDownClass()

    def setUp(self) -> None:
        if MCSProcessManager.process_dispatcher.is_service_running(
            CONTROLLERNODE_SERVICE
        ):
            self.controller_node_cmd = 'start'
        else:
            self.controller_node_cmd = 'stop'
        # prevent to get 'start-limit-hit' systemd error, see MCOL-5186
        os.system(f'{SYSTEMCTL} reset-failed')
        return super().setUp()

    def tearDown(self) -> None:
        os.system(
            f'{SYSTEMCTL} {self.controller_node_cmd} {CONTROLLERNODE_SERVICE}'
        )
        return super().tearDown()
