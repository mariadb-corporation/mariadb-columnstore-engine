import configparser
import subprocess
import unittest
from contextlib import contextmanager
from os import path, remove
from pathlib import Path
from shutil import copyfile

import cherrypy
import requests
requests.packages.urllib3.disable_warnings()

from cmapi_server.constants import (
    EM_PATH_SUFFIX, MCS_EM_PATH, MCS_BRM_CURRENT_PATH, S3_BRM_CURRENT_PATH
)
from cmapi_server.controllers.dispatcher import (
    dispatcher, jsonify_error,_version
)
from cmapi_server.test.unittest_global import (
    create_self_signed_certificate, cert_filename, cmapi_config_filename,
    tmp_cmapi_config_filename
)
from mcs_node_control.models.node_config import NodeConfig


@contextmanager
def run_server():
    if not path.exists(cert_filename):
        create_self_signed_certificate()
    cherrypy.engine.start()
    cherrypy.engine.wait(cherrypy.engine.states.STARTED)
    yield
    cherrypy.engine.exit()
    cherrypy.engine.block()


def get_current_key():
    app_config = configparser.ConfigParser()
    try:
        with open(cmapi_config_filename, 'r') as _config_file:
            app_config.read_file(_config_file)
    except FileNotFoundError:
        return ''

    if 'Authentication' not in app_config.sections():
        return ''
    return app_config['Authentication'].get('x-api-key', '')

class TestEMEndpoints(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not path.exists(tmp_cmapi_config_filename):
            f = open(tmp_cmapi_config_filename, 'x')
            f.close()
            copyfile(cmapi_config_filename, tmp_cmapi_config_filename)

    @classmethod
    def tearDownClass(cls):
        if path.exists(tmp_cmapi_config_filename):
            copyfile(tmp_cmapi_config_filename, cmapi_config_filename)
            remove(tmp_cmapi_config_filename)

    def get_examplar_bytes(self, element: str):
        node_config = NodeConfig()
        if node_config.s3_enabled():
            ret = subprocess.run(
                ["smcat", S3_BRM_CURRENT_PATH], stdout=subprocess.PIPE
            )
            element_current_suffix = ret.stdout.decode("utf-8").rstrip()
            element_current_filename = f'{EM_PATH_SUFFIX}/{element_current_suffix}_{element}'
            ret = subprocess.run(
                ["smcat", element_current_filename], stdout=subprocess.PIPE
            )
            result = ret.stdout
        else:
            element_current_name = Path(MCS_BRM_CURRENT_PATH)
            element_current_filename = element_current_name.read_text().rstrip()
            element_current_file = Path(
                f'{MCS_EM_PATH}/{element_current_filename}_{element}'
            )
            result = element_current_file.read_bytes()
        return result

    def test_em(self):
        app = cherrypy.tree.mount(root=None,
                                  config=cmapi_config_filename)
        app.config.update({
            '/': {
                'request.dispatch': dispatcher,
                'error_page.default': jsonify_error,
            },
            'config': {
                'path': cmapi_config_filename,
            },
        })
        cherrypy.config.update(cmapi_config_filename)

        api_key = get_current_key()
        try:
            with run_server():
                url = f"https://localhost:8640/cmapi/{_version}/node/meta/em"
                # Auth failure
                headers = {'x-api-key': None}
                r = requests.get(url, verify=False, headers=headers)
                self.assertEqual(r.status_code, 401)
                # OK
                headers = {'x-api-key': api_key}
                r = requests.get(url, verify=False, headers=headers)
                extent_map = self.get_examplar_bytes('em')
                self.assertEqual(r.status_code, 200)
                self.assertEqual(r.content, extent_map)
        except:
            cherrypy.engine.exit()
            cherrypy.engine.block()
            raise


    def test_journal(self):
        app = cherrypy.tree.mount(root=None,
                                  config=cmapi_config_filename)
        app.config.update({
            '/': {
                'request.dispatch': dispatcher,
                'error_page.default': jsonify_error,
            },
            'config': {
                'path': cmapi_config_filename,
            },
        })
        cherrypy.config.update(cmapi_config_filename)

        api_key = get_current_key()
        try:
            with run_server():
                url = f"https://localhost:8640/cmapi/{_version}/node/meta/journal"
                # Auth failure
                headers = {'x-api-key': None}
                r = requests.get(url, verify=False, headers=headers)
                self.assertEqual(r.status_code, 401)
                # OK
                headers = {'x-api-key': api_key}
                r = requests.get(url, verify=False, headers=headers)
                journal = self.get_examplar_bytes('journal')
                self.assertEqual(r.status_code, 200)
                self.assertEqual(r.content, journal)
        except:
            cherrypy.engine.exit()
            cherrypy.engine.block()
            raise


    def test_vss(self):
        app = cherrypy.tree.mount(root=None,
                                  config=cmapi_config_filename)
        app.config.update({
            '/': {
                'request.dispatch': dispatcher,
                'error_page.default': jsonify_error,
            },
            'config': {
                'path': cmapi_config_filename,
            },
        })
        cherrypy.config.update(cmapi_config_filename)

        api_key = get_current_key()
        try:
            with run_server():
                url = f"https://localhost:8640/cmapi/{_version}/node/meta/vss"
                # Auth failure
                headers = {'x-api-key': None}
                r = requests.get(url, verify=False, headers=headers)
                self.assertEqual(r.status_code, 401)
                # OK
                headers = {'x-api-key': api_key}
                r = requests.get(url, verify=False, headers=headers)
                vss = self.get_examplar_bytes('vss')
                self.assertEqual(r.status_code, 200)
                self.assertEqual(r.content, vss)
        except:
            cherrypy.engine.exit()
            cherrypy.engine.block()
            raise


    def test_vbbm(self):
        app = cherrypy.tree.mount(root=None,
                                  config=cmapi_config_filename)
        app.config.update({
            '/': {
                'request.dispatch': dispatcher,
                'error_page.default': jsonify_error,
            },
            'config': {
                'path': cmapi_config_filename,
            },
        })
        cherrypy.config.update(cmapi_config_filename)

        api_key = get_current_key()
        try:
            with run_server():
                url = f"https://localhost:8640/cmapi/{_version}/node/meta/vbbm"
                # Auth failure
                headers = {'x-api-key': None}
                r = requests.get(url, verify=False, headers=headers)
                self.assertEqual(r.status_code, 401)
                # OK
                headers = {'x-api-key': api_key}
                r = requests.get(url, verify=False, headers=headers)
                vbbm = self.get_examplar_bytes('vbbm')
                self.assertEqual(r.status_code, 200)
                self.assertEqual(r.content, vbbm)
        except:
            cherrypy.engine.exit()
            cherrypy.engine.block()
            raise
