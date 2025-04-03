import cherrypy
import unittest
import os
import socket
from shutil import copyfile
from contextlib import contextmanager

from cmapi_server import helpers, node_manipulation
from mcs_node_control.models.node_config import NodeConfig
from cmapi_server.controllers.dispatcher import dispatcher, jsonify_error
from cmapi_server.managers.certificate import CertificateManager
from cmapi_server.test.unittest_global import (
    mcs_config_filename, cmapi_config_filename, tmp_mcs_config_filename,
    tmp_cmapi_config_filename,
)


@contextmanager
def start_server():
    CertificateManager.create_self_signed_certificate_if_not_exist()

    app = cherrypy.tree.mount(root = None, config = cmapi_config_filename)
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

    cherrypy.engine.start()
    cherrypy.engine.wait(cherrypy.engine.states.STARTED)
    yield

    cherrypy.engine.exit()
    cherrypy.engine.block()

class TestTransactions(unittest.TestCase):
    def setUp(self):
        if not os.path.exists(tmp_mcs_config_filename):
            f = open(tmp_mcs_config_filename, 'x')
            f.close()
        copyfile(mcs_config_filename, tmp_mcs_config_filename)

    def tearDown(self):
        if os.path.exists(tmp_mcs_config_filename):
            copyfile(tmp_mcs_config_filename, mcs_config_filename)
            os.remove(tmp_mcs_config_filename)

    @classmethod
    def setUpClass(cls):
        if not os.path.exists(tmp_cmapi_config_filename):
            f = open(tmp_cmapi_config_filename, 'x')
            f.close()
            copyfile(cmapi_config_filename, tmp_cmapi_config_filename)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(tmp_cmapi_config_filename):
            copyfile(tmp_cmapi_config_filename, cmapi_config_filename)
            os.remove(tmp_cmapi_config_filename)

    def test_start_commit(self):
        print(" ******** Running TestTransactions.test_start_commit()")
        with start_server():
            try:
                hostname = socket.gethostname()
                myaddr = socket.gethostbyname(hostname)
                node_manipulation.add_node(
                    myaddr, mcs_config_filename, mcs_config_filename
                )
                result = helpers.start_transaction(
                    cmapi_config_filename, mcs_config_filename,
                    optional_nodes = [myaddr]
                )
                self.assertTrue(result[0])
                self.assertEqual(len(result[2]), 1)
                self.assertEqual(result[2][0], myaddr)
                helpers.commit_transaction(result[1], cmapi_config_filename, mcs_config_filename, nodes = result[2])
            except:
                cherrypy.engine.exit()
                cherrypy.engine.block()
                raise

    def test_start_rollback(self):
        print(" ******** Running TestTransactions.test_start_rollback()")
        with start_server():
            try:
                hostname = socket.gethostname()
                myaddr = socket.gethostbyname(hostname)
                node_manipulation.add_node(
                    myaddr, mcs_config_filename, mcs_config_filename
                )
                result = helpers.start_transaction(
                    cmapi_config_filename, mcs_config_filename,
                    optional_nodes = [myaddr]
                )
                self.assertTrue(result[0])
                self.assertEqual(len(result[2]), 1)
                self.assertEqual(result[2][0], myaddr)
                helpers.rollback_transaction(result[1], cmapi_config_filename, mcs_config_filename)    # not specifying nodes here to exercise the nodes = None path
            except:
                cherrypy.engine.exit()
                cherrypy.engine.block()
                raise

    def test_broadcast_new_config(self):
        print(" ******** Running TestTransactions.test_broadcast_new_config()")
        with start_server():
            try:
                myaddr = socket.gethostbyname(socket.gethostname())
                node_manipulation.add_node(myaddr, mcs_config_filename, mcs_config_filename)

                # Note, 1.2.3.4 is intentional -> doesn't exist, so shouldn't end up in the node list returned
                print(
                    "\n\nNOTE!  This is expected to pause here for ~10s, "
                    "this isn't an error, yet.\n"
                )
                result = helpers.start_transaction(
                    cmapi_config_filename, mcs_config_filename,
                    optional_nodes = ['1.2.3.4']
                )
                self.assertTrue(result[0])
                self.assertEqual(len(result[2]), 1)
                self.assertEqual(result[2][0], myaddr)
                try:
                    helpers.broadcast_new_config(
                        mcs_config_filename,
                        cmapi_config_filename=cmapi_config_filename,
                        test_mode=True,
                        nodes = result[2]
                    )
                    # not specifying nodes here to exercise the
                    # nodes = None path
                    helpers.commit_transaction(
                        result[1], cmapi_config_filename, mcs_config_filename
                    )
                except Exception as exc:
                    self.fail(
                        'Unexpected broadcast_new_config failure with error: '
                        f'{exc}'
                    )
                    raise
            except:
                cherrypy.engine.exit()
                cherrypy.engine.block()
                raise

    def test_update_rev_and_manager(self):
        print(" ******** Running TestTransactions.test_update_rev_and_manager()")
        with start_server():
            try:

                myaddr = socket.gethostbyname(socket.gethostname())
                node_manipulation.add_node(
                    myaddr, mcs_config_filename, mcs_config_filename
                )
                helpers.update_revision_and_manager(mcs_config_filename, "./update_rev1.xml")
                nc = NodeConfig()
                root = nc.get_current_config_root("./update_rev1.xml")
                self.assertEqual(root.find("./ConfigRevision").text, "2")
                self.assertEqual(root.find("./ClusterManager").text, socket.gethostbyname(socket.gethostname()))
            except:
                cherrypy.engine.exit()
                cherrypy.engine.block()
                raise

        os.remove("./update_rev1.xml")
