import logging
import os
import unittest
from pathlib import Path
from shutil import rmtree

from cmapi_server.constants import MCS_MODULE_FILE_PATH
from mcs_node_control.models.node_status import NodeStatus


logging.basicConfig(level='DEBUG')


class NodeStatusTest(unittest.TestCase):
    def test_dbrm_cluster_mode(self):
        node_status = NodeStatus()
        # use subprocess.run to capture stdout
        os.system('/usr/bin/dbrmctl readwrite')
        self.assertEqual(node_status.get_cluster_mode(), 'readwrite')
        os.system('/usr/bin/dbrmctl readonly')
        self.assertEqual(node_status.get_cluster_mode(), 'readonly')
        # kill controllernode and test it

    def test_dbrm_status(self):
        node_status = NodeStatus()
        self.assertEqual(node_status.get_dbrm_status(), 'master')

    def test_dbroots(self):
        try:
            node_status = NodeStatus()
            dbroot_ids = [1, 2, 3]
            path = '/tmp/dbroots/'
            for e in dbroot_ids:
                p = Path(path + 'data' + str(e))
                p.mkdir(parents = True, exist_ok = True)
            for e in node_status.get_dbroots(path=path):
                self.assertEqual(e in dbroot_ids, True)
        except AssertionError as e:
            rmtree(path)
            raise e

    def test_module_id(self):
        node_status = NodeStatus()
        module_file = Path(MCS_MODULE_FILE_PATH)
        examplar_id = int(module_file.read_text()[2:])
        self.assertEqual(examplar_id, node_status.get_module_id())


if __name__ == '__main__':
    unittest.main()
