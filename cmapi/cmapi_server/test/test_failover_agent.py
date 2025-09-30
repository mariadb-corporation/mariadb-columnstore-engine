import logging

from mcs_node_control.models.node_config import NodeConfig

from cmapi_server.failover_agent import FailoverAgent
from cmapi_server.node_manipulation import add_node, remove_node
from cmapi_server.test.mock_resolution import simple_resolution_mock, make_local_resolution_builder
from cmapi_server.test.unittest_global import BaseNodeManipTestCase, tmp_mcs_config_filename

logging.basicConfig(level='DEBUG')


class TestFailoverAgent(BaseNodeManipTestCase):
    LOCAL_NODE = 'local.node'
    REMOTE_NODE = 'remote.node'
    LOCAL_IP = '104.17.191.14'
    REMOTE_IP = '203.0.113.5'

    def test_activateNodes(self):
        self.tmp_files = ('./activate0.xml', './activate1.xml')
        fa = FailoverAgent()
        with (
            make_local_resolution_builder()
            .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
            .add_mapping(self.REMOTE_NODE, self.REMOTE_IP, bidirectional=True)
            .build()
        ):
            # First node is added with _replace_localhost
            fa.activateNodes(
                [self.LOCAL_NODE], tmp_mcs_config_filename, self.tmp_files[0],
                test_mode=True
            )
            # Second node doesn't exist in the config skeleton
            add_node(self.REMOTE_IP, self.tmp_files[0], self.tmp_files[1])

        nc = NodeConfig()
        root = nc.get_current_config_root(self.tmp_files[1])
        pm_count = int(root.find('./PrimitiveServers/Count').text)
        self.assertEqual(pm_count, 2)
        node = root.find('./PMS1/IPAddr')
        # After _replace_localhost, PMS1 should use the resolved IP
        self.assertEqual(node.text, self.LOCAL_IP)
        node = root.find('./pm1_WriteEngineServer/IPAddr')
        # After _replace_localhost, WES1 should use the resolved IP
        self.assertEqual(node.text, self.LOCAL_IP)
        node = root.find('./PMS2/IPAddr')
        self.assertEqual(node.text, self.REMOTE_IP)
        node = root.find('./pm2_WriteEngineServer/IPAddr')
        self.assertEqual(node.text, self.REMOTE_IP)
        remove_node(self.REMOTE_IP, self.tmp_files[1], self.tmp_files[1])

    def test_deactivateNodes(self):
        self.tmp_files = (
            './deactivate0.xml','./deactivate1.xml', './deactivate2.xml'
        )
        with (
            make_local_resolution_builder()
            .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
            .add_mapping(self.REMOTE_NODE, self.REMOTE_IP, bidirectional=True)
            .build()
        ):
            fa = FailoverAgent()
            fa.activateNodes(
                [self.LOCAL_NODE], tmp_mcs_config_filename, self.tmp_files[0],
                test_mode=True
            )
            add_node(self.REMOTE_IP, self.tmp_files[0], self.tmp_files[1])
            fa.deactivateNodes(
                [self.LOCAL_NODE], self.tmp_files[1], self.tmp_files[2],
                test_mode=True
            )

        nc = NodeConfig()
        root = nc.get_current_config_root(self.tmp_files[2])
        pm_count = int(root.find('./PrimitiveServers/Count').text)
        self.assertEqual(pm_count, 1)
        node = root.find('./PMS1/IPAddr')
        self.assertEqual(node.text, self.REMOTE_IP)
        # TODO: Fix node_manipulation add_node logic and _replace_localhost
        # node = root.find('./PMS2/IPAddr')
        # self.assertEqual(node, None)
        node = root.find('./pm1_WriteEngineServer/IPAddr')
        # After deactivation, remaining WES should be the remote test IP
        self.assertEqual(node.text, self.REMOTE_IP)
        node = root.find('./pm2_WriteEngineServer/IPAddr')
        self.assertTrue(node is None)
        #node = root.find("./ConfigRevision")
        #self.assertEqual(node.text, "3")

        # make sure there are no traces of mysql.com,
        # or an ip addr that isn't localhost or 127.0.0.1
        all_nodes = root.findall('./')
        for node in all_nodes:
            # No traces of the LOCAL_NODE should remain
            self.assertFalse(node.text == self.LOCAL_NODE)
            if node.tag in ['IPAddr', 'Node']:
                self.assertEqual(node.text, self.REMOTE_IP)

    def test_designatePrimaryNode(self):
        self.tmp_files = (
            './primary-node0.xml', './primary-node1.xml', './primary-node2.xml'
        )
        fa = FailoverAgent()
        with (
            make_local_resolution_builder()
            .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
            .add_mapping(self.REMOTE_NODE, self.REMOTE_IP, bidirectional=True)
            .build()
        ):
            fa.activateNodes(
                [self.LOCAL_NODE], tmp_mcs_config_filename, self.tmp_files[0],
                test_mode=True
            )
            add_node(self.REMOTE_IP, self.tmp_files[0], self.tmp_files[1])
            fa.movePrimaryNode(
                'placeholder', self.tmp_files[1], self.tmp_files[2], test_mode=True
            )

        nc = NodeConfig()
        root = nc.get_current_config_root(self.tmp_files[2])
        pm_count = int(root.find('./PrimitiveServers/Count').text)
        self.assertEqual(pm_count, 2)
        node = root.find('./PMS1/IPAddr')
        self.assertEqual(node.text, self.LOCAL_IP)
        node = root.find('./PMS2/IPAddr')
        self.assertEqual(node.text, self.REMOTE_IP)
        node = root.find('./pm1_WriteEngineServer/IPAddr')
        self.assertEqual(node.text, self.LOCAL_IP)
        node = root.find('./pm2_WriteEngineServer/IPAddr')
        self.assertEqual(node.text, self.REMOTE_IP)

        for tag in ['ExeMgr1', 'DMLProc', 'DDLProc']:
            node = root.find(f'./{tag}/IPAddr')
            self.assertEqual(node.text, self.LOCAL_IP, f'{tag} should be on primary')

        self.assertEqual(self.LOCAL_NODE, root.find('./PrimaryNode').text)


    def test_enterStandbyMode(self):
        fa = FailoverAgent()
        fa.enterStandbyMode(test_mode=True)
