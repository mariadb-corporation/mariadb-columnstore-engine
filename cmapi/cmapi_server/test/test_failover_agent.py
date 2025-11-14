import logging

from mcs_node_control.models.node_config import NodeConfig

from cmapi_server.failover_agent import FailoverAgent
from cmapi_server.managers.network import NetworkManager
from cmapi_server.node_manipulation import add_node, remove_node
from cmapi_server.test.mock_resolution import simple_resolution_mock, make_local_resolution_builder
from cmapi_server.test.unittest_global import BaseNodeManipTestCase, tmp_mcs_config_filename


logging.basicConfig(level='DEBUG')


class TestFailoverAgent(BaseNodeManipTestCase):
    LOCAL_NODE = 'local.node'
    REMOTE_NODE = 'remote.node'
    LOCAL_IP = '104.17.191.14'
    REMOTE_IP = '203.0.113.5'

    def _activate_add_remove_scenario(self, shared_storage_on: bool, use_rebalance_dbroots: bool):
        """Common flow used by activate-nodes tests with minimal duplication.

        Steps:
        - Optionally toggle shared_storage_on
        - activateNodes(new node)
        - add_node(host)
        - verify expected layout (2 nodes, new node as PMS1)
        - remove_node(new node) with configurable use_rebalance_dbroots
        """
        self.tmp_files = ('./activate0.xml', './activate1.xml')
        # Toggle shared storage as requested and remember to restore it back.
        original_shared_storage = self._set_shared_storage(shared_storage_on)
        try:
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

            # Note: when shared storage is off by default in a single node setup,
            # disabling use_rebalance_dbroots avoids requiring dbroot 1 presence during
            # _move_primary. In the shared storage ON scenario, we enable rebalancing to validate
            # that path as well.
            remove_node(
                self.REMOTE_IP,
                self.tmp_files[1],
                self.tmp_files[1],
                use_rebalance_dbroots=use_rebalance_dbroots,
            )
        finally:
            # Restore original shared storage flag to avoid cross-test side effects
            _ = self._set_shared_storage(original_shared_storage)

    def test_activate_nodes_shared_storage_off(self):
        """Shared storage OFF (default in single-node), no rebalance."""
        self._activate_add_remove_scenario(
            shared_storage_on=False,
            use_rebalance_dbroots=False,
        )

    def test_activate_nodes_shared_storage_on(self):
        """Shared storage ON, enforce rebalance during removal."""
        self._activate_add_remove_scenario(
            shared_storage_on=True,
            use_rebalance_dbroots=True,
        )

    def test_deactivateNodes(self):
        self.tmp_files = (
            './deactivate0.xml','./deactivate1.xml', './deactivate2.xml'
        )
        original_shared_storage = self._set_shared_storage(True)
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

        _ = self._set_shared_storage(original_shared_storage)

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
