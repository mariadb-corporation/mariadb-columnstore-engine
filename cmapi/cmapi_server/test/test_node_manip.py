import logging
import socket
import unittest
from unittest.mock import patch

from lxml import etree
from mcs_node_control.models.node_config import NodeConfig

from cmapi_server import node_manipulation
from cmapi_server.constants import MCS_DATA_PATH
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.node_manipulation import (
    add_dbroots_of_other_nodes,
    remove_dbroots_of_node,
    update_dbroots_of_read_replicas,
)
from cmapi_server.test.mock_resolution import (
    MockResolutionBuilder,
    make_local_resolution_builder,
)
from cmapi_server.test.unittest_global import BaseNodeManipTestCase, tmp_mcs_config_filename, single_node_xml

logging.basicConfig(level='DEBUG')

class NodeManipTester(BaseNodeManipTestCase):
    LOCAL_NODE = 'local.node'
    REMOTE_NODE = 'remote.node'
    LOCAL_IP = '104.17.191.14'
    REMOTE_IP = '203.0.113.5'

    def test_add_remove_node(self):
        self.tmp_files = (
            './test-output0.xml', './test-output1.xml', './test-output2.xml'
        )

        with (
            make_local_resolution_builder()
            .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
            .add_mapping(self.REMOTE_NODE, self.REMOTE_IP, bidirectional=True)
            .build()
        ), patch(
            'cmapi_server.node_manipulation.update_dbroots_of_read_replicas'
        ) as mock_update_dbroots_of_read_replicas:
            node_manipulation.add_node(
                self.LOCAL_NODE, tmp_mcs_config_filename, self.tmp_files[0]
            )
            mock_update_dbroots_of_read_replicas.assert_not_called()

            node_manipulation.add_node(
                self.REMOTE_IP, self.tmp_files[0], self.tmp_files[1]
            )
            # No read replicas present
            mock_update_dbroots_of_read_replicas.assert_not_called()

        # get a NodeConfig, read test.xml
        # look for some of the expected changes.
        # Total verification will take too long to code up right now.
        nc = NodeConfig()
        root = nc.get_current_config_root(self.tmp_files[1])
        pms_node_ipaddr = root.find('./PMS1/IPAddr')
        self.assertEqual(pms_node_ipaddr.text, self.LOCAL_IP)
        pms_node_ipaddr = root.find('./PMS2/IPAddr')
        self.assertEqual(pms_node_ipaddr.text, self.REMOTE_IP)
        node = root.find('./ExeMgr2/IPAddr')
        self.assertEqual(node.text, self.REMOTE_IP)

        with (
            make_local_resolution_builder()
            .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
            .add_mapping(self.REMOTE_NODE, self.REMOTE_IP, bidirectional=True)
            .build()
        ), patch(
            'cmapi_server.node_manipulation.update_dbroots_of_read_replicas'
        ) as mock_update_dbroots_of_read_replicas:
            node_manipulation.remove_node(
                self.LOCAL_NODE, self.tmp_files[1], self.tmp_files[2], test_mode=True
            )
            # No read replicas present
            mock_update_dbroots_of_read_replicas.assert_not_called()

        nc = NodeConfig()
        root = nc.get_current_config_root(self.tmp_files[2])
        node = root.find('./PMS1/IPAddr')
        self.assertEqual(node.text, self.REMOTE_IP)
        # TODO: Fix node_manipulation add_node logic and _replace_localhost
        # node = root.find('./PMS2/IPAddr')
        # self.assertEqual(node, None)

    def test_add_remove_read_replica_node(self):
        """add_node(read_replica=True) should add a read replica node into the config,
        it does not add a WriteEngineServer (WES) and does not own dbroots"""
        self.tmp_files = (
            './config_output_rw.xml', './config_output_ro.xml', './config_output_ro_removed.xml'
        )

        # Add this host as a read-write node (local)
        with (
            make_local_resolution_builder()
            .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
            .add_mapping(self.REMOTE_NODE, self.REMOTE_IP, bidirectional=True)
            .build()
        ):
            node_manipulation.add_node(self.LOCAL_NODE, single_node_xml, self.tmp_files[0])

        # Mock _rebalance_dbroots and _move_primary_node (only after the first node is added)
        with (
            make_local_resolution_builder()
            .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
            .add_mapping(self.REMOTE_NODE, self.REMOTE_IP, bidirectional=True)
            .build()
        ), patch(
            'cmapi_server.node_manipulation._rebalance_dbroots'
        ) as mock_rebalance_dbroots, patch(
            'cmapi_server.node_manipulation._move_primary_node'
        ) as mock_move_primary_node, patch(
            'cmapi_server.node_manipulation.update_dbroots_of_read_replicas'
        ) as mock_update_dbroots_of_read_replicas:
            # Add a read replica
            node_manipulation.add_node(
                self.REMOTE_NODE, self.tmp_files[0], self.tmp_files[1], read_replica=True
            )

            nc = NodeConfig()
            root = nc.get_current_config_root(self.tmp_files[1])

            # Check that get_read_replicas infers the read replica correctly
            read_replicas = nc.get_read_replicas(root)
            self.assertEqual(len(read_replicas), 1)
            self.assertEqual(read_replicas[0], self.REMOTE_IP)

            # Check if PMS was added
            pms_node_ipaddr = root.find('./PMS2/IPAddr')
            self.assertEqual(pms_node_ipaddr.text, self.REMOTE_IP)

            # Check that WriteEngineServer was not added
            wes_node = root.find('./pm2_WriteEngineServer')
            self.assertIsNone(wes_node)

            mock_rebalance_dbroots.assert_not_called()
            mock_move_primary_node.assert_not_called()
            # With a read replica added, update should be called
            mock_update_dbroots_of_read_replicas.assert_called_once()
            mock_update_dbroots_of_read_replicas.reset_mock()

            # Test read replica removal
            with (
                make_local_resolution_builder()
                .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
                .add_mapping(self.REMOTE_NODE, self.REMOTE_IP, bidirectional=True)
                .build()
            ):

                node_manipulation.remove_node(self.REMOTE_NODE, self.tmp_files[1], self.tmp_files[2])

            nc = NodeConfig()
            root = nc.get_current_config_root(self.tmp_files[2])
            read_replicas = nc.get_read_replicas(root)
            self.assertEqual(len(read_replicas), 0)

            for section_path in ['InactiveNodes', 'DesiredNodes', 'ActiveNodes']:
                section = root.find(section_path)
                self.assertFalse(any(n.text == self.REMOTE_IP for n in section.findall('./Node')))

            mock_rebalance_dbroots.assert_not_called()
            mock_move_primary_node.assert_not_called()
            mock_update_dbroots_of_read_replicas.assert_not_called()

    def test_add_dbroots_nodes_rebalance(self):
        self.tmp_files = (
            './extra-dbroots-0.xml', './extra-dbroots-1.xml', './extra-dbroots-2.xml'
        )
        # add 2 dbroots, let's see what happens
        nc = NodeConfig()
        root = nc.get_current_config_root(tmp_mcs_config_filename)

        sysconf_node = root.find('./SystemConfig')
        dbroot_count_node = sysconf_node.find('./DBRootCount')
        dbroot_count = int(dbroot_count_node.text) + 2
        dbroot_count_node.text = str(dbroot_count)
        etree.SubElement(sysconf_node, 'DBRoot2').text = '/dummy_path/data2'
        etree.SubElement(sysconf_node, 'DBRoot10').text = '/dummy_path/data10'
        nc.write_config(root, self.tmp_files[0])

        with (
            make_local_resolution_builder()
            .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
            .add_mapping(self.REMOTE_NODE, self.REMOTE_IP, bidirectional=True)
            .build()
        ):
            node_manipulation.add_node(self.LOCAL_NODE, self.tmp_files[0], self.tmp_files[1])

        # Do eyeball verification for now.
        nc = NodeConfig()
        root = nc.get_current_config_root(self.tmp_files[1])
        node = root.find('./PMS2/IPAddr')
        self.assertEqual(node.text, self.LOCAL_IP)

        hostname = socket.gethostname()
        with (
            make_local_resolution_builder()
            .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
            .add_mapping(self.REMOTE_NODE, self.REMOTE_IP, bidirectional=True)
            .build()
        ):
            node_manipulation.remove_node(hostname, self.tmp_files[1], self.tmp_files[2], test_mode=True)

    def test_add_dbroot(self):
        self.tmp_files = (
            './dbroot-test0.xml', './dbroot-test1.xml', './dbroot-test2.xml',
            './dbroot-test3.xml', './dbroot-test4.xml'
        )
        # add a dbroot, verify it exists
        with (
            make_local_resolution_builder()
            .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
            .add_mapping(self.REMOTE_NODE, self.REMOTE_IP, bidirectional=True)
            .build()
        ):
            id = node_manipulation.add_dbroot(tmp_mcs_config_filename, self.tmp_files[0])
        self.assertEqual(id, 2)
        nc = NodeConfig()
        root = nc.get_current_config_root(self.tmp_files[0])
        self.assertEqual(2, int(root.find('./SystemConfig/DBRootCount').text))
        self.assertEqual(f'{MCS_DATA_PATH}/data2', root.find('./SystemConfig/DBRoot2').text)

        # add a node, verify we can add a dbroot to each of them
        with (
            make_local_resolution_builder()
            .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
            .add_mapping(self.REMOTE_NODE, self.REMOTE_IP, bidirectional=True)
            .build()
        ):
            node_manipulation.add_node(self.LOCAL_NODE, tmp_mcs_config_filename, self.tmp_files[1])
            node_manipulation.add_node(self.REMOTE_IP, self.tmp_files[1], self.tmp_files[2])

            id1 = node_manipulation.add_dbroot(self.tmp_files[2], self.tmp_files[3], host=self.LOCAL_NODE)
            id2 = node_manipulation.add_dbroot(self.tmp_files[3], self.tmp_files[4], host=self.REMOTE_IP)
        self.assertEqual(id1, 2)
        self.assertEqual(id2, 3)

        root = nc.get_current_config_root(self.tmp_files[4])
        dbroot_count1 = int(root.find('./SystemModuleConfig/ModuleDBRootCount1-3').text)
        dbroot_count2 = int(root.find('./SystemModuleConfig/ModuleDBRootCount2-3').text)
        self.assertEqual(dbroot_count1 + dbroot_count2, 3)

        unique_dbroots = set()
        for i in range(1, dbroot_count1 + 1):
            unique_dbroots.add(int(root.find(f'./SystemModuleConfig/ModuleDBRootID1-{i}-3').text))
        for i in range(1, dbroot_count2 + 1):
            unique_dbroots.add(int(root.find(f'./SystemModuleConfig/ModuleDBRootID2-{i}-3').text))

        self.assertEqual(list(unique_dbroots), [1, 2, 3])

    def test_change_primary_node(self):
        # add a node, make it the primary, verify expected result
        self.tmp_files = ('./primary-node0.xml', './primary-node1.xml')
        with (
            make_local_resolution_builder()
            .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
            .add_mapping(self.REMOTE_NODE, self.REMOTE_IP, bidirectional=True)
            .build()
        ):
            node_manipulation.add_node(self.LOCAL_NODE, tmp_mcs_config_filename, self.tmp_files[0])

            node_manipulation.move_primary_node(self.tmp_files[0], self.tmp_files[1])

        root = NodeConfig().get_current_config_root(self.tmp_files[1])

        self.assertEqual(root.find('./ExeMgr1/IPAddr').text, self.LOCAL_IP)
        self.assertEqual(root.find('./DMLProc/IPAddr').text, self.LOCAL_IP)
        self.assertEqual(root.find('./DDLProc/IPAddr').text, self.LOCAL_IP)
        # This version doesn't support IPv6
        dbrm_controller_ip = root.find('./DBRM_Controller/IPAddr').text
        self.assertEqual(dbrm_controller_ip, self.LOCAL_NODE)
        self.assertEqual(root.find('./PrimaryNode').text, self.LOCAL_NODE)

    def test_unassign_dbroot1(self):
        self.tmp_files = ('./tud-0.xml', './tud-1.xml', './tud-2.xml', './tud-3.xml')
        with (
            make_local_resolution_builder()
            .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
            .add_mapping(self.REMOTE_NODE, self.REMOTE_IP, bidirectional=True)
            .build()
        ):
            node_manipulation.add_node(self.LOCAL_NODE, tmp_mcs_config_filename, self.tmp_files[0])

        root = NodeConfig().get_current_config_root(self.tmp_files[0])
        (name, addr) = node_manipulation.find_dbroot1(root)
        self.assertEqual(name, self.LOCAL_NODE)

        # add a second node and more dbroots to make the test slightly more robust
        with (
            make_local_resolution_builder()
            .add_mapping(socket.gethostname(), self.LOCAL_IP, bidirectional=True)
            .add_mapping(self.LOCAL_NODE, self.LOCAL_IP, bidirectional=True)
            .build()
        ):
            node_manipulation.add_node(socket.gethostname(), self.tmp_files[0], self.tmp_files[1])

            node_manipulation.add_dbroot(self.tmp_files[1], self.tmp_files[2], socket.gethostname())
            node_manipulation.add_dbroot(self.tmp_files[2], self.tmp_files[3], self.LOCAL_NODE)

        root = NodeConfig().get_current_config_root(self.tmp_files[3])
        (name, addr) = node_manipulation.find_dbroot1(root)
        self.assertEqual(name, self.LOCAL_NODE)

        node_manipulation.unassign_dbroot1(root)
        caught_it = False
        try:
            node_manipulation.find_dbroot1(root)
        except node_manipulation.NodeNotFoundException:
            caught_it = True

        self.assertTrue(caught_it)

    def test_add_node_hostname_reverse_mismatch(self):
        """Adding a node by hostname should fail if reverse DNS doesn't map
        back to the provided hostname (neither exact nor FQDN starting with it).
        """
        self.tmp_files = ('./rev-mismatch-0.xml',)
        bad_hostname = 'badhost'

        with MockResolutionBuilder().add_mapping(bad_hostname, '10.0.0.5', bidirectional=False).build():
            with self.assertRaises(CMAPIBasicError):
                node_manipulation.add_node(bad_hostname, tmp_mcs_config_filename, self.tmp_files[0])


class TestDBRootsManipulation(unittest.TestCase):
    our_module_idx = 3
    ro_node1_ip = '192.168.1.3'
    ro_node2_ip = '192.168.1.4'

    def setUp(self):
        # Mock initial XML structure (add two nodes and two dbroots)
        self.root = etree.Element('Columnstore')
        # Add two PM modules with IP addresses
        smc = etree.SubElement(self.root, 'SystemModuleConfig')
        module_count = etree.SubElement(smc, 'ModuleCount3')
        module_count.text = '2'
        module1_ip = etree.SubElement(smc, 'ModuleIPAddr1-1-3')
        module1_ip.text = '192.168.1.1'
        module2_ip = etree.SubElement(smc, 'ModuleIPAddr2-1-3')
        module2_ip.text = '192.168.1.2'

        system_config = etree.SubElement(self.root, 'SystemConfig')
        dbroot_count = etree.SubElement(system_config, 'DBRootCount')
        dbroot_count.text = '2'
        dbroot1 = etree.SubElement(system_config, 'DBRoot1')
        dbroot1.text = '/data/dbroot1'
        dbroot2 = etree.SubElement(system_config, 'DBRoot2')
        dbroot2.text = '/data/dbroot2'

    def test_get_pm_module_num_to_addr_map(self):
        result = node_manipulation.get_pm_module_num_to_addr_map(self.root)

        expected = {
            1: '192.168.1.1',
            2: '192.168.1.2',
        }
        self.assertEqual(result, expected)

    def test_add_dbroots_of_other_nodes(self):
        '''add_dbroots_of_other_nodes must add dbroots of other nodes into mapping of the node.'''
        add_dbroots_of_other_nodes(self.root, self.our_module_idx)

        # Check that ModuleDBRootCount of the module was updated
        module_count = self.root.find(f'./SystemModuleConfig/ModuleDBRootCount{self.our_module_idx}-3')
        self.assertIsNotNone(module_count)
        self.assertEqual(module_count.text, '2')

        # Check that dbroots were added to ModuleDBRootID{module_num}-{i}-3
        dbroot1 = self.root.find(f'./SystemModuleConfig/ModuleDBRootID{self.our_module_idx}-1-3')
        dbroot2 = self.root.find(f'./SystemModuleConfig/ModuleDBRootID{self.our_module_idx}-2-3')
        self.assertIsNotNone(dbroot1)
        self.assertIsNotNone(dbroot2)
        self.assertEqual(dbroot1.text, '1')
        self.assertEqual(dbroot2.text, '2')

    def test_remove_dbroots_of_node(self):
        '''Test that remove_dbroots_of_node correctly removes dbroots from the node's mapping'''
        # Add dbroot association to the node
        smc = self.root.find('./SystemModuleConfig')
        dbroot1 = etree.SubElement(smc, f'ModuleDBRootID{self.our_module_idx}-1-3')
        dbroot1.text = '1'
        dbroot2 = etree.SubElement(smc, f'ModuleDBRootID{self.our_module_idx}-2-3')
        dbroot2.text = '2'
        # Add ModuleDBRootCount to the node
        module_count = etree.SubElement(smc, f'ModuleDBRootCount{self.our_module_idx}-3')
        module_count.text = '2'

        remove_dbroots_of_node(self.root, self.our_module_idx)

        # Check that ModuleDBRootCount was removed
        module_count = self.root.find(f'./SystemModuleConfig/ModuleDBRootCount{self.our_module_idx}-3')
        self.assertIsNone(module_count)
        # Check that dbroot mappings of the module were removed
        dbroot1 = self.root.find(f'./SystemModuleConfig/ModuleDBRootID{self.our_module_idx}-1-3')
        dbroot2 = self.root.find(f'./SystemModuleConfig/ModuleDBRootID{self.our_module_idx}-2-3')
        self.assertIsNone(dbroot1)
        self.assertIsNone(dbroot2)

    def test_update_dbroots_of_replica_nodes(self):
        """Test that update_dbroots_of_replica_nodes adds all existing dbroots to
        all existing replica nodes"""
        # Add two new new modules to the XML structure (two already exist)
        smc = self.root.find('./SystemModuleConfig')
        module_count = smc.find('./ModuleCount3')
        module_count.text = '4'
        module3_ip = etree.SubElement(smc, 'ModuleIPAddr3-1-3')
        module3_ip.text = self.ro_node1_ip
        module4_ip = etree.SubElement(smc, 'ModuleIPAddr4-1-3')
        module4_ip.text = self.ro_node2_ip

        # Mark modules 1 and 2 as write-enabled by adding WriteEngineServer sections
        wes1 = etree.SubElement(self.root, 'pm1_WriteEngineServer')
        etree.SubElement(wes1, 'IPAddr').text = '192.168.1.1'
        etree.SubElement(wes1, 'Port').text = '8630'
        wes2 = etree.SubElement(self.root, 'pm2_WriteEngineServer')
        etree.SubElement(wes2, 'IPAddr').text = '192.168.1.2'
        etree.SubElement(wes2, 'Port').text = '8630'

        update_dbroots_of_read_replicas(self.root)

        # Check that read replicas have all the dbroots
        for ro_module_idx in range(3, 5):
            module_count = self.root.find(f'./SystemModuleConfig/ModuleDBRootCount{ro_module_idx}-3')
            self.assertIsNotNone(module_count)
            self.assertEqual(module_count.text, '2')

            dbroot1 = self.root.find(f'./SystemModuleConfig/ModuleDBRootID{ro_module_idx}-1-3')
            dbroot2 = self.root.find(f'./SystemModuleConfig/ModuleDBRootID{ro_module_idx}-2-3')
            self.assertIsNotNone(dbroot1)
            self.assertIsNotNone(dbroot2)
            self.assertEqual(dbroot1.text, '1')
            self.assertEqual(dbroot2.text, '2')
