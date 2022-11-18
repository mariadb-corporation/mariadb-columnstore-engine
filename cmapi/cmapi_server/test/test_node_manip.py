import logging
import socket

from lxml import etree

from cmapi_server import node_manipulation
from cmapi_server.constants import MCS_DATA_PATH
from cmapi_server.test.unittest_global import (
    tmp_mcs_config_filename, BaseNodeManipTestCase
)
from mcs_node_control.models.node_config import NodeConfig


logging.basicConfig(level='DEBUG')


class NodeManipTester(BaseNodeManipTestCase):

    def test_add_remove_node(self):
        self.tmp_files = (
            './test-output0.xml','./test-output1.xml','./test-output2.xml'
        )
        hostaddr = socket.gethostbyname(socket.gethostname())
        node_manipulation.add_node(
            self.NEW_NODE_NAME, tmp_mcs_config_filename, self.tmp_files[0]
        )
        node_manipulation.add_node(
            hostaddr, self.tmp_files[0], self.tmp_files[1]
        )

        # get a NodeConfig, read test.xml
        # look for some of the expected changes.
        # Total verification will take too long to code up right now.
        nc = NodeConfig()
        root = nc.get_current_config_root(self.tmp_files[1])
        pms_node_ipaddr = root.find('./PMS1/IPAddr')
        self.assertEqual(pms_node_ipaddr.text, self.NEW_NODE_NAME)
        pms_node_ipaddr = root.find('./PMS2/IPAddr')
        self.assertEqual(pms_node_ipaddr.text, hostaddr)
        node = root.find("./ExeMgr2/IPAddr")
        self.assertEqual(node.text, hostaddr)

        node_manipulation.remove_node(
            self.NEW_NODE_NAME, self.tmp_files[1], self.tmp_files[2],
            test_mode=True
        )
        nc = NodeConfig()
        root = nc.get_current_config_root(self.tmp_files[2])
        node = root.find('./PMS1/IPAddr')
        self.assertEqual(node.text, hostaddr)
        # TODO: Fix node_manipulation add_node logic and _replace_localhost
        # node = root.find('./PMS2/IPAddr')
        # self.assertEqual(node, None)

    def test_add_dbroots_nodes_rebalance(self):
        self.tmp_files = (
            './extra-dbroots-0.xml', './extra-dbroots-1.xml',
            './extra-dbroots-2.xml'
        )
        # add 2 dbroots, let's see what happen
        nc = NodeConfig()
        root = nc.get_current_config_root(tmp_mcs_config_filename)

        sysconf_node = root.find('./SystemConfig')
        dbroot_count_node = sysconf_node.find('./DBRootCount')
        dbroot_count = int(dbroot_count_node.text) + 2
        dbroot_count_node.text = str(dbroot_count)
        etree.SubElement(sysconf_node, 'DBRoot2').text = '/dummy_path/data2'
        etree.SubElement(sysconf_node, 'DBRoot10').text = '/dummy_path/data10'
        nc.write_config(root, self.tmp_files[0])

        node_manipulation.add_node(
            self.NEW_NODE_NAME, self.tmp_files[0], self.tmp_files[1]
        )

        # get a NodeConfig, read test.xml
        # look for some of the expected changes.
        # Total verification will take too long to code up right now.
        # Do eyeball verification for now.
        nc = NodeConfig()
        root = nc.get_current_config_root(self.tmp_files[1])
        node = root.find("./PMS2/IPAddr")
        self.assertEqual(node.text, self.NEW_NODE_NAME)

        hostname = socket.gethostname()
        # awesome, I saw dbroots 1 and 10 get assigned to node 1,
        # and dbroot 2 assigned to node 2
        # now, remove node 1 (hostname) and see what we get

        node_manipulation.remove_node(
            hostname, self.tmp_files[1], self.tmp_files[2],
            test_mode=True
        )

    def test_add_dbroot(self):
        self.tmp_files = (
            './dbroot-test0.xml', './dbroot-test1.xml', './dbroot-test2.xml',
            './dbroot-test3.xml', './dbroot-test4.xml'
        )
        # add a dbroot, verify it exists

        id = node_manipulation.add_dbroot(
            tmp_mcs_config_filename, self.tmp_files[0]
        )
        self.assertEqual(id, 2)
        nc = NodeConfig()
        root = nc.get_current_config_root(self.tmp_files[0])
        self.assertEqual(2, int(root.find('./SystemConfig/DBRootCount').text))
        self.assertEqual(
            f'{MCS_DATA_PATH}/data2',
            root.find('./SystemConfig/DBRoot2').text
        )

        # add a node, verify we can add a dbroot to each of them
        hostname = socket.gethostname()
        node_manipulation.add_node(
            hostname, tmp_mcs_config_filename, self.tmp_files[1]
        )
        node_manipulation.add_node(
            self.NEW_NODE_NAME, self.tmp_files[1], self.tmp_files[2]
        )
        id1 = node_manipulation.add_dbroot(
            self.tmp_files[2], self.tmp_files[3], host=self.NEW_NODE_NAME
        )
        id2 = node_manipulation.add_dbroot(
            self.tmp_files[3], self.tmp_files[4], host=hostname
        )
        self.assertEqual(id1, 2)
        self.assertEqual(id2, 3)

        root = nc.get_current_config_root(self.tmp_files[4])
        dbroot_count1 = int(
            root.find('./SystemModuleConfig/ModuleDBRootCount1-3').text
        )
        dbroot_count2 = int(
            root.find('./SystemModuleConfig/ModuleDBRootCount2-3').text
        )
        self.assertEqual(dbroot_count1 + dbroot_count2, 3)

        unique_dbroots = set()
        for i in range(1, dbroot_count1 + 1):
            unique_dbroots.add(int(
                root.find(f'./SystemModuleConfig/ModuleDBRootID1-{i}-3').text)
            )
        for i in range(1, dbroot_count2 + 1):
            unique_dbroots.add(int(
                root.find(f'./SystemModuleConfig/ModuleDBRootID2-{i}-3').text)
            )

        self.assertEqual(list(unique_dbroots), [1, 2, 3])

    def test_change_primary_node(self):
        # add a node, make it the primary, verify expected result
        self.tmp_files = ('./primary-node0.xml', './primary-node1.xml')
        node_manipulation.add_node(
            self.NEW_NODE_NAME, tmp_mcs_config_filename, self.tmp_files[0]
        )
        node_manipulation.move_primary_node(
            self.tmp_files[0], self.tmp_files[1]
        )

        root = NodeConfig().get_current_config_root(self.tmp_files[1])

        self.assertEqual(
            root.find('./ExeMgr1/IPAddr').text, self.NEW_NODE_NAME
        )
        self.assertEqual(
            root.find('./DMLProc/IPAddr').text, self.NEW_NODE_NAME
        )
        self.assertEqual(
            root.find('./DDLProc/IPAddr').text, self.NEW_NODE_NAME
        )
        # This version doesn't support IPv6
        dbrm_controller_ip = root.find("./DBRM_Controller/IPAddr").text
        self.assertEqual(dbrm_controller_ip, self.NEW_NODE_NAME)
        self.assertEqual(root.find('./PrimaryNode').text, self.NEW_NODE_NAME)

    def test_unassign_dbroot1(self):
        self.tmp_files = (
            './tud-0.xml', './tud-1.xml', './tud-2.xml', './tud-3.xml',
        )
        node_manipulation.add_node(
            self.NEW_NODE_NAME, tmp_mcs_config_filename, self.tmp_files[0]
        )
        root = NodeConfig().get_current_config_root(self.tmp_files[0])
        (name, addr) = node_manipulation.find_dbroot1(root)
        self.assertEqual(name, self.NEW_NODE_NAME)

        # add a second node and more dbroots to make the test slightly more robust
        node_manipulation.add_node(
            socket.gethostname(), self.tmp_files[0], self.tmp_files[1]
        )
        node_manipulation.add_dbroot(
            self.tmp_files[1], self.tmp_files[2], socket.gethostname()
        )
        node_manipulation.add_dbroot(
            self.tmp_files[2], self.tmp_files[3], self.NEW_NODE_NAME
        )

        root = NodeConfig().get_current_config_root(self.tmp_files[3])
        (name, addr) = node_manipulation.find_dbroot1(root)
        self.assertEqual(name, self.NEW_NODE_NAME)

        node_manipulation.unassign_dbroot1(root)
        caught_it = False
        try:
            node_manipulation.find_dbroot1(root)
        except node_manipulation.NodeNotFoundException:
            caught_it = True

        self.assertTrue(caught_it)
