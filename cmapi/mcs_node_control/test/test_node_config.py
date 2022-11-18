import logging
import os
import subprocess
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from lxml import etree

from cmapi_server.constants import CMAPI_DEFAULT_CONF_PATH
from mcs_node_control.models.dbrm import (
    DBRM, set_cluster_mode
)
from mcs_node_control.models.node_config import NodeConfig
from mcs_node_control.models.misc import read_module_id
from mcs_node_control.models.node_status import NodeStatus
from mcs_node_control.test.settings import CONFIG_PATH_NEW, CONFIG_PATH_OLD


MCS_NODE_MODELS = 'mcs_node_control.models'
NODE_CONFIG_MODULE = f'{MCS_NODE_MODELS}.node_config'


logging.basicConfig(level='DEBUG')


# These tests needs working DBRM worker.
class NodeConfigTest(TestCase):

    @mock.patch(f'{NODE_CONFIG_MODULE}.mkdir')
    @mock.patch(f'{NODE_CONFIG_MODULE}.chown')
    @mock.patch(f'{NODE_CONFIG_MODULE}.read_module_id', return_value=1)
    @mock.patch(
        f'{NODE_CONFIG_MODULE}.NodeConfig.in_active_nodes',
        return_value=False
    )
    def test_apply_config(self, *_args):
        """Test apply configuration file."""
        with TemporaryDirectory() as tmp_dirname:
            config_filepath = os.path.join(tmp_dirname, 'Columnstore.xml')

            copyfile(CONFIG_PATH_OLD, config_filepath)
            # change config
            parser = etree.XMLParser(load_dtd=True)
            # new_tree = etree.parse('/etc/columnstore/Columnstore.xml', parser=parser)
            new_tree = etree.parse(CONFIG_PATH_NEW, parser=parser)

            node_config = NodeConfig()
            xml_string = node_config.to_string(new_tree)

            node_config.apply_config(config_filepath, xml_string)

            # compare configurations
            config_file = Path(config_filepath)
            xml_string_written = config_file.read_text()
            self.assertEqual(xml_string_written, xml_string)
            # copy must exists
            config_file_copy = Path(f"{config_filepath}.cmapi.save")
            self.assertTrue(config_file_copy.exists())

    @mock.patch(f'{NODE_CONFIG_MODULE}.mkdir')
    @mock.patch(f'{NODE_CONFIG_MODULE}.chown')
    @mock.patch(f'{NODE_CONFIG_MODULE}.read_module_id', return_value=1)
    @mock.patch(
        f'{NODE_CONFIG_MODULE}.NodeConfig.in_active_nodes',
        return_value=False
    )
    def test_rollback_config(self, *_args):
        """"Test rollback applied configuration file."""
        with TemporaryDirectory() as tmp_dirname:
            config_filepath = os.path.join(tmp_dirname, 'Columnstore.xml')
            copyfile(CONFIG_PATH_OLD, config_filepath)

            old_config_file = Path(CONFIG_PATH_OLD)
            old_xml_string = old_config_file.read_text()
            new_config_file = Path(CONFIG_PATH_NEW)
            new_xml_string = new_config_file.read_text()

            node_config = NodeConfig()
            node_config.apply_config(config_filepath, new_xml_string)
            node_config.rollback_config(config_filepath)

            config_file = Path(config_filepath)
            xml_string_restored = config_file.read_text()
            self.assertEqual(xml_string_restored, old_xml_string)

    def test_get_current_config(self):
        """Test get current config from file."""
        config_file = Path(CONFIG_PATH_OLD)
        node_config = NodeConfig()
        self.assertEqual(
            node_config.get_current_config(CONFIG_PATH_OLD),
            config_file.read_text()
        )

    def test_set_cluster_mode(self):
        """Test set cluster mode.

        TODO:
            - move from here. There are no set_cluster_mode in NodeConfig
            - split to unit and integrational tests
            - make unittests for raising exception
        """

        for mode in ['readonly', 'readwrite']:
            with self.subTest(mode=mode):
                fake_mode = mode
                set_cluster_mode(mode)
                with DBRM() as dbrm:
                    if dbrm.get_dbrm_status() != 'master':
                        fake_mode = 'readonly'
                    self.assertEqual(dbrm.get_cluster_mode(), fake_mode)
                    self.assertEqual(dbrm._get_cluster_mode(), mode)

    def test_get_dbrm_conn_info(self):
        node_config = NodeConfig()
        root = node_config.get_current_config_root(CONFIG_PATH_OLD)
        master_conn_info = node_config.get_dbrm_conn_info(root)

        tree = ET.parse(CONFIG_PATH_OLD)
        master_ip = tree.find('./DBRM_Controller/IPAddr').text
        master_port = tree.find('./DBRM_Controller/Port').text

        self.assertEqual(master_conn_info['IPAddr'], master_ip)
        self.assertEqual(master_conn_info['Port'], master_port)

    def test_is_primary_node(self):
        try:
            current_master = None
            node_config = NodeConfig()
            root = node_config.get_current_config_root()
            current_master = node_config.get_dbrm_conn_info(root)['IPAddr']
            list_ips = "ip -4 -o addr | awk '!/^[0-9]*: ?lo|link\/ether/ {print $4}'"
            result = subprocess.run(list_ips,
                                     shell=True,
                                     stdout=subprocess.PIPE)
            local_addresses = result.stdout.decode('ASCII').split('\n')
            local_addresses = [addr.split('/')[0] for addr in local_addresses if len(addr)]
            os.system(f"mcsSetConfig DBRM_Controller IPAddr {local_addresses[0]}")
            self.assertTrue(node_config.is_primary_node())
            os.system(f"mcsSetConfig DBRM_Controller IPAddr 8.8.8.8")
            self.assertFalse(node_config.is_primary_node())
            os.system(f"mcsSetConfig DBRM_Controller IPAddr {current_master}")
        except AssertionError as e:
            if current_master is not None:
                os.system(f"mcsSetConfig DBRM_Controller IPAddr \
{current_master}")
            raise e

    def test_get_network_interfaces(self):
        node_config = NodeConfig()
        addresses = list(node_config.get_network_addresses())
        exemplar_addresses = []
        list_ips = "ip -4 -o addr | awk '!/^[0-9]*: ?lo|link\/ether/ {print $4}'"
        result = subprocess.run(list_ips,
                                 shell=True,
                                 stdout=subprocess.PIPE)
        exemplar_addresses += result.stdout.decode('ASCII').split('\n')
        list_ips = "ip -6 -o addr | awk '!/^[0-9]*: ?lo|link\/ether/ {print $4}'"
        result = subprocess.run(list_ips,
                                 shell=True,
                                 stdout=subprocess.PIPE)
        exemplar_addresses += result.stdout.decode('ASCII').split('\n')
        golden_addresses = [addr.split('/')[0] for addr in exemplar_addresses if len(addr) > 0]
        for addr in golden_addresses:
            self.assertTrue(addr in addresses)

    def test_is_single_node(self):
        try:
            current_master = None
            node_config = NodeConfig()
            root = node_config.get_current_config_root()
            current_master = node_config.get_dbrm_conn_info(root)['IPAddr']
            os.system(f"mcsSetConfig DBRM_Controller IPAddr 127.0.0.1")
            self.assertTrue(node_config.is_single_node())
            os.system(f"mcsSetConfig DBRM_Controller IPAddr 8.8.8.8")
            self.assertFalse(node_config.is_single_node())
            os.system(f"mcsSetConfig DBRM_Controller IPAddr {current_master}")
        except AssertionError as e:
            if current_master is not None:
                os.system(f"mcsSetConfig DBRM_Controller IPAddr \
{current_master}")
            raise e

    @mock.patch(f'{NODE_CONFIG_MODULE}.read_module_id', return_value=1)
    def test_get_module_net_address(self, *args):
        with TemporaryDirectory() as tmp_dirname:
            config_filepath = os.path.join(tmp_dirname, 'Columnstore.xml')
            copyfile(CONFIG_PATH_OLD, config_filepath)

            module_address = None
            node_config = NodeConfig()
            current_module_id = read_module_id()
            module_address_sh = (
                f'mcsGetConfig -c {config_filepath} '
                f'SystemModuleConfig ModuleIPAddr{current_module_id}-1-3'
            )
            result = subprocess.run(
                module_address_sh, shell=True, stdout=subprocess.PIPE
            )
            module_address = result.stdout.decode('ASCII').split('\n')[0]
            dummy_address = '8.8.8.8'
            os.system(
                f'mcsSetConfig -c {config_filepath} '
                f'SystemModuleConfig ModuleIPAddr{current_module_id}-1-3 '
                f'{dummy_address}'
            )
            root = node_config.get_current_config_root(config_filepath)
            self.assertEqual(
                dummy_address, node_config.get_module_net_address(root)
            )
            self.assertNotEqual(
                module_address, node_config.get_module_net_address(root)
            )
            os.system(
                f'mcsSetConfig -c {config_filepath} SystemModuleConfig '
                f'ModuleIPAddr{current_module_id}-1-3 {module_address}'
            )
            root = node_config.get_current_config_root(config_filepath)
            self.assertEqual(
                module_address, node_config.get_module_net_address(root)
            )

    def test_get_new_module_id(self):
        try:
            current_module_id = None
            current_module_address = None
            node_config = NodeConfig()
            current_module_id = read_module_id()
            root = node_config.get_current_config_root()
            current_module_address = node_config.get_module_net_address(root)
            os.system(f"mcsSetConfig SystemModuleConfig \
    ModuleIPAddr{current_module_id}-1-3 8.8.8.8")
            os.system(f"mcsSetConfig SystemModuleConfig \
    ModuleIPAddr{current_module_id+42}-1-3 {current_module_address}")
            root = node_config.get_current_config_root()
            self.assertEqual(current_module_id+42,
                             node_config.get_new_module_id(root))
            self.assertNotEqual(current_module_id,
                                node_config.get_new_module_id(root))
            os.system(f"mcsSetConfig SystemModuleConfig \
    ModuleIPAddr{current_module_id}-1-3 {current_module_address}")
            os.system(f"mcsSetConfig -x SystemModuleConfig \
    ModuleIPAddr{current_module_id+42}-1-3 {current_module_address}")
            root = node_config.get_current_config_root()
            self.assertEqual(current_module_id,
                         node_config.get_new_module_id(root))
        except AssertionError as e:
            if current_module_id is not None and current_module_address is not None:
                os.system(f"mcsSetConfig SystemModuleConfig \
        ModuleIPAddr{current_module_id}-1-3 {current_module_address}")
                os.system(f"mcsSetConfig -x SystemModuleConfig \
    ModuleIPAddr{current_module_id+42}-1-3 {current_module_address}")

    def test_dbroots_to_create(self):
        try:
            node_config = NodeConfig()
            current_module_id = read_module_id()
            dummy_dbroots = [42, 43]
            dbroot_seq_id = 2
            for d in dummy_dbroots:
                os.system(f"mcsSetConfig SystemModuleConfig \
    ModuleDBRootID{current_module_id}-{dbroot_seq_id}-3 {d}")
                dbroot_seq_id += 1
            root = node_config.get_current_config_root()
            dbroots_to_create = list(node_config.dbroots_to_create(root=root,                                                                                                         module_id=current_module_id))
            for d in dbroots_to_create:
                self.assertTrue(d in dummy_dbroots)
        except AssertionError as e:
            dbroot_seq_id = 2
            for d in dummy_dbroots:
                os.system(f"mcsSetConfig -x SystemModuleConfig \
    ModuleDBRootID{current_module_id}-{dbroot_seq_id}-3 {d}")
                dbroot_seq_id += 1
            raise e

        dbroot_seq_id = 2
        for d in dummy_dbroots:
            os.system(f"mcsSetConfig -x SystemModuleConfig \
ModuleDBRootID{current_module_id}-{dbroot_seq_id}-3 {d}")
            dbroot_seq_id += 1


if __name__ == '__main__':
    unittest.main()
