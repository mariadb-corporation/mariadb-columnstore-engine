import unittest
from unittest.mock import patch, mock_open, MagicMock
import socket
import ssl
import json
import os
import subprocess
from urllib.error import HTTPError, URLError
from urllib.request import Request
from xml.etree.ElementTree import Element
import importlib.util

import mcssavebrm

class TestMcsSavebrmFunctions(unittest.TestCase):
    @patch('mcssavebrm.configparser.ConfigParser.get', return_value='test_api_key')
    @patch('mcssavebrm.configparser.ConfigParser.read')
    def test_get_api_key(self, mock_read, mock_get):
        self.assertEqual(mcssavebrm.get_api_key(), 'test_api_key')
        mock_read.assert_called_once_with(mcssavebrm.CMAPI_CONFIG_PATH)

    def test_get_unverified_context(self):
        ctx = mcssavebrm.get_unverified_context()
        self.assertFalse(ctx.check_hostname)
        self.assertEqual(ctx.verify_mode, ssl.CERT_NONE)

    @patch('mcssavebrm.urlopen')
    @patch('mcssavebrm.get_unverified_context')
    def test_cmapi_available(self, mock_get_unverified_context, mock_urlopen):
        mock_get_unverified_context.return_value = ssl._create_unverified_context()
        mock_urlopen.side_effect = HTTPError(None, 404, 'Not Found', None, None)
        self.assertTrue(mcssavebrm.cmapi_available())

    @patch('mcssavebrm.fcntl.ioctl')
    @patch('mcssavebrm.socket.socket')
    def test_get_ip_address_by_nic(self, mock_socket, mock_ioctl):
        mock_socket_inst = MagicMock()
        mock_socket.return_value = mock_socket_inst
        mock_ioctl.return_value = b'\x00' * 20 + b'\x7f\x00\x00\x01'
        self.assertEqual(mcssavebrm.get_ip_address_by_nic('lo'), '127.0.0.1')

    @patch('mcssavebrm.get_ip_address_by_nic', return_value='127.0.0.1')
    @patch('mcssavebrm.socket.gethostbyaddr', return_value=('localhost', [], []))
    @patch('mcssavebrm.socket.if_nameindex', return_value=[(1, 'lo')])
    def test_is_primary_fallback(self, mock_if_nameindex, mock_gethostbyaddr, mock_get_ip_address_by_nic):
        self.assertTrue(mcssavebrm.is_primary_fallback('localhost'))

    @patch('mcssavebrm.cmapi_available', return_value=True)
    @patch('mcssavebrm.urlopen')
    @patch('mcssavebrm.get_unverified_context')
    @patch('mcssavebrm.get_api_key', return_value='test_api_key')
    def test_is_node_primary(self, mock_get_api_key, mock_get_unverified_context, mock_urlopen, mock_cmapi_available):
        mock_get_unverified_context.return_value = ssl._create_unverified_context()
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({'is_primary': 'True'}).encode('utf-8')
        mock_urlopen.return_value.__enter__.return_value = mock_response
        root = MagicMock()
        self.assertTrue(mcssavebrm.is_node_primary(root))

    @patch('os.path.getsize', return_value=1024)
    def test_get_file_size(self, mock_getsize):
        self.assertEqual(mcssavebrm.get_file_size('test_file'), 1024)

    @patch('mcssavebrm.get_file_size', return_value=500)
    def test_em_is_empty(self, mock_get_file_size):
        self.assertTrue(mcssavebrm.em_is_empty('test_prefix'))

    @patch('os.remove')
    @patch('os.listdir', return_value=[f'test_file{i}' for i in range(50)])
    def test_clean_up_backup_brm_files(self, mock_listdir, mock_remove):
        mcssavebrm.clean_up_backup_brm_files('/dummy/path')
        self.assertEqual(mock_remove.call_count, 10)

    @patch('os.remove')
    @patch('mcssavebrm.glob.glob', return_value=['test_file1', 'test_file2'])
    def test_remove_files_by_prefix_if_exist(self, mock_glob, mock_remove):
        mcssavebrm.remove_files_by_prefix_if_exist('test_prefix')
        mock_remove.assert_any_call('test_file1')
        mock_remove.assert_any_call('test_file2')

    @patch('xml.etree.ElementTree.parse')
    def test_get_config_root_from_file(self, mock_parse):
        mock_tree = MagicMock()
        mock_parse.return_value = mock_tree
        self.assertEqual(mcssavebrm.get_config_root_from_file('test_file'), mock_tree.getroot())

    @patch('time.time', return_value=1624478400)
    def test_get_epoch_prefix(self, mock_time):
        self.assertEqual(mcssavebrm.get_epoch_prefix(), 'backup_1624478400')

    @patch('mcssavebrm.get_epoch_prefix', return_value='backup_1624478400')
    @patch('mcssavebrm.get_save_brm_dir_path', return_value='/tmp/columnstore_tmp_files/rdwrscratch/')
    def test_get_save_brm_path_prefix(self, mock_get_epoch_prefix, mock_get_save_brm_dir_path):
        root = MagicMock()
        self.assertIn('backup_1624478400_BRM_saves', mcssavebrm.get_save_brm_path_prefix(root))

    @patch('subprocess.check_call')
    def test_call_save_brm(self, mock_check_call):
        self.assertEqual(mcssavebrm.call_save_brm('test_path'), 'test_path')

    @patch('mcssavebrm.call_save_brm', return_value='test_path')
    @patch('mcssavebrm.get_save_brm_path_prefix', return_value='test_path')
    def test_call_save_brm_locally(self, mock_get_save_brm_path_prefix, mock_call_save_brm):
        root = MagicMock()
        self.assertEqual(mcssavebrm.call_save_brm_locally(root), 'test_path')

    @patch('subprocess.check_call')
    @patch('mcssavebrm.get_save_brm_path_prefix', return_value='test_path')
    def test_call_save_brm_with_local_fallback(self, mock_get_save_brm_path_prefix, mock_check_call):
        root = MagicMock()
        mock_check_call.side_effect = [subprocess.CalledProcessError(1, 'test'), None]
        with self.assertRaises(SystemExit):
            mcssavebrm.call_save_brm_with_local_fallback(root)


if __name__ == '__main__':
    unittest.main()