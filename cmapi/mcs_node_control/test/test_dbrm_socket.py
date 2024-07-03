import io
import logging
import unittest

from mcs_node_control.models.dbrm_socket import MAGIC_BYTES, DBRMSocketHandler


logging.basicConfig(level='DEBUG')


class TestDBRMSocketHandler(unittest.TestCase):

    def test_myreceive_to_magic(self):
        response_data = b'\x01\x00\x00\x00\x00'
        valid_magic = b'%s%s' % (MAGIC_BYTES, response_data)
        first_unknow = b'A%s%s' % (MAGIC_BYTES, response_data)
        partial_first_magic = b'%s%s%s' % (
            MAGIC_BYTES[:3], MAGIC_BYTES, response_data
        )
        sock_responses = [valid_magic, first_unknow, partial_first_magic]
        for sock_response in sock_responses:
            with self.subTest(sock_response=sock_response):
                data_stream = io.BytesIO(sock_response)
                data_stream.recv = data_stream.read
                dbrm_socket = DBRMSocketHandler()
                # pylint: disable=protected-access
                dbrm_socket._socket = data_stream
                dbrm_socket._receive_magic()
                self.assertEqual(data_stream.read(), response_data)
