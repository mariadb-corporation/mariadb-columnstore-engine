"""Tests for all the CEJ (Cross Engine Join) related stuff."""
import os
import subprocess
import sys
import unittest
from shutil import which

from cmapi_server.handlers.cej import CEJPasswordHandler
from cmapi_server.constants import MCS_SECRETS_FILE_PATH


class SecretsTestCase(unittest.TestCase):
    """Test case for checking .secrets file related stuff."""

    @unittest.skipIf(which('cskeys') is None,
                     'This MCS version doesn\'t provide "cskeys" tool.')
    def test_cspasswd_decrypt_algorithm(self) -> None:
        """Test to check decrypt algorithm.

        Check that CEJ password decrypting algorithm is the same between
        "cspasswd" tool in MCS and in CMAPI.
        """

        test_passwd = 'columstore is the best'
        # create .secrets file using cskeys util
        ret = subprocess.run(
            'cskeys', shell=True, stdout=subprocess.PIPE, check=True
        )
        encrypted_passwd = subprocess.check_output(
            ['cspasswd', test_passwd]
        ).decode(sys.stdout.encoding).strip()
        self.assertEqual(
            test_passwd, CEJPasswordHandler.decrypt_password(encrypted_passwd)
        )
        os.remove(MCS_SECRETS_FILE_PATH)
