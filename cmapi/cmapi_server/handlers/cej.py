"""Module contains all things related to working with .secrets file."""
import json
import logging
import os

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding

from cmapi_server.constants import MCS_SECRETS_FILE_PATH
from cmapi_server.exceptions import CEJError


AES_BLOCK_SIZE_BITS = algorithms.AES.block_size
AES_IV_BIN_SIZE = int(AES_BLOCK_SIZE_BITS/8)
# two hex chars for each byte
AES_IV_HEX_SIZE = AES_IV_BIN_SIZE * 2


class CEJPasswordHandler():
    """Handler for CrossEngineSupport password decryption."""

    @classmethod
    def secretsfile_exists(cls):
        """Check the .secrets file in MCS_SECRETS_FILE_PATH.

        :return: True if file exists and not empty.
        :rtype: bool
        """
        try:
            if (
                os.path.isfile(MCS_SECRETS_FILE_PATH) and
                os.path.getsize(MCS_SECRETS_FILE_PATH) > 0
            ):
                return True
        except Exception:
            # TODO: remove after check if python 3.8 everytime exist
            #       in package because isfile and getsize not rasing
            #       exceptions after 3.8
            logging.warning(
                'Something went wrong while detecting the .secrets file.',
                exc_info=True
            )
        return False

    @classmethod
    def get_secrets_json(cls):
        """Get json from .secrets file.

        :raises CEJError: on empty\corrupted\wrong format .secrets file
        :return: json from .secrets file
        :rtype: dict
        """
        if not cls.secretsfile_exists():
            raise CEJError(f'{MCS_SECRETS_FILE_PATH} file does not exist.')
        with open(MCS_SECRETS_FILE_PATH) as secrets_file:
            try:
                secrets_json = json.load(secrets_file)
            except Exception:
                logging.error(
                    'Something went wrong while loading json from '
                    f'{MCS_SECRETS_FILE_PATH}',
                    exc_info=True
                )
                raise CEJError(
                    f'Looks like file {MCS_SECRETS_FILE_PATH} is corrupted or'
                    'has wrong format.'
                ) from None
            return secrets_json

    @classmethod
    def decrypt_password(cls, enc_data:str):
        """Decrypt CEJ password if needed.

        :param enc_data: encrypted initialization vector + password in hex str
        :type enc_data: str
        :return: decrypted CEJ password
        :rtype: str
        """
        if not cls.secretsfile_exists():
            logging.warning('Unencrypted CrossEngineSupport password used.')
            return enc_data

        logging.info('Encrypted CrossEngineSupport password found.')

        try:
            iv = bytes.fromhex(enc_data[:AES_IV_HEX_SIZE])
            encrypted_passwd = bytes.fromhex(enc_data[AES_IV_HEX_SIZE:])
        except ValueError as value_error:
            raise CEJError(
                'Non-hexadecimal number found in encrypted CEJ password.'
            ) from value_error

        secrets_json = cls.get_secrets_json()
        encryption_key_hex = secrets_json.get('encryption_key')
        if not encryption_key_hex:
            raise CEJError(
                f'Empty "encryption key" found in {MCS_SECRETS_FILE_PATH}'
            )
        try:
            encryption_key = bytes.fromhex(encryption_key_hex)
        except ValueError as value_error:
            raise CEJError(
                'Non-hexadecimal number found in encryption key from '
                f'{MCS_SECRETS_FILE_PATH} file.'
            ) from value_error
        cipher = Cipher(
            algorithms.AES(encryption_key),
            modes.CBC(iv)
        )
        decryptor = cipher.decryptor()
        unpadder = padding.PKCS7(AES_BLOCK_SIZE_BITS).unpadder()
        padded_passwd_bytes = (
            decryptor.update(encrypted_passwd)
            + decryptor.finalize()
        )
        passwd_bytes = (
            unpadder.update(padded_passwd_bytes) + unpadder.finalize()
        )
        return passwd_bytes.decode()
