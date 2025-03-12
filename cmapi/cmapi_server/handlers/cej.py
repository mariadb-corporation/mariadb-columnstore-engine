"""Module contains all things related to working with .secrets file."""
import binascii
import json
import logging
import os
import pwd
import stat
from shutil import copyfile

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding

from cmapi_server.constants import MCS_DATA_PATH, MCS_SECRETS_FILENAME
from cmapi_server.exceptions import CEJError


AES_BLOCK_SIZE_BITS = algorithms.AES.block_size
AES_IV_BIN_SIZE = int(AES_BLOCK_SIZE_BITS/8)
# two hex chars for each byte
AES_IV_HEX_SIZE = AES_IV_BIN_SIZE * 2


class CEJPasswordHandler():
    """Handler for CrossEngineSupport password decryption."""

    @classmethod
    def secretsfile_exists(cls, directory: str = MCS_DATA_PATH) -> bool:
        """Check the .secrets file in directory. Default MCS_SECRETS_FILE_PATH.

        :param directory: path to the directory with .secrets file
        :type directory: str, optional
        :return: True if file exists and not empty.
        :rtype: bool
        """
        secrets_file_path = os.path.join(directory, MCS_SECRETS_FILENAME)
        try:
            if (
                os.path.isfile(secrets_file_path) and
                os.path.getsize(secrets_file_path) > 0
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
    def get_secrets_json(cls, directory: str = MCS_DATA_PATH) -> dict:
        """Get json from .secrets file.

        :param directory: path to the directory with .secrets file
        :type directory: str, optional
        :return: json from .secrets file
        :rtype: dict
        :raises CEJError: on empty\corrupted\wrong format .secrets file
        """
        secrets_file_path = os.path.join(directory, MCS_SECRETS_FILENAME)
        if not cls.secretsfile_exists(directory=directory):
            raise CEJError(f'{secrets_file_path} file does not exist.')
        with open(secrets_file_path) as secrets_file:
            try:
                secrets_json = json.load(secrets_file)
            except Exception:
                logging.error(
                    'Something went wrong while loading json from '
                    f'{secrets_file_path}',
                    exc_info=True
                )
                raise CEJError(
                    f'Looks like file {secrets_file_path} is corrupted or'
                    'has wrong format.'
                ) from None
            return secrets_json

    @classmethod
    def decrypt_password(
        cls, enc_data: str, directory: str = MCS_DATA_PATH
    ) -> str:
        """Decrypt CEJ password if needed.

        :param directory: path to the directory with .secrets file
        :type directory: str, optional
        :param enc_data: encrypted initialization vector + password in hex str
        :type enc_data: str
        :return: decrypted CEJ password
        :rtype: str
        """
        secrets_file_path = os.path.join(directory, MCS_SECRETS_FILENAME)
        if not cls.secretsfile_exists(directory=directory):
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

        secrets_json = cls.get_secrets_json(directory=directory)
        encryption_key_hex = secrets_json.get('encryption_key', None)
        if not encryption_key_hex:
            raise CEJError(
                f'Empty "encryption key" found in {secrets_file_path}'
            )
        try:
            encryption_key = bytes.fromhex(encryption_key_hex)
        except ValueError as value_error:
            raise CEJError(
                'Non-hexadecimal number found in encryption key from '
                f'{secrets_file_path} file.'
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

    @classmethod
    def encrypt_password(
        cls, passwd: str, directory: str = MCS_DATA_PATH
    ) -> str:
        """Encrypt CEJ password.

        :param directory: path to the directory with .secrets file
        :type directory: str, optional
        :param passwd: CEJ password
        :type passwd: str
        :return: encrypted CEJ password in uppercase hex format
        :rtype: str
        """

        secrets_file_path = os.path.join(directory, MCS_SECRETS_FILENAME)
        iv = os.urandom(AES_IV_BIN_SIZE)

        secrets_json = cls.get_secrets_json(directory=directory)
        encryption_key_hex = secrets_json.get('encryption_key')
        if not encryption_key_hex:
            raise CEJError(
                f'Empty "encryption key" found in {secrets_file_path}'
            )
        try:
            encryption_key = bytes.fromhex(encryption_key_hex)
        except ValueError as value_error:
            raise CEJError(
                'Non-hexadecimal number found in encryption key from '
                f'{secrets_file_path} file.'
            ) from value_error
        cipher = Cipher(
            algorithms.AES(encryption_key),
            modes.CBC(iv)
        )

        encryptor = cipher.encryptor()
        padder = padding.PKCS7(algorithms.AES.block_size).padder()
        padded_data = padder.update(passwd.encode()) + padder.finalize()

        encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
        encrypted_passwd_bytes = iv + encrypted_data
        return encrypted_passwd_bytes.hex().upper()

    @classmethod
    def generate_secrets_data(cls) -> dict:
        """Generate secrets data for .secrets file.

        :return: secrets data
        :rtype: dict
        """
        key_length = 32  # AES256 key_size
        encryption_key = os.urandom(key_length)
        encryption_key_hex = binascii.hexlify(encryption_key).decode()
        secrets_dict = {
            'description': 'Columnstore CrossEngineSupport password encryption/decryption key',
            'encryption_cipher': 'EVP_aes_256_cbc',
            'encryption_key': encryption_key_hex
        }

        return secrets_dict

    @classmethod
    def save_secrets(
        cls, secrets: dict, directory: str = MCS_DATA_PATH,
        owner: str = 'mysql'
    ) -> None:
        """Write secrets to .secrets file.

        :param directory: path to the directory with .secrets file
        :type directory: str, optional
        :param secrets: secrets dict
        :type secrets: dict
        :param filepath: path to the .secrets file
        :type filepath: str, optional
        :param owner: owner of the file
        :type owner: str, optional
        """
        secrets_file_path = os.path.join(directory, MCS_SECRETS_FILENAME)
        if cls.secretsfile_exists(directory=directory):
            if cls.get_secrets_json(directory=directory) != secrets:
                copyfile(
                    secrets_file_path,
                    os.path.join(
                        directory,
                        f'{MCS_SECRETS_FILENAME}.cmapi.save'
                    )
                )
                logging.warning(
                    f'Backup of {secrets_file_path} file created.'
                )
            else:
                logging.debug(
                    f'No changes in {secrets_file_path} file detected.'
                )
                return

        try:
            with open(
                secrets_file_path, 'w', encoding='utf-8'
            ) as secrets_file:
                json.dump(secrets, secrets_file)
        except Exception as exc:
            raise CEJError(f'Write to .secrets file failed.') from exc

        try:
            os.chmod(secrets_file_path, stat.S_IRUSR)
            userinfo = pwd.getpwnam(owner)
            os.chown(secrets_file_path, userinfo.pw_uid, userinfo.pw_gid)
            logging.debug(
                f'Permissions of {secrets_file_path} file set to {owner}:read.'
            )
            logging.debug(
                f'Ownership of {secrets_file_path} file given to {owner}.'
            )
        except Exception as exc:
            raise CEJError(
                f'Failed to set permissions or ownership for .secrets file.'
            ) from exc