"""Module related to CMAPI self-signed certificate management logic."""
import os
import logging
from datetime import datetime, timedelta, timezone

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


CERT_FILENAME = './cmapi_server/self-signed.crt'
KEY_FILENAME = './cmapi_server/self-signed.key'
CERT_DAYS = 365


class CertificateManager():
    """Class with methods to manage self-signed certificate."""

    @staticmethod
    def create_self_signed_certificate() -> None:
        """Create self-signed certificate."""
        key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )

        with open(KEY_FILENAME, "wb") as f:
            f.write(key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()),
            )

        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, 'US'),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, 'California'),
            x509.NameAttribute(NameOID.LOCALITY_NAME, 'Redwood City'),
            x509.NameAttribute(
                NameOID.ORGANIZATION_NAME, 'MariaDB Corporation'
            ),
            x509.NameAttribute(NameOID.COMMON_NAME, 'mariadb.com'),
        ])

        basic_contraints = x509.BasicConstraints(ca=True, path_length=0)

        cert = x509.CertificateBuilder(
        ).subject_name(
            subject
        ).issuer_name(
            issuer
        ).public_key(
            key.public_key()
        ).serial_number(
            x509.random_serial_number()
        ).not_valid_before(
            datetime.now(timezone.utc)
        ).not_valid_after(
            datetime.now(timezone.utc) + timedelta(days=CERT_DAYS)
        ).add_extension(
            basic_contraints,
            False
        ).add_extension(
            x509.SubjectAlternativeName([x509.DNSName('localhost')]),
            critical=False
        ).sign(key, hashes.SHA256(), default_backend())

        with open(CERT_FILENAME, 'wb') as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        logging.info('Created self signed sertificate for CMAPI API access.')
    
    @staticmethod
    def create_self_signed_certificate_if_not_exist() -> None:
        """Create self-signed certificate if not exist."""
        if not os.path.exists(CERT_FILENAME):
            CertificateManager.create_self_signed_certificate()

    @staticmethod
    def days_before_expire() -> int:
        """Calculates how many days before expiration left.

        Creates self-signed cetificate if certificate doesn't exist.

        :return: days left
        :rtype: int
        """
        CertificateManager.create_self_signed_certificate_if_not_exist()
        with open(CERT_FILENAME, 'rb') as cert_file:
            cert_data = cert_file.read()
        cert = x509.load_pem_x509_certificate(cert_data)
        days_before_expire = (cert.not_valid_after - datetime.now()).days
        return days_before_expire

    @staticmethod
    def renew_certificate() -> None:
        """Creates new self signed certificate.
        
        Creates self-signed cetificate if certificate doesn't exist or
        expires in a 1 day or less.
        """
        if CertificateManager.days_before_expire() <= 0:
            logging.warning(
                'Self signed certificate nearly to expire. Renewing.'
            )
            CertificateManager.create_self_signed_certificate()
