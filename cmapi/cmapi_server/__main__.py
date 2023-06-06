#!/usr/bin/env python3

"""
CherryPy-based webservice daemon with background threads
"""

import logging
import os
import threading
import time
from datetime import datetime, timedelta

import cherrypy
from cherrypy.process import plugins
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

# TODO: fix dispatcher choose logic because code executing in endpoints.py
#       while import process, this cause module logger misconfiguration
from cmapi_server.logging_management import config_cmapi_server_logging
config_cmapi_server_logging()

from cmapi_server import helpers
from cmapi_server.constants import DEFAULT_MCS_CONF_PATH, CMAPI_CONF_PATH
from cmapi_server.controllers.dispatcher import dispatcher, jsonify_error
from cmapi_server.failover_agent import FailoverAgent
from cmapi_server.managers.process import MCSProcessManager
from cmapi_server.managers.application import AppManager
from failover.node_monitor import NodeMonitor
from mcs_node_control.models.dbrm_socket import SOCK_TIMEOUT, DBRMSocketHandler
from mcs_node_control.models.node_config import NodeConfig


cert_filename = './cmapi_server/self-signed.crt'


def worker():
    """Background Timer that runs clean_txn_by_timeout() every 5 seconds
    TODO: this needs to be fixed/optimized. I don't like creating the thread
    repeatedly.
    """
    while True:
        t = threading.Timer(5.0, clean_txn_by_timeout)
        t.start()
        t.join()


def clean_txn_by_timeout():
    txn_section = app.config.get('txn', None)
    timeout_timestamp = txn_section.get('timeout') if txn_section is not None else None
    current_timestamp = int(datetime.now().timestamp())
    if timeout_timestamp is not None and current_timestamp > timeout_timestamp:
        txn_config_changed = txn_section.get('config_changed', None)
        if txn_config_changed is True:
            node_config = NodeConfig()
            node_config.rollback_config()
            node_config.apply_config(
                xml_string=node_config.get_current_config()
            )
        app.config.update({
                'txn': {
                    'id': 0,
                    'timeout': 0,
                    'manager_address': '',
                    'config_changed': False,
                },
            })


class TxnBackgroundThread(plugins.SimplePlugin):
    """CherryPy plugin to create a background worker thread"""
    app = None

    def __init__(self, bus, app):
        super(TxnBackgroundThread, self).__init__(bus)
        self.t = None
        self.app = app

    def start(self):
        """Plugin entrypoint"""

        self.t = threading.Thread(target=worker, name='TxnBackgroundThread')
        self.t.daemon = True
        self.t.start()

    # Start at a higher priority than "Daemonize" (which we're not using
    # yet but may in the future)
    start.priority = 85


class FailoverBackgroundThread(plugins.SimplePlugin):
    """CherryPy plugin to start the thread for failover monitoring."""

    def __init__(self, bus, turned_on):
        super().__init__(bus)
        self.node_monitor = NodeMonitor(agent=FailoverAgent())
        self.running = False
        self.turned_on = turned_on
        if self.turned_on:
            logging.info(
                'Failover is turned ON by default or in CMAPI config file.'
            )
        else:
            logging.info('Failover is turned OFF in CMAPI config file.')

    def _start(self):
        if self.running:
            return
        self.bus.log('Starting Failover monitor thread.')
        self.node_monitor.start()
        self.running = True

    def _stop(self):
        if not self.running:
            return
        self.bus.log('Stopping Failover monitor thread.')
        self.node_monitor.stop()
        self.running = False

    def _subscriber(self, run_failover: bool):
        if not self.turned_on:
            return
        if not isinstance(run_failover, bool):
            self.bus.log(f'Got wrong obj in failover channel {run_failover}')
            return
        if run_failover:
            self._start()
        else:
            self._stop()

    def start(self):
        self.bus.subscribe('failover', self._subscriber)

    def stop(self):
        cherrypy.engine.unsubscribe('failover', self._subscriber)
        self._stop()


def create_self_signed_certificate():
    key_filename = './cmapi_server/self-signed.key'

    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )

    with open(key_filename, "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()),
        )

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, 'US'),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, 'California'),
        x509.NameAttribute(NameOID.LOCALITY_NAME, 'Redwood City'),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, 'MariaDB'),
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
        datetime.utcnow()
    ).not_valid_after(
        datetime.utcnow() + timedelta(days=365)
    ).add_extension(
        basic_contraints,
        False
    ).add_extension(
        x509.SubjectAlternativeName([x509.DNSName('localhost')]),
        critical=False
    ).sign(key, hashes.SHA256(), default_backend())

    with open(cert_filename, 'wb') as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))


if __name__ == '__main__':
    logging.info(f'CMAPI Version: {AppManager.get_version()}')

    # TODO: read cmapi config filepath as an argument
    helpers.cmapi_config_check()

    if not os.path.exists(cert_filename):
        create_self_signed_certificate()

    app = cherrypy.tree.mount(root=None, config=CMAPI_CONF_PATH)
    app.config.update({
        '/': {
            'request.dispatch': dispatcher,
            'error_page.default': jsonify_error,
        },
        'config': {
            'path': CMAPI_CONF_PATH,
        },
    })

    cherrypy.config.update(CMAPI_CONF_PATH)
    cfg_parser = helpers.get_config_parser(CMAPI_CONF_PATH)
    dispatcher_name, dispatcher_path = helpers.get_dispatcher_name_and_path(
        cfg_parser
    )
    MCSProcessManager.detect(dispatcher_name, dispatcher_path)
    # If we don't have auto_failover flag in the config turn it ON by default.
    turn_on_failover = cfg_parser.getboolean(
        'application', 'auto_failover', fallback=True
    )
    TxnBackgroundThread(cherrypy.engine, app).subscribe()
    # subscribe FailoverBackgroundThread plugin code to bus channels
    # code below not starting "real" failover background thread
    FailoverBackgroundThread(cherrypy.engine, turn_on_failover).subscribe()
    cherrypy.engine.start()
    cherrypy.engine.wait(cherrypy.engine.states.STARTED)

    success = False
    config_mtime = os.path.getmtime(DEFAULT_MCS_CONF_PATH)
    # if the mtime changed, we infer that a put_config was run on this node,
    # and we now have a current config file.
    # TODO: Research all affected cases and remove/rewrite this loop below.
    #       Previously this affects endless waiting time while starting
    #       application after upgrade.
    #       Do we have any cases when we need to try syncing config with other
    #       nodes with endless retry?
    if not helpers.in_maintenance_state(DEFAULT_MCS_CONF_PATH):
        while (
            not success
            and config_mtime == os.path.getmtime(DEFAULT_MCS_CONF_PATH)
        ):
            try:
                success = helpers.get_current_config_file()
            except Exception:
                logging.info(
                    'Main got exception while get_current_config_file',
                    exc_info=True
                )
                success = False
            if not success:
                delay = 10
                logging.warning(
                    'Failed to fetch the current config file, '
                    f'retrying in {delay}s'
                )
                time.sleep(delay)

        config_mtime = os.path.getmtime(DEFAULT_MCS_CONF_PATH)
        helpers.wait_for_deactivation_or_put_config(config_mtime)

        dbrm_socket = DBRMSocketHandler()
        # TODO: fix DBRM message show on nodes restart.
        #       Use DBRM() context manager.
        try:
            dbrm_socket.connect()
            dbrm_socket._detect_protocol()
            dbrm_socket.close()
        except Exception:
            logging.warning(
                'Something went wrong while trying to detect dbrm protocol.\n'
                'Seems "controllernode" process isn\'t started.\n'
                'This is just a notification, not a problem.\n'
                'Next detection will started at first node\\cluster '
                'status check.\n'
                f'This can cause extra {SOCK_TIMEOUT} seconds delay while\n'
                'first attempt to get status.',
                exc_info=True
            )
    else:
        logging.info(
            'In maintenance state, not syncing config from other nodes.'
        )

    if turn_on_failover:
        if not helpers.in_maintenance_state(DEFAULT_MCS_CONF_PATH):
            cherrypy.engine.publish('failover', True)
        else:
            logging.info('In maintenance state, not starting Failover.')

    AppManager.started = True
    cherrypy.engine.block()
