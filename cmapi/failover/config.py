import configparser
import logging
import threading
from os.path import getmtime

from cmapi_server.constants import DEFAULT_MCS_CONF_PATH, DEFAULT_SM_CONF_PATH
from mcs_node_control.models.node_config import NodeConfig


class Config:
    config_file = ''

    # params read from the config file
    _desired_nodes = []
    _active_nodes = []
    _inactive_nodes = []
    _primary_node = ''
    _my_name = None  # derived from config file

    config_lock = threading.Lock()
    last_mtime = 0
    die = False
    logger = None

    def __init__(self, config_file=DEFAULT_MCS_CONF_PATH):
        self.config_file = config_file
        self.logger = logging.getLogger()

    def getDesiredNodes(self):
        self.config_lock.acquire()
        self.check_reload()
        ret = self._desired_nodes
        self.config_lock.release()
        return ret

    def getActiveNodes(self):
        self.config_lock.acquire()
        self.check_reload()
        ret = self._active_nodes
        self.config_lock.release()
        return ret

    def getInactiveNodes(self):
        self.config_lock.acquire()
        self.check_reload()
        ret = self._inactive_nodes
        self.config_lock.release()
        return ret

    def getAllNodes(self):
        """Returns a 3-element tuple describing the status of all nodes.

        index 0 = all nodes in the cluster
        index 1 = all active nodes
        index 2 = all inactive nodes
        """
        self.config_lock.acquire()
        self.check_reload()
        ret = (self._desired_nodes, self._active_nodes, self._inactive_nodes)
        self.config_lock.release()
        return ret

    def getPrimaryNode(self):
        self.config_lock.acquire()
        self.check_reload()
        ret = self._primary_node
        self.config_lock.release()
        return ret

    def is_shared_storage(self, sm_config_file=DEFAULT_SM_CONF_PATH):
        """Check if SM is S3 or not.

        :param sm_config_file: path to SM config,
            defaults to DEFAULT_SM_CONF_PATH
        :type sm_config_file: str, optional
        :return: True if SM is S3 otherwise False
        :rtype: bool

        TODO: remove in next releases, useless?
        """
        sm_config = configparser.ConfigParser()
        sm_config.read(sm_config_file)
        # only LocalStorage or S3 can be returned for now
        storage = sm_config.get(
            'ObjectStorage', 'service', fallback='LocalStorage'
        )
        return storage.lower() == 's3'

    def check_reload(self):
        """Check config reload.

        Returns True if reload happened, False otherwise.
        """
        if self.last_mtime != getmtime(self.config_file):
            self.load_config()
            return True
        return False

    def who_am_I(self):
        self.config_lock.acquire()
        self.check_reload()
        ret = self._my_name
        self.config_lock.release()
        return ret

    def load_config(self):
        try:
            node_config = NodeConfig()
            root = node_config.get_current_config_root(self.config_file)
            last_mtime = getmtime(self.config_file)
        except Exception:
            self.logger.warning(
                f'Failed to parse config file {self.config_file}.',
                exc_info=True
            )
            return False

        node_tmp = root.findall('./DesiredNodes/Node')
        if len(node_tmp) == 0:
            self.logger.warning(
                f'The config file {self.config_file} is missing entries '
                'in the DesiredNodes section'
            )
            return False

        desired_nodes = [node.text for node in node_tmp]
        active_nodes = [
            node.text for node in root.findall('./ActiveNodes/Node')
        ]
        inactive_nodes = [
            node.text for node in root.findall('./InactiveNodes/Node')
        ]

        node_tmp = root.find('./PrimaryNode')
        if node_tmp is None or len(node_tmp.text) == 0:
            self.logger.warning(
                f'The config file {self.config_file} is missing a valid '
                'PrimaryNode entry'
            )
            return False
        primary_node = node_tmp.text

        # find my name in this cluster
        names = set(node_config.get_network_addresses_and_names())
        all_nodes = set(desired_nodes)
        intersection = all_nodes & names
        if len(intersection) > 1:
            my_name = intersection.pop()
            self.logger.warning(
                'This node has multiple names in the list of desired nodes, '
                'was it added more than once? Some things may not work in '
                f'this configuration. Using {my_name} as the name for this '
                'node.'
            )
        elif len(intersection) == 0:
            self.logger.warning(
                'This node has no entry in the list of desired nodes.'
            )
            my_name = None
        elif len(intersection) == 1:
            my_name = intersection.pop()
            # handles the initial 0-node special case
            if my_name == '127.0.0.1':
                my_name = None

        self.logger.info(f'Loaded the config file, my name is {my_name}')

        desired_nodes.sort()
        active_nodes.sort()
        inactive_nodes.sort()
        self._desired_nodes = desired_nodes
        self._active_nodes = active_nodes
        self._inactive_nodes = inactive_nodes
        self._primary_node = primary_node
        self.last_mtime = last_mtime
        self._my_name = my_name
        return True
