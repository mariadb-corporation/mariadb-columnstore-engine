import configparser
import grp
import logging
import pwd
import re
import socket
from os import mkdir, replace, chown
from pathlib import Path
from shutil import copyfile
from xml.dom import minidom   # to pick up pretty printing functionality

from lxml import etree

from cmapi_server.constants import (
    DEFAULT_MCS_CONF_PATH, DEFAULT_SM_CONF_PATH,
    MCS_MODULE_FILE_PATH,
)
# from cmapi_server.managers.process import MCSProcessManager
from mcs_node_control.models.misc import (
    read_module_id, get_dbroots_list
)
from mcs_node_control.models.network_ifaces import get_network_interfaces


module_logger = logging.getLogger()


class NodeConfig:
    """Class to operate with the configuration file.

    The class instance applies new config or retrives current.

    config_filename and output_filename allow tests to override
    the input & output of this fcn
    The output in this case may be a config file upgraded to version 1.
    """
    def get_current_config_root(
        self, config_filename: str = DEFAULT_MCS_CONF_PATH, upgrade=True
    ):
        """Retrieves current configuration.

        Read the config and returns Element.
        TODO: pretty the same function in misc.py - review

        :rtype: lxml.Element
        """
        parser = etree.XMLParser(load_dtd=True)
        tree = etree.parse(config_filename, parser=parser)
        self.upgrade_config(tree=tree, upgrade=upgrade)
        return tree.getroot()

    def get_root_from_string(self, config_string: str):
        root = etree.fromstring(config_string)
        self.upgrade_config(root=root)
        return root

    def upgrade_from_v0(self, root):
        revision = etree.SubElement(root, 'ConfigRevision')
        revision.text = '1'
        cluster_manager = etree.SubElement(root, 'ClusterManager')
        cluster_manager.text = str(self.get_module_net_address(root=root))
        cluster_name = etree.SubElement(root, 'ClusterName')
        cluster_name.text = 'MyCluster'

        # Need to get the addresses/host names of all nodes.
        # Should all be listed as DBRM_worker nodes
        addrs = set()
        num = 1
        max_node = 1
        while True:
            node = root.find(f'./DBRM_Worker{num}/IPAddr')
            if node is None:
                break
            if node.text != '0.0.0.0':
                addrs.add(node.text)
                if max_node < num:
                    max_node = num
            num += 1

        # NextNodeId can be derived from the max DBRM_worker entry with non-0
        # ip address
        next_node_id = etree.SubElement(root, 'NextNodeId')
        next_node_id.text = str(max_node + 1)

        # NextDBRootId is the max current dbroot in use + 1
        num = 1
        max_dbroot = 1
        while num < 100:
            node = root.find(f'./SystemConfig/DBRoot{num}')
            if node is not None:
                max_dbroot = num
            num += 1
        next_dbroot_id = etree.SubElement(root, 'NextDBRootId')
        next_dbroot_id.text = str(max_dbroot + 1)

        # The current primary node is listed under DBRMControllerNode.
        # Might as well start with that.
        primary_node_addr = root.find('./DBRM_Controller/IPAddr').text

        # Put them all in the DesiredNodes and ActiveNodes sections
        desired_nodes = etree.SubElement(root, 'DesiredNodes')
        active_nodes = etree.SubElement(root, 'ActiveNodes')
        for addr in addrs:
            node = etree.SubElement(desired_nodes, 'Node')
            node.text = addr
            node = etree.SubElement(active_nodes, 'Node')
            node.text = addr

        # Add an empty InactiveNodes section and set the primary node addr
        inactive_nodes = etree.SubElement(root, 'InactiveNodes')
        primary_node = etree.SubElement(root, 'PrimaryNode')
        primary_node.text = primary_node_addr

        # Add Maintenance tag and set to False
        maintenance = etree.SubElement(root, 'Maintenance')
        maintenance.text = str(False).lower()

    def upgrade_config(self, tree=None, root=None, upgrade=True):
        """
        Add the parts that might be missing after an upgrade from an earlier
        version.

        .. note:: one or the other optional parameter should be specified (?)
        """
        if root is None and tree is not None:
            root = tree.getroot()

        rev_node = root.find('./ConfigRevision')

        if rev_node is None and upgrade:
            self.upgrade_from_v0(root)
            # as we add revisions, add add'l checks on rev_node.text here

    def write_config(self, tree, filename=DEFAULT_MCS_CONF_PATH):
        tmp_filename = filename + ".cmapi.tmp"
        with open(tmp_filename, "w") as f:
            f.write(self.to_string(tree))
        replace(tmp_filename, filename)    # atomic replacement

    def to_string(self, tree):
        # TODO: try to use lxml to do this to avoid the add'l dependency
        xmlstr = minidom.parseString(etree.tostring(tree)).toprettyxml(
            indent="    "
        )
        # fix annoying issue of extra newlines added by toprettyxml()
        xmlstr = '\n'.join([
            line.rstrip() for line in xmlstr.split('\n') if line.strip() != ""
        ])
        return xmlstr

    def get_dbrm_conn_info(self, root=None):
        """Retrievs current DBRM master IP and port

        Read the config and returns a dict with the connection information.

        :rtype: dict
        """
        if root is None:
            return None
        addr = ''
        port = 0
        for el in root:
            if el.tag == 'DBRM_Controller':
                for subel in el:
                    if subel.tag == 'IPAddr':
                        addr = subel.text
                    elif subel.tag == 'Port':
                        port = subel.text
                return {'IPAddr': addr, 'Port': port}

        return None

    def apply_config(
        self, config_filename: str = DEFAULT_MCS_CONF_PATH,
        xml_string: str = None, sm_config_filename: str = None,
        sm_config_string: str = None
    ):
        """Applies the configuration WIP.

        Instance iterates over the xml nodes.

        : param config_filename: string 4 testing
        : param xml_string: string

        :rtype: bool
        """
        if xml_string is None:
            return

        current_root = self.get_current_config_root(config_filename)
        parser = etree.XMLParser(load_dtd=True)
        new_root = etree.fromstring(xml_string, parser=parser)

        try:
            # We don't change module ids for non-single nodes.
            # if self.is_single_node(root=current_root):
            #     set_module_id(self.get_new_module_id(new_root))

            # make sure all of the dbroot directories exist on this node
            for dbroot in self.get_all_dbroots(new_root):
                try:
                    node = new_root.find(f'./SystemConfig/DBRoot{dbroot}')
                    mkdir(node.text, mode=0o755)

                    # if we are using the systemd dispatcher we need to change
                    # ownership on any created dirs to mysql:mysql
                    # TODO: remove conditional once container dispatcher will
                    #       use non-root by default
                    # TODO: what happened if we change ownership in container?
                    #       check the container installations works as expected
                    # from cmapi_server.managers.process import MCSProcessManager
                    # if MCSProcessManager.dispatcher_name == 'systemd':
                    uid = pwd.getpwnam('mysql').pw_uid
                    gid = grp.getgrnam('mysql').gr_gid
                    chown(node.text, uid, gid)
                except FileExistsError:
                    pass
            # Save current config
            config_file = Path(config_filename)
            config_dir = config_file.resolve().parent
            copyfile(
                config_file, f'{config_dir}/{config_file.name}.cmapi.save'
            )

            # Save new config
            self.write_config(tree=new_root, filename=config_filename)

            # Save current and new storagemanager config
            if sm_config_string and sm_config_filename:
                sm_config_file = Path(sm_config_filename)
                sm_config_dir = sm_config_file.resolve().parent
                copyfile(
                    sm_config_file,
                    f'{sm_config_dir}/{sm_config_file.name}.cmapi.save'
                )
                with open(sm_config_filename, 'w') as sm_config_file:
                    sm_config_file.write(sm_config_string)
            # TODO: review
            # figure out what to put in the 'module' file to make
            # the old oam library happy
            module_file = None
            try:
                pm_num = self.get_current_pm_num(new_root)
                with open(MCS_MODULE_FILE_PATH, 'w') as module_file:
                    module_file.write(f'pm{pm_num}\n')
                module_logger.info(
                    f'Wrote "pm{pm_num}" to {MCS_MODULE_FILE_PATH}'
                )
            except Exception:
                module_logger.error(
                    'Failed to get or set this node\'s pm number.\n'
                    'You may observe add\'l errors as a result.\n',
                    exc_info=True
                )
        except:
            # Raise an appropriate exception
            module_logger.error(
                f'{self.apply_config.__name__} throws an exception.'
                'The original config must be restored by '
                'explicit ROLLBACK command or timeout.',
                exc_info=True
            )
            raise

    def in_active_nodes(self, root):
        my_names = set(self.get_network_addresses_and_names())
        active_nodes = [
            node.text for node in root.findall("./ActiveNodes/Node")
        ]
        for node in active_nodes:
            if node in my_names:
                return True
        return False

    def get_current_pm_num(self, root):
        # Find this node in the Module* tags, return the module number

        my_names = set(self.get_network_addresses_and_names())
        smc_node = root.find("./SystemModuleConfig")
        pm_count = int(smc_node.find("./ModuleCount3").text)
        for pm_num in range(1, pm_count + 1):
            ip_addr = smc_node.find(f"./ModuleIPAddr{pm_num}-1-3").text
            name = smc_node.find(f"./ModuleHostName{pm_num}-1-3").text
            if ip_addr in my_names:
                module_logger.info(f" -- Matching against ModuleIPAddr{pm_num}-1-3, which says {ip_addr}")
                return pm_num
            if name in my_names:
                module_logger.info(f" -- Matching against ModuleHostName{pm_num}-1-3, which says {name}")
                return pm_num
        raise Exception("Did not find my IP addresses or names in the SystemModuleConfig section")

    def rollback_config(self, config_filename: str = DEFAULT_MCS_CONF_PATH):
        """Rollback the configuration.

        Copyback the copy of the configuration file.

        : param config_filename: Columnstore config file path
        :rtype: dict
        """
        # TODO: Rollback doesn't restart needed processes?
        config_file = Path(config_filename)
        config_dir = config_file.resolve().parent
        backup_path = f"{config_dir}/{config_file.name}.cmapi.save"
        config_file_copy = Path(backup_path)
        if config_file_copy.exists():
            replace(backup_path, config_file)   # atomic replacement

    def get_current_config(self, config_filename: str = DEFAULT_MCS_CONF_PATH):
        """Retrievs current configuration.

        Read the config and convert it into bytes string.

        :rtype: string

        ..TODO: fix using self.get_current_config_root()
        """
        parser = etree.XMLParser(load_dtd=True)
        tree = etree.parse(config_filename, parser=parser)
        self.upgrade_config(tree=tree)
        # TODO: Unicode? UTF-8 may be?
        return etree.tostring(
            tree.getroot(), pretty_print=True, encoding='unicode'
        )

    def get_current_sm_config(
        self, config_filename: str = DEFAULT_SM_CONF_PATH
    ) -> str:
        """Retrievs current SM configuration

        Read the config and convert it into a string.

        :rtype: str
        """
        func_name = 'get_current_sm_config'
        sm_config_path = Path(config_filename)
        try:
            return sm_config_path.read_text(encoding='utf-8')
        except FileNotFoundError:
            module_logger.error(f"{func_name} SM config {config_filename} not found.")
            return ''

    def s3_enabled(self, config_filename: str = DEFAULT_SM_CONF_PATH) -> bool:
        """Checks if SM is enabled

        Reads SM config and checks if storage set to S3.
        It also checks for additional settings in the XML that must be set too.

        :rtype: bool
        """
        func_name = 's3_enabled'
        sm_config = configparser.ConfigParser()
        if len(sm_config.read(config_filename)) > 0:
            storage = sm_config.get('ObjectStorage', 'service')
            if storage is None:
                storage = 'LocalStorage'

            if storage.lower() == 's3':
                config_root = self.get_current_config_root()
                if not config_root.find('./Installation/DBRootStorageType').text.lower() == "storagemanager":
                    module_logger.error(f"{func_name} DBRootStorageType.lower() != storagemanager")
                if not config_root.find('./StorageManager/Enabled').text.lower() == "y":
                    module_logger.error(f"{func_name} StorageManager/Enabled.lower() != y")
                if not config_root.find('./SystemConfig/DataFilePlugin').text == "libcloudio.so":
                    module_logger.error(f"{func_name} SystemConfig/DataFilePlugin != libcloudio.so")

                return True
        else:
            module_logger.error(f"{func_name} SM config {config_filename} not found.")

        return False

    def get_network_addresses(self):
        """Retrievs the list of the network addresses

        Generator that yields network interface addresses.

        :rtype: str
        """
        for ni in get_network_interfaces():
            for fam in [socket.AF_INET, socket.AF_INET6]:
                addrs = ni.addresses.get(fam)
                if addrs is not None:
                    for addr in addrs:
                        yield(addr)

    def get_network_addresses_and_names(self):
        """Retrievs the list of the network addresses, hostnames, and aliases

        Generator that yields network interface addresses, hostnames, and aliases

        :rtype: str
        """
        for ni in get_network_interfaces():
            for fam in [socket.AF_INET, socket.AF_INET6]:
                addrs = ni.addresses.get(fam)
                if addrs is not None:
                    for addr in addrs:
                        yield(addr)
                        try:
                            (host, aliases, _) = socket.gethostbyaddr(addr)
                        except:
                            continue
                        yield host
                        for alias in aliases:
                            yield alias

    def is_primary_node(self, root=None):
        """Checks if this node is the primary node.

        Reads the config and compares DBRM_Controller IP or
        hostname with the this node's IP and hostname.

        :rtype: bool
        """
        if root is None:
            root = self.get_current_config_root()

        primary_address = self.get_dbrm_conn_info(root)['IPAddr']
        return primary_address in self.get_network_addresses_and_names()

    def is_single_node(self,
                       root=None):
        """Checks if this node is the single node.

        Reads the config and compares DBRMMaster IP with the predefined localhost addresses.

        :rtype: bool
        """
        if root is None:
            root = self.get_current_config_root()

        master_address = self.get_dbrm_conn_info(root)['IPAddr']
        if master_address in ['127.0.0.1', 'localhost', '::1']:
            return True
        return False

    def get_new_module_id(self, new_root=None):
        """Retrieves new module id.

        Reads new XML config and searches IP belongs to this host in SystemModuleConfig.ModuleIPAddrX-1-3. X is the new module id.

        :rtype: int
        """
        func_name = 'get_new_module_id'
        current_module_id = read_module_id()

        if new_root is None:
            module_logger.error(f'{func_name} Empty new XML tree root.')
            return current_module_id

        net_address = self.get_module_net_address(new_root, current_module_id)
        # Use getaddrinfo in case of IPv6
        if net_address is None:
            module_logger.error(f'{func_name} Columnstore.xml has unknown value in SystemModuleConfig.\
ModuleIPAddr{current_module_id}-1-3.')
            raise RuntimeError('net_address is None.')
        if socket.gethostbyname(net_address) in self.get_network_addresses():
            return current_module_id

        # Use getaddrinfo in case of IPv6
        # This fires for a added node when node id changes from 1 to something
        for module_entry in self.get_modules_addresses(new_root):
            if module_entry['addr'] is not None:
                net_addr = socket.gethostbyname(module_entry['addr'])
                if net_addr in self.get_network_addresses():
                    module_logger.debug(f'{func_name} New module id \
{module_entry["id"]}')
                    return int(module_entry['id'])

        module_logger.error(f'{func_name} Cannot find new module id for \
the node.')
        raise RuntimeError('Fail to find module id.')

    def get_module_net_address(self, root=None, module_id: int = None):
        """Retrieves the module network address.

        Reads new XML config and returns IP or
        hostname from SystemModuleConfig.ModuleIPAddrX-1-3.

        :rtype: string
        """
        func_name = 'get_module_net_address'
        if module_id is None:
            module_id = read_module_id()

        if root is None:
            module_logger.error(f'{func_name} Empty XML root.')
            return

        for el in root:
            if el.tag == 'SystemModuleConfig':
                for subel in el:
                    if subel.tag == f'ModuleIPAddr{module_id}-1-3':
                        module_logger.debug(
                            f'{func_name} Module {module_id} '
                            f'network address {subel.text}'
                        )
                        return subel.text

        module_logger.error(f'{func_name} Module {module_id} was not found.')
        return

    def get_modules_addresses(self, root=None):
        """Retrieves the modules network addresses.

        Reads new XML config and returns IP or hostname from
        SystemModuleConfig.ModuleIPAddrX-1-3 with X being a node id.

        :rtype: dict
        """
        func_name = 'get_module_addresses'
        if root is None:
            module_logger.error(f'{func_name} Empty XML root.')
            return None

        regex_string = 'ModuleIPAddr[0-9]+-1-3'
        for el in root:
            if el.tag == 'SystemModuleConfig':
                for subel in el:
                    module_ip_m = re.match(regex_string, subel.tag)
                    if module_ip_m is not None:
                        id_m = re.search('[0-9]+', module_ip_m.group(0))
                        module_id = id_m.group(0)
                        module_logger.debug(
                            f'{func_name} Module {module_id} '
                            f'network address {subel.text}'
                        )
                        yield {'addr': subel.text, 'id': module_id}


        module_logger.error(f'{func_name} Module {module_id} was not found.')
        return None

    def dbroots_to_create(self, root=None, module_id:int=None):
        """Generates dbroot ids if there are new dbroots to be created/renamed

        Reads new XML config and generates dbroot ids if on-disk dbroots differs from the config's set.

        :rtype: generator of strings
        """
        func_name = 'dbroots_to_create'
        if module_id is None:
            module_id = read_module_id()

        if root is None:
            module_logger.error(f'{func_name} Empty XML root.')
            return

        current_dbroot_list = get_dbroots_list()

        regex_string = f'ModuleDBRootID{module_id}-[0-9]+-3'
        for el in root:
            if el.tag == 'SystemModuleConfig':
                for subel in el:
                    if re.match(regex_string, subel.tag) is not None and \
int(subel.text) not in current_dbroot_list:
                        module_logger.debug(f'{func_name} Module {module_id} \
has dbroot {subel.text}')
                        yield int(subel.text)
        return

    def get_all_dbroots(self, root):
        dbroots = []
        smc_node = root.find("./SystemModuleConfig")
        mod_count = int(smc_node.find("./ModuleCount3").text)
        for i in range(1, mod_count+1):
            for j in range(1, int(smc_node.find(f"./ModuleDBRootCount{i}-3").text) + 1):
                dbroots.append(smc_node.find(f"./ModuleDBRootID{i}-{j}-3").text)
        return dbroots
