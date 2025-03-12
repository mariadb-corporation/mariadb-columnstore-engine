'''
This file contains functions to manipulate the CS config file to add a node,
remove a node, etc.  Should be synchronized externally.
'''

import datetime
import logging
import os
import shutil
import socket
import subprocess
import time
from typing import Optional

import requests
from lxml import etree
from mcs_node_control.models.node_config import NodeConfig
from tracing.traced_session import get_traced_session

from cmapi_server import helpers
from cmapi_server.constants import (
    CMAPI_CONF_PATH,
    CMAPI_SINGLE_NODE_XML,
    DEFAULT_MCS_CONF_PATH,
    LOCALHOST_IPS,
    LOCALHOSTS,
    MCS_DATA_PATH,
)
from cmapi_server.managers.network import NetworkManager

PMS_NODE_PORT = '8620'
EXEMGR_NODE_PORT = '8601'


# TODO: add some description of the public interfaces...
#       Split this file or change the structure and move functions out of here


def switch_node_maintenance(
    maintenance_state: bool,
    input_config_filename: str = DEFAULT_MCS_CONF_PATH,
    output_config_filename: str = DEFAULT_MCS_CONF_PATH,
):
    """Change maintenance flag value in Columnstore xml config file.

    :param maintenance_state: state of maintenance flag
    :type maintenance_state: bool
    :param input_config_filename: mcs input config path,
                                  defaults to DEFAULT_MCS_CONF_PATH
    :type input_config_filename: str, optional
    :param output_config_filename: mcs output config path,
                                   defaults to DEFAULT_MCS_CONF_PATH
    :type output_config_filename: str, optional
    """
    node_config = NodeConfig()
    config_root = node_config.get_current_config_root(input_config_filename)
    maintenance_element = config_root.find('Maintenance')
    # should be done in upgrade_from_v0, but looks like better to doublecheck
    if maintenance_element is None:
        maintenance_element = etree.SubElement(config_root, 'Maintenance')
    maintenance_element.text = str(maintenance_state).lower()
    node_config.write_config(config_root, filename=output_config_filename)
    # TODO: probably move publishing to cherrypy.engine failover channel here?

def add_node(
    node: str, input_config_filename: str = DEFAULT_MCS_CONF_PATH,
    output_config_filename: Optional[str] = None,
    use_rebalance_dbroots: bool = True,
    read_replica: bool = False,
):
    """Add node to a cluster.

    Check whether or not '127.0.0.1' or 'localhost' are in the config file, and
    if so, replace those instances with this node's external hostname
    Do we need to detect IP addresses given as node, and use the hostname?
        - if we're always using hostnames or always using addrs everywhere
          it won't matter
    Add to the PMS section
    Add an ExeMgr section
    Add the DBRM workers
    Add the writeengineservers
    Add "Module*" keys
    Move DBRoots (moved to a separate function)
    Update CEJ to point to ExeMgr1 (for now)
    Update the list of active nodes

    :param node: node address or hostname
    :type node: str
    :param input_config_filename: mcs input config path,
                                  defaults to DEFAULT_MCS_CONF_PATH
    :type input_config_filename: str, optional
    :param output_config_filename: mcs output config path, defaults to None
    :type output_config_filename: Optional[str], optional
    :param use_rebalance_dbroots: rebalance dbroots or not, defaults to True
    :type use_rebalance_dbroots: bool, optional
    """
    node_config = NodeConfig()
    c_root = node_config.get_current_config_root(input_config_filename)

    logging.info('Adding node %s', node)
    # If a hostname (not IP) is provided, ensure fwd/rev DNS consistency.
    # Skip validation for localhost aliases to preserve legacy single-node flows.
    if not NetworkManager.is_ip(node) and not NetworkManager.is_only_loopback_hostname(node):
        NetworkManager.validate_hostname_fwd_rev(node)

    try:
        if not _replace_localhost(c_root, node):
            ip4, _ = NetworkManager.resolve_ip_and_hostname(node)
            pm_num = _add_node_to_PMS(c_root, ip4)

            if not read_replica:
                _add_WES(c_root, pm_num, ip4)
            else:
                logging.info('Node is read replica, skipping WES addition.')

            _add_DBRM_Worker(c_root, ip4)
            _add_Module_entries(c_root, ip4)
            _add_active_node(c_root, ip4)
            _add_node_to_ExeMgrs(c_root, ip4)
            if use_rebalance_dbroots:
                if not read_replica:
                    _rebalance_dbroots(c_root)
                    _move_primary_node(c_root)

            if NodeConfig().get_read_replicas(c_root):
                update_dbroots_of_read_replicas(c_root)
    except Exception:
        logging.error(
            'Caught exception while adding node, config file is unchanged',
            exc_info=True
        )
        raise
    else:
        if output_config_filename is None:
            node_config.write_config(c_root)
        else:
            node_config.write_config(c_root, filename=output_config_filename)


def remove_node(
    node: str, input_config_filename: str = DEFAULT_MCS_CONF_PATH,
    output_config_filename: Optional[str] = None,
    deactivate_only: bool = False,
    use_rebalance_dbroots: bool = True, **kwargs
):
    """Remove node from a cluster.

    - Rebuild the PMS section w/o node
    - Remove the DBRM_Worker entry
    - Remove the WES entry
    - Rebuild the "Module*" entries w/o node
    - Update the list of active / inactive / desired nodes

    :param node: node address or hostname
    :type node: str
    :param input_config_filename: mcs input config path,
                                  defaults to DEFAULT_MCS_CONF_PATH
    :type input_config_filename: str, optional
    :param output_config_filename: mcs output config path, defaults to None
    :type output_config_filename: Optional[str], optional
    :param deactivate_only: indicates whether the node is being removed
                            completely from the cluster, or whether it has gone
                            offline and should still be monitored in case it
                            comes back.
                            Note!  this does not pick a new primary node,
                            use the move_primary_node() fcn to change that.,
                            defaults to True
    :type deactivate_only: bool, optional
    :param use_rebalance_dbroots: rebalance dbroots or not, defaults to True
    :type use_rebalance_dbroots: bool, optional
    """
    node_config = NodeConfig()
    c_root = node_config.get_current_config_root(input_config_filename)
    logging.info('Removing node %s', node)

    try:
        active_nodes = helpers.get_active_nodes(input_config_filename)

        ip4, _ = NetworkManager.resolve_ip_and_hostname(node)

        if len(active_nodes) > 1:
            pm_num = _remove_node_from_PMS(c_root, ip4)

            is_read_replica = ip4 in node_config.get_read_replicas(c_root)
            if not is_read_replica:
                _remove_WES(c_root, pm_num)

            _remove_DBRM_Worker(c_root, ip4)
            _remove_Module_entries(c_root, ip4)
            _remove_from_ExeMgrs(c_root, ip4)

            if deactivate_only:
                _deactivate_node(c_root, ip4)
            else:
                # TODO: unspecific name, need to think of a better one
                _remove_node(c_root, node)

            if use_rebalance_dbroots and not is_read_replica:
                _rebalance_dbroots(c_root)
                _move_primary_node(c_root)

            if NodeConfig().get_read_replicas(c_root):
                update_dbroots_of_read_replicas(c_root)
        else:
            # TODO:
            #   - IMO undefined behaviour here. Removing one single node
            #     in some cases can produces Single node cluster.
            #   - No MCS services stopped after removing node.
            #
            # reproduce:
            #   add node by ip then remove using localhost, got one active node
            #   127.0.0.1. If use same name got no active nodes but all working
            #   mcs processes.
            shutil.copyfile(
                CMAPI_SINGLE_NODE_XML,
                output_config_filename
                if output_config_filename
                else input_config_filename
            )
            return

    except Exception:
        logging.error(
            'remove_node(): Caught exception, did not modify the config file',
            exc_info=True
        )
        raise
    else:
        if output_config_filename is None:
            node_config.write_config(c_root)
        else:
            node_config.write_config(c_root, filename=output_config_filename)


def rebalance_dbroots(
    input_config_filename: Optional[str] = None,
    output_config_filename: Optional[str] = None
) -> None:
    """Rebalance dbroots between nodes.

    :param input_config_filename: input mcs config path, defaults to None
    :type input_config_filename: Optional[str], optional
    :param output_config_filename: oputput mcs config path, defaults to None
    :type output_config_filename: Optional[str], optional
    :rases: Exception if error happens while rebalancing.
    """
    node_config = NodeConfig()
    if input_config_filename is None:
        c_root = node_config.get_current_config_root()
    else:
        c_root = node_config.get_current_config_root(
            config_filename=input_config_filename
        )

    try:
        _rebalance_dbroots(c_root)
    except Exception:
        logging.error(
            'Caught exception while rebalancing dbroots, did not modify '
            'the config file.',
            exc_info=True
        )
        raise
    else:
        if output_config_filename is None:
            node_config.write_config(c_root)
        else:
            node_config.write_config(c_root, filename=output_config_filename)

# all params are optional.  If node_id is unset, it will add a dbroot but not attach it to a node.
# if node_id is set, it will attach the new dbroot to that node.  Node_id should be either
# 'pm1' 'PM1' or '1'.  Those three all refer to node 1 as identified by the Module* entries in the
# config file.  TBD whether we need a different identifier for the node.  Maybe the hostname instead
#
# returns the id of the new dbroot on success
# raises an exception on error
def add_dbroot(input_config_filename = None, output_config_filename = None, host = None) -> int:
    node_config = NodeConfig()
    if input_config_filename is None:
        c_root = node_config.get_current_config_root()
    else:
        c_root = node_config.get_current_config_root(config_filename = input_config_filename)

    try:
        ret = _add_dbroot(c_root, host)
    except Exception as e:
        logging.error(f"add_dbroot(): Caught exception: '{str(e)}', did not modify the config file")
        raise

    if output_config_filename is None:
        node_config.write_config(c_root)
    else:
        node_config.write_config(c_root, filename = output_config_filename)
    return ret

def move_primary_node(
    input_config_filename=None, output_config_filename=None, **kwargs
):
    node_config = NodeConfig()
    if input_config_filename is None:
        c_root = node_config.get_current_config_root()
    else:
        c_root = node_config.get_current_config_root(
            config_filename=input_config_filename
        )

    try:
        _move_primary_node(c_root)
    except Exception:
        logging.error(
            'move_primary_node(): did not modify the config file',
            exc_info=True
        )
        raise
    else:
        if output_config_filename is None:
            node_config.write_config(c_root)
        else:
            node_config.write_config(c_root, filename=output_config_filename)


def find_dbroot1(root):
    smc_node = root.find("./SystemModuleConfig")
    pm_count = int(smc_node.find("./ModuleCount3").text)
    for pm_num in range(1, pm_count + 1):
        dbroot_count = int(smc_node.find(f"./ModuleDBRootCount{pm_num}-3").text)
        for dbroot_num in range(1, dbroot_count + 1):
            dbroot = smc_node.find(f"./ModuleDBRootID{pm_num}-{dbroot_num}-3").text
            if dbroot == "1":
                name = smc_node.find(f"ModuleHostName{pm_num}-1-3").text
                addr = smc_node.find(f"ModuleIPAddr{pm_num}-1-3").text
                return (name, addr)
    raise NodeNotFoundException("Could not find dbroot 1 in the list of dbroot assignments!")


def _move_primary_node(root):
    '''
    Verify new_primary is in the list of active nodes

    Change ExeMgr1
    Change CEJ
    Change DMLProc
    Change DDLProc
    Change Contollernode
    Change PrimaryNode
    '''

    hostname, ip4 = new_primary = find_dbroot1(root)
    logging.info(f"_move_primary_node(): dbroot 1 is assigned to {new_primary}")
    active_nodes = root.findall("./ActiveNodes/Node")
    found = False
    for node in active_nodes:
        if node.text in (hostname, ip4):
            found = True
            break
    if not found:
        raise NodeNotFoundException(f"{(hostname, ip4)} is not in the list of active nodes")

    root.find("./ExeMgr1/IPAddr").text = ip4
    root.find("./DMLProc/IPAddr").text = ip4
    root.find("./DDLProc/IPAddr").text = ip4
    # keep controllernode as hostname
    # b/c if IP used got troubles on a SKYSQL side
    # on the other hand if hostname/fqdn used customers with wrong address
    # resolving can got troubles on their side
    # related issues: MCOL-4804, MCOL-4440, MCOL-5017, DBAAS-7442
    root.find("./DBRM_Controller/IPAddr").text = hostname
    root.find("./PrimaryNode").text = hostname


def _add_active_node(root, node):
    '''
    if in inactiveNodes, delete it there
    if not in desiredNodes, add it there
    if not in activeNodes, add it there

    We keep IP address for consistency. If hostname is used (for example, in old configs),
    we replace it with the IP address.
    '''

    ip4, hostname = NetworkManager.resolve_ip_and_hostname(node)
    # If reverse lookup failed, hostname may be None. Use the IP as a
    # fallback so removal by hostname also works consistently.
    if hostname is None:
        hostname = ip4

    # Remove both hostname and IP form before adding IP to avoid checking if
    #   some of them is already there. Then we add by IP
    desired_nodes = root.find("./DesiredNodes")
    __remove_helper(desired_nodes, hostname)
    __remove_helper(desired_nodes, ip4)
    etree.SubElement(desired_nodes, "Node").text = ip4

    # The same with active nodes
    active_nodes = root.find("./ActiveNodes")
    __remove_helper(active_nodes, hostname)
    __remove_helper(active_nodes, ip4)
    etree.SubElement(active_nodes, "Node").text = ip4

    # Remove from Inactive
    inactive_nodes = root.find("./InactiveNodes")
    __remove_helper(inactive_nodes, hostname)
    __remove_helper(inactive_nodes, ip4)


def __remove_helper(parent_node, node):
    nodes = list(parent_node.findall("./Node"))
    for n in nodes:
        if n.text == node:
            parent_node.remove(n)


def _remove_node(root, node):
    '''
    remove node from DesiredNodes, InactiveNodes, ActiveNodes
    '''
    # Remove both hostname and IPv4 forms
    ip4, hostname = NetworkManager.resolve_ip_and_hostname(node)
    # If reverse lookup failed, normalize hostname to ip so we always try
    # removing both variants from lists.
    if hostname is None:
        hostname = ip4
    for lst in (
        root.find("./DesiredNodes"),
        root.find("./InactiveNodes"),
        root.find("./ActiveNodes"),
    ):
        __remove_helper(lst, hostname)
        __remove_helper(lst, ip4)


# This moves a node from ActiveNodes to InactiveNodes
def _deactivate_node(root, node):
    """Move node from ActiveNodes to InactiveNodes. Store as IPv4."""
    ip4, hostname = NetworkManager.resolve_ip_and_hostname(node)
    if hostname is None:
        hostname = ip4

    active_nodes = root.find("./ActiveNodes")
    __remove_helper(active_nodes, hostname)
    __remove_helper(active_nodes, ip4)

    # Remove both hostname and IP form before adding IP to avoid checking if
    #   some of them is already there. Then we add by IP
    inactive_nodes = root.find("./InactiveNodes")
    __remove_helper(inactive_nodes, hostname)
    __remove_helper(inactive_nodes, ip4)
    etree.SubElement(inactive_nodes, "Node").text = ip4


def _add_dbroot(root, host) -> int:
    """Add a dbroot to the system.

    Attach it to node_id if it's specified.
    Increment the nextdbrootid.

    :param root: xml config root
    :type root: xml.Tree.ElementTree.Element
    :param host: host
    :type host: any?
    :raises NodeNotFoundException: if node not in a cluster
    :return: Added dbroot number
    :rtype: int
    """
    sysconf_node = root.find('./SystemConfig')
    dbroot_count_node = sysconf_node.find('./DBRootCount')
    dbroot_count = int(dbroot_count_node.text)
    dbroot_count += 1
    dbroot_count_node.text = str(dbroot_count)

    next_dbroot_node = root.find('./NextDBRootId')
    next_dbroot_id = int(next_dbroot_node.text)

    # Use DBRoot path from Columnstore.xml
    dbroot1_path = sysconf_node.find('./DBRoot1')
    if dbroot1_path is not None:
        dbroots_path = os.path.dirname(dbroot1_path.text)
    else:
        dbroots_path = MCS_DATA_PATH

    etree.SubElement(
        sysconf_node, f'DBRoot{next_dbroot_id}'
    ).text = os.path.join(dbroots_path, f'data{next_dbroot_id}')

    current_dbroot_id = next_dbroot_id

    # find an unused dbroot id from 1-99
    for i in range(1, 100):
        if sysconf_node.find(f'./DBRoot{i}') is None:
            next_dbroot_id = i
            break
    next_dbroot_node.text = str(next_dbroot_id)

    if host is None:
        return current_dbroot_id

    # Attach it to the specified node

    # get the existing dbroot info for pm X
    smc_node = root.find('./SystemModuleConfig')

    # find the node id we're trying to add to
    mod_count = int(smc_node.find('./ModuleCount3').text)
    node_id = 0
    for i in range(1, mod_count+1):
        ip_addr = smc_node.find(f'./ModuleIPAddr{i}-1-3').text
        hostname = smc_node.find(f'./ModuleHostName{i}-1-3').text
        if host == ip_addr or host == hostname:
            node_id = i
            break
    if node_id == 0:
        raise NodeNotFoundException(
            f'Host {host} is not currently part of the cluster'
        )

    dbroot_count_node = smc_node.find(f'./ModuleDBRootCount{node_id}-3')
    dbroot_count = int(dbroot_count_node.text)
    dbroot_count += 1
    etree.SubElement(
        smc_node, f'ModuleDBRootID{node_id}-{dbroot_count}-3'
    ).text = str(current_dbroot_id)
    dbroot_count_node.text = str(dbroot_count)
    return current_dbroot_id


# check if the node is the master, maxscale might have chose new master
# so use CEJ User to check via mariadb if the node has slave connections
def is_master():
    node_config = NodeConfig()
    root = node_config.get_current_config_root()
    host, port, username, password = helpers.get_cej_info(root)

    if username is None:
        return False

    cmd = (
        f"/usr/bin/mariadb -h '{host}' "
                         f"-P '{port}' "
                         f"-u '{username}' "
                         f"--password='{password}' "
                          "-sN -e "
                          "\"SELECT COUNT(1) AS slave_threads "
                          "FROM information_schema.PROCESSLIST "
                          "WHERE USER = 'system user' "
                          "AND COMMAND LIKE 'Slave%';\""
    )

    ret = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True)
    if ret.returncode == 0:
        response = ret.stdout.decode("utf-8").strip()
        # Primary will have no slave_threads
        if response == '0':
            return True
        else:
            return False
    return None


def unassign_dbroot1(root):
    smc_node = root.find("./SystemModuleConfig")
    pm_count = int(smc_node.find("./ModuleCount3").text)
    owner_id = 0
    for i in range(1, pm_count + 1):
        dbroot_count_node = smc_node.find(f"./ModuleDBRootCount{i}-3")
        dbroot_count = int(dbroot_count_node.text)
        dbroot_list = []
        for j in range(1, dbroot_count + 1):
            dbroot = smc_node.find(f"./ModuleDBRootID{i}-{j}-3").text
            if dbroot == "1":
                owner_id = i                  # this node has dbroot 1
            else:
                dbroot_list.append(dbroot)    # the dbroot assignments to keep
        if owner_id != 0:
            break
    if owner_id == 0:
        return     # dbroot 1 is already unassigned (primary node must have gone down)

    # remove the dbroot entries for node owner_id
    for i in range(1, dbroot_count + 1):
        doomed_node = smc_node.find(f"./ModuleDBRootID{owner_id}-{i}-3")
        smc_node.remove(doomed_node)
    # create the new dbroot entries
    dbroot_count_node.text = str(len(dbroot_list))
    i = 1
    for dbroot in dbroot_list:
        etree.SubElement(smc_node, f"ModuleDBRootID{owner_id}-{i}-3").text = dbroot
        i += 1


def _get_existing_db_roots(root: etree.Element) -> list[int]:
    '''Get all the existing dbroot IDs from the config file'''
    # There can be holes in the dbroot numbering, so can't just scan from [1-dbroot_count]
    # Going to scan from 1-99 instead
    sysconf_node = root.find("./SystemConfig")
    existing_dbroots = []
    for num in range(1, 100):
        node = sysconf_node.find(f"./DBRoot{num}")
        if node is not None:
            existing_dbroots.append(num)
    return existing_dbroots


def _rebalance_dbroots(root, test_mode=False):
    # TODO: add code to detect whether we are using shared storage or not.  If not, exit
    # without doing anything.

    '''
    this will be a pita
    identify unassigned dbroots
    assign those to the node with the fewest dbroots

    then,
    id the nodes with the most dbroots and the least dbroots
    when most - least <= 1, we're done
    else, move a dbroot from the node with the most to the one with the least

    Not going to try to be clever about the alg.  We're dealing with small lists.
    Aiming for simplicity and comprehensibility.
    '''

    '''
    Borderline hack here.  We are going to remove dbroot1 from its current host so that
    it will always look for the current replication master and always resolve the discrepancy
    between what maxscale and what cmapi choose for the primary/master node.

    We know of 2 constraints around primary node selection.
        1) dbroot 1 has to be assigned to the primary node b/c controllernode and possibly
           other processes try to access data1 directly
        2) The primary node has to be the same as the master replication node chosen by
           Maxscale b/c there is a schema sync issue

    Right now the code is doing this because we discovered these restrictions late in the dev
    process:
        1) unassign dbroot 1 to force new primary node selection
        2) look for the master repl node
        3) put dbroot 1 on it
        4) look for dbroot 1
        5) make it the primary node

    Once we are done with the constraint discovery process, we should refactor this.
    '''
    unassign_dbroot1(root)

    current_mapping = get_current_dbroot_mapping(root)
    sysconf_node = root.find("./SystemConfig")
    existing_dbroots = _get_existing_db_roots(root)

    # assign the unassigned dbroots
    unassigned_dbroots = set(existing_dbroots) - set(current_mapping[0])

    '''
    If dbroot 1 is in the unassigned list, then we need to put it on the node that will be the next
    primary node.  Need to choose the same node as maxscale here.  For now, we will wait until
    maxscale does the replication reconfig, then choose the new master.  Later,
    we will choose the node using the same method that maxscale does to avoid
    the need to go through the mariadb client.

    If this process goes on longer than 1 min, then we will assume there is no maxscale,
    so this should choose where dbroot 1 should go itself.
    '''
    if 1 in unassigned_dbroots:
        logging.info("Waiting for Maxscale to choose the new repl master...")
        smc_node = root.find("./SystemModuleConfig")
        # Maybe iterate over the list of ModuleHostName tags instead
        pm_count = int(smc_node.find("./ModuleCount3").text)
        found_master = False
        final_time = datetime.datetime.now() + datetime.timedelta(seconds = 30)

        # skip this if in test mode.
        retry = True
        while not found_master and datetime.datetime.now() < final_time and not test_mode:
            for node_num in range(1, pm_count + 1):
                node_ip = smc_node.find(f"./ModuleIPAddr{node_num}-1-3").text
                node_name = smc_node.find(f"./ModuleHostName{node_num}-1-3").text
                if pm_count == 1:
                    found_master = True
                else:
                    cfg_parser = helpers.get_config_parser(CMAPI_CONF_PATH)
                    key = helpers.get_current_key(cfg_parser)
                    version = helpers.get_version()
                    headers = {'x-api-key': key}
                    url = f"https://{node_ip}:8640/cmapi/{version}/node/new_primary"
                    try:
                        r = get_traced_session().request(
                            'GET', url, verify=False, headers=headers, timeout=10
                        )
                        r.raise_for_status()
                        r = r.json()
                        is_primary = r['is_primary']
                        if is_primary is None:
                            # neither True nor False
                            # possible node is not ready, leave retry as-is
                            pass
                        elif is_primary:
                            found_master = True
                    except requests.exceptions.Timeout:
                        # timed out
                        # possible node is not ready, leave retry as-is
                        pass
                    except Exception:
                        retry = False

                if not found_master:
                    if not retry:
                        logging.info("There was an error retrieving replication master")
                        break
                    else:
                        continue

                # assign dbroot 1 to this node, put at the front of the list
                current_mapping[node_num].insert(0, 1)
                unassigned_dbroots.remove(1)
                logging.info(f"The new replication master is {node_name}")
                break
            if not found_master:
                logging.info("New repl master has not been chosen yet")
                time.sleep(1)
        if not found_master:
            logging.info("Maxscale has not reconfigured repl master, continuing...")

    for dbroot in unassigned_dbroots:
        (_min, min_index) = _find_min_max_length(current_mapping)[0]
        if dbroot != 1:
            current_mapping[min_index].append(dbroot)
        else:
            # make dbroot 1 move only if the new node goes down by putting it at the front of the list
            current_mapping[min_index].insert(0, dbroot)

    # balance the distribution
    ((_min, min_index), (_max, max_index)) = _find_min_max_length(current_mapping)
    while _max - _min > 1:
        current_mapping[min_index].append(current_mapping[max_index].pop(-1))
        ((_min, min_index), (_max, max_index)) = _find_min_max_length(current_mapping)

    # write the new mapping
    sysconf_node = root.find("./SystemModuleConfig")
    for i in range(1, len(current_mapping)):
        dbroot_count_node = sysconf_node.find(f"./ModuleDBRootCount{i}-3")
        # delete the original assignments for node i
        for dbroot_num in range(1, int(dbroot_count_node.text) + 1):
            old_node = sysconf_node.find(f"./ModuleDBRootID{i}-{dbroot_num}-3")
            sysconf_node.remove(old_node)

        # write the new assignments for node i
        dbroot_count_node.text = str(len(current_mapping[i]))
        for dbroot_num in range(len(current_mapping[i])):
            etree.SubElement(sysconf_node, f"ModuleDBRootID{i}-{dbroot_num+1}-3").text = str(current_mapping[i][dbroot_num])


# returns ((min, index-of-min), (max, index-of-max))
def _find_min_max_length(mappings):
    _min = 100
    min_index = -1
    _max = -1
    max_index = -1
    for i in range(1, len(mappings)):
        this_len = len(mappings[i])
        if this_len < _min:
            _min = this_len
            min_index = i
        if this_len > _max:
            _max = this_len
            max_index = i
    return ((_min, min_index), (_max, max_index))


# returns a list indexed by node_num, where the value is a list of dbroot ids (ints)
# so, list[1] == [1, 2, 3] would mean that node 1 has dbroots 1, 2, & 3.
# To align the list with node IDs, element 0 is a list with all of the assigned dbroots
def get_current_dbroot_mapping(root):
    '''
    get the current node count
    iterate over the ModuleDBRootIDX-Y-3 entries to build the mapping
    '''

    smc_node = root.find("./SystemModuleConfig")
    node_count = int(smc_node.find("./ModuleCount3").text)
    current_mapping = [[]]

    for i in range(1, node_count + 1):
        dbroot_count = int(smc_node.find(f"./ModuleDBRootCount{i}-3").text)
        dbroots_on_this_node = []
        for dbroot_num in range(1, dbroot_count + 1):
            dbroot_id = int(smc_node.find(f"./ModuleDBRootID{i}-{dbroot_num}-3").text)
            dbroots_on_this_node.append(dbroot_id)
            current_mapping[0].append(dbroot_id)
        current_mapping.append(dbroots_on_this_node)

    return current_mapping


def _remove_Module_entries(root, node):
    '''
        figure out which module_id node is
        store info from the other modules
            ModuleIPAddr
            ModuleHostName
            ModuleDBRootCount
            ModuleDBRootIDs
        delete all of those tags
        write new versions
        write new ModuleCount3 value
        write new NextNodeID
    '''
    smc_node = root.find("./SystemModuleConfig")
    mod_count_node = smc_node.find("./ModuleCount3")
    current_module_count = int(mod_count_node.text)
    node_module_id = 0

    for num in range(1, current_module_count + 1):
        m_ip_node = smc_node.find(f"./ModuleIPAddr{num}-1-3")
        m_name_node = smc_node.find(f"./ModuleHostName{num}-1-3")
        if node == m_ip_node.text or node == m_name_node.text:
            node_module_id = num
            break
    if node_module_id == 0:
        logging.warning(f"remove_module_entries(): did not find node {node} in the Module* entries of the config file")
        return

    # Get the existing info except for node, remove the existing nodes
    new_module_info = []
    for num in range(1, current_module_count + 1):
        m_ip_node = smc_node.find(f"./ModuleIPAddr{num}-1-3")
        m_name_node = smc_node.find(f"./ModuleHostName{num}-1-3")
        dbrc_node = smc_node.find(f"./ModuleDBRootCount{num}-3")
        dbr_count = int(dbrc_node.text)
        smc_node.remove(dbrc_node)
        dbroots = []
        for i in range(1, dbr_count + 1):
            dbr_node = smc_node.find(f"./ModuleDBRootID{num}-{i}-3")
            dbroots.append(dbr_node.text)
            smc_node.remove(dbr_node)

        if node != m_ip_node.text and node != m_name_node.text:
            new_module_info.append((m_ip_node.text, m_name_node.text, dbroots))

        smc_node.remove(m_ip_node)
        smc_node.remove(m_name_node)

    # Regenerate these entries
    current_module_count = len(new_module_info)
    for num in range(1, current_module_count + 1):
        (ip, name, dbroots) = new_module_info[num - 1]
        etree.SubElement(smc_node, f"ModuleIPAddr{num}-1-3").text = ip
        etree.SubElement(smc_node, f"ModuleHostName{num}-1-3").text = name
        etree.SubElement(smc_node, f"ModuleDBRootCount{num}-3").text = str(len(dbroots))
        for i in range(1, len(dbroots) + 1):
            etree.SubElement(smc_node, f"ModuleDBRootID{num}-{i}-3").text = dbroots[i - 1]

    # update NextNodeId and ModuleCount3
    nni_node = root.find("./NextNodeId")
    nni_node.text = str(current_module_count + 1)
    mod_count_node.text = str(current_module_count)


def _remove_WES(root, pm_num):
    '''
    Avoid gaps in pm numbering where possible.
    Read the existing pmX_WriteEngineServer entries except where X = pm_num,
    Delete them,
    Write new entries

    Not sure yet, but I believe for the dbroot -> PM mapping to work, the node # in the Module
    entries has to match the pm # in other fields.  They should be written consistently and intact
    already, but this is a guess at this point.  Short-term, a couple options. 1) Construct an argument
    that they are maintained consistently right now.  2) Add consistency checking logic, and on a mismatch,
    remove all affected sections and reconstruct them with add_node() and add_dbroot().

    Longer term, make the config file less stupid.  Ex:
    <PrimProcPort>
    <ExeMgrPort>
    ...
    <PrimaryNode>hostname<PrimaryNode>
    <PM1>
        <IPAddr>hostname-or-ipv4</IPAddr>
        <DBRoots>1,2,3</DBRoots>
    </PM1>
    ...


    ^^ The above is all we need to figure out where everything is and what each node should run
    '''

    pm_count = int(root.find("./PrimitiveServers/Count").text)
    pms = []
    # This is a bit of a hack.  We already decremented the pm count; need to add 2 to this loop instead of 1
    # to scan the full range of these entries [1, pm_count + 2)
    for i in range(1, pm_count + 2):
        node = root.find(f"./pm{i}_WriteEngineServer")
        if node is not None:
            if i != pm_num:
                pms.append(node.find("./IPAddr").text)
            root.remove(node)

    # Write the new entries
    for i in range(1, len(pms) + 1):
        wes = etree.SubElement(root, f"pm{i}_WriteEngineServer")
        etree.SubElement(wes, "IPAddr").text = pms[i - 1]
        etree.SubElement(wes, "Port").text = "8630"


def _remove_DBRM_Worker(root, node):
    '''
    regenerate the DBRM_Worker list without node
    update NumWorkers
    '''

    num = 1
    workers = []
    while True:
        w_node = root.find(f"./DBRM_Worker{num}")
        if w_node is not None:
            addr = w_node.find("./IPAddr").text
            if addr != "0.0.0.0" and addr != node:
                workers.append(addr)
            root.remove(w_node)
        else:
            break
        num += 1

    for num in range(len(workers)):
        w_node = etree.SubElement(root, f"DBRM_Worker{num+1}")
        etree.SubElement(w_node, "IPAddr").text = workers[num]
        etree.SubElement(w_node, "Port").text = "8700"
    root.find("./DBRM_Controller/NumWorkers").text = str(len(workers))


def _remove_from_ExeMgrs(root, node):
    """Remove the corresponding ExeMgrX section from the config."""
    num = 1
    ems = []
    # TODO: use loop by nodes count instead of "while True"
    while True:
        em_node = root.find(f"./ExeMgr{num}")
        if em_node is not None:
            addr = em_node.find("./IPAddr").text
            if addr != "0.0.0.0" and addr != node:
                ems.append(addr)
            root.remove(em_node)
        else:
            break
        num += 1

    for num in range(len(ems)):
        em_node = etree.SubElement(root, f"ExeMgr{num+1}")
        etree.SubElement(em_node, "IPAddr").text = ems[num]
        etree.SubElement(em_node, "Port").text = "8601"
        etree.SubElement(em_node, "Module").text = "unassigned"


def _remove_node_from_PMS(root, node):
    '''
    find the PM number we're removing
    replace existing PMS entries
    '''
    connections_per_pm = int(
        root.find("./PrimitiveServers/ConnectionsPerPrimProc").text
    )
    count_node = root.find("./PrimitiveServers/Count")
    pm_count = int(count_node.text)

    # get current list of PMs to avoid changing existing assignments
    pm_list = []
    pm_num = 0
    for num in range(1, pm_count+1):
        addr = root.find(f"./PMS{num}/IPAddr")
        if addr.text != node:
            pm_list.append(addr.text)
        else:
            pm_num = num

    if pm_num == 0:
        return 0

    # remove the existing PMS entries
    num = 1
    while True:
        pmsnode = root.find(f"./PMS{num}")
        if pmsnode is not None:
            root.remove(pmsnode)
        else:
            break
        num += 1

    # generate new list
    pm_count = len(pm_list)
    count_node.text = str(pm_count)
    pm_list.append(node)
    for num in range(pm_count*connections_per_pm):
        pmsnode = etree.SubElement(root, f"PMS{num+1}")
        addrnode = etree.SubElement(pmsnode, "IPAddr")
        addrnode.text = pm_list[num % pm_count]
        portnode = etree.SubElement(pmsnode, "Port")
        portnode.text = PMS_NODE_PORT

    return pm_num

def _add_Module_entries(root, node: str) -> None:
    '''
    get new node id
    add ModuleIPAddr, ModuleHostName, ModuleDBRootCount (don't set ModuleDBRootID* here)
    set ModuleCount3 and NextNodeId
    no need to rewrite existing entries for this fcn
    '''

    # XXXPAT: No guarantee these are the values used in the rest of the system.
    # TODO: what should we do with complicated network configs where node has
    #       several ips and\or several hostnames
    ip4, hostname = NetworkManager.resolve_ip_and_hostname(node)
    if hostname is None:
        logging.warning(f'Could not resolve hostname for {node}, using IP address as hostname')
        hostname = ip4
    logging.info(f'Using ip address {ip4} and hostname {hostname}')

    smc_node = root.find('./SystemModuleConfig')
    mod_count_node = smc_node.find('./ModuleCount3')
    nnid_node = root.find('./NextNodeId')
    nnid = int(nnid_node.text)
    current_module_count = int(mod_count_node.text)

    # look for existing entries and fix if they exist
    for i in range(1, nnid):
        curr_ip_node = smc_node.find(f'./ModuleIPAddr{i}-1-3')
        curr_name_node = smc_node.find(f'./ModuleHostName{i}-1-3')
        # TODO: NETWORK: seems it's useless even in very rare cases.
        #       Even simpler to rewrite resolved IP an Hostname
        # if we find a matching IP address, but it has a different hostname,
        # update the addr
        if curr_ip_node is not None and curr_ip_node.text == ip4:
            logging.info(f'Found ip address %s already at ModuleIPAddr{i}-1-3', ip4)
            if curr_name_node.text != hostname:
                logging.info(
                    'Hostname doesn\'t match, updating address to '
                    f'{ip4!r} / {hostname!r}'
                )
                smc_node.find(f'ModuleHostName{i}-1-3').text = hostname
            else:
                logging.info('No update for ModuleIPAddr{i}-1-3 is necessary')
                return

        # if we find a matching hostname, update the ip addr
        if curr_name_node is not None and curr_name_node.text == hostname:
            logging.info(
                f'Found existing entry for {hostname!r}, updating its '
                f'address to {ip4!r}'
            )
            curr_ip_node.text = ip4
            return

    etree.SubElement(smc_node, f'ModuleIPAddr{nnid}-1-3').text = ip4
    etree.SubElement(smc_node, f'ModuleHostName{nnid}-1-3').text = hostname
    etree.SubElement(smc_node, f'ModuleDBRootCount{nnid}-3').text = '0'
    mod_count_node.text = str(current_module_count + 1)
    nnid_node.text = str(nnid + 1)


def _add_WES(root, pm_num, node):
    """Add WriteEngineServer entry for a PM, storing canonical IPv4 address.
    `node` may be a hostname or an IP; we normalize to IPv4 to avoid
    mismatches when comparing against ModuleIPAddr entries.
    """
    ip4, _hostname = NetworkManager.resolve_ip_and_hostname(node)
    wes_node = etree.SubElement(root, f"pm{pm_num}_WriteEngineServer")
    etree.SubElement(wes_node, "IPAddr").text = ip4
    etree.SubElement(wes_node, "Port").text = "8630"


def _add_DBRM_Worker(root, node):
    '''
    find the highest numbered DBRM_Worker entry, or one that isn't used atm
    prune unused entries
    add this node at the end
    '''

    num = 1
    already_exists = False
    while True:
        e_node = root.find(f"./DBRM_Worker{num}")
        if e_node is None:
            break
        addr = e_node.find("./IPAddr").text
        if addr == "0.0.0.0":
            root.remove(e_node)
        elif addr == node:
            logging.info(f"_add_DBRM_Worker(): node {node} is already a worker node")
            already_exists = True
        num += 1

    if already_exists:
        return

    num_workers_node = root.find("./DBRM_Controller/NumWorkers")
    num_workers = int(num_workers_node.text) + 1
    brm_node = etree.SubElement(root, f"DBRM_Worker{num_workers}")
    etree.SubElement(brm_node, "Port").text = "8700"
    etree.SubElement(brm_node, "IPAddr").text = node
    num_workers_node.text = str(num_workers)


def _add_node_to_ExeMgrs(root, node):
    """Find the highest numbered ExeMgr entry, add this node at the end."""
    num = 1
    while True:
        e_node = root.find(f"./ExeMgr{num}")
        if e_node is None:
            break
        addr = e_node.find("./IPAddr")
        if addr.text == node:
            logging.info(f"_add_node_to_ExeMgrs(): node {node} already exists")
            return
        num += 1
    e_node = etree.SubElement(root, f"ExeMgr{num}")
    addr_node = etree.SubElement(e_node, "IPAddr")
    addr_node.text = node
    port_node = etree.SubElement(e_node, "Port")
    port_node.text = EXEMGR_NODE_PORT


def _add_node_to_PMS(root, node):
    '''
    the PMS section is a sequential list of connections descriptions

    For example, if ConnectionsPerPrimProc is 2, and the Count is 2, then
    the PMS entries look like this:

    PMS1 = connection 1 of PM 1
    PMS2 = connection 1 of PM 2

    The easiest way to add a node is probably to generate a whole new list.
    '''
    count_node = root.find('./PrimitiveServers/Count')
    pm_count = int(count_node.text)

    # get current list of PMs to avoid changing existing assignments
    pm_list = {}
    new_pm_num = 0
    for num in range(1, pm_count+1):
        addr = root.find(f'./PMS{num}/IPAddr')
        pm_list[num] = addr.text
        if addr.text == node and new_pm_num == 0:
            logging.info(f'_add_node_to_PMS(): node {node} already exists')
            new_pm_num = num

    # remove the existing PMS entries
    num = 1
    while True:
        pmsnode = root.find(f'./PMS{num}')
        if pmsnode is not None:
            root.remove(pmsnode)
        else:
            break
        num += 1

    # generate new list
    if new_pm_num == 0:
        pm_count += 1
        count_node.text = str(pm_count)
        pm_list[pm_count] = node
        new_pm_num = pm_count
    for num in range(pm_count):
        pmsnode = etree.SubElement(root, f'PMS{num+1}')
        addrnode = etree.SubElement(pmsnode, 'IPAddr')
        addrnode.text = pm_list[(num % pm_count) + 1]
        portnode = etree.SubElement(pmsnode, 'Port')
        portnode.text = PMS_NODE_PORT

    return new_pm_num

def _replace_localhost(root: etree.Element, node: str) -> bool:
    # if DBRM_Controller/IPAddr is 127.0.0.1 or localhost,
    # then replace all instances, else do nothing.
    controller_host = root.find('./DBRM_Controller/IPAddr')
    if controller_host.text not in LOCALHOSTS:
        logging.debug(
            'Nothing to replace, DBRM_Controller/IPAddr isn\'t localhost.'
        )
        return False

    # TODO use NetworkManager here
    # getaddrinfo returns list of 5-tuples (..., sockaddr)
    # use sockaddr to retrieve ip, sockaddr = (address, port) for AF_INET
    ipaddr = socket.getaddrinfo(node, 8640, family=socket.AF_INET)[0][-1][0]
    # signifies that node is an IP addr already
    if ipaddr == node:
        # use the primary hostname if given an ip addr
        hostname = socket.gethostbyaddr(ipaddr)[0]
    else:
        hostname = node   # use whatever name they gave us
    logging.info(
        f'add_node(): replacing 127.0.0.1/localhost with {ipaddr}/{hostname} '
        f'as this node\'s name. Be sure {hostname} resolves to {ipaddr} on '
         'all other nodes in the cluster.'
    )

    nodes_to_reassign = [n for n in root.findall('.//') if n.text in LOCALHOSTS]

    # Host field is contained within CrossEngineSupport User and QueryTele.
    # Leave these values as default (will be local IP)
    exclude = ['Host']
    for n in nodes_to_reassign:
        try:
            path = root.getroottree().getpath(n)
        except Exception:
            path = n.tag
        old_val = n.text

        if 'ModuleIPAddr' in n.tag:
            n.text = ipaddr
            logging.info(f"Replaced %s (was %s) with IP %s", path, old_val, ipaddr)
            continue
        if 'ModuleHostName' in n.tag:
            n.text = hostname
            logging.info(f"Replaced %s (was %s) with hostname %s", path, old_val, hostname)
            continue

        # Generic fields: replace localhost IPs with ipaddr, hostnames with hostname
        if n.tag not in exclude:
            is_local_ip = old_val in LOCALHOST_IPS
            new_val = ipaddr if is_local_ip else hostname
            if is_local_ip:
                new_val = ipaddr
                logging.info(f"Replaced %s (was %s) with IP %s", path, old_val, new_val)
            else:
                new_val = hostname
                logging.info(f"Replaced %s (was %s) with hostname %s", path, old_val, new_val)
            n.text = new_val

    old_controller = controller_host.text
    controller_host.text = hostname # keep controllernode as fqdn
    logging.info(f"Replaced %s (was %s) with hostname %s", './DBRM_Controller/IPAddr', old_controller, hostname)

    return True

# New Exception types
class NodeNotFoundException(Exception):
    pass


def get_pm_module_num_to_addr_map(root: etree.Element) -> dict[int, str]:
    """Get a mapping of PM module numbers to their IP addresses"""
    module_num_to_addr = {}
    smc_node = root.find("./SystemModuleConfig")
    mod_count = int(smc_node.find("./ModuleCount3").text)
    for i in range(1, mod_count + 1):
        ip_addr = smc_node.find(f"./ModuleIPAddr{i}-1-3").text
        module_num_to_addr[i] = ip_addr
    return module_num_to_addr


def update_dbroots_of_read_replicas(root: etree.Element) -> None:
    """Read replicas do not have their own dbroots, but they must have all the
    dbroots of the other nodes. Sets the list of dbroots of each read replica to
    the list of all dbroots in the cluster.
    """
    nc = NodeConfig()
    pm_num_to_addr = get_pm_module_num_to_addr_map(root)
    for read_replica in nc.get_read_replicas(root):
        # Get PM num by IP address
        this_ip_pm_num = None
        for pm_num, pm_addr in pm_num_to_addr.items():
            if pm_addr == read_replica:
                this_ip_pm_num = pm_num
                break

        if this_ip_pm_num is not None:
            # Add dbroots of other nodes to this read replica
            add_dbroots_of_other_nodes(root, this_ip_pm_num)
        else:  # This should not happen
            err_msg = f"Could not find PM number for read replica {read_replica}"
            logging.error(err_msg)
            raise NodeNotFoundException(err_msg)


def add_dbroots_of_other_nodes(root: etree.Element, module_num: int) -> None:
    """Adds all the dbroots listed in the config to this read replica"""
    existing_dbroots = _get_existing_db_roots(root)
    sysconf_node = root.find("./SystemModuleConfig")

    # Remove existing dbroots from this module
    remove_dbroots_of_node(root, module_num)

    # Write node's dbroot count
    dbroot_count_node = etree.SubElement(
        sysconf_node, f"ModuleDBRootCount{module_num}-3"
    )
    dbroot_count_node.text = str(len(existing_dbroots))

    # Write new dbroot IDs to the module mapping
    for i, dbroot_id in enumerate(existing_dbroots, start=1):
        dbroot_id_node = etree.SubElement(
            sysconf_node, f"ModuleDBRootID{module_num}-{i}-3"
        )
        dbroot_id_node.text = str(dbroot_id)

    logging.info(
        "Added %d dbroots to read replica %d: %s",
        len(existing_dbroots), module_num, sorted(existing_dbroots)
    )


def remove_dbroots_of_node(root: etree.Element, module_num: int) -> None:
    """Removes all the dbroots listed in the config from this read replica"""
    sysconf_node = root.find("./SystemModuleConfig")
    dbroot_count_node = sysconf_node.find(f"./ModuleDBRootCount{module_num}-3")
    if dbroot_count_node is not None:
        sysconf_node.remove(dbroot_count_node)

    # Remove existing dbroot IDs
    for i in range(1, 100):
        dbroot_id_node = sysconf_node.find(f"./ModuleDBRootID{module_num}-{i}-3")
        if dbroot_id_node is not None:
            sysconf_node.remove(dbroot_id_node)