"""Module with helpers functions.

TODO: remove NodeConfig usage and move to arguments (eg. nc or root)
"""

import asyncio
import configparser
import datetime
import logging
import os
import socket
import time
from collections import namedtuple
from functools import partial
from random import random
from shutil import copyfile
from typing import Tuple, Optional
from urllib.parse import urlencode, urlunparse

import aiohttp
import lxml.objectify
import requests

from cmapi_server.exceptions import CMAPIBasicError
# Bug in pylint https://github.com/PyCQA/pylint/issues/4584
requests.packages.urllib3.disable_warnings()  # pylint: disable=no-member

from cmapi_server.constants import (
    CMAPI_CONF_PATH, CMAPI_DEFAULT_CONF_PATH, DEFAULT_MCS_CONF_PATH,
    DEFAULT_SM_CONF_PATH, LOCALHOSTS
)
from cmapi_server.handlers.cej import CEJPasswordHandler
from cmapi_server.managers.process import MCSProcessManager
from mcs_node_control.models.node_config import NodeConfig


def get_id() -> int:
    """Generate pseudo random id for transaction.

    :return: id for internal transaction
    :rtype: int

    ..TODO: need to change transaction id format and generation method?
    """
    return int(random() * 1000000)


def start_transaction(
    config_filename: str = CMAPI_CONF_PATH,
    cs_config_filename: str = DEFAULT_MCS_CONF_PATH,
    extra_nodes: Optional[list] = None,
    remove_nodes: Optional[list] = None,
    optional_nodes: Optional[list] = None,
    txn_id: Optional[int] = None,
    timeout: float = 300.0
):
    """Start internal CMAPI transaction.

    Returns (success, txnid, nodes). success = True means it successfully
    started a transaction, False means it didn't. If True, then txnid will have
    the transaction ID and the list of nodes the transaction was started on.
    If False, the txnid and nodes have undefined values.

    :param config_filename: cmapi config filepath,
                            defaults to CMAPI_CONF_PATH
    :type config_filename: str, optional
    :param cs_config_filename: columnstore xml config filepath,
                               defaults to DEFAULT_MCS_CONF_PATH
    :type cs_config_filename: str, optional
    :param extra_nodes: extra nodes, defaults to None
    :type extra_nodes: Optional[list], optional
    :param remove_nodes: remove nodes, defaults to None
    :type remove_nodes: Optional[list], optional
    :param optional_nodes: optional nodes, defaults to None
    :type optional_nodes: Optional[list], optional
    :param txn_id: id for transaction to start, defaults to None
    :type txn_id: Optional[int], optional
    :param timeout: time in seconds for cmapi transaction lock before it ends
                    automatically, defaults to 300
    :type timeout: float, optional
    :return: (success, txn_id, nodes)
    :rtype: tuple[bool, int, list[str]]
    """
    if txn_id is None:
        txn_id = get_id()
    # TODO: Somehow change that logic for eg using several input types
    #       (str\list\set) and detect which one we got.
    extra_nodes = extra_nodes or []
    remove_nodes = remove_nodes or []
    optional_nodes = optional_nodes or []

    cfg_parser = get_config_parser(config_filename)
    api_key = get_current_key(cfg_parser)

    version = get_version()

    headers = {'x-api-key': api_key}
    body = {'id' : txn_id}
    final_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)

    success = False
    while datetime.datetime.now() < final_time and not success:
        successes = []

        # it's painful to look at, but if this call fails to get a lock on
        # every server, it may be because a node went down, and the config file
        # was updated.  So, update the list on every iteration.
        #
        # There is a race here between reading the config and getting the txn.
        # What can stop it with the current design is using a mutex here,
        # and having config updates come from only one node.
        # For changes coming from failover, this will be true.
        #
        # There is also a race on the config file in general.
        # Need to read it before you can get a lock, and need to lock it before
        # it can be read reliably. Resolution TBD. File locking?  Retries?

        # TODO: need to work with data types of nodes here
        unfiltered_nodes = [
            *get_active_nodes(cs_config_filename),
            *extra_nodes,
            *optional_nodes
        ]
        tmp_active_nodes = {
            node for node in unfiltered_nodes
            if node not in remove_nodes
        }
        active_nodes = set()

        # resolve localhost addrs
        for node in tmp_active_nodes:
            if node in ['127.0.0.1', 'localhost', '::1']:
                active_nodes.add(socket.gethostbyname(socket.gethostname()))
            else:
                active_nodes.add(node)
        # this copy will be updated if an optional node can't be reached
        real_active_nodes = set(active_nodes)
        logging.trace(f'Active nodes on start transaction {active_nodes}')
        for node in active_nodes:
            url = f'https://{node}:8640/cmapi/{version}/node/begin'
            node_success = False
            logging.trace(f'Processing node "{node}"')
            for retry in range(5):
                logging.trace(
                    f'In {retry} attempt for node {node} and active nodes var '
                    f'is {active_nodes} and real active nodes var is '
                    f'{real_active_nodes}'
                )
                try:
                    # who knows how much time has gone by...
                    # Update timeout to keep nodes in sync +/-
                    body['timeout'] = (
                        final_time - datetime.datetime.now()
                    ).seconds
                    r = requests.put(
                        url, verify=False, headers=headers, json=body,
                        timeout=10
                    )

                    # a 4xx error from our endpoint;
                    # likely another txn is running
                    # Breaking here will cause a rollback on nodes we have
                    # successfully started a txn on so far. Then it will try
                    # again to get a transaction on all nodes. Put all
                    # conditions where that is the desired behavior here.
                    if int(r.status_code / 100) == 4:
                        logging.debug(
                             'Got a 4xx error while beginning transaction '
                            f'with response text {r.text}'
                        )
                        break  # TODO: useless, got break in finally statement
                    # TODO: is there any case to separate 4xx
                    #       from all other error codes
                    r.raise_for_status()
                    node_success = True
                    break
                except requests.Timeout:
                    logging.warning(
                        f'start_transaction(): timeout on node {node}'
                    )
                except Exception:
                    logging.warning(
                         'start_transaction(): got error during request '
                        f'to node {node}',
                        exc_info=True
                    )
                finally:
                    if not node_success and node in optional_nodes:
                        logging.info(
                            f'start_transaction(): node {node} is optional;'
                            'ignoring the error'
                        )
                        real_active_nodes.remove(node)
                        break

                # wait 1 sec and try on this node again
                time.sleep(1)

            if not node_success and node not in optional_nodes:
                rollback_txn_attempt(api_key, version, txn_id, successes)
                # wait up to 5 secs and try the whole thing again
                time.sleep(random() * 5)
                break
            elif node_success:
                successes.append(node)

        # TODO: a little more work needs to be done here.  If not all of the active-nodes
        # are up when start is called, this will fail.  It should succeed if 'enough' nodes
        # are up (> 50%).
        success = (len(successes) == len(real_active_nodes))

    return (success, txn_id, successes)

def rollback_txn_attempt(key, version, txnid, nodes):
    headers = {'x-api-key': key}
    body = {'id': txnid}
    for node in nodes:
        url = f"https://{node}:8640/cmapi/{version}/node/rollback"
        for retry in range(5):
            try:
                r = requests.put(
                    url, verify=False, headers=headers, json=body, timeout=5
                )
                r.raise_for_status()
            except requests.Timeout:
                logging.warning(
                    f'rollback_txn_attempt(): timeout on node "{node}"'
                )
            except Exception:
                logging.error(
                    (
                        f'rollback_txn_attempt(): got unrecognised error '
                        f'during request to "{node}".'
                    ),
                    exc_info=True
                )
            else:
                break
            time.sleep(1)

# on a failure to rollback or commit a txn on a subset of nodes, what are the options?
#  - open a new txn and revert the changes on the nodes that respond
#  - go forward with the subset.  If those nodes are still up, they will have a config that is out of sync.
#     -> for now, going to assume that the node went down, and that when it comes back up, its config
#        will be sync'd

def rollback_transaction(
    id, config_filename=CMAPI_CONF_PATH,
    cs_config_filename=DEFAULT_MCS_CONF_PATH, nodes=None
):
    cfg_parser = get_config_parser(config_filename)
    key = get_current_key(cfg_parser)
    version = get_version()
    if nodes is None:
        nodes = get_active_nodes(cs_config_filename)
    rollback_txn_attempt(key, version, id, nodes)


def commit_transaction(
    id, config_filename=CMAPI_CONF_PATH,
    cs_config_filename=DEFAULT_MCS_CONF_PATH, nodes = None
):
    cfg_parser = get_config_parser(config_filename)
    key = get_current_key(cfg_parser)
    version = get_version()
    if nodes is None:
        nodes = get_active_nodes(cs_config_filename)

    headers = {'x-api-key': key}
    body = {'id': id}

    for node in nodes:
        url = f"https://{node}:8640/cmapi/{version}/node/commit"
        for retry in range(5):
            try:
                r = requests.put(url, verify = False, headers = headers, json = body, timeout = 5)
                r.raise_for_status()
            except requests.Timeout as e:
                logging.warning(f"commit_transaction(): timeout on node {node}")
            except Exception as e:
                logging.warning(f"commit_transaction(): got error during request to {node}: {str(e)}")
            else:
                break
            time.sleep(1)


def broadcast_new_config(
    cs_config_filename: str = DEFAULT_MCS_CONF_PATH,
    cmapi_config_filename: str = CMAPI_CONF_PATH,
    sm_config_filename: str = DEFAULT_SM_CONF_PATH,
    test_mode: bool = False,
    nodes: Optional[list] = None,
    timeout: Optional[int] = None
) -> None:
    """Send new config to nodes in async way.

    :param cs_config_filename: Columnstore.xml path,
                               defaults to DEFAULT_MCS_CONF_PATH
    :type cs_config_filename: str, optional
    :param cmapi_config_filename: cmapi config path,
                                  defaults to CMAPI_CONF_PATH
    :type cmapi_config_filename: str, optional
    :param sm_config_filename: storage manager config path,
                               defaults to DEFAULT_SM_CONF_PATH
    :type sm_config_filename: str, optional
    :param test_mode: for test purposes, defaults to False TODO: remove
    :type test_mode: bool, optional
    :param nodes: nodes list for config put, defaults to None
    :type nodes: Optional[list], optional
    :param timeout: timeout passing to gracefully stop DMLProc TODO: for next
                    releases. Could affect all logic of broadcacting new config
    :type timeout: Optional[int], optional
    :raises CMAPIBasicError: If Broadcasting config to nodes failed with errors
    """

    # TODO: move this from multiple places to one, eg to helpers
    cfg_parser = get_config_parser(cmapi_config_filename)
    key = get_current_key(cfg_parser)
    version = get_version()
    if nodes is None:
        nodes = get_active_nodes(cs_config_filename)

    nc = NodeConfig()
    root = nc.get_current_config_root(config_filename=cs_config_filename)
    with open(cs_config_filename) as f:
        config_text = f.read()

    with open(sm_config_filename) as f:
        sm_config_text = f.read()

    headers = {'x-api-key': key}
    body = {
        'manager': root.find('./ClusterManager').text,
        'revision': root.find('./ConfigRevision').text,
        'timeout': 300,
        'config': config_text,
        'cs_config_filename': cs_config_filename,
        'sm_config_filename': sm_config_filename,
        'sm_config': sm_config_text
    }
    # TODO: remove test mode here and replace it by mock in tests
    if test_mode:
        body['test'] = True

    async def update_config(node: str, headers: dict, body: dict) -> None:
        """Update remote node config asyncronously.

        :param node: node ip address or hostname
        :type node: str
        :param headers: headers for request
        :type headers: dict
        :param body: request body
        :type body: dict
        :raises CMAPIBasicError: If request failed by status code
        :raises CMAPIBasicError: If request failed by some undefined error
        :raises CMAPIBasicError: If request failed by timeout
        :raises CMAPIBasicError: If undefined error happened
        """
        url = f'https://{node}:8640/cmapi/{version}/node/config'

        async with aiohttp.ClientSession() as session:
            try:
                async with session.put(
                    url, headers=headers, json=body, ssl=False, timeout=120
                ) as response:
                    response.raise_for_status()
                logging.info(f'Node "{node}" config put successfull.')
            except aiohttp.ClientResponseError as err:
                message = (
                    f'Node "{node}" config put failed with status: '
                    f'"{err.status}" and message: {err.message}'
                )
                logging.error(message)
                raise CMAPIBasicError(message)
            except aiohttp.ClientError as err:
                message = (
                        f'Node "{node}" config put failed with ClientError: '
                        f'{str(err)}'
                    )
                logging.error(message)
                raise CMAPIBasicError(message)
            except asyncio.TimeoutError:
                message = f'Node "{node}" config put failed by Timeout: '
                logging.error(message)
                raise CMAPIBasicError(message)
            except Exception as err:
                message = (
                    f'Node "{node}" config put failed by undefined exception: '
                    f'{str(err)}'
                )
                logging.error(message)
                raise CMAPIBasicError(message)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    tasks = [
        update_config(node, headers, body)
        for node in nodes
    ]
    error_messages = []
    results = loop.run_until_complete(
        asyncio.gather(*tasks, return_exceptions=True)
    )
    for result in results:
        if isinstance(result, Exception):
            error_messages.append(result.message)
    loop.close()
    if error_messages:
        errors_str = ', '.join(error_messages)
        final_message = (
            f'Broadcasting config to nodes failed with errors: {errors_str}'
        )
        raise CMAPIBasicError(final_message)


# Might be more appropriate to put these in node_manipulation?
def update_revision_and_manager(
    input_config_filename: Optional[str] = None,
    output_config_filename: Optional[str] = None
):
    """Update MCS xml config revision and cluster manager tags.

    :param input_config_filename: , defaults to None
    :type input_config_filename: Optional[str], optional
    :param output_config_filename: _description_, defaults to None
    :type output_config_filename: Optional[str], optional
    """
    nc = NodeConfig()

    if input_config_filename is None:
        root = nc.get_current_config_root()
    else:
        root = nc.get_current_config_root(input_config_filename)

    try:
        rev_node = root.find('./ConfigRevision')
        cur_revision = int(rev_node.text) + 1
        rev_node.text = str(cur_revision)
        root.find('./ClusterManager').text = str(
            nc.get_module_net_address(root=root, module_id=1)
        )
    except Exception:
        logging.error(
            'Caught exception while updating MCS config revision cluster '
            'manager tag, will not write new config',
            exc_info=True
        )
    else:
        if output_config_filename is None:
            nc.write_config(root)
        else:
            nc.write_config(root, filename = output_config_filename)


def get_config_parser(
    config_filepath: str = CMAPI_CONF_PATH
) -> configparser.ConfigParser:
    """Get config parser from cmapi server ini config file.

    :param config_filename: cmapi server conf path, defaults to CMAPI_CONF_PATH
    :type config_filename: str, optional
    :return: config parser
    :rtype: configparser.ConfigParser
    """
    cfg_parser = configparser.ConfigParser()
    try:
        with open(config_filepath, 'r', encoding='utf-8') as cfg_file:
            cfg_parser.read_file(cfg_file)
    except PermissionError as e:
        # TODO: looks like it's useless here, because of creating config
        #       from default on cmapi server startup
        #       Anyway looks like it has to raise error and then
        #       return 500 error
        logging.error(
            'CMAPI cannot create configuration file. '
            'API key stored in memory only.',
            exc_info=True
        )
    return cfg_parser


def save_cmapi_conf_file(cfg_parser, config_filepath: str = CMAPI_CONF_PATH):
    """Save config file from config parser.

    :param cfg_parser: config parser to save
    :type cfg_parser: configparser.ConfigParser
    :param config_filepath: cmapi config filepath, defaults to CMAPI_CONF_PATH
    :type config_filepath: str, optional
    """
    try:
        with open(config_filepath, 'w', encoding='utf-8') as cfg_file:
            cfg_parser.write(cfg_file)
    except PermissionError:
        logging.error(
            'CMAPI cannot save configuration file due to permissions. '
            'Some values still can be stored in memory.',
            exc_info=True
        )


def get_active_nodes(config: str = DEFAULT_MCS_CONF_PATH) -> list:
    """Get active nodes from Columnstore.xml.

    Actually this is only names of nodes by which node have been added.

    :param config: xml  config path, defaults to DEFAULT_MCS_CONF_PATH
    :type config: str, optional
    :return: active nodes
    :rtype: list
    """
    nc = NodeConfig()
    root = nc.get_current_config_root(config, upgrade=False)
    nodes = root.findall('./ActiveNodes/Node')
    return [ node.text for node in nodes ]


def get_desired_nodes(config=DEFAULT_MCS_CONF_PATH):
    nc = NodeConfig()
    root = nc.get_current_config_root(config, upgrade=False)
    nodes = root.findall("./DesiredNodes/Node")
    return [ node.text for node in nodes ]


def in_maintenance_state(config=DEFAULT_MCS_CONF_PATH):
    nc = NodeConfig()
    root = nc.get_current_config_root(config, upgrade=False)
    raw_state = root.find('./Maintenance')
    # if no Maintainace tag in xml config found
    state = False
    if raw_state is not None:
        # returns True on "true" string else return false
        state = lxml.objectify.BoolElement(raw_state.text)
    return state


def get_current_key(config_parser):
    """Get API key for cmapi server endpoints from ini config.

    :param config_parser: config parser
    :type config_parser: configparser.ConfigParser
    :return: api key
    :rtype: str
    """
    # ConfigParser reading value as is , for eg with quotes
    return config_parser.get('Authentication', 'x-api-key', fallback='')


def get_version():
    from cmapi_server.controllers.dispatcher import _version
    return _version


def get_dbroots(node, config=DEFAULT_MCS_CONF_PATH):
    # TODO: somehow duplicated with NodeConfig.get_all_dbroots?
    nc = NodeConfig()
    root = nc.get_current_config_root(config)
    dbroots = []
    smc_node = root.find('./SystemModuleConfig')
    mod_count = int(smc_node.find('./ModuleCount3').text)
    for i in range(1, mod_count+1):
        ip_addr = smc_node.find(f'./ModuleIPAddr{i}-1-3').text
        hostname = smc_node.find(f'./ModuleHostName{i}-1-3').text
        node_fqdn = socket.gethostbyaddr(hostname)[0]

        if node in LOCALHOSTS and hostname != 'localhost':
            node = socket.gethostbyaddr(socket.gethostname())[0]
        elif node not in LOCALHOSTS and hostname == 'localhost':
            # hostname will only be loclahost if we are in one node cluster
            hostname = socket.gethostbyaddr(socket.gethostname())[0]


        if node == ip_addr or node == hostname or node == node_fqdn:
            for j in range(
                1, int(smc_node.find(f"./ModuleDBRootCount{i}-3").text) + 1
            ):
                dbroots.append(
                    smc_node.find(f"./ModuleDBRootID{i}-{j}-3").text
                )
    return dbroots


def get_current_config_file(
    config_filename=DEFAULT_MCS_CONF_PATH,
    cmapi_config_filename=CMAPI_CONF_PATH
):
    """Start a transaction on all DesiredNodes, which are all optional.

    - the transaction prevents config changes from being made at the same time
    - get the config from each node
    - discard config files for different clusters
    - call put_config on the config file with the highest revision number found
    - end the transaction
    """

    logging.info('get_current_config_file(): seeking the current config file')

    cfg_parser = get_config_parser(cmapi_config_filename)
    key = get_current_key(cfg_parser)
    nc = NodeConfig()
    root = nc.get_current_config_root(config_filename = config_filename)
    # TODO: here we got set of ip addresses of DesiredNodes
    #       but after that we convert them to list and send as
    #       an optional_nodes argument to start_transaction()
    #       So need to work with data type of nodes.
    desired_nodes = {
        node.text for node in root.findall('./DesiredNodes/Node')
    }
    if len(desired_nodes) <= 1:
        return True

    current_rev = int(root.find('ConfigRevision').text)
    cluster_name = root.find('ClusterName').text
    highest_rev = current_rev
    highest_node = 'localhost'
    highest_config = nc.to_string(root)

    # TODO: data type of optional_nodes set -> list
    #       Need to work with it inside and outside of start_transaction
    (success, txn_id, nodes) = start_transaction(
        cs_config_filename=config_filename,
        optional_nodes=list(desired_nodes)
    )
    localhost_aliases = set(nc.get_network_addresses_and_names())
    other_nodes = set(nodes) - localhost_aliases
    if not success or len(other_nodes) == 0:
        if success:
            commit_transaction(txn_id, nodes = nodes)
        return False

    nodes_in_same_cluster = 0
    for node in nodes:
        if node in localhost_aliases:
            continue

        headers = {'x-api-key' : key}
        url = f'https://{node}:8640/cmapi/{get_version()}/node/config'
        try:
            r = requests.get(url, verify=False, headers=headers, timeout=5)
            r.raise_for_status()
            config = r.json()['config']
        except Exception as e:
            logging.warning(
                'get_current_config_file(): got an error fetching the '
                f'config file from {node}: {str(e)}'
            )
            continue
        tmp_root = nc.get_root_from_string(config)
        name_node = tmp_root.find('ClusterName')
        if name_node is None or name_node.text != cluster_name:
            continue
        nodes_in_same_cluster += 1
        rev_node = tmp_root.find('ConfigRevision')
        if rev_node is None or int(rev_node.text) <= highest_rev:
            continue
        highest_rev = int(rev_node.text)
        highest_config = config
        highest_node = node

    nc.apply_config(config_filename=config_filename, xml_string=highest_config)
    # TODO: do we need restart node here?
    commit_transaction(txn_id, cs_config_filename=config_filename, nodes=nodes)

    # todo, we might want stronger criteria for a large cluster.
    # Right now we want to reach at least one other node
    # (if there is another node)
    if len(desired_nodes) > 1 and nodes_in_same_cluster < 1:
        logging.error(
            'get_current_config_file(): failed to contact enough nodes '
            f'in my cluster ({cluster_name}) to reliably retrieve a current '
            'configuration file.  Manual intervention may be required.'
        )
        # TODO: addition error handling.
        try:
            MCSProcessManager.stop_node(is_primary=nc.is_primary_node())
        except CMAPIBasicError as err:
            logging.error(err.message)
        return False

    if highest_rev != current_rev:
        logging.info(
            'get_current_config_file(): Accepted the config file from'
            f' {highest_node}'
        )
    else:
        logging.info(
            'get_current_config_file(): This node has the current config file'
        )
    return True


def wait_for_deactivation_or_put_config(
    config_mtime, config_filename=DEFAULT_MCS_CONF_PATH
):
    '''
    if a multi-node cluster...
    Wait for either a put_config operation (as determined by monitoring the mtime of config_filename),
    or wait for this node to be removed from active_nodes,
    or wait for a period long enough for this to be considered a 'long' outage (30s right now, as determined
    by the failover code.  TODO: make that time period configurable...

    Activating failover after one of these three events should allow this node to join the cluster either as part
    of the failover behavior, or as part of the cluster-wide start cmd.
    '''

    my_names = set(NodeConfig().get_network_addresses_and_names())
    desired_nodes = get_desired_nodes(config_filename)
    if len(desired_nodes) == 1 and desired_nodes[0] in my_names:
        logging.info("wait_for_deactivation_or_put_config: Single-node cluster, safe to continue")
        return

    final_time = datetime.datetime.now() + datetime.timedelta(seconds = 40)
    while config_mtime == os.path.getmtime(config_filename) and \
      len(my_names.intersection(set(get_active_nodes(config_filename)))) > 0 and \
      datetime.datetime.now() < final_time:
        logging.info("wait_for_deactivation_or_put_config: Waiting...")
        time.sleep(5)

    if config_mtime != os.path.getmtime(config_filename):
        logging.info("wait_for_deactivation_or_put_config: A new config was received, safe to continue.")
    elif len(my_names.intersection(set(get_active_nodes(config_filename)))) == 0:
        logging.info("wait_for_deactivation_or_put_config: Was removed from the cluster, safe to continue.")
    else:
        logging.info("wait_for_deactivation_or_put_config: Time limit reached, continuing.")


# This isn't used currently.  Remove once we decide there is no need for it.
def if_primary_restart(
    config_filename=DEFAULT_MCS_CONF_PATH,
    cmapi_config_filename=CMAPI_CONF_PATH
):
    nc = NodeConfig()
    root = nc.get_current_config_root(config_filename = config_filename)
    primary_node = root.find("./PrimaryNode").text

    if primary_node not in nc.get_network_addresses_and_names():
        return

    cfg_parser = get_config_parser(cmapi_config_filename)
    key = get_current_key(cfg_parser)
    headers = { "x-api-key" : key }
    body = { "config": config_filename }

    logging.info("if_primary_restart(): restarting the cluster.")
    url = f"https://{primary_node}:8640/cmapi/{get_version()}/cluster/start"
    endtime = datetime.datetime.now() + datetime.timedelta(seconds = 600)   # questionable how long to retry
    success = False
    while not success and datetime.datetime.now() < endtime:
        try:
            response = requests.put(url, verify = False, headers = headers, json = body, timeout = 60)
            response.raise_for_status()
            success = True
        except Exception as e:
            logging.warning(f"if_primary_restart(): failed to start the cluster, got {str(e)}")
            time.sleep(10)
    if not success:
        logging.error(f"if_primary_restart(): failed to start the cluster.  Manual intervention is required.")


def get_cej_info(config_root):
    """Get CEJ (Cross Engine Join) info.

    Get credentials from CrossEngineSupport section in Columnstore.xml .
    Decrypt CEJ user password if needed.

    :param config_root: config root element from Columnstore.xml file
    :type config_root: lxml.Element
    :return: cej_host, cej_port, cej_username, cej_password
    :rtype: tuple
    """
    cej_node = config_root.find('./CrossEngineSupport')
    cej_host = cej_node.find('Host').text or '127.0.0.1'
    cej_port = cej_node.find('Port').text or '3306'
    cej_username = cej_node.find('./User').text
    cej_password = cej_node.find('./Password').text or ''

    if not cej_username:
        logging.error(
            'Columnstore.xml has an empty CrossEngineSupport.User tag'
        )
    if not cej_password:
        logging.warning(
            'Columnstore.xml has an empty CrossEngineSupport.Password tag'
        )

    if CEJPasswordHandler.secretsfile_exists():
        cej_password = CEJPasswordHandler.decrypt_password(cej_password)

    return cej_host, cej_port, cej_username, cej_password


def system_ready(config_filename=DEFAULT_MCS_CONF_PATH):
    """Indicates whether the node is ready to accept queries.

    :param config_filename: columnstore xml config filepath,
                            defaults to DEFAULT_MCS_CONF_PATH
    :type config_filename: str, optional
    :return: tuple of 2 booleans
    :rtype: tuple
    """
    nc = NodeConfig()
    root = nc.get_current_config_root(config_filename)
    host, port, username, password = get_cej_info(root)

    if username is None:
        # Second False indicates not to retry inside calling function's
        # retry loop
        return False, False

    cmd = (
        f"/usr/bin/mariadb -h '{host}' "
                         f"-P '{port}' "
                         f"-u '{username}' "
                         f"--password='{password}' "
                          "-sN -e "
                          "\"SELECT mcssystemready();\""
    )

    import subprocess
    ret = subprocess.run(cmd, stdout=subprocess.PIPE, shell = True)
    if ret.returncode == 0:
        response = ret.stdout.decode("utf-8").strip()
        if response == '1':
            return True, False
        else:
            return False, True
    return False, False


def cmapi_config_check(cmapi_conf_path: str = CMAPI_CONF_PATH):
    """Check if cmapi config file exists and copy default config if not.

    :param cmapi_conf_path: cmapi conf path, defaults to CMAPI_CONF_PATH
    :type cmapi_conf_path: str, optional
    """
    if not os.path.exists(cmapi_conf_path):
        logging.info(
            f'There is no config file at "{cmapi_conf_path}". '
            f'So copy default config from {CMAPI_DEFAULT_CONF_PATH} there.'
        )
        copyfile(CMAPI_DEFAULT_CONF_PATH, cmapi_conf_path)


def dequote(input_str: str) -> str:
    """Dequote input string.

    If a string has single or double quotes around it, remove them.
    Make sure the pair of quotes match.
    If a matching pair of quotes is not found, return the string unchanged.

    :param input_str: input probably quoted string
    :type input_str: str
    :return: unquoted string
    :rtype: str
    """
    if (
        len(input_str) >= 2 and
        input_str[0] == input_str[-1]
    ) and input_str.startswith(("'", '"')):
        return input_str[1:-1]
    return input_str


def get_dispatcher_name_and_path(
        config_parser: configparser.ConfigParser
    ) -> Tuple[str, str]:
    """Get dispatcher name and path from cmapi conf file.

    :param config_parser: cmapi conf file parser
    :type config_parser: configparser.ConfigParser
    :return: dispatcher name and path strings
    :rtype: tuple[str, str]
    """
    dispatcher_name = dequote(
        config_parser.get('Dispatcher', 'name', fallback='systemd')
    )
    # TODO: used only for next releases for CustomDispatcher class
    #       remove if useless
    dispatcher_path = dequote(
        config_parser.get('Dispatcher', 'path', fallback='')
    )
    return dispatcher_name, dispatcher_path


def build_url(
        base_url: str, query_params: dict, scheme: str = 'https',
        path: str = '', params: str = '', fragment: str = '',
        port: Optional[int] = None
) -> str:
    """Build url with query params.

    :param base_url: base url address
    :type base_url: str
    :param query_params: query params
    :type query_params: dict
    :param scheme: url scheme, defaults to 'https'
    :type scheme: str, optional
    :param path: url path, defaults to ''
    :type path: str, optional
    :param params: params, defaults to ''
    :type params: str, optional
    :param fragment: fragment, defaults to ''
    :type fragment: str, optional
    :param port: port for base url, defaults to None
    :type port: Optional[int], optional
    :return: url with query params
    :rtype: str
    """
    # namedtuple to match the internal signature of urlunparse
    Components = namedtuple(
        typename='Components',
        field_names=['scheme', 'netloc', 'path', 'params', 'query', 'fragment']
    )
    return urlunparse(
        Components(
            scheme=scheme,
            netloc=f'{base_url}:{port}' if port else base_url,
            path=path,
            params=params,
            query=urlencode(query_params),
            fragment=fragment
        )
    )
