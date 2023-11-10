"""Cluster typer application.

Formally this module contains all subcommands for "mcs cluster" cli command.
"""
import logging
import time
from typing import List, Optional

import pyotp
import requests
import typer
from typing_extensions import Annotated

from cmapi_server.constants import (
    CMAPI_CONF_PATH, DEFAULT_MCS_CONF_PATH, SECRET_KEY
)
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.handlers.cluster import ClusterHandler
from cmapi_server.helpers import (
    commit_transaction, get_config_parser, get_current_key, get_id,
    get_version, start_transaction, rollback_transaction, build_url
)
from mcs_cluster_tool.decorators import handle_output
from mcs_node_control.models.node_config import NodeConfig


logger = logging.getLogger('mcs_cli')
app = typer.Typer(
    help='MariaDB Columnstore cluster management command line tool.'
)
node_app = typer.Typer(help='Cluster nodes management.')
app.add_typer(node_app, name='node')
set_app = typer.Typer(help='Set cluster parameters.')
app.add_typer(set_app, name='set')


@app.command()
@handle_output
def status():
    """Get status information."""
    return ClusterHandler.status(logger=logger)


@app.command()
@handle_output
def stop(
    interactive: Annotated[
        bool,
        typer.Option(
            '--interactive/--no-interactive', '-i/-no-i',
            help=(
                'Use this option on a highly loaded cluster. '
                'It prevents data loss.'
            ),
        )
    ] = False,
    timeout: Annotated[
        int,
        typer.Option(
            '-t', '--timeout',
            help=(
                'Time in seconds to wait DMLproc gracefully stops. '
                'Use this with caution: on a working cluster '
                'low values could cause data loss. '
                'In interactive mode means delay time between promts.'
            )
        )
    ] = 15,
    force: Annotated[
        bool,
        typer.Option(
            '--force/--no-force', '-f/-no-f',
            help=(
                'Force stopping a cluster.'
                'WARNING: It could cause data corruption or/and data loss.'
            ),
            #TODO: hide from help till not investigated in decreased timeout
            #      affect
            hidden=True
        )
    ] = False
):
    """Stop the Columnstore cluster."""
    transaction_id = None
    if interactive:
        # TODO: for standalone cli tool need to change primary detection
        #       method. Partially move logic below to ClusterController
        nc = NodeConfig()
        root = nc.get_current_config_root(
            config_filename=DEFAULT_MCS_CONF_PATH
        )
        primary_node = root.find("./PrimaryNode").text

        cfg_parser = get_config_parser(CMAPI_CONF_PATH)
        api_key = get_current_key(cfg_parser)

        version = get_version()

        headers = {'x-api-key': api_key}

        # start_time = str(datetime.now())
        transaction_id = get_id()

        try:
            suceeded, transaction_id, successes = start_transaction(
                cs_config_filename=DEFAULT_MCS_CONF_PATH, id=transaction_id
            )
        except Exception as err:
            rollback_transaction(
                transaction_id, cs_config_filename=DEFAULT_MCS_CONF_PATH
            )
            raise CMAPIBasicError(
                'Error while starting the transaction.'
            ) from err
        if not suceeded:
            rollback_transaction(
                transaction_id, cs_config_filename=DEFAULT_MCS_CONF_PATH
            )
            raise CMAPIBasicError('Starting transaction isn\'t successful.')

        if suceeded and len(successes) == 0:
            rollback_transaction(
                transaction_id, cs_config_filename=DEFAULT_MCS_CONF_PATH
            )
            raise CMAPIBasicError('There are no nodes in the cluster.')

        body = {'force': False, 'timeout': timeout}
        url = f'https://{primary_node}:8640/cmapi/{version}/node/stop_dmlproc'
        try:
            resp = requests.put(
                url, verify=False, headers=headers, json=body,
                timeout=timeout+1
            )
            resp.raise_for_status()
        except Exception as err:
            rollback_transaction(
                transaction_id, cs_config_filename=DEFAULT_MCS_CONF_PATH
            )
            raise CMAPIBasicError(
                f'Error while stopping DMLProc on primary node.'
            ) from err

        force = True
        while True:
            time.sleep(timeout)
            url = build_url(
                base_url=primary_node, port=8640,
                query_params={'process_name': 'DMLProc'},
                path=f'cmapi/{version}/node/is_process_running',
            )
            try:
                resp = requests.get(
                    url, verify=False, headers=headers, timeout=timeout
                )
                resp.raise_for_status()
            except Exception as err:
                rollback_transaction(
                    transaction_id, cs_config_filename=DEFAULT_MCS_CONF_PATH
                )
                raise CMAPIBasicError(
                    f'Error while getting mcs DMLProc status.'
                ) from err

            # check DMLPRoc state
            # if ended, show message and break
            dmlproc_running = resp.json()['running']
            if not dmlproc_running:
                print(
                    'DMLProc stopped gracefully. '
                    'Continue stopping other processes.'
                )
                break
            else:
                force = typer.confirm(
                    'DMLProc still working. '
                    'Do you want to force stop? WARNING: It could cause data '
                    'loss and broken cluster.',
                    prompt_suffix=' '
                )
                if force:
                    break
                else:
                    continue

    if force:
        # TODO: investigate more on how changing the hardcoded timeout
        #       could affect put_config (helpers.py broadcast_config) operation
        timeout = 0
    return ClusterHandler.shutdown(
        logger=logger, transaction_id=transaction_id
    )


@app.command()
@handle_output
def start():
    """Start the Columnstore cluster."""
    return ClusterHandler.start(logger=logger)


@app.command()
@handle_output
def restart():
    """Restart the Columnstore cluster."""
    stop_result = ClusterHandler.shutdown(logger=logger)
    if 'error' in stop_result:
        return stop_result
    result = ClusterHandler.start(logger=logger)
    result['stop_timestamp'] = stop_result['timestamp']
    return result


@node_app.command()
@handle_output
def add(
    nodes: Optional[List[str]] = typer.Option(
        ...,
        '--node',  # command line argument name
        help=(
            'node IP, name or FQDN. '
            'Can be used multiple times to add several nodes at a time.'
        )
    )
):
    """Add nodes to the Columnstore cluster."""
    result = []
    for node in nodes:
        result.append(ClusterHandler.add_node(node, logger=logger))
    return result


@node_app.command()
@handle_output
def remove(nodes: Optional[List[str]] = typer.Option(
        ...,
        '--node',  # command line argument name
        help=(
            'node IP, name or FQDN. '
            'Can be used multiple times to remove several nodes at a time.'
        )
    )
):
    """Remove nodes from the Columnstore cluster."""
    result = []
    for node in nodes:
        result.append(ClusterHandler.remove_node(node, logger=logger))
    return result


@set_app.command()
@handle_output
def mode(cluster_mode: str = typer.Option(
        ...,
        '--mode',
        help=(
            'cluster mode to set. '
            '"readonly" or "readwrite" are the only acceptable values.'
        )
    )
):
    """Set Columnstore cluster mode."""
    if cluster_mode not in ('readonly', 'readwrite'):
        raise typer.BadParameter(
            '"readonly" or "readwrite" are the only acceptable modes now.'
        )
    return ClusterHandler.set_mode(cluster_mode, logger=logger)


@set_app.command()
@handle_output
def api_key(key: str = typer.Option(..., help='API key to set.')):
    """Set API key for communication with cluster nodes via API.

    WARNING: this command will affect API key value on all cluster nodes.
    """
    if not key:
        raise typer.BadParameter('Empty API key not allowed.')

    totp = pyotp.TOTP(SECRET_KEY)

    return ClusterHandler.set_api_key(key, totp.now(), logger=logger)


@set_app.command()
@handle_output
def log_level(level: str = typer.Option(..., help='Logging level to set.')):
    """Set logging level on all cluster nodes for develop purposes.

    WARNING: this could dramatically affect the number of log lines.
    """
    if not level:
        raise typer.BadParameter('Empty log level not allowed.')

    return ClusterHandler.set_log_level(level, logger=logger)
