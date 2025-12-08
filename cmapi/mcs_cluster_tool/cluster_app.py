"""Cluster typer application.

Formally this module contains all subcommands for "mcs cluster" cli command.
"""
import logging
import time
from datetime import datetime, timedelta
from typing import List, Optional

import requests
import typer
from typing_extensions import Annotated
from rich.console import Console
from rich.table import Table

from cmapi_server.constants import (
    CMAPI_CONF_PATH, DEFAULT_MCS_CONF_PATH, REQUEST_TIMEOUT
)
from cmapi_server.controllers.api_clients import ClusterControllerClient
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.helpers import (
    get_config_parser, get_current_key, get_version, build_url
)
from cmapi_server.managers.transaction import TransactionManager
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
client = ClusterControllerClient()


@app.command(rich_help_panel='cluster and single node commands')
@handle_output
def status(
    human_readable: Annotated[
        bool,
        typer.Option(
            '-h', '--human-readable',
            help='Output cluster status in human-readable text instead of JSON.'
        )
    ] = False,
):
    """Get status information."""
    client.request_timeout = REQUEST_TIMEOUT
    result = client.get_status()

    if not human_readable:
        return result

    def _fmt_uptime(seconds: Optional[int]) -> str:
        if seconds is None:
            return 'N/A'
        try:
            total = int(seconds)
        except (TypeError, ValueError):
            return str(seconds)

        td = timedelta(seconds=total)
        days = td.days
        # remainder seconds within the day
        rem = td.seconds
        hours, rem = divmod(rem, 3600)
        minutes, secs = divmod(rem, 60)

        parts = []
        if days:
            parts.append(f'{days}d')
        if hours:
            parts.append(f'{hours}h')
        if minutes:
            parts.append(f'{minutes}m')
        parts.append(f'{secs}s')
        return ' '.join(parts)

    timestamp = result.get('timestamp')
    # total nodes are all keys except control ones
    control_keys = {'timestamp', 'num_nodes'}
    node_names = sorted([k for k in result.keys() if k not in control_keys])
    total_nodes = len(node_names)
    reachable = result.get('num_nodes', total_nodes)

    console = Console(record=True)
    if timestamp:
        console.print(f'Cluster status at {timestamp}')
    console.print(f'Nodes: {reachable}/{total_nodes} reachable')

    table = Table(
        'Node', 'State', 'MariaDB status', 'DBRM mode', 'Cluster Mode', 'Module ID', 'Uptime',
        'DBRoots', 'Services(PID)', title=None, show_lines=True
    )

    for node in node_names:
        info = result.get(node, {})
        state = info.get('state', 'unknown')
        dbrm = info.get('dbrm_mode', 'unknown')
        cluster_mode_val = info.get('cluster_mode', 'unknown')
        module_id = info.get('module_id', 'N/A')
        uptime = _fmt_uptime(info.get('uptime'))
        dbroots = info.get('dbroots') or []
        services = info.get('services') or []
        mariadbd_running = info.get('mariadbd_running', 'Unknown')
        error = info.get('error')

        dbroots_str = ','.join(map(str, dbroots)) if dbroots else '-'
        if services:
            svc_str = ', '.join(
                f"{svc.get('name','?')}({svc.get('pid','?')})" for svc in services
            )
        else:
            svc_str = '-'

        # Append error note to state if present (keeps table compact)
        state_display = f'{state}' if not error else f'{state} (note: {error})'

        if mariadbd_running != 'Unknown':
            mariadb_status = 'Online' if mariadbd_running else 'Offline'
        else:
            mariadb_status = 'Unknown'

        table.add_row(
            node,
            state_display,
            mariadb_status,
            str(dbrm),
            str(cluster_mode_val),
            str(module_id),
            str(uptime),
            dbroots_str,
            svc_str,
        )

    console.print(table)
    return console.export_text().rstrip()


@app.command(rich_help_panel='cluster and single node commands')
@handle_output
@TransactionManager(
    timeout=timedelta(days=1).total_seconds(), handle_signals=True
)
def stop(
    interactive: Annotated[
        bool,
        typer.Option(
            '--interactive/--no-interactive', '-i/-no-i',
            help=(
                'Use this option on active cluster as interactive stop '
                'waits for current writes to complete in DMLProc before '
                'shutting down. Ensuring consistency, preventing data loss '
                'of active writes.'
            ),
        )
    ] = False,
    timeout: Annotated[
        int,
        typer.Option(
            '-t', '--timeout',
            help=(
                'Time in seconds to wait for DMLproc to gracefully stop.'
                'Warning: Low wait timeout values could result in data loss '
                'if the cluster is very active.'
                'In interactive mode means delay time between promts.'
            )
        )
    ] = 15,
    force: Annotated[
        bool,
        typer.Option(
            '--force/--no-force', '-f/-no-f',
            help=(
                'Force stops Columnstore.'
                'Warning: This could cause data corruption and/or data loss.'
            ),
            #TODO: hide from help till not investigated in decreased timeout
            #      affect
            hidden=True
        )
    ] = False
):
    """Stop the Columnstore cluster."""

    start_time = str(datetime.now())
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
        body = {'force': False, 'timeout': timeout}
        url = f'https://{primary_node}:8640/cmapi/{version}/node/stop_dmlproc'
        try:
            resp = requests.put(
                url, verify=False, headers=headers, json=body,
                timeout=timeout+1
            )
            resp.raise_for_status()
        except Exception as err:
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
                raise CMAPIBasicError(
                    f'Error while getting mcs DMLProc status.'
                ) from err

            # check DMLPRoc state
            # if ended, show message and break
            dmlproc_running = resp.json()['running']
            if not dmlproc_running:
                logging.info(
                    'DMLProc stopped gracefully. '
                    'Continue stopping other processes.'
                )
                break
            else:
                force = typer.confirm(
                    'DMLProc is still running. '
                    'Do you want to force stop? '
                    'WARNING: Could cause data loss and/or broken cluster.',
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

    #TODO: bypass timeout here
    resp = client.shutdown_cluster({'in_transaction': True})
    return {'timestamp': start_time}


@app.command(rich_help_panel='cluster and single node commands')
@handle_output
@TransactionManager(
    timeout=timedelta(days=1).total_seconds(), handle_signals=True
)
def start():
    """Start the Columnstore cluster."""
    return client.start_cluster({'in_transaction': True})


@app.command(rich_help_panel='cluster and single node commands')
@handle_output
@TransactionManager(
    timeout=timedelta(days=1).total_seconds(), handle_signals=True
)
def restart():
    """Restart the Columnstore cluster."""
    stop_result = client.shutdown_cluster({'in_transaction': True})
    if 'error' in stop_result:
        return stop_result
    result = client.start_cluster({'in_transaction': True})
    result['stop_timestamp'] = stop_result['timestamp']
    return result


@node_app.command(rich_help_panel='cluster node commands')
@handle_output
def add(
    nodes: Optional[List[str]] = typer.Option(
        ...,
        '--node',  # command line argument name
        help=(
            'node IP, name or FQDN. '
            'Can be used multiple times to add several nodes at a time.'
        )
    ),
    read_replica: bool = typer.Option(
        False,
        '--read-replica',
        help=(
            'Add node (or nodes, if more than one is passed) as read replicas.'
        )
    )
):
    """Add nodes to the Columnstore cluster."""
    result = []
    with TransactionManager(
        timeout=timedelta(days=1).total_seconds(), handle_signals=True,
        extra_nodes=nodes
    ):
        for node in nodes:
            result.append(
                client.add_node({'node': node, 'read_replica': read_replica})
            )
    return result


@node_app.command(rich_help_panel='cluster node commands')
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
    with TransactionManager(
        timeout=timedelta(days=1).total_seconds(), handle_signals=True,
        remove_nodes=nodes
    ):
        for node in nodes:
            result.append(client.remove_node(node))
    return result


@set_app.command()
@handle_output
@TransactionManager(
    timeout=timedelta(days=1).total_seconds(), handle_signals=True
)
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
    client.request_timeout = REQUEST_TIMEOUT
    return client.set_mode(cluster_mode)


@set_app.command()
@handle_output
def api_key(key: str = typer.Option(..., help='API key to set.')):
    """Set API key for communication with cluster nodes via API.

    WARNING: this command will affect API key value on all cluster nodes.
    """
    if not key:
        raise typer.BadParameter('Empty API key not allowed.')
    client.request_timeout = REQUEST_TIMEOUT
    return client.set_api_key(key)


#TODO: remove in next releases
@set_app.command()
@handle_output
def log_level(level: str = typer.Option(..., help='Logging level to set.')):
    """Set logging level on all cluster nodes for develop purposes.

    WARNING: this could dramatically affect the number of log lines.
    """
    if not level:
        raise typer.BadParameter('Empty log level not allowed.')
    client.request_timeout = REQUEST_TIMEOUT
    return client.set_log_level(level)
