"""Cluster typer application.

Formally this module contains all subcommands for "mcs cluster" cli command.
"""
import logging
from typing import List, Optional

import pyotp
import typer

from cmapi_server.constants import SECRET_KEY
from cmapi_server.handlers.cluster import ClusterHandler
from mcs_cluster_tool.decorators import handle_output


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
def stop():
    """Stop the Columnstore cluster."""
    return ClusterHandler.shutdown(logger=logger)


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
