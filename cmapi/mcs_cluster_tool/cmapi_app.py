"""Cmapi typer application.

Formally this module contains all subcommands for "mcs cmapi" cli command.
"""
import logging
from typing import Optional
from typing_extensions import Annotated

import requests
import typer

from cmapi_server.constants import CMAPI_CONF_PATH
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.helpers import get_config_parser, get_current_key, get_version, build_url
from mcs_cluster_tool.decorators import handle_output


logger = logging.getLogger('mcs_cli')
app = typer.Typer(
    help='Commands related to CMAPI itself.'
)
config_app = typer.Typer(help='Manage CMAPI configuration.')
app.add_typer(config_app, name='config')


@app.command()
@handle_output
def is_ready(
    node: Annotated[
        str,
        typer.Option(
            '--node',
            help=('Which node to check the CMAPI is ready to handle requests.')
        )
    ] = '127.0.0.1'
):
    """Check CMAPI is ready to handle requests."""
    url = f'https://{node}:8640/cmapi/ready'
    try:
        resp = requests.get(url, verify=False, timeout=15)
        resp.raise_for_status()
        r_json = resp.json()
    except requests.exceptions.HTTPError as err:
        if err.response.status_code == 503:
            raise CMAPIBasicError('CMAPI is not ready.') from err
        else:
            raise CMAPIBasicError(
                'Got unexpected HTTP return code '
                f'"{err.response.status_code}" while getting CMAPI ready '
                'state.'
            ) from err
    except Exception as err:
        raise CMAPIBasicError('Got an error getting CMAPI ready state.') from err
    logger.debug('Successfully get CMAPI ready state.')
    return r_json


@config_app.command()
@handle_output
def set(
    sampling_interval_seconds: Annotated[
        Optional[int],
        typer.Option(
            '--sampling-interval-seconds',
            min=1,
            help='Failover sampling interval in seconds.'
        )
    ] = None,
):
    """Set CMAPI configuration on all nodes."""
    if sampling_interval_seconds is None:
        raise typer.BadParameter('Provide at least one option to set.')

    cfg_parser = get_config_parser(CMAPI_CONF_PATH)
    api_key = get_current_key(cfg_parser)
    version = get_version()
    url = build_url(
        base_url='localhost', port=8640,
        path=f'cmapi/{version}/cmapi_config',
    )
    headers = {'x-api-key': api_key}
    body = {}
    if sampling_interval_seconds is not None:
        body['failover_sampling_interval_seconds'] = sampling_interval_seconds

    try:
        resp = requests.patch(url, verify=False, headers=headers, json=body)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as err:
        status = err.response.status_code if err.response is not None else 'unknown'
        raise CMAPIBasicError(
            f'Got HTTP {status} while updating CMAPI config.'
        ) from err
    except Exception as err:
        raise CMAPIBasicError('Failed to update CMAPI config.') from err
