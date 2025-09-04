"""Cmapi typer application.

Formally this module contains all subcommands for "mcs cmapi" cli command.
"""
import logging
from typing_extensions import Annotated

import requests
import typer

from cmapi_server.exceptions import CMAPIBasicError
from mcs_cluster_tool.decorators import handle_output


logger = logging.getLogger('mcs_cli')
app = typer.Typer(
    help='Commands related to CMAPI itself.'
)


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
