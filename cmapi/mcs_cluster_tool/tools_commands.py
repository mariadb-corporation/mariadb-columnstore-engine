import logging
import os
import secrets
from datetime import datetime, timedelta

import typer
from typing_extensions import Annotated


from cmapi_server.constants import (
    MCS_DATA_PATH, MCS_SECRETS_FILENAME, REQUEST_TIMEOUT, TRANSACTION_TIMEOUT,
)
from cmapi_server.controllers.api_clients import ClusterControllerClient
from cmapi_server.exceptions import CEJError
from cmapi_server.handlers.cej import CEJPasswordHandler
from cmapi_server.managers.transaction import TransactionManager
from mcs_cluster_tool.decorators import handle_output


logger = logging.getLogger('mcs_cli')
# pylint: disable=unused-argument, too-many-arguments, too-many-locals
# pylint: disable=invalid-name, line-too-long


@handle_output
def cskeys(
    user: Annotated[
        str,
        typer.Option(
            '-u', '--user',
            help='Designate the owner of the generated file.',
        )
    ] = 'mysql',
    directory: Annotated[
        str,
        typer.Argument(
            help='The directory where to store the file in.',
        )
    ] = MCS_DATA_PATH
):
    """
    This utility generates a random AES encryption key and init vector
    and writes them to disk. The data is written to the file '.secrets',
    in the specified directory. The key and init vector are used by
    the utility 'cspasswd' to encrypt passwords used in Columnstore
    configuration files, as well as by Columnstore itself to decrypt the
    passwords.

    WARNING: Re-creating the file invalidates all existing encrypted
    passwords in the configuration files.
    """
    filepath = os.path.join(directory, MCS_SECRETS_FILENAME)
    if CEJPasswordHandler().secretsfile_exists(directory=directory):
        typer.echo(
            (
                f'Secrets file "{filepath}" already exists. '
                'Delete it before generating a new encryption key.'
            ),
            color='red',
        )
        raise typer.Exit(code=1)
    elif not os.path.exists(os.path.dirname(filepath)):
        typer.echo(
            f'Directory "{directory}" does not exist.',
            color='red'
        )
        raise typer.Exit(code=1)

    new_secrets_data = CEJPasswordHandler().generate_secrets_data()
    try:
        CEJPasswordHandler().save_secrets(
            new_secrets_data, owner=user, directory=directory
        )
        typer.echo(f'Permissions of "{filepath}" set to owner:read.')
        typer.echo(f'Ownership of "{filepath}" given to {user}.')
    except CEJError as cej_error:
        typer.echo(cej_error.message, color='red')
        raise typer.Exit(code=2)
    raise typer.Exit(code=0)


@handle_output
def cspasswd(
    password: Annotated[
        str,
        typer.Option(
            help='Password to encrypt/decrypt',
            prompt=True, confirmation_prompt=True, hide_input=True
        )
    ],
    decrypt: Annotated[
        bool,
        typer.Option(
            '--decrypt',
            help='Decrypt an encrypted password instead.',
        )
    ] = False
):
    """
    Encrypt a Columnstore plaintext password using the encryption key in
    the key file.
    """
    if decrypt:
        try:
            decrypted_password = CEJPasswordHandler().decrypt_password(
                password
            )
        except CEJError as cej_error:
            typer.echo(cej_error.message, color='red')
            raise typer.Exit(code=1)
        typer.echo(f'Decoded password: {decrypted_password}', color='green')
    else:
        try:
            encoded_password = CEJPasswordHandler().encrypt_password(password)
        except CEJError as cej_error:
            typer.echo(cej_error.message, color='red')
            raise typer.Exit(code=1)
        typer.echo(f'Encoded password: {encoded_password}', color='green')
    raise typer.Exit(code=0)


@handle_output
def bootstrap_single_node(
    key: Annotated[
        str,
        typer.Option(
            '--api-key',
            help='API key to set.',
        )
    ] = ''
):
    """Bootstrap a single node (localhost) Columnstore instance."""
    node = 'localhost'
    client = ClusterControllerClient(request_timeout=REQUEST_TIMEOUT)
    if not key:
        # Generate API key if not provided
        key = secrets.token_urlsafe(32)
    # handle_output decorator will catch, show and log errors
    api_key_set_resp = client.set_api_key(key)
    # if operation takes minutes, then it is better to raise by timeout
    with TransactionManager(
        timeout=TRANSACTION_TIMEOUT, handle_signals=True,
        extra_nodes=[node]
    ):
        add_node_resp = client.add_node({'node': node})

    result = {
        'timestamp': str(datetime.now()),
        'set_api_key_resp': api_key_set_resp,
        'add_node_resp': add_node_resp,
    }
    return result
