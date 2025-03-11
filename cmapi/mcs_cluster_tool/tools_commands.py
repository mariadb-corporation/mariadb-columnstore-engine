import logging
import os

import typer
from typing_extensions import Annotated


from cmapi_server.constants import MCS_SECRETS_FILE_PATH
from cmapi_server.exceptions import CEJError
from cmapi_server.handlers.cej import CEJPasswordHandler
from mcs_cluster_tool.decorators import handle_output


logger = logging.getLogger('mcs_cli')
# pylint: disable=unused-argument, too-many-arguments, too-many-locals
# pylint: disable=invalid-name, line-too-long


@handle_output
def cskeys(
    filepath: Annotated[
        str,
        typer.Option(
            '-f', '--filepath',
            help='Path to the output file',
        )
    ] = MCS_SECRETS_FILE_PATH,
    username: Annotated[
        str,
        typer.Option(
            '-u', '--username',
            help='Username for the key',
        )
    ] = 'mysql',
):
    if CEJPasswordHandler().secretsfile_exists():
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
            f'Directory "{os.path.dirname(filepath)}" does not exist.',
            color='red'
        )
        raise typer.Exit(code=1)

    new_secrets_data = CEJPasswordHandler().generate_secrets_data()
    try:
        CEJPasswordHandler().save_secrets(new_secrets_data, owner=username)
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
            help='Decrypt the provided password',
        )
    ] = False
):
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