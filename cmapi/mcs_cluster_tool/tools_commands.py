import logging
import os
import secrets
import sys
from datetime import datetime
from typing import Optional

import typer
from typing_extensions import Annotated


from cmapi_server.constants import (
    MCS_DATA_PATH, MCS_SECRETS_FILENAME, REQUEST_TIMEOUT, TRANSACTION_TIMEOUT,
    CMAPI_CONF_PATH,
)
from cmapi_server.controllers.api_clients import ClusterControllerClient
from cmapi_server.exceptions import CEJError
from cmapi_server.handlers.cej import CEJPasswordHandler
from cmapi_server.helpers import get_config_parser
from cmapi_server.managers.transaction import TransactionManager
from cmapi_server.process_dispatchers.base import BaseDispatcher
from mcs_cluster_tool.constants import MCS_COLUMNSTORE_REVIEW_SH
from mcs_cluster_tool.decorators import handle_output
from mcs_cluster_tool.helpers import cook_sh_arg



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


@handle_output
def review(
    _version: Annotated[
        Optional[bool],
        typer.Option(
            '--version',
            help='Only show the header with version information.',
            show_default=False
        )
    ] = None,
    _logs: Annotated[
        Optional[bool],
        typer.Option(
            '--logs',
            help=(
                'Create a compressed archive of logs for MariaDB Support '
                'Ticket'
            ),
            show_default=False
        )
    ] = None,
    _path: Annotated[
        Optional[str],
        typer.Option(
            '--path',
            help=(
                'Define the path for where to save files/tarballs and outputs '
                'of this script.'
            ),
            show_default=False
        )
    ] = None,
    _backupdbrm: Annotated[
        Optional[bool],
        typer.Option(
            '--backupdbrm',
            help=(
                'Takes a compressed backup of extent map files in dbrm '
                'directory.'
            ),
            show_default=False
        )
    ] = None,
    _testschema: Annotated[
        Optional[bool],
        typer.Option(
            '--testschema',
            help=(
                'Creates a test schema, tables, imports, queries, drops '
                'schema.'
            ),
            show_default=False
        )
    ] = None,
    _testschemakeep: Annotated[
        Optional[bool],
        typer.Option(
            '--testschemakeep',
            help=(
                'Creates a test schema, tables, imports, queries, does not '
                'drop.'
            ),
            show_default=False
        )
    ] = None,
    _ldlischema: Annotated[
        Optional[bool],
        typer.Option(
            '--ldlischema',
            help=(
                'Using ldli, creates test schema, tables, imports, queries, '
                'drops schema.'
            ),
            show_default=False
        )
    ] = None,
    _ldlischemakeep: Annotated[
        Optional[bool],
        typer.Option(
            '--ldlischemakeep',
            help=(
                'Using ldli, creates test schema, tables, imports, queries, '
                'does not drop.'
            ),
            show_default=False
        )
    ] = None,
    _emptydirs: Annotated[
        Optional[bool],
        typer.Option(
            '--emptydirs',
            help='Searches /var/lib/columnstore for empty directories.',
            show_default=False
        )
    ] = None,
    _notmysqldirs: Annotated[
        Optional[bool],
        typer.Option(
            '--notmysqldirs',
            help=(
                'Searches /var/lib/columnstore for directories not owned by '
                'mysql.'
            ),
            show_default=False
        )
    ] = None,
    _emcheck: Annotated[
        Optional[bool],
        typer.Option(
            '--emcheck',
            help='Checks the extent map for orphaned and missing files.',
            show_default=False
        )
    ] = None,
    _s3check: Annotated[
        Optional[bool],
        typer.Option(
            '--s3check',
            help='Checks the extent map against S3 storage.',
            show_default=False
        )
    ] = None,
    _pscs: Annotated[
        Optional[bool],
        typer.Option(
            '--pscs',
            help=(
                'Adds the pscs command. pscs lists running columnstore '
                'processes.'
            ),
            show_default=False
        )
    ] = None,
    _schemasync: Annotated[
        Optional[bool],
        typer.Option(
            '--schemasync',
            help='Fix out-of-sync columnstore tables (CAL0009).',
            show_default=False
        )
    ] = None,
    _tmpdir: Annotated[
        Optional[bool],
        typer.Option(
            '--tmpdir',
            help=(
                'Ensure owner of temporary dir after reboot (MCOL-4866 & '
                'MCOL-5242).'
            ),
            show_default=False
        )
    ] = None,
    _checkports: Annotated[
        Optional[bool],
        typer.Option(
            '--checkports',
            help='Checks if ports needed by Columnstore are opened.',
            show_default=False
        )
    ] = None,
    _eustack: Annotated[
        Optional[bool],
        typer.Option(
            '--eustack',
            help='Dumps the stack of Columnstore processes.',
            show_default=False
        )
    ] = None,
    _clearrollback: Annotated[
        Optional[bool],
        typer.Option(
            '--clearrollback',
            help='Clear any rollback fragments from dbrm files.',
            show_default=False
        )
    ] = None,
    _killcolumnstore: Annotated[
        Optional[bool],
        typer.Option(
            '--killcolumnstore',
            help=(
                'Stop columnstore processes gracefully, then kill remaining '
                'processes.'
            ),
            show_default=False
        )
    ] = None,
    _color: Annotated[
        Optional[str],
        typer.Option(
            '--color',
            help=(
                'print headers in color. Options: [none,red,blue,green,yellow,'
                'magenta,cyan, none] prefix color with l for light.'
            ),
            show_default=False
        )
    ] = None,
):
    """
    This script performs various maintenance and diagnostic tasks for
    MariaDB ColumnStore, including log archiving, extent map backups,
    schema and table testing, directory and ownership checks, extent map
    validation, S3 storage comparison, process management, table
    synchronization, port availability checks, stack dumps, cleanup of
    rollback fragments, and graceful process termination.

    If database is up, this script will connect as root@localhost via socket.
    """

    arguments = []
    for arg_name, value in locals().items():
        sh_arg = cook_sh_arg(arg_name, value, separator='=')
        if sh_arg is None:
            continue
        # columnstore_review.sh accepts only --arg=value format
        arguments.append(sh_arg)
    cmd = f'{MCS_COLUMNSTORE_REVIEW_SH} {" ".join(arguments)}'
    success, _ = BaseDispatcher.exec_command(cmd, stdout=sys.stdout)
    if not success:
        raise typer.Exit(code=1)
    raise typer.Exit(code=0)


# Sentry subcommand app
sentry_app = typer.Typer(help='Manage Sentry DSN configuration for error tracking.')


@sentry_app.command()
@handle_output
def show():
    """Show current Sentry DSN configuration."""
    try:
        # Read existing config
        cfg_parser = get_config_parser(CMAPI_CONF_PATH)
        
        if not cfg_parser.has_section('Sentry'):
            typer.echo('Sentry is disabled (no configuration found).', color='yellow')
            raise typer.Exit(code=0)
        
        dsn = cfg_parser.get('Sentry', 'dsn', fallback='').strip().strip("'\"")
        environment = cfg_parser.get('Sentry', 'environment', fallback='development').strip().strip("'\"")
        
        if not dsn:
            typer.echo('Sentry is disabled (DSN is empty).', color='yellow')
        else:
            typer.echo('Sentry is enabled:', color='green')
            typer.echo(f'  DSN: {dsn}')
            typer.echo(f'  Environment: {environment}')
        
    except Exception as e:
        typer.echo(f'Error reading configuration: {str(e)}', color='red')
        raise typer.Exit(code=1)
    
    raise typer.Exit(code=0)


@sentry_app.command()
@handle_output
def enable(
    dsn: Annotated[
        str,
        typer.Argument(
            help='Sentry DSN URL to enable for error tracking.',
        )
    ],
    environment: Annotated[
        str,
        typer.Option(
            '--environment', '-e',
            help='Sentry environment name (default: development).',
        )
    ] = 'development'
):
    """Enable Sentry error tracking with the provided DSN."""
    if not dsn:
        typer.echo('DSN cannot be empty.', color='red')
        raise typer.Exit(code=1)
    
    try:
        # Read existing config
        cfg_parser = get_config_parser(CMAPI_CONF_PATH)
        
        # Add or update Sentry section
        if not cfg_parser.has_section('Sentry'):
            cfg_parser.add_section('Sentry')
        
        cfg_parser.set('Sentry', 'dsn', f"'{dsn}'")
        cfg_parser.set('Sentry', 'environment', f"'{environment}'")
        
        # Write config back to file
        with open(CMAPI_CONF_PATH, 'w') as config_file:
            cfg_parser.write(config_file)
        
        typer.echo('Sentry error tracking enabled successfully.', color='green')
        typer.echo(f'DSN: {dsn}', color='green')
        typer.echo(f'Environment: {environment}', color='green')
        typer.echo('Note: Restart cmapi service for changes to take effect.', color='yellow')
        
    except Exception as e:
        typer.echo(f'Error updating configuration: {str(e)}', color='red')
        raise typer.Exit(code=1)
    
    raise typer.Exit(code=0)


@sentry_app.command()
@handle_output
def disable():
    """Disable Sentry error tracking by removing the configuration."""
    try:
        # Read existing config
        cfg_parser = get_config_parser(CMAPI_CONF_PATH)
        
        if not cfg_parser.has_section('Sentry'):
            typer.echo('Sentry is already disabled (no configuration found).', color='yellow')
            raise typer.Exit(code=0)
        
        # Remove the entire Sentry section
        cfg_parser.remove_section('Sentry')
        
        # Write config back to file
        with open(CMAPI_CONF_PATH, 'w') as config_file:
            cfg_parser.write(config_file)
        
        typer.echo('Sentry error tracking disabled successfully.', color='green')
        typer.echo('Note: Restart cmapi service for changes to take effect.', color='yellow')
        
    except Exception as e:
        typer.echo(f'Error updating configuration: {str(e)}', color='red')
        raise typer.Exit(code=1)
    
    raise typer.Exit(code=0)