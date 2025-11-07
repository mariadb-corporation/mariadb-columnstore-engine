import logging
import os
import secrets
import sys
import time
from datetime import datetime, timedelta
from typing import Optional
import ast
from collections import Counter

import requests
import typer
from typing_extensions import Annotated
from rich.console import Console
from rich.progress import (
    BarColumn, Progress, SpinnerColumn, TimeElapsedColumn,
)
from rich.table import Table


from cmapi_server.constants import (
    MCS_DATA_PATH, MCS_SECRETS_FILENAME, REQUEST_TIMEOUT, TRANSACTION_TIMEOUT,
    CMAPI_CONF_PATH, CMAPI_PORT,
)
from cmapi_server.controllers.api_clients import (
    AppControllerClient, ClusterControllerClient, NodeControllerClient
)
from cmapi_server.exceptions import CEJError, CMAPIBasicError
from cmapi_server.handlers.cej import CEJPasswordHandler
from cmapi_server.helpers import get_active_nodes, get_config_parser
from cmapi_server.managers.transaction import TransactionManager
from cmapi_server.managers.upgrade.utils import ComparableVersion
from cmapi_server.process_dispatchers.base import BaseDispatcher
from mcs_cluster_tool.constants import MCS_COLUMNSTORE_REVIEW_SH, INSTALL_ES_LOG_FILEPATH
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
        key = secrets.token_urlsafe(32)  #pylint: disable=no-member
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


@handle_output
def healthcheck():
    """Check the health of the MCS cluster."""
    with TransactionManager(
        timeout=timedelta(minutes=5).total_seconds(), handle_signals=True,
    ):
        client = ClusterControllerClient(request_timeout=REQUEST_TIMEOUT)
        result = client.get_health({'in_transaction': True})
        # TODO: just a placeholder for now
        #       need to implement result in a table format with color
        typer.echo(
            'Cluster health check completed successfully.',
            color='green'
        )
    raise typer.Exit(code=0)


@handle_output
def install_es(
    token: Annotated[
        str,
        typer.Option(
            '--token',
            help='ES API Token to use for the upgrade.',
            show_default=False
        )
    ],
    target_version: Annotated[
        str,
        typer.Option(
            '-v', '--version',
            help='ES version to upgdate.',
            show_default=False
        )
    ] = 'latest',
    ignore_mismatch: Annotated[
        bool,
        typer.Option(
            '--ignore-mismatch',
            help='Proceed even if nodes report different installed package versions (use majority as baseline).',
            show_default=False
        )
    ] = False,
):
    """
    [Beta]
    Install the specified MDB ES version.
    If the version is 'latest', it will upgrade to the latest tested version
    available.
    """
    new_handler = logging.FileHandler(INSTALL_ES_LOG_FILEPATH, mode='w')
    new_handler.setLevel(logging.DEBUG)
    new_handler.setFormatter(logging.getLogger('mcs_cli').handlers[0].formatter)
    for logger_name in ("", "mcs_cli"):
        current_logger = logging.getLogger(logger_name)
        current_logger.addHandler(new_handler)
    console = Console()
    console.clear()
    console.rule('[bold green][Beta] MariaDB ES Installer')

    console.print('This utility is now in Beta.', style='yellow underline')
    console.print(
        (
            'Downgrades are supported up to MariaDB 10.6.9-5 and Columnstore 22.08.4.'
            'Make sure you have a backup of your data before proceeding. '
            'If you encounter any issues, please report them to MariaDB Support.'
        ),
        style='underline'
    )

    # Collect output (tables/messages) to render AFTER the progress bar finishes
    post_output: list = []  # items can be strings with rich markup or Rich renderables
    exit_code: int = 0
    def post_print(msg: str, color: Optional[str] = None):
        if color:
            post_output.append(f'[{color}]{msg}[/{color}]')
        else:
            post_output.append(msg)

    active_nodes = get_active_nodes()

    node_api_client = NodeControllerClient()
    cluster_api_client = ClusterControllerClient()
    app_api_client = AppControllerClient()


    node_api_client.validate_es_token(token)
    if target_version == 'latest':
        response = node_api_client.get_latest_mdb_version()
        target_version = response['latest_mdb_version']
    else:
        try:
            node_api_client.validate_mdb_version(token, target_version, throw_real_exp=True)
        except requests.exceptions.HTTPError as exc:
            resp = exc.response
            error_msg = str(exc)
            if resp.status_code == 422:
                try:
                    resp_json = resp.json()
                    error_msg = resp_json.get('error', resp_json)
                except requests.exceptions.JSONDecodeError:
                    error_msg = resp.text
            console.print('ERROR:', style='red')
            console.print(error_msg, style='underline')
            console.rule()
            raise typer.Exit(code=1)

    # Retrieve current versions; if nodes are mismatched, prettify the server error.
    # If --ignore-mismatch is passed we will continue, choosing the majority version
    # of each package as the baseline "current" version.
    try:
        versions = cluster_api_client.get_versions()
    except CMAPIBasicError as exc:  # custom API client error
        msg = exc.message
        mismatch_marker = 'Packages versions:'
        if mismatch_marker in msg:
            try:
                dict_part = msg.split(mismatch_marker, 1)[1].strip()
                packages_versions = ast.literal_eval(dict_part)
            except Exception:  # pragma: no cover - defensive
                # Could not parse, fall back to original behavior
                console.print(f"[red]{msg}[/red]")
                raise typer.Exit(code=1)

            console.print('Detected package version mismatch across nodes:', style='yellow')
            mismatch_table = Table('Node', 'Server', 'Columnstore', 'CMAPI')

            server_vals = [v.get('server_version') for v in packages_versions.values()]
            cs_vals = [v.get('columnstore_version') for v in packages_versions.values()]
            cmapi_vals = [v.get('cmapi_version') for v in packages_versions.values()]
            server_common = Counter(server_vals).most_common(1)[0][0] if server_vals else None
            cs_common = Counter(cs_vals).most_common(1)[0][0] if cs_vals else None
            cmapi_common = Counter(cmapi_vals).most_common(1)[0][0] if cmapi_vals else None

            def style(val, common):
                if val is None:
                    return '[red]-[/red]'
                return f'[green]{val}[/green]' if val == common else f'[red]{val}[/red]'

            for node, vers in sorted(packages_versions.items()):
                mismatch_table.add_row(
                    node,
                    style(vers.get('server_version'), server_common),
                    style(vers.get('columnstore_version'), cs_common),
                    style(vers.get('cmapi_version'), cmapi_common),
                )
            # Print after progress unless we're going to exit early
            if not ignore_mismatch:
                # No progress has started yet; render now and exit
                console.print(mismatch_table)
                console.print('[yellow]All nodes must have identical package versions before running install-es. '
                               'Please align versions (upgrade/downgrade individual nodes) and retry, '
                               'or rerun with --ignore-mismatch to force.[/yellow]')
                raise typer.Exit(code=1)

            console.print(mismatch_table)
            # Forced continuation path
            console.print(
                (
                    'Proceeding despite mismatch ( --ignore-mismatch ). Using majority versions '
                    'as baseline.'
                ),
                style='yellow'
            )
            versions = {
                'server_version': server_common or server_vals[0],
                'columnstore_version': cs_common or cs_vals[0],
                'cmapi_version': cmapi_common or cmapi_vals[0],
            }
        else:
            # Not a mismatch we recognize; rethrow for decorator to handle
            raise
    mdb_curr_ver = versions['server_version']
    mcs_curr_ver = versions['columnstore_version']
    cmapi_curr_ver = versions['cmapi_version']
    mdb_curr_ver_comp = ComparableVersion(mdb_curr_ver)
    mdb_target_ver_comp = ComparableVersion(target_version)

    console.print('Currently installed vesions:', style='green')
    table = Table('ES version', 'Columnstore version', 'CMAPI version')
    table.add_row(mdb_curr_ver, mcs_curr_ver, cmapi_curr_ver)
    console.print(table)
    is_downgrade = False
    if mdb_curr_ver_comp == mdb_target_ver_comp:
        console.print('[green]The target MariaDB ES version is already installed.[/green]')
        raise typer.Exit(code=0)
    elif mdb_curr_ver_comp > mdb_target_ver_comp:
        downgrade = typer.confirm(
            'Target version is older than currently installed. '
            'Are you sure you really want to downgrade?\n'
            'WARNING: Could cause data loss and/or broken cluster.',
            prompt_suffix=' '
        )
        if not downgrade:
            raise typer.Exit(code=1)
        is_downgrade = True
    elif mdb_curr_ver_comp < mdb_target_ver_comp:
        upgrade = typer.confirm(
            f'Are you sure you really want to upgrade to {target_version}?',
            prompt_suffix=' '
        )
        if not upgrade:
            raise typer.Exit(code=1)

    if not active_nodes:
        post_print('No active nodes found, using localhost only.', 'yellow')
        active_nodes.append('localhost')

    with Progress(
        SpinnerColumn(),
        '[progress.description]{task.description}',
        BarColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        step1_stop_cluster = progress.add_task('Stopping MCS cluster...', total=None)
        with TransactionManager(
            timeout=timedelta(days=1).total_seconds(), handle_signals=True
        ):
            cluster_stop_resp = cluster_api_client.shutdown_cluster(
                {'in_transaction': True}
            )
        progress.update(
            step1_stop_cluster, description='[green]MCS Cluster stopped ✓', total=100,
            completed=True
        )
        progress.stop_task(step1_stop_cluster)

        step2_stop_mariadb = progress.add_task('Stopping MariaDB server...', total=None)
        # TODO: put MaxScale into maintainance mode
        mariadb_stop_resp = cluster_api_client.stop_mariadb(
            {'in_transaction': True}
        )
        progress.update(
            step2_stop_mariadb, description='[green]MariaDB server stopped ✓', total=100,
            completed=True
        )
        progress.stop_task(step2_stop_mariadb)

        step3_install_es_repo = progress.add_task(
            'Installing MariaDB ES repository...', total=None
        )
        inst_repo_response = cluster_api_client.install_repo(
            token=token, mariadb_version=target_version
        )
        progress.update(
            step3_install_es_repo, description='[green]Repository installed ✓', total=100,
            completed=True
        )
        progress.stop_task(step3_install_es_repo)

        if target_version == 'latest':
            # PackageManager accepts latest versions so no need to get numeric
            mdb_target_ver = mcs_target_ver = cmapi_target_ver = 'latest'
        else:
            step3_5_get_available_versions = progress.add_task(
                'Getting available versions of packages...', total=None
            )
            available_versions_resp = node_api_client.repo_pkg_versions()
            mdb_target_ver = available_versions_resp['server_version']
            mcs_target_ver = available_versions_resp['columnstore_version']
            cmapi_target_ver = available_versions_resp['cmapi_version']
            progress.update(
                step3_5_get_available_versions,
                description=(
                    f'[green]Available versions: ES {mdb_target_ver}, '
                    f'Columnstore {mcs_target_ver}, CMAPI {cmapi_target_ver} ✓'
                ),
                total=100, completed=True
            )
            progress.stop_task(step3_5_get_available_versions)

        step4_preupgrade_backup = progress.add_task(
            'Starting pre-upgrade backup DBRM and configs on each node...', total=None
        )
        backup_response = cluster_api_client.preupgrade_backup()
        progress.update(
            step4_preupgrade_backup, description='[green]PreUpgrade Backup completed ✓',
            total=100, completed=True
        )
        progress.stop_task(step4_preupgrade_backup)

        step5_upgrade_mdb_mcs = progress.add_task(
            'Upgrading MariaDB and Columnstore on each node...', total=None
        )
        mdb_mcs_upgrade_response = cluster_api_client.upgrade_mdb_mcs(
            mariadb_version=mdb_target_ver, columnstore_version=mcs_target_ver
        )
        progress.update(
            step5_upgrade_mdb_mcs,
            description=f'[green]Upgraded to MariaDB {mdb_target_ver} and Columnstore {mcs_target_ver} ✓',
            total=100, completed=True
        )
        progress.stop_task(step5_upgrade_mdb_mcs)

        step6_install_cmapi = progress.add_task('Upgrading CMAPI on each node...', total=None)
        try:
            cmapi_upgrade_response = cluster_api_client.upgrade_cmapi(version=cmapi_target_ver)
            # cmapi_updater service has 5 s timeout to give CMAPI time to handle response,
            # we need to wait when API become unreachable after CMAPI stop.
            time.sleep(6)
        except requests.exceptions.ConnectionError:
            # during upgrade the connection drop is expected
            pass

        # Prepare per-node readiness tracking
        progress.update(
            step6_install_cmapi, description='Waiting CMAPI to be ready on each node...',
            completed=None
        )
        start_time = datetime.now()
        timeout_seconds = 300
        # status per node: {'status': 'PENDING'|'READY'|'ERROR'|'TIMEOUT', 'details': str}
        node_states = {
            node: {'status': 'PENDING', 'details': ''} for node in active_nodes
        }

        # Build a dedicated client per node (localhost already covered)
        per_node_clients: dict[str, AppControllerClient] = {}
        for node in active_nodes:
            if node in ('localhost', '127.0.0.1'):
                per_node_clients[node] = AppControllerClient()
            else:
                per_node_clients[node] = AppControllerClient(
                    base_url=f'https://{node}:{CMAPI_PORT}'
                )

        ready_count_prev = -1
        while (datetime.now() - start_time) < timedelta(seconds=timeout_seconds):
            ready_count = 0
            for node, client_obj in per_node_clients.items():
                # Skip nodes that already finalized (READY or ERROR)
                if node_states[node]['status'] in ('READY', 'ERROR'):
                    if node_states[node]['status'] == 'READY':
                        ready_count += 1
                    continue
                try:
                    node_response = client_obj.get_ready()
                    if node_response.get('started') is True:
                        node_states[node]['status'] = 'READY'
                        node_states[node]['details'] = 'Service started'
                        ready_count += 1
                except requests.exceptions.HTTPError as err:
                    # 503 means not ready yet, anything else mark as ERROR
                    if err.response.status_code == 503:
                        node_states[node]['details'] = 'Starting...'
                    else:
                        node_states[node]['status'] = 'ERROR'
                        node_states[node]['details'] = f'HTTP {err.response.status_code}'
                except requests.exceptions.ConnectionError:
                    # still restarting
                    node_states[node]['details'] = 'Connection refused'
                except FileNotFoundError as fnf_err:  # pragma: no cover - defensive
                    # Transient race: config file not yet created; do not fail immediately
                    missing_path = str(fnf_err).split(":")[-1].strip()
                    node_states[node]['details'] = f'Config pending ({missing_path})'
                except Exception as err:  # pragma: no cover - defensive
                    node_states[node]['status'] = 'ERROR'
                    node_states[node]['details'] = f'Unexpected: {err}'

            # Update progress description only when count changes to reduce flicker
            if ready_count != ready_count_prev:
                progress.update(
                    step6_install_cmapi,
                    description=(
                        f'Waiting CMAPI to be ready on each node... '
                        f'({ready_count}/{len(active_nodes)} ready)'
                    ),
                    completed=None
                )
                ready_count_prev = ready_count

            if ready_count == len(active_nodes):
                break
            time.sleep(1)

        # Mark TIMEOUT for nodes still pending
        for node, state in node_states.items():
            if state['status'] == 'PENDING':
                state['status'] = 'TIMEOUT'
                state['details'] = f'Not ready after {timeout_seconds}s'

        # Display per-node table after progress ends
        status_table = Table('Node', 'Status', 'Details')
        failures = False
        for node, state in sorted(node_states.items()):
            status = state['status']
            details = state['details']
            color_map = {
                'READY': 'green',
                'PENDING': 'yellow',
                'TIMEOUT': 'red',
                'ERROR': 'red',
            }
            style = color_map.get(status, 'white')
            status_table.add_row(node, f'[{style}]{status}[/{style}]', details)
            if status in ('TIMEOUT', 'ERROR'):
                failures = True
        # Defer table rendering
        post_output.append(status_table)

        if failures:
            progress.update(
                step6_install_cmapi,
                description='[red]CMAPI did not start successfully on all nodes ✗',
                total=100,
                completed=True
            )
            progress.stop_task(step6_install_cmapi)
            exit_code = 1
            post_print('CMAPI did not start successfully on all nodes.', 'red')
        else:
            progress.update(
                step6_install_cmapi,
                description='[green]CMAPI is ready on all nodes ✓',
                total=100,
                completed=True
            )
            progress.stop_task(step6_install_cmapi)

        if failures:
            # skip any automatic restarts on failure
            pass
        elif is_downgrade:
            note_panel = Table('Action', 'Status')
            note_panel.add_row('Automatic restart (MariaDB, Cluster, Health)', '[yellow]SKIPPED (downgrade)')
            post_output.append(note_panel)
            post_print(
                'Downgrade detected: automatic service restarts were skipped. '
                'Please manually start MariaDB and the ColumnStore cluster, and verify health.',
                'yellow'
            )
            post_print('Suggested manual sequence:', 'yellow')
            post_print('  1) systemctl start mariadb', 'yellow')
            post_print('  2) Use mcs-cluster tool to start cluster if needed', 'yellow')
            exit_code = 0
        else:
            step7_start_mariadb = progress.add_task('Starting MariaDB server...', total=None)
            # TODO: put MaxScale from maintainance into working mode
            mariadb_start_resp = cluster_api_client.start_mariadb({'in_transaction': True})
            progress.update(
                step7_start_mariadb, description='[green]MariaDB server started ✓', completed=True
            )
            progress.stop_task(step7_start_mariadb)

            step8_start_cluster = progress.add_task('Starting MCS cluster...', total=None)
            cluster_start_resp = cluster_api_client.start_cluster(
                {'in_transaction': True}
            )
            with TransactionManager(
                timeout=timedelta(days=1).total_seconds(), handle_signals=True
            ):
                cluster_start_resp = cluster_api_client.start_cluster({'in_transaction': True})
            progress.update(
                step8_start_cluster, description='[green]MCS Cluster started ✓', completed=True
            )
            progress.stop_task(step8_start_cluster)
            post_print('Upgrade completed and services restarted successfully.', 'green')

    # Render any deferred output now that the progress bar is complete
    for item in post_output:
        console.print(item)

    raise typer.Exit(code=exit_code)
