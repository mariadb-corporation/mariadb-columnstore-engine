import logging
import os
import secrets
import sys
import time
from datetime import datetime, timedelta
from typing import Optional

import requests
import typer
from typing_extensions import Annotated
from rich.console import Console
from rich.progress import (
    BarColumn, Progress, SpinnerColumn, TimeElapsedColumn,
)
from rich.table import Table


from cmapi_server.constants import (
    CMAPI_CONF_PATH,
    MCS_DATA_PATH,
    MCS_SECRETS_FILENAME,
    REQUEST_TIMEOUT,
    TRANSACTION_TIMEOUT,
)
from cmapi_server.controllers.api_clients import (
    ClusterControllerClient, NodeControllerClient
)
from cmapi_server.exceptions import CEJError, CMAPIBasicError
from cmapi_server.handlers.cej import CEJPasswordHandler
from cmapi_server.helpers import get_active_nodes, get_config_parser, get_current_key
from cmapi_server.managers.transaction import TransactionManager
from cmapi_server.managers.upgrade.preinstall import PreInstallManager
from cmapi_server.managers.upgrade.utils import ComparableVersion
from cmapi_server.process_dispatchers.base import BaseDispatcher
from mcs_cluster_tool.constants import MCS_COLUMNSTORE_REVIEW_SH
from mcs_cluster_tool.decorators import handle_output
from mcs_cluster_tool.helpers import cook_sh_arg
from mcs_cluster_tool.install_es_helpers import (
    INSTALL_ES_CMAPI_UPGRADE_SLEEP,
    INSTALL_ES_LONG_TRANSACTION_TIMEOUT,
    build_node_status_table,
    call_upgrade_agents_on_all_nodes,
    get_current_versions,
    setup_install_es_logging,
    stop_upgrade_agents_on_cluster,
    validate_es_token_and_version,
    wait_for_cmapi_ready,
    wait_for_upgrade_agents_ready,
)


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
            help=(
                'Proceed even if nodes report different installed package versions '
                '(use majority as baseline).'
            ),
            show_default=False
        )
    ] = False,
    skip_cmapi: Annotated[
        bool,
        typer.Option(
            '--skip-cmapi',
            help=(
                'Skip CMAPI upgrade/install entirely. Use when CMAPI does not '
                'need to be updated (e.g. custom/latest CMAPI version already installed).'
            ),
            show_default=False
        )
    ] = False,
    allow_cmapi_downgrade: Annotated[
        bool,
        typer.Option(
            '--allow-cmapi-downgrade',
            help=(
                'Allow CMAPI to be downgraded when performing a downgrade. '
                'By default, CMAPI will NOT be downgraded to prevent meeting '
                'an already fixed issues and loss the new features.'
            ),
            show_default=False
        )
    ] = False,
):
    """
    [Beta]
    Install the specified MDB ES version.
    If the version is 'latest', it will upgrade to the latest tested version
    available for your OS.
    """
    setup_install_es_logging()
    active_nodes = get_active_nodes()
    node_api_client = NodeControllerClient()
    cluster_api_client = ClusterControllerClient()

    console = Console()
    console.clear()
    console.rule('[bold green][Beta] MariaDB ES Installer[/bold green]')

    console.print('This utility is now in Beta.', style='yellow underline')
    console.print(
        (
            'Make sure you have a backup of your data before proceeding.\n'
            'If you encounter any issues, please report them to MariaDB Support.\n'
            'NOTE: Downgrades are supported up to MariaDB 10.6.9-5 and Columnstore 22.08.4.\n'
            '      Not supported through major versions, e.g. 11.4.xx -> 10.6.xx or 11.8.xx -> 11.4.xx.'
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

    # Validate token and resolve target version
    target_version = validate_es_token_and_version(
        node_api_client, token, target_version, console
    )

    # Prechecks: fail fast, before the cluster is stopped.
    console.print('Running pre-upgrade checks...', style='green')
    try:
        PreInstallManager.check_gtid_strict_mode()
        PreInstallManager.check_installed_plugins()
    except CMAPIBasicError as exc:
        console.print('[red]Pre-upgrade check failed:[/red]')
        console.print(exc.message)
        raise typer.Exit(code=1)
    console.print('[green]Pre-upgrade checks passed ✓[/green]')

    # Retrieve current versions (handles mismatch display)
    versions = get_current_versions(cluster_api_client, console, ignore_mismatch)
    mdb_curr_ver = versions['server_version']
    mcs_curr_ver = versions['columnstore_version']
    cmapi_curr_ver = versions['cmapi_version']
    mdb_curr_ver_comp = ComparableVersion(mdb_curr_ver)
    mdb_target_ver_comp = ComparableVersion(target_version)

    console.print('Currently installed versions:', style='green')
    table = Table('ES version', 'Columnstore version', 'CMAPI version')
    table.add_row(mdb_curr_ver, mcs_curr_ver, cmapi_curr_ver)
    console.print(table)
    is_downgrade = False
    if mdb_curr_ver_comp == mdb_target_ver_comp:
        console.print('[green]The target MariaDB ES version is already installed.[/green]')
        raise typer.Exit(code=0)
    elif mdb_curr_ver_comp > mdb_target_ver_comp:
        # Prevent unsupported major-version downgrades, e.g. 11.8 -> 11.4
        # or 11.4 -> 10.6.
        curr_major = mdb_curr_ver_comp.version_nums[:2]
        target_major = mdb_target_ver_comp.version_nums[:2]
        if curr_major > target_major:
            console.print('[red]ERROR:[/red] Major-version downgrades are not supported.')
            console.print(
                f'[red]Refusing to downgrade MariaDB ES {mdb_curr_ver} -> {target_version}.[/red]'
            )
            console.print('[yellow]Please downgrade within the same major.minor series.[/yellow]')
            raise typer.Exit(code=1)

        downgrade = typer.confirm(
            'Target version is older than currently installed. '
            'Are you sure you really want to downgrade?\n'
            'WARNING: This operation could cause data loss and/or broken cluster.\n'
            'NOTE: CMAPI will not be downgraded unless `--allow-cmapi-downgrade` is set.',
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

    # Determine whether CMAPI should be upgraded/installed
    should_upgrade_cmapi = True
    if skip_cmapi:
        should_upgrade_cmapi = False
        console.print('[yellow]CMAPI upgrade/install will be skipped (--skip-cmapi).[/yellow]')
    elif is_downgrade and not allow_cmapi_downgrade:
        should_upgrade_cmapi = False
        console.print(
            '[yellow]CMAPI downgrade will be skipped (--allow-cmapi-downgrade not set). '
            'Current CMAPI version will be preserved.[/yellow]'
        )

    if not active_nodes:
        post_print('No active nodes found, used localhost.', 'yellow')
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
            timeout=INSTALL_ES_LONG_TRANSACTION_TIMEOUT, handle_signals=True
        ):
            cluster_api_client.shutdown_cluster({'in_transaction': True})
        progress.update(
            step1_stop_cluster, description='[green]MCS Cluster stopped ✓', total=100,
            completed=True
        )
        progress.stop_task(step1_stop_cluster)

        # Start upgrade agents on all nodes after MCS is stopped (port 8619 is free).
        # The agent provides a universal command execution API for post-upgrade fixes.
        # We use the CMAPI endpoint to start agents (no SSH needed), but we'll use
        # the agent's own /shutdown endpoint to stop them (works even after CMAPI downgrade).
        api_key = get_current_key(get_config_parser())
        step1_5_start_agents = progress.add_task(
            'Starting upgrade agents on all nodes...', total=None
        )

        # Step 1: Request CMAPI to start agents on all nodes
        try:
            start_response = cluster_api_client.start_upgrade_agent(
                {'autoshutdown_timeout': 3600}
            )
            # Check which nodes reported success
            start_success = {
                node: resp.get('status') == 'started'
                for node, resp in start_response.items()
                if node != 'timestamp'
            }
        except requests.RequestException as e:
            logger.error(f'Failed to start upgrade agents via CMAPI: {e}')
            start_success = {}

        # Step 2: Wait for agents to actually be ready (respond to health checks)
        if start_success:
            agent_results = wait_for_upgrade_agents_ready(
                list(start_success.keys()), api_key, progress=progress, task_id=step1_5_start_agents
            )
        else:
            agent_results = {}

        agents_started = all(agent_results.values()) if agent_results else False
        if agents_started:
            progress.update(
                step1_5_start_agents,
                description='[green]Upgrade agents started ✓',
                total=100, completed=True
            )
        else:
            failed_nodes = [n for n, ok in agent_results.items() if not ok]
            progress.update(
                step1_5_start_agents,
                description=f'[yellow]Upgrade agents: some nodes failed ({failed_nodes}) ⚠',
                total=100, completed=True
            )
        progress.stop_task(step1_5_start_agents)

        step2_stop_mariadb = progress.add_task('Stopping MariaDB server...', total=None)
        # TODO: put MaxScale into maintainance mode
        cluster_api_client.stop_mariadb({'in_transaction': True})
        progress.update(
            step2_stop_mariadb, description='[green]MariaDB server stopped ✓', total=100,
            completed=True
        )
        progress.stop_task(step2_stop_mariadb)

        step3_install_es_repo = progress.add_task(
            'Installing MariaDB ES repository...', total=None
        )
        cluster_api_client.install_repo(token=token, mariadb_version=target_version)
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

            # This catches cases where Columnstore package isn't available for the target OS
            # but the package manager returns the currently installed version. Currently known
            # issue with apt on debian12 removed package version shows in `apt show` and in `apt
            # policy` if no other candidates found)
            requested_changes = {
                'MariaDB server': (mdb_curr_ver, mdb_target_ver),
                'Columnstore': (mcs_curr_ver, mcs_target_ver),
            }
            if should_upgrade_cmapi:
                requested_changes['CMAPI'] = (cmapi_curr_ver, cmapi_target_ver)

            unchanged = [
                name for name, (cur, tgt) in requested_changes.items()
                if ComparableVersion(cur) == ComparableVersion(tgt)
            ]
            if unchanged and len(unchanged) != len(requested_changes):
                progress.update(
                    step3_5_get_available_versions,
                    description='[red]Required packages not found in repository ✗',
                    total=100,
                    completed=True,
                )
                progress.stop_task(step3_5_get_available_versions)
                progress.stop()
                console.print('[red]ERROR:[/red] Repository does not provide required packages:')
                console.print(f"[red]- {',\n-'.join(unchanged)}[/red]")
                console.print(
                    (
                        '[yellow]Nothing was installed yet. Please choose a different ES version. '
                        'Probably this version does not exist for your OS.[/yellow]'
                    )
                )
                raise typer.Exit(code=1)
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
        cluster_api_client.preupgrade_backup()
        progress.update(
            step4_preupgrade_backup, description='[green]PreUpgrade Backup completed ✓',
            total=100, completed=True
        )
        progress.stop_task(step4_preupgrade_backup)

        step5_upgrade_mdb_mcs = progress.add_task(
            'Upgrading MariaDB and Columnstore on each node...', total=None
        )
        cluster_api_client.upgrade_mdb_mcs(
            mariadb_version=mdb_target_ver, columnstore_version=mcs_target_ver
        )
        progress.update(
            step5_upgrade_mdb_mcs,
            description=f'[green]Upgraded to MariaDB {mdb_target_ver} and Columnstore {mcs_target_ver} ✓',
            total=100, completed=True
        )
        progress.stop_task(step5_upgrade_mdb_mcs)

        # Fix columnstore.cnf on each node: the old cnf was renamed to .bak
        # by postrm during package removal, and a fresh cnf was installed with
        # the new package.  The agent adds loose- prefix to bare columnstore_*
        # variables (prevents MariaDB startup errors) and merges back any user
        # customizations from the .bak file.
        if agents_started:
            step5_5_fix_cnf = progress.add_task(
                'Fixing columnstore.cnf on each node...', total=None
            )
            cnf_fix_results = call_upgrade_agents_on_all_nodes(
                nodes=active_nodes,
                api_key=api_key,
                method_name='fix_columnstore_cnf',
                progress=progress,
                task_id=step5_5_fix_cnf,
            )
            cnf_fix_failed = []
            cnf_fix_changed = []
            for node, res in cnf_fix_results.items():
                if not isinstance(res, dict):
                    cnf_fix_failed.append(node)
                    continue
                if res.get('error') or not res.get('success'):
                    cnf_fix_failed.append(node)
                    logger.warning(
                        'columnstore.cnf fix failed on %s: %s',
                        node, res.get('error_message', ''),
                    )
                elif res.get('loose_prefixed') or res.get('merged_from_backup'):
                    cnf_fix_changed.append(node)

            if cnf_fix_failed:
                progress.update(
                    step5_5_fix_cnf,
                    description=f'[yellow]columnstore.cnf fix failed on: {cnf_fix_failed} ⚠',
                    total=100, completed=True
                )
            elif cnf_fix_changed:
                progress.update(
                    step5_5_fix_cnf,
                    description='[green]columnstore.cnf fixed and merged ✓',
                    total=100, completed=True
                )
            else:
                progress.update(
                    step5_5_fix_cnf,
                    description='[green]columnstore.cnf OK ✓',
                    total=100, completed=True
                )
            progress.stop_task(step5_5_fix_cnf)

        # For downgrades: start MariaDB BEFORE upgrading CMAPI, because older CMAPI
        # versions lack the endpoint to control MariaDB service. Starting MariaDB
        # doesn't affect a stopped Columnstore cluster or CMAPI.
        if is_downgrade and should_upgrade_cmapi:
            step5_5_start_mariadb = progress.add_task(
                'Starting MariaDB server before CMAPI downgrade...', total=None
            )
            cluster_api_client.start_mariadb({'in_transaction': True})
            progress.update(
                step5_5_start_mariadb,
                description='[green]MariaDB server started (pre-CMAPI downgrade) ✓',
                total=100, completed=True
            )
            progress.stop_task(step5_5_start_mariadb)

        if should_upgrade_cmapi:
            step6_install_cmapi = progress.add_task('Upgrading CMAPI on each node...', total=None)
            try:
                cluster_api_client.upgrade_cmapi(version=cmapi_target_ver)
                # cmapi_updater service has 5 s timeout to give CMAPI time to handle response,
                # we need to wait when API become unreachable after CMAPI stop.
                time.sleep(INSTALL_ES_CMAPI_UPGRADE_SLEEP)
            except requests.exceptions.ConnectionError:
                # during upgrade the connection drop is expected
                pass

            # Wait for CMAPI to be ready on all nodes
            progress.update(
                step6_install_cmapi, description='Waiting CMAPI to be ready on each node...',
                completed=None
            )
            node_states, failures = wait_for_cmapi_ready(active_nodes, progress, step6_install_cmapi)

            # Build and defer the status table
            status_table = build_node_status_table(node_states)
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
        else:
            # CMAPI upgrade skipped — restart CMAPI via upgrade agents so it
            # picks up the changed MDB/MCS packages underneath.
            failures = []
            if agents_started:
                step6a_restart_cmapi = progress.add_task(
                    'Restarting CMAPI on each node (upgrade skipped)...', total=None
                )
                restart_results = call_upgrade_agents_on_all_nodes(
                    nodes=active_nodes,
                    api_key=api_key,
                    method_name='restart_cmapi',
                    timeout=120,
                    progress=progress,
                    task_id=step6a_restart_cmapi,
                )
                restart_failed_nodes = [
                    node for node, res in restart_results.items()
                    if not isinstance(res, dict) or not res.get('success')
                ]
                if restart_failed_nodes:
                    progress.update(
                        step6a_restart_cmapi,
                        description=(
                            f'[yellow]CMAPI restart failed on some nodes: {restart_failed_nodes} ⚠'
                        ),
                        total=100, completed=True,
                    )
                    for node in restart_failed_nodes:
                        res = restart_results.get(node, {})
                        err_msg = res.get('error_message') or res.get('error', '')
                        logger.warning('CMAPI restart failed on %s: %s', node, err_msg)
                else:
                    progress.update(
                        step6a_restart_cmapi,
                        description='[green]CMAPI restarted on all nodes ✓',
                        total=100, completed=True,
                    )
                progress.stop_task(step6a_restart_cmapi)

                # Wait for CMAPI to be ready after restart
                step6b_wait_cmapi = progress.add_task(
                    'Waiting for CMAPI to be ready after restart...', total=None
                )
                time.sleep(INSTALL_ES_CMAPI_UPGRADE_SLEEP)
                node_states, failures = wait_for_cmapi_ready(
                    active_nodes, progress, step6b_wait_cmapi
                )
                status_table = build_node_status_table(node_states)
                post_output.append(status_table)

                if failures:
                    progress.update(
                        step6b_wait_cmapi,
                        description='[red]CMAPI did not start successfully on all nodes ✗',
                        total=100, completed=True,
                    )
                    exit_code = 1
                    post_print('CMAPI did not start successfully on all nodes after restart.', 'red')
                else:
                    progress.update(
                        step6b_wait_cmapi,
                        description='[green]CMAPI is ready on all nodes ✓',
                        total=100, completed=True,
                    )
                progress.stop_task(step6b_wait_cmapi)
            else:
                post_print(
                    'CMAPI upgrade skipped and no upgrade agents available to restart CMAPI.',
                    'yellow',
                )

        if failures:
            # skip any automatic restarts on failure
            pass
        elif is_downgrade and should_upgrade_cmapi:
            # MariaDB was already started before CMAPI downgrade in step 5.5
            note_panel = Table('Action', 'Status')
            note_panel.add_row('MariaDB server', '[green]STARTED (before CMAPI downgrade)')
            post_output.append(note_panel)
        else:
            step7_start_mariadb = progress.add_task('Starting MariaDB server...', total=None)
            # TODO: put MaxScale from maintainance into working mode
            cluster_api_client.start_mariadb({'in_transaction': True})
            progress.update(
                step7_start_mariadb, description='[green]MariaDB server started ✓', completed=True
            )
            progress.stop_task(step7_start_mariadb)

        # Fix MariaDB CLI config compatibility after downgrade.
        # Older MariaDB versions may not support some config options
        # (e.g., 'quick', 'quick-max-column-width') that were added in newer versions.
        # We use the upgrade agent for this since it's independent of CMAPI version.
        if agents_started:
            step7_patch1_fix_mdb_cli_config = progress.add_task(
                'Checking MariaDB clients config compatibility...', total=None
            )
            fix_result = call_upgrade_agents_on_all_nodes(
                nodes=active_nodes,
                api_key=api_key,
                method_name='fix_mariadb_cli_config',
                progress=progress,
                task_id=step7_patch1_fix_mdb_cli_config,
            )
            # Check results: needed_fix, success, removed_options, error_message
            nodes_fixed = []
            nodes_failed = []
            for node, result in fix_result.items():
                if not isinstance(result, dict):
                    nodes_failed.append(node)
                    continue
                if result.get('error'):
                    # Request failed
                    nodes_failed.append(node)
                elif result.get('needed_fix'):
                    if result.get('success'):
                        nodes_fixed.append(node)
                    else:
                        nodes_failed.append(node)
                        logger.warning(
                            f'Config fix failed on {node}: {result.get("error_message")}'
                        )

            if nodes_failed:
                progress.update(
                    step7_patch1_fix_mdb_cli_config,
                    description=f'[yellow]MariaDB config fix failed on some nodes: {nodes_failed} ⚠',
                    total=100, completed=True
                )
            elif nodes_fixed:
                progress.update(
                    step7_patch1_fix_mdb_cli_config,
                    description='[green]MariaDB clients config fixed for downgrade ✓',
                    total=100, completed=True
                )
            else:
                progress.update(
                    step7_patch1_fix_mdb_cli_config,
                    description='[green]MariaDB clients config OK ✓',
                    total=100, completed=True
                )
            progress.stop_task(step7_patch1_fix_mdb_cli_config)

        # Stop upgrade agents BEFORE starting MCS cluster to free port 8619.
        # The agents have served their purpose for post-upgrade fixes.
        if agents_started:
            step7_5_stop_agents = progress.add_task(
                'Stopping upgrade agents...', total=None
            )
            stop_results = stop_upgrade_agents_on_cluster(
                active_nodes, api_key, progress, step7_5_stop_agents
            )
            all_stopped = all(stop_results.values())
            if all_stopped:
                progress.update(
                    step7_5_stop_agents,
                    description='[green]Upgrade agents stopped ✓',
                    total=100, completed=True
                )
            else:
                failed_nodes = [n for n, ok in stop_results.items() if not ok]
                progress.update(
                    step7_5_stop_agents,
                    description=f'[yellow]Some agents may still be running ({failed_nodes}) ⚠',
                    total=100, completed=True
                )
            progress.stop_task(step7_5_stop_agents)

        # Start the cluster for both upgrades and downgrades (skip only on failure)
        if not failures:
            step8_start_cluster = progress.add_task('Starting MCS cluster...', total=None)
            with TransactionManager(
                timeout=INSTALL_ES_LONG_TRANSACTION_TIMEOUT, handle_signals=True
            ):
                cluster_api_client.start_cluster({'in_transaction': True})
            progress.update(
                step8_start_cluster, description='[green]MCS Cluster started ✓', completed=True
            )
            progress.stop_task(step8_start_cluster)
            post_print('Upgrade completed and services restarted successfully.', 'green')

    # Render any deferred output now that the progress bar is complete
    for item in post_output:
        console.print(item)

    raise typer.Exit(code=exit_code)
