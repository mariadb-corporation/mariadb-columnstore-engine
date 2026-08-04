"""Helper functions and constants for the install_es command."""
import ast
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime, timedelta

import requests
import typer
from rich.console import Console
from rich.progress import Progress
from rich.table import Table

from cmapi_server.constants import (
    CMAPI_PORT,
    UPGRADE_AGENT_START_TIMEOUT,
    UPGRADE_DIR,
    UPGRADE_LOG_FILEPATH,
)
from cmapi_server.controllers.api_clients import (
    AppControllerClient,
    ClusterControllerClient,
    NodeControllerClient,
    UpgradeAgentClient,
)
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.helpers import validate_cej_credentials


# install_es constants
INSTALL_ES_CMAPI_READY_TIMEOUT = 300  # seconds to wait for CMAPI to be ready
INSTALL_ES_CMAPI_UPGRADE_SLEEP = 6    # seconds to wait after CMAPI upgrade requested
INSTALL_ES_LONG_TRANSACTION_TIMEOUT = timedelta(days=1).total_seconds()


def setup_install_es_logging() -> None:
    """Configure file logging for the ``install_es`` operation."""
    os.makedirs(UPGRADE_DIR, exist_ok=True)
    new_handler = logging.FileHandler(UPGRADE_LOG_FILEPATH, mode='w')
    new_handler.setLevel(logging.DEBUG)
    new_handler.setFormatter(logging.getLogger('mcs_cli').handlers[0].formatter)
    for logger_name in ('', 'mcs_cli'):
        current_logger = logging.getLogger(logger_name)
        current_logger.addHandler(new_handler)


def validate_es_token_and_version(
    node_api_client: NodeControllerClient,
    token: str,
    target_version: str,
    console: Console,
) -> str:
    """Validate ES token and target version.

    :param node_api_client: Node API client.
    :param token: MariaDB ES token.
    :param target_version: Target version (numeric or ``"latest"``).
    :param console: Rich console used to print an error message.
    :return: Resolved target version (numeric version if ``"latest"`` was passed).
    :rtype: str
    :raises typer.Exit: Exits with code 1 on validation failure.
    """
    node_api_client.validate_es_token(token)

    if target_version == 'latest':
        response = node_api_client.get_latest_mdb_version()
        return response['latest_mdb_version']

    # Detect partial version prefixes (e.g. "10", "10.6", "11", "11.4").
    # Full versions have 4 numeric parts (e.g. "10.6.25-21" -> 4 parts).
    version_parts = re.split(r'[.\-_]', target_version)
    if 4 > len(version_parts) > 0:
        response = node_api_client.get_latest_mdb_version(mdb_ver_prefix=target_version)
        return response['latest_mdb_version']

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

    return target_version


def validate_cej_preupgrade(console: Console) -> None:
    """Pre-upgrade check for Cross-Engine Join (CEJ) credentials.

    Newer Columnstore/CMAPI versions refuse to START the cluster when CEJ
    credentials are missing or cannot be decrypted (previously such issues
    were only logged and the cluster kept working). A customer whose cluster
    currently works despite broken/empty CEJ credentials would therefore end
    up with a cluster that fails to start right after the upgrade.

    This check runs BEFORE anything is changed so such a customer is warned and
    can fix the credentials first. It handles both encrypted and
    unencrypted CEJ passwords, guaranteeing parity with the validation done at
    cluster start.

    :param console: Rich console used to print the error message.
    :raises typer.Exit: Exits with code 1 when CEJ credentials are invalid.
    """
    logger = logging.getLogger('mcs_cli')
    try:
        validate_cej_credentials()
    except CMAPIBasicError as exc:
        logger.error('Pre-upgrade CEJ credentials check failed: %s', exc.message)
        console.print(
            '[red]ERROR:[/red] Cross-Engine Join (CEJ) credentials check failed.'
        )
        console.print(f'[red]{exc.message}[/red]')
        console.print(
            '[yellow]After the upgrade Columnstore will refuse to start the '
            'cluster while the CEJ credentials are invalid.\n'
            'Nothing has been changed yet. Please fix the CrossEngineSupport '
            'credentials in Columnstore.xml (and the .secrets file if the '
            'password is encrypted) and retry. Check '
            '[link=https://mariadb.com/docs/server/architecture/topologies/htap/step-3-start-and-configure-mariadb-enterprise-server#create-the-utility-user]the documentation[/link] '
            'for details.[/yellow]'
        )
        raise typer.Exit(code=1)


def get_current_versions(
    cluster_api_client: ClusterControllerClient,
    console: Console,
    ignore_mismatch: bool,
) -> dict:
    """Retrieve current package versions from the cluster.

    Handles version mismatch across nodes, displaying a table and either
    exiting or continuing based on ``ignore_mismatch``.

    :param cluster_api_client: Cluster API client.
    :param console: Rich console used to render mismatch information.
    :param ignore_mismatch: If ``True``, continue even when versions differ.
    :return: Dict with keys ``server_version``, ``columnstore_version``,
        ``cmapi_version``.
    :rtype: dict
    :raises typer.Exit: Exits with code 1 when mismatch is detected and
        ``ignore_mismatch`` is ``False``.
    :raises CMAPIBasicError: When the API call fails for reasons other than a
        recognized version-mismatch payload.
    """
    try:
        return cluster_api_client.get_versions()
    except CMAPIBasicError as exc:
        msg = exc.message
        mismatch_marker = 'Packages versions:'
        if mismatch_marker not in msg:
            # Not a mismatch we recognize; rethrow for decorator to handle
            raise

        try:
            dict_part = msg.split(mismatch_marker, 1)[1].strip()
            packages_versions = ast.literal_eval(dict_part)
        except Exception:  # pragma: no cover - defensive
            console.print(f'[red]{msg}[/red]')
            raise typer.Exit(code=1)

        server_vals = [v.get('server_version') for v in packages_versions.values()]
        cs_vals = [v.get('columnstore_version') for v in packages_versions.values()]
        cmapi_vals = [v.get('cmapi_version') for v in packages_versions.values()]
        server_common = Counter(server_vals).most_common(1)[0][0] if server_vals else None
        cs_common = Counter(cs_vals).most_common(1)[0][0] if cs_vals else None
        cmapi_common = Counter(cmapi_vals).most_common(1)[0][0] if cmapi_vals else None

        # On a single node there is nothing to compare; also skip when all
        # nodes actually report the same versions (false-positive mismatch).
        has_real_mismatch = (
            len(packages_versions) > 1
            and (
                len(set(server_vals)) > 1
                or len(set(cs_vals)) > 1
                or len(set(cmapi_vals)) > 1
            )
        )

        if not has_real_mismatch:
            return {
                'server_version': server_common or server_vals[0],
                'columnstore_version': cs_common or cs_vals[0],
                'cmapi_version': cmapi_common or cmapi_vals[0],
            }
        if has_real_mismatch:
            console.print('Detected package version mismatch across nodes:', style='yellow')
        mismatch_table = Table('Node', 'Server', 'Columnstore', 'CMAPI')

        def style_version(val, common):
            if val is None:
                return '[red]-[/red]'
            return f'[green]{val}[/green]' if val == common else f'[red]{val}[/red]'

        for node, vers in sorted(packages_versions.items()):
            mismatch_table.add_row(
                node,
                style_version(vers.get('server_version'), server_common),
                style_version(vers.get('columnstore_version'), cs_common),
                style_version(vers.get('cmapi_version'), cmapi_common),
            )

        console.print(mismatch_table)

        if not ignore_mismatch:
            console.print(
                '[yellow]All nodes must have identical package versions before running '
                'install-es. Please align versions (upgrade/downgrade individual nodes) '
                'and retry, or rerun with --ignore-mismatch to force.[/yellow]'
            )
            raise typer.Exit(code=1)

        console.print(
            'Proceeding despite mismatch ( --ignore-mismatch ). '
            'Using majority versions as baseline.',
            style='yellow'
        )
        return {
            'server_version': server_common or server_vals[0],
            'columnstore_version': cs_common or cs_vals[0],
            'cmapi_version': cmapi_common or cmapi_vals[0],
        }


def wait_for_cmapi_ready(
    active_nodes: list[str],
    progress: Progress,
    task_id,
) -> tuple[dict[str, dict], bool]:
    """Poll all nodes until CMAPI is ready or timeout is reached.

    :param active_nodes: List of node hostnames/IPs to check.
    :param progress: Rich ``Progress`` instance for updating status.
    :param task_id: The progress task ID to update.
    :return: Tuple ``(node_states, has_failures)`` where ``node_states`` maps
        ``node -> {"status": str, "details": str}``.
    :rtype: tuple[dict[str, dict], bool]
    """
    start_time = datetime.now()
    timeout_seconds = INSTALL_ES_CMAPI_READY_TIMEOUT

    # status per node: {'status': 'PENDING'|'READY'|'ERROR'|'TIMEOUT', 'details': str}
    node_states = {
        node: {'status': 'PENDING', 'details': ''} for node in active_nodes
    }

    # Build a dedicated client per node
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
                # 503 means not ready yet if ready endpoint is implemented
                if err.response.status_code == 503:
                    node_states[node]['details'] = 'Starting...'
                # 404 means endpoint not implemented in this CMAPI version;
                # if CMAPI can respond at all, treat it as ready.
                elif err.response.status_code == 404:
                    node_states[node]['status'] = 'READY'
                    node_states[node]['details'] = 'Ready (no /ready endpoint)'
                    ready_count += 1
                else:
                    node_states[node]['status'] = 'ERROR'
                    node_states[node]['details'] = f'HTTP {err.response.status_code}'
            except requests.exceptions.ConnectionError:
                # still restarting
                node_states[node]['details'] = 'Connection refused'
            except FileNotFoundError as fnf_err:  # pragma: no cover - defensive
                # Transient race: config file not yet created; do not fail immediately
                missing_path = str(fnf_err).split(':')[-1].strip()
                node_states[node]['details'] = f'Config pending ({missing_path})'
            except Exception as err:  # pragma: no cover - defensive
                node_states[node]['status'] = 'ERROR'
                node_states[node]['details'] = f'Unexpected: {err}'

        # Update progress description only when count changes to reduce flicker
        if ready_count != ready_count_prev:
            progress.update(
                task_id,
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

    # Determine if there were failures
    failures = any(
        state['status'] in ('TIMEOUT', 'ERROR')
        for state in node_states.values()
    )

    return node_states, failures


def build_node_status_table(node_states: dict[str, dict]) -> Table:
    """Build a Rich ``Table`` showing per-node CMAPI readiness status.

    :param node_states: Mapping of node name to a dict containing ``status`` and
        ``details``.
    :return: A ready-to-render Rich table.
    :rtype: rich.table.Table
    """
    status_table = Table('Node', 'Status', 'Details')
    color_map = {
        'READY': 'green',
        'PENDING': 'yellow',
        'TIMEOUT': 'red',
        'ERROR': 'red',
    }
    for node, state in sorted(node_states.items()):
        status = state['status']
        details = state['details']
        style = color_map.get(status, 'white')
        status_table.add_row(node, f'[{style}]{status}[/{style}]', details)
    return status_table


def wait_for_upgrade_agents_ready(
    nodes: list[str],
    api_key: str,
    timeout: int = UPGRADE_AGENT_START_TIMEOUT,
    progress: Progress = None,
    task_id=None
) -> dict[str, bool]:
    """Wait for upgrade agents to become ready on all nodes.

    This polls each node until the agent responds to health checks, or until
    ``timeout`` is reached. Intended to be used after starting agents via a
    CMAPI endpoint.

    :param nodes: List of node hostnames/IPs.
    :param api_key: API key for authentication.
    :param timeout: Maximum time to wait for each node (seconds).
    :param progress: Optional Rich Progress for status updates.
    :param task_id: Optional progress task ID.
    :return: Mapping ``node -> ready``.
    :rtype: dict[str, bool]
    """
    logger = logging.getLogger('mcs_cli')
    results = {}

    for i, node in enumerate(nodes, start=1):
        if progress and task_id is not None:
            progress.update(
                task_id,
                description=f'Waiting for upgrade agent on {node} ({i}/{len(nodes)})...'
            )

        client = UpgradeAgentClient(node, api_key)
        start_time = datetime.now()
        ready = False

        while (datetime.now() - start_time).total_seconds() < timeout:
            if client.is_running():
                logger.info(f'Upgrade agent ready on {node}')
                ready = True
                break
            time.sleep(1)

        if not ready:
            logger.warning(f'Upgrade agent not ready on {node} after {timeout}s')

        results[node] = ready

    return results


def stop_upgrade_agents_on_cluster(
    nodes: list[str],
    api_key: str,
    progress: Progress = None,
    task_id=None
) -> dict[str, bool]:
    """Stop upgrade agents on all cluster nodes.

    :param nodes: List of node hostnames/IPs.
    :param api_key: API key for authentication.
    :param progress: Optional Rich Progress for status updates.
    :param task_id: Optional progress task ID.
    :return: Dict mapping ``node -> success``.
    :rtype: dict[str, bool]
    """
    logger = logging.getLogger('mcs_cli')
    results = {}

    for i, node in enumerate(nodes, start=1):
        if progress and task_id is not None:
            progress.update(
                task_id,
                description=f'Stopping upgrade agent on {node} ({i}/{len(nodes)})...'
            )
        client = UpgradeAgentClient(node, api_key)
        try:
            client.shutdown()
            results[node] = True
        except requests.RequestException as e:
            logger.warning(f'Failed to stop upgrade agent on {node}: {e}')
            results[node] = False

    return results


def call_upgrade_agents_on_all_nodes(
    nodes: list[str],
    api_key: str,
    method_name: str,
    timeout: float = 30.0,
    progress: Progress = None,
    task_id=None,
) -> dict[str, dict]:
    """Execute an UpgradeAgentClient method on all nodes via the upgrade agent.

    :param nodes: List of node hostnames/IPs.
    :param api_key: API key for authentication.
    :param method_name: Name of the UpgradeAgentClient method to call.
    :param timeout: Request timeout in seconds.
    :param progress: Optional Rich Progress for status updates.
    :param task_id: Optional progress task ID.
    :return: Dict mapping node -> method result or error dict.
    :rtype: dict[str, dict]
    """
    logger = logging.getLogger('mcs_cli')
    results = {}

    for i, node in enumerate(nodes, start=1):
        if progress and task_id is not None:
            progress.update(
                task_id,
                description=f'Calling {method_name} on {node} ({i}/{len(nodes)})...'
            )

        client = UpgradeAgentClient(node, api_key, timeout=timeout)
        try:
            method = getattr(client, method_name)
            results[node] = method()
        except requests.RequestException as e:
            logger.error(f'Failed to call {method_name} on {node}: {e}')
            results[node] = {
                'success': False,
                'error': str(e)
            }

    return results
