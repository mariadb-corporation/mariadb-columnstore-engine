#!/usr/bin/env python3
"""
Upgrade Agent - A temporary lightweight server for executing commands during upgrades/downgrades.

This agent runs on each node during upgrade/downgrade operations and provides
a secure API to execute commands. It uses a Columnstore port (8619) which is
typically should be open in customer firewalls.

Security:
- Uses HTTPS with temporary self-signed certificates generated at startup
- Requires API key authentication (same as CMAPI)
- Only runs temporarily during upgrade operations
- Automatically shuts down when commanded or after timeout

Usage:
    python -m cmapi_server.managers.upgrade.upgrade_agent --api-key <key> [--autoshutdown-timeout 3600]

    # The agent provides these endpoints:
    # POST /shutdown - Gracefully shutdown the agent
    # POST /fix-mariadb-cli-config - Fix mariadb CLI config by removing unsupported options in old versions
    # POST /fix-columnstore-cnf - Fix columnstore.cnf: loose- prefix + merge old user values from .bak
    # POST /restart-cmapi - Restart the mariadb-columnstore-cmapi systemd service
    # GET /health - Health check
"""
import argparse
import asyncio
import configparser
import logging
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Header
from pydantic import BaseModel

# Eagerly import the anyio asyncio backend so it is already loaded into
# ``sys.modules`` before the CMAPI package upgrade/downgrade replaces files
# on disk.  Without this, ``anyio`` tries a lazy ``importlib.import_module``
# on the first request that needs ``run_in_threadpool`` (used by FastAPI
# dependency injection) and fails with ``ModuleNotFoundError`` because the
# on-disk package tree no longer matches the running interpreter.
import anyio._backends._asyncio  # noqa: F401

from cmapi_server.constants import (
    CMAPI_SYSTEMD_SERVICE_NAME,
    MDB_COLUMNSTORE_RHEL_CNF_PATH,
    MDB_COLUMNSTORE_DEB_CNF_PATH,
    UNSUPPORTED_MARIADB_CLI_OPTIONS,
    UPGRADE_DIR,
    UPGRADE_AGENT_PORT,
    UPGRADE_AGENT_SERVER_TIMEOUT,
)

from cmapi_server.managers.process import CmapiProcessManager


LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


def _setup_logging(log_file: str | None) -> logging.Logger:
    """Configure upgrade agent logging.

    Sets up the root logger with a stderr handler (and optionally a file
    handler).  Uvicorn is told *not* to configure its own loggers
    (``log_config=None`` in ``uvicorn.Config``), so all uvicorn output
    propagates through the root logger and ends up in the same place
    with the same format.
    """
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.insert(0, logging.FileHandler(log_file, encoding='utf-8'))

    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        handlers=handlers,
        force=True,          # reset any existing root-logger config
    )

    return logging.getLogger('upgrade_agent')


logger = logging.getLogger('upgrade_agent')


def get_columnstore_cnf_path() -> str:
    """Return the most likely Columnstore cnf path for this host.

    We support two distro layouts:
    - RHEL-like: /etc/my.cnf.d/columnstore.cnf
    - Debian-like: /etc/mysql/mariadb.conf.d/columnstore.cnf

    Prefer the path that exists on disk. If neither exists raise an error.
    """
    if os.path.exists(MDB_COLUMNSTORE_DEB_CNF_PATH):
        return MDB_COLUMNSTORE_DEB_CNF_PATH
    if os.path.exists(MDB_COLUMNSTORE_RHEL_CNF_PATH):
        return MDB_COLUMNSTORE_RHEL_CNF_PATH
    raise FileNotFoundError(
        f'Neither {MDB_COLUMNSTORE_DEB_CNF_PATH} nor {MDB_COLUMNSTORE_RHEL_CNF_PATH} exists'
    )


def _normalize_cnf_key(key: str) -> str:
    """Strip ``loose-`` prefix for key comparison."""
    return key[len('loose-'):] if key.startswith('loose-columnstore_') else key


def _ensure_loose_prefix(key: str) -> str:
    """Add ``loose-`` prefix to bare ``columnstore_*`` keys."""
    if key.startswith('columnstore_'):
        return f'loose-{key}'
    return key


def _read_cnf(path: str) -> configparser.ConfigParser:
    """Read a ``.cnf`` file preserving key case and standalone options."""
    cfg = configparser.ConfigParser(allow_no_value=True, strict=False)
    cfg.optionxform = str  # preserve case
    cfg.read(path, encoding='utf-8')
    return cfg


# Pydantic models for request/response validation
class HealthResponse(BaseModel):
    """Response model for health check."""
    status: str
    timestamp: str
    hostname: str


class ShutdownResponse(BaseModel):
    """Response model for shutdown."""
    status: str
    timestamp: str


class FixMariaDBCliConfigResponse(BaseModel):
    """Response model for fix-mariadb-cli-config endpoint."""
    needed_fix: bool
    success: bool
    removed_options: list[str]
    error_message: str


class FixColumnstoreCnfResponse(BaseModel):
    """Response model for fix-columnstore-cnf endpoint."""
    success: bool
    cnf_path: str
    pkg_backup_path: str
    loose_prefixed: list[str]
    merged_from_backup: list[str]
    error_message: str


class RestartCmapiResponse(BaseModel):
    """Response model for restart-cmapi endpoint."""
    success: bool
    error_message: str


# Global state for the upgrade agent
class UpgradeAgentState:
    """Global state container for the upgrade agent."""
    api_key: str = ''
    server: Optional[uvicorn.Server] = None
    timeout_task: Optional[asyncio.Task] = None


state = UpgradeAgentState()


def verify_api_key(x_api_key: str = Header(...)) -> str:
    """Dependency to verify API key authentication."""
    if x_api_key != state.api_key:
        logger.warning('Unauthorized request: invalid API key')
        raise HTTPException(status_code=401, detail='Unauthorized')
    return x_api_key


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown."""
    logger.info('Upgrade agent starting up')
    yield
    logger.info('Upgrade agent shutting down')
    if state.timeout_task and not state.timeout_task.done():
        state.timeout_task.cancel()


# Create FastAPI app
app = FastAPI(
    title='Upgrade Agent',
    description='Temporary lightweight server for executing commands during upgrades',
    version='1.0.0',
    lifespan=lifespan,
)


@app.get('/health', response_model=HealthResponse)
async def health_check():
    """Health check endpoint - no auth required."""
    hostname = socket.gethostname()
    logger.info('Health check requested on %s', hostname)
    return HealthResponse(
        status='ok',
        timestamp=datetime.now().isoformat(),
        hostname=hostname,
    )


@app.post('/shutdown', response_model=ShutdownResponse)
async def shutdown_agent(_: str = Depends(verify_api_key)):
    """Shutdown the agent gracefully."""
    logger.info('Shutdown endpoint called, initiating graceful shutdown')

    # Signal shutdown after sending response
    if state.server:
        state.server.should_exit = True
        logger.info('Server should_exit flag set to True')
    else:
        logger.warning('Shutdown requested but no server instance found')

    return ShutdownResponse(
        status='shutting_down',
        timestamp=datetime.now().isoformat()
    )

async def _check_mariadb_cli() -> subprocess.CompletedProcess:
    """Run ``mariadb -V`` and return the completed process."""
    return await asyncio.to_thread(
        subprocess.run,
        ['mariadb', '-V'],
        capture_output=True,
        text=True,
        timeout=30,
    )


@app.post('/fix-mariadb-cli-config', response_model=FixMariaDBCliConfigResponse)
async def fix_mariadb_cli_config(_: str = Depends(verify_api_key)):
    """Fix MariaDB CLI config by removing unsupported options.

    Checks if the mariadb CLI works and patches columnstore.cnf if needed
    by removing options that are not supported in the current version.
    """
    logger.info('fix-mariadb-cli-config endpoint called')
    result = FixMariaDBCliConfigResponse(
        needed_fix=False,
        success=True,
        removed_options=[],
        error_message=''
    )

    try:
        # Step 1: Check if mariadb CLI works
        logger.info('Step 1: Checking if mariadb CLI works (running "mariadb -V")')
        check_proc = await _check_mariadb_cli()

        if check_proc.returncode == 0:
            logger.info(
                'MariaDB CLI works fine (rc=0), output: %s',
                check_proc.stdout.strip(),
            )
            return result

        # Step 2: Parse error for unsupported options
        error_output = check_proc.stderr or check_proc.stdout
        logger.warning(
            'MariaDB CLI failed (rc=%d). stderr: %s',
            check_proc.returncode,
            error_output.strip(),
        )
        pattern = r"unknown variable '([^'=]+)"
        matches = re.findall(pattern, error_output)
        logger.info(
            'Step 2: Parsed unknown variables from error output: %s', matches
        )
        unsupported = [opt for opt in matches if opt in UNSUPPORTED_MARIADB_CLI_OPTIONS]
        logger.info(
            'Filtered to known unsupported options: %s (known list: %s)',
            unsupported,
            UNSUPPORTED_MARIADB_CLI_OPTIONS,
        )

        if not unsupported:
            logger.warning(
                'MariaDB CLI failed but not due to known unsupported options; '
                'no automatic fix possible'
            )
            return result

        # Step 3: Patch the config file
        result.needed_fix = True
        cnf_path = get_columnstore_cnf_path()
        logger.info(
            'Step 3: Patching config file %s, removing options: %s',
            cnf_path,
            unsupported,
        )

        try:
            with open(cnf_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            logger.info(
                'Read %d lines from %s', len(lines), cnf_path
            )

            # Backup original config before modifying
            backup_path = cnf_path + '.cmapi_upgrade_agent.bak'
            shutil.copy2(cnf_path, backup_path)
            logger.info('Backed up original config to %s', backup_path)

            new_lines = []
            for line in lines:
                stripped = line.strip()
                # Check if line starts with any unsupported option
                should_remove = False
                for opt in unsupported:
                    if stripped == opt or stripped.startswith(opt):
                        should_remove = True
                        result.removed_options.append(opt)
                        logger.info('Removing line: %s', stripped)
                        break
                if not should_remove:
                    new_lines.append(line)

            with open(cnf_path, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)
            logger.info(
                'Wrote %d lines back to %s (removed %d lines)',
                len(new_lines),
                cnf_path,
                len(lines) - len(new_lines),
            )

        except OSError as e:
            logger.error(
                'Failed to patch config file %s: %s',
                cnf_path,
                e,
            )
            result.success = False
            result.error_message = f'Failed to patch config: {e}'
            return result

        # Step 4: Verify the fix
        if result.removed_options:
            logger.info(
                'Step 4: Verifying fix by running "mariadb -V" again'
            )
            verify_proc = await _check_mariadb_cli()
            if verify_proc.returncode != 0:
                logger.error(
                    'Config patched but mariadb CLI still fails (rc=%d): %s',
                    verify_proc.returncode,
                    (verify_proc.stderr or verify_proc.stdout).strip(),
                )
                result.success = False
                result.error_message = 'Config patched but CLI still fails'
            else:
                logger.info(
                    'Verification passed, mariadb CLI works after patching'
                )

    except subprocess.TimeoutExpired as e:
        logger.error('Command timed out: %s', e)
        result.success = False
        result.error_message = f'Command timed out: {e}'
    except Exception as e:
        logger.error('Error fixing MariaDB CLI config: %s', e, exc_info=True)
        result.success = False
        result.error_message = str(e)

    logger.info(
        'fix-mariadb-cli-config result: needed_fix=%s, success=%s, '
        'removed_options=%s, error_message=%r',
        result.needed_fix,
        result.success,
        result.removed_options,
        result.error_message,
    )
    return result


@app.post('/fix-columnstore-cnf', response_model=FixColumnstoreCnfResponse)
async def fix_columnstore_cnf(_: str = Depends(verify_api_key)):
    """Fix columnstore.cnf after package upgrade/downgrade.

    1. Back up the new cnf (from package) as ``columnstore.cnf.pkg``
    2. Add ``loose-`` prefix to any bare ``columnstore_*`` variables
    3. Merge custom values from ``columnstore.cnf.bak`` (saved by postrm)

    The ``.bak`` file is created by the Debian postrm (or RPM %postun)
    script when the old package is removed.  If no ``.bak`` exists the
    merge step is skipped and only the ``loose-`` fix is applied.
    """
    logger.info('fix-columnstore-cnf endpoint called')
    result = FixColumnstoreCnfResponse(
        success=True,
        cnf_path='',
        pkg_backup_path='',
        loose_prefixed=[],
        merged_from_backup=[],
        error_message='',
    )

    try:
        cnf_path = get_columnstore_cnf_path()
        result.cnf_path = cnf_path
        bak_path = cnf_path + '.bak'
        pkg_backup_path = cnf_path + '.pkg'
        result.pkg_backup_path = pkg_backup_path

        logger.info('columnstore.cnf path: %s', cnf_path)

        # Step 1: Back up the new (package) cnf before any modifications
        shutil.copy2(cnf_path, pkg_backup_path)
        logger.info('Backed up package cnf to %s', pkg_backup_path)

        # Step 2: Read new cnf and fix loose- prefix on columnstore_* keys
        new_cfg = _read_cnf(cnf_path)
        for section in new_cfg.sections():
            for key in list(new_cfg.options(section)):
                new_key = _ensure_loose_prefix(key)
                if new_key != key:
                    new_cfg.set(section, new_key, new_cfg.get(section, key))
                    new_cfg.remove_option(section, key)
                    result.loose_prefixed.append(key)
        if result.loose_prefixed:
            logger.info(
                'Added loose- prefix to %d variable(s): %s',
                len(result.loose_prefixed), result.loose_prefixed,
            )

        # Step 3: Merge old user values from .bak if it exists
        if os.path.exists(bak_path):
            logger.info('Found backup at %s, merging user values', bak_path)
            old_cfg = _read_cnf(bak_path)
            for section in old_cfg.sections():
                if not new_cfg.has_section(section):
                    new_cfg.add_section(section)
                # Build normalized_key -> without_loose_key: loose_prefixed_key map in new config
                new_nk_map = {
                    _normalize_cnf_key(k): k
                    for k in new_cfg.options(section)
                }
                for old_key in old_cfg.options(section):
                    old_nk = _normalize_cnf_key(old_key)
                    old_value = old_cfg.get(section, old_key)
                    if old_nk in new_nk_map:
                        # Key exists in both: keep new key form, use old value
                        # giving precedence to the old value
                        new_cfg.set(section, new_nk_map[old_nk], old_value)
                    else:
                        # Key only in old: add with loose- prefix if needed
                        new_cfg.set(
                            section, _ensure_loose_prefix(old_key), old_value,
                        )
                    result.merged_from_backup.append(old_nk)
            if result.merged_from_backup:
                logger.info(
                    'Merged %d variable(s) from backup: %s',
                    len(result.merged_from_backup), result.merged_from_backup,
                )
        else:
            logger.info('No .bak file found at %s, skipping merge .cnf files', bak_path)

        # Step 4: Write the result
        with open(cnf_path, 'w', encoding='utf-8') as f:
            new_cfg.write(f)
        logger.info('Wrote fixed cnf to %s', cnf_path)

    except Exception as e:
        logger.error('Error fixing columnstore.cnf: %s', e, exc_info=True)
        result.success = False
        result.error_message = str(e)

    logger.info(
        'fix-columnstore-cnf result: success=%s, loose_prefixed=%s, '
        'merged_from_backup=%s, error_message=%r',
        result.success,
        result.loose_prefixed,
        result.merged_from_backup,
        result.error_message,
    )
    return result


@app.post('/restart-cmapi', response_model=RestartCmapiResponse)
async def restart_cmapi(_: str = Depends(verify_api_key)):
    """Restart the mariadb-columnstore-cmapi systemd service.

    Stops and then starts the CMAPI systemd service.  The restart is
    executed in a background thread so that the response can still be
    sent back to the caller before the process is replaced.
    """
    logger.info('restart-cmapi endpoint called')
    result = RestartCmapiResponse(success=True, error_message='')

    try:
        logger.info('Restarting CMAPI service via CmapiProcessManager')
        success = await asyncio.to_thread(CmapiProcessManager.restart)
        if not success:
            logger.error('CmapiProcessManager.restart() returned False')
            result.success = False
            result.error_message = f'Failed to restart {CMAPI_SYSTEMD_SERVICE_NAME} service'
        else:
            logger.info('Successfully restarted CMAPI service')
    except Exception as e:
        logger.error('Error restarting CMAPI service: %s', e, exc_info=True)
        result.success = False
        result.error_message = str(e)

    logger.info(
        'restart-cmapi result: success=%s, error_message=%r',
        result.success, result.error_message,
    )
    return result


class UpgradeAgentServer:
    """HTTPS server wrapper for the upgrade agent."""

    def __init__(
        self,
        api_key: str,
        port: int = UPGRADE_AGENT_PORT,
        autoshutdown_timeout: int = UPGRADE_AGENT_SERVER_TIMEOUT,
    ):
        self.api_key = api_key
        self.port = port
        self.autoshutdown_timeout = autoshutdown_timeout
        self._server: Optional[uvicorn.Server] = None

    def _check_port_available(self) -> bool:
        """Check if the port is available."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('', self.port))
                logger.info('Port %d is available', self.port)
                return True
        except OSError as e:
            logger.error('Port %d is not available: %s', self.port, e)
            return False

    def _generate_temp_cert(self) -> tuple[str, str]:
        """Generate temporary self-signed certificate."""
        temp_dir = tempfile.mkdtemp(prefix='upgrade_agent_')
        cert_path = os.path.join(temp_dir, 'cert.pem')
        key_path = os.path.join(temp_dir, 'key.pem')

        logger.info('Generating temporary self-signed certificate in %s', temp_dir)
        # Use openssl to generate cert
        proc = subprocess.run([
            'openssl', 'req', '-x509', '-newkey', 'rsa:2048',
            '-keyout', key_path,
            '-out', cert_path,
            '-days', '1',
            '-nodes',
            '-subj', '/CN=upgrade-agent'
        ], check=True, capture_output=True)
        logger.info(
            'Generated temporary certificate: cert=%s, key=%s', cert_path, key_path
        )
        return cert_path, key_path

    async def _timeout_handler(self):
        """Handle server timeout."""
        logger.info(
            'Auto-shutdown timer active, will shut down in %ds', self.autoshutdown_timeout
        )
        await asyncio.sleep(self.autoshutdown_timeout)
        logger.warning(
            'Server auto-shutdown timeout reached after %ds, shutting down',
            self.autoshutdown_timeout,
        )
        if self._server:
            self._server.should_exit = True

    async def _run_server(self):
        """Run the uvicorn server."""
        cert_path, key_path = self._generate_temp_cert()

        logger.info(
            'Configuring uvicorn: host=0.0.0.0, port=%d, ssl_certfile=%s, '
            'ssl_keyfile=%s, log_level=info',
            self.port,
            cert_path,
            key_path,
        )
        config = uvicorn.Config(
            app,
            host='0.0.0.0',
            port=self.port,
            ssl_certfile=cert_path,
            ssl_keyfile=key_path,
            log_level='info',
            log_config=None,
        )
        self._server = uvicorn.Server(config)
        state.server = self._server  # Store in global state for /shutdown endpoint

        # Setup signal handlers within the event loop
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig,
                lambda s=sig: self._handle_signal(s)
            )
        logger.info('Signal handlers registered for SIGTERM and SIGINT')

        # Start timeout task if configured
        if self.autoshutdown_timeout > 0:
            state.timeout_task = asyncio.create_task(self._timeout_handler())
            logger.info(
                'Auto-shutdown timeout task started (%ds)', self.autoshutdown_timeout
            )

        # Run server (blocks until should_exit is set)
        logger.info('Starting uvicorn server (blocking until shutdown)')
        await self._server.serve()
        logger.info('Uvicorn server has exited')

    def _handle_signal(self, signum):
        """Handle shutdown signals."""
        sig_name = signal.Signals(signum).name
        logger.info('Received signal %s (%d), initiating shutdown', sig_name, signum)
        if self._server:
            self._server.should_exit = True
        else:
            logger.warning('Signal received but no server instance to shut down')

    def start(self):
        """Start the upgrade agent server."""
        logger.info(
            'Upgrade agent initializing (hostname=%s, pid=%d)',
            socket.gethostname(),
            os.getpid(),
        )
        if not self._check_port_available():
            logger.error(f'Port {self.port} is already in use')
            sys.exit(1)

        # Set global state
        state.api_key = self.api_key

        logger.info(f'Upgrade agent starting on port {self.port}')
        logger.info(f'Auto shutdown timeout: {self.autoshutdown_timeout}s')

        # Run the async server
        asyncio.run(self._run_server())

        logger.info('Upgrade agent stopped')


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Upgrade Agent for MariaDB Columnstore')
    parser.add_argument(
        '--api-key', required=True,
        help='API key for authentication (same as CMAPI)'
    )
    parser.add_argument(
        '--port', type=int, default=UPGRADE_AGENT_PORT,
        help=f'Port to listen on (default: {UPGRADE_AGENT_PORT})'
    )
    parser.add_argument(
        '--autoshutdown-timeout', type=int, default=UPGRADE_AGENT_SERVER_TIMEOUT,
        help=f'Server will automatically shutdown after timeout in seconds (default: {UPGRADE_AGENT_SERVER_TIMEOUT})'
    )
    parser.add_argument(
        '--log-file',
        default=None,
        help=(
            'Log file path. If not specified, logs are written to '
            f'"{UPGRADE_DIR}" with an auto-generated filename.'
        ),
    )

    args = parser.parse_args()

    # Configure logging as early as possible.
    log_path: str | None
    if args.log_file:
        log_path = args.log_file
        try:
            parent = os.path.dirname(log_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        except OSError as exc:
            print(f'Failed to create log directory for "{log_path}": {exc}', file=sys.stderr)
            sys.exit(1)
    else:
        try:
            os.makedirs(UPGRADE_DIR, exist_ok=True)
        except OSError as exc:
            print(f'Failed to create log dir "{UPGRADE_DIR}": {exc}', file=sys.stderr)
            sys.exit(1)
        log_path = os.path.join(
            UPGRADE_DIR,
            f'upgrade_agent_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
        )

    logger = _setup_logging(log_path)
    logger.info('Logging to %s', log_path)
    logger.info(
        'Parsed arguments: port=%d, autoshutdown_timeout=%d, log_file=%s',
        args.port,
        args.autoshutdown_timeout,
        log_path,
    )

    server = UpgradeAgentServer(
        api_key=args.api_key,
        port=args.port,
        autoshutdown_timeout=args.autoshutdown_timeout,
    )
    server.start()


if __name__ == '__main__':
    main()
