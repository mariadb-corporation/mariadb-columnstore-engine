import logging
from typing import Any, Dict, List, Optional, Union

import pyotp
import requests


from cmapi_server.constants import (
    CMAPI_CONF_PATH,
    CURRENT_NODE_CMAPI_URL,
    SECRET_KEY,
    UPGRADE_AGENT_PORT,
    _version
)
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.helpers import get_config_parser, get_current_key
from tracing.traced_session import get_traced_session


class BaseClient:
    """Base class for API clients.

    This class is not intended to be used directly, but rather as a
    base class for other API clients. It provides a common interface
    for making requests to the API and handling responses.
    WARNING: This class only handles the API requests, it does not
             handle the transaction management. So it should be started
             at level above using TransactionManager (decorator or context
             manager).
    """
    def __init__(
        self, base_url: str = CURRENT_NODE_CMAPI_URL, request_timeout: Optional[float] = None
    ):
        """Initialize the BaseClient with the base URL.

        :param base_url: The base URL for the API endpoints,
                         defaults to CURRENT_NODE_CMAPI_URL
        """
        self.base_url = base_url
        self.request_timeout = request_timeout
        self.cmd_class = None
        self.url_template = f'{self.base_url}/cmapi/{_version}/{{cmd_class}}/{{endpoint}}'

    def _build_url(self, endpoint: str) -> str:
        """Build the URL for the given endpoint.

        Subclasses can override this method to customize URL construction.

        :param endpoint: The API endpoint to call.
        :return: The full URL for the request.
        """
        return self.url_template.format(cmd_class=self.cmd_class, endpoint=endpoint)

    def _request(
        self, method: str, endpoint: str, data: Optional[Dict[str, Any]] = None,
        throw_real_exp: bool = False
    ) -> Union[Dict[str, Any], List[Any]]:
        """Make a request to the API.

        :param method: The HTTP method to use.
        :param endpoint: The API endpoint to call.
        :param data: The data to send with the request.
        :return: The response from the API.
        """
        url = self._build_url(endpoint)
        cmapi_cfg_parser = get_config_parser(CMAPI_CONF_PATH)
        key = get_current_key(cmapi_cfg_parser)
        headers = {'x-api-key': key}
        if method in ['PUT', 'POST', 'DELETE']:
            headers['Content-Type'] = 'application/json'
            data = {'in_transaction': True, **(data or {})}
        try:
            response = get_traced_session().request(
                method, url, headers=headers,
                params=data if method == 'GET' else None,
                json=data if method in ('PUT', 'POST', 'DELETE') else None,
                timeout=self.request_timeout, verify=False
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError as exc:
            message = (
                f'API client could not connect to {url}. '
                'Is cmapi server running and reachable?'
            )
            logging.error(message)
            if throw_real_exp:
                raise exc
            raise CMAPIBasicError(message)
        except requests.HTTPError as exc:
            resp = exc.response
            request_url = exc.request.url if exc.request else url
            status_code = resp.status_code if resp is not None else 'N/A'
            error_msg = str(exc)
            if status_code == 422:
                # in this case we think cmapi server returned some value but
                # had error during running endpoint handler code
                try:
                    resp_json = resp.json()
                    error_msg = resp_json.get('error', resp_json)
                except requests.exceptions.JSONDecodeError:
                    error_msg = resp.text
            message = (
                f'API client got an HTTPError exception in request to {request_url} with code '
                f'{status_code} and error: {error_msg}'
            )
            logging.error(message)
            if throw_real_exp:
                raise exc
            raise CMAPIBasicError(message)
        except requests.exceptions.Timeout:
            message = f'Request to {url} timed out after {self.request_timeout}'
            logging.error(message)
            if throw_real_exp:
                raise exc
            raise CMAPIBasicError(message)
        except requests.exceptions.RequestException as exc:
            resp = exc.response
            request_url = exc.request.url if exc.request else url
            status_code = resp.status_code if exc.response is not None else 'N/A'
            message = (
                'API client got an undefined error in request to '
                f'{request_url} with code {status_code!r} and '
                f'error: {str(exc)}'
            )
            logging.error(message)
            if throw_real_exp:
                raise exc
            raise CMAPIBasicError(message)


class ClusterControllerClient(BaseClient):
    """Client for the ClusterController API.
    This class provides methods for interacting with the cluster
    management API, including starting and stopping the cluster,
    adding and removing nodes, and getting the cluster status.
    """
    def __init__(
        self, base_url: str = CURRENT_NODE_CMAPI_URL, request_timeout: Optional[float] = None
    ):
        """Initialize the BaseClient with the base URL.

        :param base_url: The base URL for the API endpoints,
                         defaults to CURRENT_NODE_CMAPI_URL
        :type base_url: str, optional
        :param request_timeout: request timeout, defaults to None
        :type request_timeout: Optional[float], optional
        """
        super().__init__(base_url, request_timeout)
        self.cmd_class = 'cluster'

    def start_cluster(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Start the cluster.

        :return: The response from the API.
        """
        return self._request('PUT', 'start', extra)

    def shutdown_cluster(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Shutdown the cluster.

        :return: The response from the API.
        """
        return self._request('PUT', 'shutdown', extra)

    def set_mode(
        self, mode: str, extra: Optional[Dict[str, Any]] = None
        ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Set the cluster mode.

        :param mode: The mode to set.
        :return: The response from the API.
        """
        return self._request('PUT', 'mode-set', {'mode': mode, **(extra or {})})

    def add_node(
        self, node_info: Dict[str, Any], extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Add a node to the cluster.

        :param node_info: Information about a node to add.
        :return: The response from the API.
        """
        #TODO: fix interface as in remove_node used or think about universal
        return self._request('PUT', 'node', {**node_info, **(extra or {})})

    def remove_node(
        self, node: str, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Remove a node from the cluster.

        :param node: node IP, name or FQDN.
        :return: The response from the API.
        """
        return self._request('DELETE', 'node', {'node': node, **(extra or {})})

    def get_status(self) -> Union[Dict[str, Any], Dict[str, str]]:
        """Get the status of the cluster.

        :return: The response from the API.
        """
        return self._request('GET', 'status')

    def get_health(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """
        Get the health of the cluster.

        :return: The response from the API.
        """
        return self._request('GET', 'health', extra)

    def set_api_key(self, api_key: str) -> Union[Dict[str, Any], Dict[str, str]]:
        """Set the API key for the cluster.

        :param api_key: The API key to set.
        :return: The response from the API.
        """
        totp = pyotp.TOTP(SECRET_KEY)
        payload = {
            'api_key': api_key,
            'verification_key': totp.now()
        }
        return self._request('PUT', 'apikey-set', payload)

    def set_log_level(self, log_level: str) -> Union[Dict[str, Any], Dict[str, str]]:
        """Set the log level for the cluster.

        :param log_level: The log level to set.
        :return: The response from the API.
        """
        return self._request('PUT', 'log-level', {'log_level': log_level})

    def load_s3data(self, s3data_info: Dict[str, Any]) -> Union[Dict[str, Any], Dict[str, str]]:
        """Load S3 data into the cluster.

        :param s3data_info: Information about the S3 data to load.
        :return: The response from the API.
        """
        return self._request('PUT', 'load_s3data', s3data_info)

    def get_versions(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Get packages versions.
        :return: The response from the API.
        """
        return self._request('GET', 'versions', extra)

    def start_mariadb(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Start MariaDB server service on each node in cluster.

        :return: The response from the API.
        """
        return self._request('PUT', 'start-mariadb', extra)

    def stop_mariadb(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Stop MariaDB server service on each node in cluster.

        :return: The response from the API.
        """
        return self._request('PUT', 'stop-mariadb', extra)

    def start_upgrade_agent(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Start upgrade agent on each node in cluster.

        The upgrade agent provides a universal command execution API for
        post-upgrade/downgrade fixes. It runs on port 8619.

        :return: The response from the API.
        """
        return self._request('PUT', 'start-upgrade-agent', extra)

    def install_repo(
        self, token: str, mariadb_version: str, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Install ES repository on each node in cluster.

        :return: The response from the API.
        """
        data = {
            'token': token,
            'mariadb_version': mariadb_version
        }
        return self._request('PUT', 'install-repo', {**data, **(extra or {})})

    def preupgrade_backup(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Backup DBRM and configs on each node in cluster.

        :return: The response from the API.
        """
        return self._request('PUT', 'preupgrade-backup', extra)

    def upgrade_mdb_mcs(
        self, mariadb_version: str, columnstore_version: str,
        extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Upgrade MariaDB and Columnstore on each node in cluster.

        :return: The response from the API.
        """
        data = {
            'mariadb_version': mariadb_version,
            'columnstore_version': columnstore_version
        }
        return self._request('PUT', 'upgrade-mdb-mcs', {**data, **(extra or {})})

    def upgrade_cmapi(
        self, version: str, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Starts cmapi_updater.service on each node and waits for new cmapi.

        :return: The response from the API.
        """
        return self._request(
            'PUT', 'upgrade-cmapi', {'version': version, **(extra or {})}
        )

    def check_shared_storage(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Check if shared storage working.

        :return: The response from the API.
        """
        return self._request('PUT', 'check-shared-storage', extra)


class NodeControllerClient(BaseClient):
    """Client for the NodeController API.

    This class provides methods for interacting with a node management
    API.
    """
    def __init__(
        self, base_url: str = CURRENT_NODE_CMAPI_URL, request_timeout: Optional[float] = None
    ):
        """Initialize the NodeControllerClient with the base URL.

        :param base_url: The base URL for the API endpoints,
                         defaults to CURRENT_NODE_CMAPI_URL
        :type base_url: str, optional
        :param request_timeout: request timeout, defaults to None
        :type request_timeout: Optional[float], optional
        """
        super().__init__(base_url, request_timeout)
        self.cmd_class = 'node'

    def get_versions(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Get packages versions installed on a node.

        :return: The response from the API.
        """
        return self._request('GET', 'versions', extra)

    def get_latest_mdb_version(
        self, mdb_ver_prefix: str = '', extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Get latest tested MDB version from repo.

        :param mdb_ver_prefix: optional major (or major.minor) version prefix
            to filter by (e.g. ``"10"``, ``"10.6"``, ``"11"``).
        :return: The response from the API.
        """
        data = {**(extra or {})}
        if mdb_ver_prefix:
            data['mdb_ver_prefix'] = mdb_ver_prefix
        return self._request('GET', 'latest-mdb-version', data or None)

    def validate_mdb_version(
        self, token: str, mariadb_version: str, extra: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Verify MariaDB ES version.

        :param token: valid ES token
        :type token: str
        :param mariadb_version: MariaDB version to verify
        :type mariadb_version: str
        :return: The response from the API
        :rtype: Union[Dict[str, Any], Dict[str, str]]
        """
        data = {
            'token': token,
            'mariadb_version': mariadb_version
        }
        return self._request('GET', 'validate-mdb-version', {**data, **(extra or {})}, **kwargs)

    def validate_es_token(
        self, token: str, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Verify MariaDB ES token is correct.

        :param token: ES token to verify
        :type token: str
        :return: The response from the API.
        """
        return self._request(
            'GET', 'validate-es-token', {'token': token, **(extra or {})}
        )

    def start_mariadb(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Start MariaDB-server service on a node.

        :return: The response from the API.
        """
        return self._request('PUT', 'start-mariadb', extra)

    def stop_mariadb(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Stop MariaDB-server service on a node.

        :return: The response from the API.
        """
        return self._request('PUT', 'stop-mariadb', extra)

    def start_upgrade_agent(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Start upgrade agent on a node.

        The upgrade agent provides a universal command execution API for
        post-upgrade/downgrade fixes. It runs on port 8619.

        :return: The response from the API.
        """
        return self._request('PUT', 'start-upgrade-agent', extra)

    def is_process_running(
        self, process_name: str, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Check if a process is running on a node.

        :return: The response from the API.
        """
        return self._request(
            'GET', 'is-process-running', {'process_name': process_name, **(extra or {})}
        )

    def repo_pkg_versions(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Get available packages versions from the repo on a node.

        :return: The response from the API.
        """
        return self._request('GET', 'repo-pkg-versions', extra)

    def install_repo(
        self, token: str, mariadb_version: str, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Install the repository on a node.

        :param token: valid ES token
        :type token: str
        :param mariadb_version: MariaDB version to verify
        :type mariadb_version: str
        :return: The response from the API.
        """
        data = {
            'token': token,
            'mariadb_version': mariadb_version
        }
        return self._request('PUT', 'install-repo', {**data, **(extra or {})})


    def preupgrade_backup(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Backup DBRM and configs on a node.

        :return: The response from the API.
        """
        return self._request('PUT', 'preupgrade-backup', extra)

    def upgrade_mdb_mcs(
        self, mariadb_version: str, columnstore_version: str,
        extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Upgrade MariaDB and Columnstore on a node.

        :return: The response from the API.
        """
        data = {
            'mariadb_version': mariadb_version,
            'columnstore_version': columnstore_version
        }
        return self._request('PUT', 'upgrade-mdb-mcs', {**data, **(extra or {})})

    def kick_cmapi_upgrade(
        self, version: str, extra: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Starting cmapi_updater.service on a node.

        :return: The response from the API.
        """
        return self._request(
            'PUT', 'kick-cmapi-upgrade', {'version': version, **(extra or {})}
        )

    def check_shared_file(
        self, file_path: str, check_sum: str
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Get packages versions installed on a node.

        :param file_path: file path to check
        :type file_path: str
        :param check_sum: expected MD5 file checksum
        :type check_sum: str
        :return: The response from the API.
        """
        data = {
            'file_path': file_path,
            'check_sum': check_sum,
        }
        return self._request('GET', 'check-shared-file', data)


class AppControllerClient(BaseClient):
    """Client for the AppController API.

    This class provides methods for interacting with a cmapi special management
    API.
    """

    def _build_url(self, endpoint: str) -> str:
        """Build URL for AppController endpoints.

        :param endpoint: The API endpoint to call.
        :return: The full URL for the request.
        """
        return f'{self.base_url}/cmapi/{endpoint}'

    def get_ready(self) -> Union[Dict[str, Any], Dict[str, str]]:
        """Get CMAPI ready or not.

        :return: The response from the API.
        """
        return self._request('GET', 'ready', None, throw_real_exp=True)


class UpgradeAgentClient:
    """Client for communicating with the upgrade agent on a node."""

    def __init__(
        self,
        host: str,
        api_key: str,
        port: int = UPGRADE_AGENT_PORT,
        timeout: float = 30.0
    ):
        """Initialize the upgrade agent client.

        :param host: Hostname or IP address of the node.
        :param api_key: API key for authentication.
        :param port: Port the upgrade agent is listening on.
        :param timeout: Request timeout in seconds.
        """
        self.host = host
        self.api_key = api_key
        self.port = port
        self.timeout = timeout
        self.base_url = f'https://{host}:{port}'
        self.logger = logging.getLogger('mcs_cli')

    def _request(self, method: str, path: str, data: dict = None) -> dict:
        """Make a request to the upgrade agent.

        :param method: HTTP method (GET, POST).
        :param path: URL path.
        :param data: Request body data.
        :return: Response JSON decoded into a dict.
        :rtype: dict
        :raises requests.RequestException: On request failure.
        """
        url = f'{self.base_url}{path}'
        headers = {'x-api-key': self.api_key}

        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=data,
            timeout=self.timeout,
            verify=False  # Using self-signed certs
        )
        if not response.ok:
            # Log the response body so the caller can see the actual error
            # returned by the upgrade agent (e.g. traceback on 500).
            try:
                body = response.json()
            except Exception:
                body = response.text
            self.logger.error(
                'Upgrade agent %s %s returned %d: %s',
                method, url, response.status_code, body,
            )
        response.raise_for_status()
        try:
            return response.json()
        except Exception as exc:
            self.logger.error(
                'Upgrade agent %s %s returned unparseable response: %s',
                method, url, response.text,
            )
            raise requests.RequestException(
                f'Unparseable JSON response from {method} {url}: {exc}. '
                f'Raw response: {response.text}'
            ) from exc


    def health_check(self) -> dict:
        """Check if the upgrade agent is running and healthy.

        :return: Health status dict with ``status``, ``timestamp``, ``hostname``.
        :rtype: dict
        """
        return self._request('GET', '/health')

    def shutdown(self) -> dict:
        """Request the upgrade agent to stop.

        :return: Shutdown confirmation dict.
        :rtype: dict
        """
        return self._request('POST', '/shutdown')

    def fix_mariadb_cli_config(self) -> dict:
        """Fix MariaDB CLI config by removing unsupported options.

        Checks if the mariadb CLI works and patches columnstore.cnf if needed
        by removing options that are not supported in the current version.

        :return: Dict with ``needed_fix``, ``success``, ``removed_options``, ``error_message``.
        :rtype: dict
        """
        return self._request('POST', '/fix-mariadb-cli-config')

    def fix_columnstore_cnf(self) -> dict:
        """Fix columnstore.cnf: loose- prefix + merge old user values from .bak.

        :return: Dict with ``success``, ``cnf_path``, ``pkg_backup_path``,
            ``loose_prefixed``, ``merged_from_backup``, ``error_message``.
        :rtype: dict
        """
        return self._request('POST', '/fix-columnstore-cnf')

    def restart_cmapi(self) -> dict:
        """Restart the mariadb-columnstore-cmapi systemd service.

        :return: Dict with ``success`` and ``error_message``.
        """
        return self._request('POST', '/restart-cmapi')

    def is_running(self) -> bool:
        """Check if the upgrade agent is running.

        :return: ``True`` if running, otherwise ``False``.
        :rtype: bool
        """
        try:
            self.health_check()
            return True
        except requests.RequestException:
            return False
