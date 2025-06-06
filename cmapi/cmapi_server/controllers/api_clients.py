import logging
from typing import Any, Dict, Optional, Union

import pyotp
import requests

from cmapi_server.constants import (
    CMAPI_CONF_PATH, CURRENT_NODE_CMAPI_URL, SECRET_KEY, _version
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
            self, base_url: str = CURRENT_NODE_CMAPI_URL,
            request_timeout: Optional[float] = None
    ):
        """Initialize the BaseClient with the base URL.

        :param base_url: The base URL for the API endpoints,
                         defaults to CURRENT_NODE_CMAPI_URL
        :type base_url: str, optional
        :param request_timeout: request timeout, defaults to None
        :type request_timeout: Optional[float], optional
        """
        self.base_url = base_url
        self.request_timeout = request_timeout
        self.cmd_url = None

    def _request(
        self, method: str, endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        throw_real_exp: bool = False
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Make a request to the API.

        :param method: The HTTP method to use.
        :param endpoint: The API endpoint to call.
        :param data: The data to send with the request.
        :return: The response from the API.
        """
        url = f'{self.cmd_url}/{endpoint}'
        cmapi_cfg_parser = get_config_parser(CMAPI_CONF_PATH)
        key = get_current_key(cmapi_cfg_parser)
        headers = {'x-api-key': key}
        if method in ['PUT', 'POST', 'DELETE']:
            headers['Content-Type'] = 'application/json'
            data = {'in_transaction': True, **(data or {})}
        try:
            response = requests.request(
                method, url, headers=headers,
                params=data if method == 'GET' else None,
                json=data if method in ('PUT', 'POST') else None,
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
        # TODO: different handler for timeout exception?
        except requests.exceptions.HTTPError as exc:
            resp = exc.response
            error_msg = str(exc)
            if resp.status_code == 422:
                # in this case we think cmapi server returned some value but
                # had error during running endpoint handler code
                try:
                    resp_json = resp.json()
                    error_msg = resp_json.get('error', resp_json)
                except requests.exceptions.JSONDecodeError:
                    error_msg = resp.text
            message = (
                f'API client got an exception in request to {exc.request.url} '
                f'with code {resp.status_code if resp is not None else "NA"} '
                f'and error: {error_msg}'
            )
            logging.error(message)
            if throw_real_exp:
                raise exc
            raise CMAPIBasicError(message)
        except requests.exceptions.RequestException as exc:
            status_code = exc.response.status_code if exc.response else 'NA'
            message = (
                'API client got an undefined error in request to '
                f'{exc.request.url} with code {status_code!r} and '
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
            self, base_url: str = CURRENT_NODE_CMAPI_URL,
            request_timeout: Optional[float] = None
    ):
        """Initialize the ClusterControllerClient with the base URL.

        :param base_url: The base URL for the API endpoints,
                         defaults to CURRENT_NODE_CMAPI_URL
        :type base_url: str, optional
        :param request_timeout: request timeout, defaults to None
        :type request_timeout: Optional[float], optional
        """
        super().__init__(base_url, request_timeout)
        self.cmd_url = f'{self.base_url}/cmapi/{_version}/cluster'

    def start_cluster(
            self, extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Start the cluster.

        :return: The response from the API.
        """
        return self._request('PUT', 'start', extra)

    def shutdown_cluster(
            self, extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Shutdown the cluster.

        :return: The response from the API.
        """
        return self._request('PUT', 'shutdown', extra)

    def set_mode(
            self, mode: str, extra: Dict[str, Any] = dict()
        ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Set the cluster mode.

        :param mode: The mode to set.
        :return: The response from the API.
        """
        return self._request('PUT', 'mode-set', {'mode': mode, **extra})

    def add_node(
            self, node_info: Dict[str, Any], extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Add a node to the cluster.

        :param node_info: Information about a node to add.
        :return: The response from the API.
        """
        #TODO: fix interface as in remove_node used or think about universal
        return self._request('PUT', 'node', {**node_info, **extra})

    def remove_node(
            self, node: str, extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Remove a node from the cluster.

        :param node: node IP, name or FQDN.
        :return: The response from the API.
        """
        return self._request('DELETE', 'node', {'node': node, **extra})

    def get_status(self) -> Union[Dict[str, Any], Dict[str, str]]:
        """Get the status of the cluster.

        :return: The response from the API.
        """
        return self._request('GET', 'status')

    def get_health(
            self, extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """
        Get the health of the cluster.

        :return: The response from the API.
        """
        return self._request('GET', 'health', extra)

    def set_api_key(
            self, api_key: str
    ) -> Union[Dict[str, Any], Dict[str, str]]:
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

    def set_log_level(
            self, log_level: str
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Set the log level for the cluster.

        :param log_level: The log level to set.
        :return: The response from the API.
        """
        return self._request('PUT', 'log-level', {'log_level': log_level})

    def load_s3data(
            self, s3data_info: Dict[str, Any]
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Load S3 data into the cluster.

        :param s3data_info: Information about the S3 data to load.
        :return: The response from the API.
        """
        return self._request('PUT', 'load_s3data', s3data_info)

    def get_versions(
            self, extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Get packages versions.
        :return: The response from the API.
        """
        return self._request('GET', 'versions', extra)

    def start_mariadb(
            self, extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Start MariaDB server service on each node in cluster.

        :return: The response from the API.
        """
        return self._request('PUT', 'start-mariadb', extra)

    def stop_mariadb(
            self, extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Stop MariaDB server service on each node in cluster.

        :return: The response from the API.
        """
        return self._request('PUT', 'stop-mariadb', extra)

    def install_repo(
            self, token: str, mariadb_version: str,
            extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Install ES repository on each node in cluster.

        :return: The response from the API.
        """
        data = {
            'token': token,
            'mariadb_version': mariadb_version
        }
        return self._request('PUT', 'install-repo', {**data, **extra})

    def preupgrade_backup(
            self, extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Backup DBRM and configs on each node in cluster.

        :return: The response from the API.
        """
        return self._request('PUT', 'preupgrade-backup', extra)

    def upgrade_mdb_mcs(
            self, mariadb_version: str, columnstore_version: str,
            extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Upgrade MariaDB and Columnstore on each node in cluster.

        :return: The response from the API.
        """
        data = {
            'mariadb_version': mariadb_version,
            'columnstore_version': columnstore_version
        }
        return self._request('PUT', 'upgrade-mdb-mcs', {**data, **extra})

    def upgrade_cmapi(
            self, version: str,
            extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Starts cmapi_updater.service on each node and waits for new cmapi.

        :return: The response from the API.
        """
        return self._request(
            'PUT', 'upgrade-cmapi', {'version': version, **extra}
        )


class NodeControllerClient(BaseClient):
    """Client for the NodeController API.

    This class provides methods for interacting with a node management
    API.
    """
    def __init__(
            self, base_url: str = CURRENT_NODE_CMAPI_URL,
            request_timeout: Optional[float] = None
    ):
        """Initialize the NodeControllerClient with the base URL.

        :param base_url: The base URL for the API endpoints,
                         defaults to CURRENT_NODE_CMAPI_URL
        :type base_url: str, optional
        :param request_timeout: request timeout, defaults to None
        :type request_timeout: Optional[float], optional
        """
        super().__init__(base_url, request_timeout)
        self.cmd_url = f'{self.base_url}/cmapi/{_version}/node'

    def get_versions(
            self, extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Get packages versions installed on a node.

        :return: The response from the API.
        """
        return self._request('GET', 'versions', extra)

    def get_latest_mdb_version(
        self, extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Get latest tested MDB version from repo.

        :return: The response from the API.
        """
        return self._request('GET', 'latest-mdb-version', extra)

    def validate_mdb_version(
            self, token: str, mariadb_version: str,
            extra: Dict[str, Any] = dict(),
            **kwargs
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
        return self._request('GET', 'validate-mdb-version', {**data, **extra}, **kwargs)

    def validate_es_token(
            self, token: str,
            extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Verify MariaDB ES token is correct.

        :param token: ES token to verify
        :type token: str
        :return: The response from the API.
        """
        return self._request(
            'GET', 'validate-es-token', {'token': token, **extra}
        )

    def start_mariadb(
            self, extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Start MariaDB-server service on a node.

        :return: The response from the API.
        """
        return self._request('PUT', 'start-mariadb', extra)

    def stop_mariadb(
            self, extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Stop MariaDB-server service on a node.

        :return: The response from the API.
        """
        return self._request('PUT', 'stop-mariadb', extra)

    def repo_pkg_versions(
            self, extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Get available packages versions from the repo on a node.

        :return: The response from the API.
        """
        return self._request('GET', 'repo-pkg-versions', extra)

    def install_repo(
            self, token: str, mariadb_version: str,
            extra: Dict[str, Any] = dict()
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
        return self._request('PUT', 'install-repo', {**data, **extra})


    def preupgrade_backup(
            self, extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Backup DBRM and configs on a node.

        :return: The response from the API.
        """
        return self._request('PUT', 'preupgrade-backup', extra)

    def upgrade_mdb_mcs(
            self, mariadb_version: str, columnstore_version: str,
            extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Upgrade MariaDB and Columnstore on a node.

        :return: The response from the API.
        """
        data = {
            'mariadb_version': mariadb_version,
            'columnstore_version': columnstore_version
        }
        return self._request('PUT', 'upgrade-mdb-mcs', {**data, **extra})

    def kick_cmapi_upgrade(
            self, version: str,
            extra: Dict[str, Any] = dict()
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Starting cmapi_updater.service on a node.

        :return: The response from the API.
        """
        return self._request(
            'PUT', 'kick-cmapi-upgrade', {'version': version, **extra}
        )


class AppControllerClient(BaseClient):
    """Client for the AppController API.

    This class provides methods for interacting with a cmapi special management
    API.
    """
    def __init__(
            self, base_url: str = CURRENT_NODE_CMAPI_URL,
            request_timeout: Optional[float] = None
    ):
        """Initialize the NodeControllerClient with the base URL.

        :param base_url: The base URL for the API endpoints,
                         defaults to CURRENT_NODE_CMAPI_URL
        :type base_url: str, optional
        :param request_timeout: request timeout, defaults to None
        :type request_timeout: Optional[float], optional
        """
        super().__init__(base_url, request_timeout)
        self.cmd_url = f'{self.base_url}/cmapi/'

    def get_ready(self) -> Union[Dict[str, Any], Dict[str, str]]:
        """Get CMAPI ready or not.

        :return: The response from the API.
        """
        return self._request('GET', 'ready', None, throw_real_exp=True)
