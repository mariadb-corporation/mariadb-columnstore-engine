import logging
from typing import Any, Dict, List, Optional, Union

import pyotp
import requests

from cmapi_server.constants import (
    CMAPI_CONF_PATH, CURRENT_NODE_CMAPI_URL, SECRET_KEY,
)
from cmapi_server.controllers.dispatcher import _version
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.helpers import get_config_parser, get_current_key
from tracing.traced_session import get_traced_session


_version = '0.4.0'


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
        """
        self.base_url = base_url
        self.request_timeout = request_timeout
        self.cmd_class = None

    def _request(
        self, method: str, endpoint: str,
        data: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], List[Any]]:
        """Make a request to the API.
        :param method: The HTTP method to use.
        :param endpoint: The API endpoint to call.
        :param data: The data to send with the request.
        :return: The response from the API.
        """

        url = f'{self.base_url}/cmapi/{_version}/{self.cmd_class}/{endpoint}'
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
        # TODO: different handler for timeout exception?
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
            raise CMAPIBasicError(message)
        except requests.exceptions.Timeout:
            message = f'Request to {url} timed out after {self.request_timeout}'
            logging.error(message)
            raise CMAPIBasicError(message)
        except requests.exceptions.RequestException as exc:
            resp = exc.response
            request_url = exc.request.url if exc.request else url
            status_code = resp.status_code if exc.response is not None else 'N/A'
            message = (
                'API client got an undefined error in request to '
                f'{request_url} with code {status_code} and '
                f'error: {str(exc)}'
            )
            logging.error(message)
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
        self.cmd_class = 'cluster'

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

        :param node_info: Information about the node to add.
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
        return self._request('put', 'load_s3data', s3data_info)

    def check_shared_storage(
            self, extra: Dict[str, Any] = dict()
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
        self.cmd_class = 'node'

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
