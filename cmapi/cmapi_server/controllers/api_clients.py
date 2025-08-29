import logging
from typing import Any, Dict, Optional, Union

import pyotp
import requests

from cmapi_server.constants import (
    CMAPI_CONF_PATH,
    CURRENT_NODE_CMAPI_URL,
    SECRET_KEY,
)
from cmapi_server.controllers.dispatcher import _version
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.helpers import get_config_parser, get_current_key
from tracing.traced_session import get_traced_session


class ClusterControllerClient:
    def __init__(
            self, base_url: str = CURRENT_NODE_CMAPI_URL,
            request_timeout: Optional[float] = None
    ):
        """Initialize the ClusterControllerClient with the base URL.

        WARNING: This class only handles the API requests, it does not
        handle the transaction management. So it should be started
        at level above using TransactionManager (decorator or context manager).

        :param base_url: The base URL for the API endpoints,
                         defaults to CURRENT_NODE_CMAPI_URL
        :type base_url: str, optional
        :param request_timeout: request timeout, defaults to None
        :type request_timeout: Optional[float], optional
        """
        self.base_url = base_url
        self.request_timeout = request_timeout

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
        return self._request('put', 'apikey-set', payload)

    def set_log_level(
            self, log_level: str
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Set the log level for the cluster.

        :param log_level: The log level to set.
        :return: The response from the API.
        """
        return self._request('put', 'log-level', {'log_level': log_level})

    def load_s3data(
            self, s3data_info: Dict[str, Any]
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Load S3 data into the cluster.

        :param s3data_info: Information about the S3 data to load.
        :return: The response from the API.
        """
        return self._request('put', 'load_s3data', s3data_info)

    def _request(
            self, method: str, endpoint: str,
            data: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Make a request to the API.

        :param method: The HTTP method to use.
        :param endpoint: The API endpoint to call.
        :param data: The data to send with the request.
        :return: The response from the API.
        """
        url = f'{self.base_url}/cmapi/{_version}/cluster/{endpoint}'
        cmapi_cfg_parser = get_config_parser(CMAPI_CONF_PATH)
        key = get_current_key(cmapi_cfg_parser)
        headers = {'x-api-key': key}
        if method in ['PUT', 'POST', 'DELETE']:
            headers['Content-Type'] = 'application/json'
            data = {'in_transaction': True, **(data or {})}
        try:
            response = get_traced_session().request(
                method, url, headers=headers, json=data,
                timeout=self.request_timeout, verify=False
            )
            response.raise_for_status()
            return response.json()
        # TODO: different handler for timeout exception?
        except requests.HTTPError as exc:
            resp = exc.response
            error_msg = str(exc)
            if resp is not None and resp.status_code == 422:
                # in this case we think cmapi server returned some value but
                # had error during running endpoint handler code
                try:
                    resp_json = resp.json()
                    error_msg = resp_json.get('error', resp_json)
                except requests.exceptions.JSONDecodeError:
                    error_msg = resp.text
            message = (
                f'API client got an exception in request to {exc.request.url if exc.request else url} '
                f'with code {resp.status_code if resp is not None else "?"} and error: {error_msg}'
            )
            logging.error(message)
            raise CMAPIBasicError(message)
        except requests.exceptions.RequestException as exc:
            request_url = getattr(exc.request, 'url', url)
            response_status = getattr(getattr(exc, 'response', None), 'status_code', '?')
            message = (
                'API client got an undefined error in request to '
                f'{request_url} with code {response_status} and '
                f'error: {str(exc)}'
            )
            logging.error(message)
            raise CMAPIBasicError(message)
