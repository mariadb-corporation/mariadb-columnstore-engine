import requests
from typing import Any, Dict, Optional, Union
from cmapi_server.controllers.dispatcher import _version
from cmapi_server.constants import CURRENT_NODE_CMAPI_URL


class ClusterControllerClient:
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
        self.base_url = base_url
        self.request_timeout = request_timeout

    def start_cluster(
            self, data: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Start the cluster.

        :return: The response from the API.
        """
        return self._request('PUT', 'start', data)

    def shutdown_cluster(
            self, data: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Shutdown the cluster.

        :return: The response from the API.
        """
        return self._request('PUT', 'shutdown', data)

    def set_mode(self, mode: str) -> Union[Dict[str, Any], Dict[str, str]]:
        """Set the cluster mode.

        :param mode: The mode to set.
        :return: The response from the API.
        """
        return self._request('PUT', 'mode-set', {'mode': mode})

    def add_node(
            self, node_info: Dict[str, Any]
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Add a node to the cluster.

        :param node_info: Information about the node to add.
        :return: The response from the API.
        """
        return self._request('PUT', 'node', node_info)

    def remove_node(
            self, node_id: str
    ) -> Union[Dict[str, Any], Dict[str, str]]:
        """Remove a node from the cluster.

        :param node_id: The ID of the node to remove.
        :return: The response from the API.
        """
        return self._request('DELETE', 'node', {'node_id': node_id})

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
        return self._request('put', 'apikey-set', {'api_key': api_key})

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
        try:
            response = requests.request(
                method, url, json=data, timeout=self.request_timeout
            )
            response.raise_for_status()
            return response.json()
        # TODO: different handler for timeout exception?
        except requests.exceptions.RequestException as e:
            return {'error': str(e)}
