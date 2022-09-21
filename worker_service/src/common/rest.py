"""Implements a rest client interface"""

from http import HTTPStatus

import requests

from common.json_loader import RecursiveNamespace


class RestClient:
    """Implements a rest client interface"""

    _DEFAULT_REQUEST_TIMEOUT_IN_SECONDS = 120
    _CONTENT_TYPE = "Content-Type"
    _SUCCESSFUL_RESPONSE_CODES = [
        HTTPStatus.OK,
        HTTPStatus.CREATED,
        HTTPStatus.ACCEPTED,
        HTTPStatus.NO_CONTENT,
        HTTPStatus.NOT_MODIFIED
    ]
    
    def __init__(self, bearer_token=None):
        self._headers = {
            "Content-Type": "application/json"
        }
        if bearer_token:
            self._headers["Authorization"] = f"Bearer {bearer_token}"
    
    def send_post_request(self, url_path, request_body=None):
        """Sends the post request"""
        return self._request(url_path, "POST", request_body)
    
    def send_get_request(self, url_path):
        """Sends the get request"""
        return self._request(url_path, "GET")

    def _request(self, url_path, method, json=None):
        """Performs REST request"""
        response = requests.request(
            method, url_path, headers=self._headers, json=json, timeout=RestClient._DEFAULT_REQUEST_TIMEOUT_IN_SECONDS)
        if response.ok:
            return response.json(object_hook=RecursiveNamespace.map_entry)
        error_status_code = response.status_code
        if error_status_code in RestClient._SUCCESSFUL_RESPONSE_CODES:
            return response
        error_header_content_type = response.headers.get(RestClient._CONTENT_TYPE)
        if error_header_content_type == "text/html":
            response_message = response.text() if response.encoding is None else response.text.decode(
                response.encoding, "ignore")
        else:
            response_message = response.json()
        raise RuntimeError(
            f"failed to perform '{method}' request to URL '{url_path}', details: {error_status_code} " +
            f"{response_message}")
