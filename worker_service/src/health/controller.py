"""Implements health service controller"""

from rest_server.constants import (
    HEALTH_MANAGER_URL, REST_PING_ENDPOINT)
from rest_server.controller import RestController, get
from rest_server.utilities import RestUrlPathUtilities


class HealthServiceController(RestController):

    """Implements health service controller."""

    PING_ERROR_MESSAGE = "failed to ping the REST server"

    def __init__(self, server_management_service_manager):
        super().__init__(HEALTH_MANAGER_URL)
        self.server_management_service_manager = server_management_service_manager

    @get(RestUrlPathUtilities.url_path_with_leading_path_separator(REST_PING_ENDPOINT))
    def ping(self):
        """Informs the client that server is alive"""
        return self.run(
            self.server_management_service_manager.ping, HealthServiceController.PING_ERROR_MESSAGE)
