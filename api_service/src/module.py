"""Implements module for Cost Fetcher service"""

from health.controller import HealthServiceController
from health.manager import HealthServiceManager
from rest_server.module import BaseModule


class CostFetcherModule(BaseModule):
    """Module class for cost fetcher service"""

    def initialize(self):
        """Initializes the cost fetcher module"""
        self.add_controller(HealthServiceController, HealthServiceManager())