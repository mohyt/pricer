"""Implements extraction service interface"""

from common.service_management import Service


class AbstractSource(Service):
    """Implements extraction service interface"""

    def extract_and_transform(self, result_handler):
        """Implements the extraction and performs transformation"""
