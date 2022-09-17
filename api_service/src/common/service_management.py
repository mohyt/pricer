"""Defines Service and ServiceManager"""

class Service:

    """Implements a base service"""

    def prepare(self):
        """Prepares the service"""

    def start(self):
        """Starts the service"""

    def stop(self):
        """Stops the service"""

    def validate(self):
        """Validates the service"""


class ServiceManager:

    """Implements a base service manager"""

    def destroy(self):
        """Destroys the service manager"""

    def prepare(self):
        """Prepares the service manager"""

    def start(self):
        """Starts the service manager"""

    def stop(self):
        """Stops the service manager"""

    def validate(self):  # pylint: disable=no-self-use
        """Validates the service manager"""
        return []
