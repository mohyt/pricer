"""Implements health service manager"""

from common.service_management import ServiceManager

class HealthServiceManager(ServiceManager):

    """Implements health service manager"""

    PONG_MESSAGE = "PONG"

    def ping(self):  # pylint: disable=no-self-use
        """Informs the client that server is alive"""
        return { "response": HealthServiceManager.PONG_MESSAGE }
