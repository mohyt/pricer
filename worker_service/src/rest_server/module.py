"""Implements a base module for REST applications"""

from abc import ABC, abstractmethod

from rest_server.utilities import RestUrlPathUtilities


class BaseModule(ABC):
    """Implements a base module for REST applications"""

    def __init__(
            self, application_context_path, rest_application, working_directory=None):
        self._application_context_path = application_context_path
        self._rest_application = rest_application
        self._service_managers = []
        self._working_directory = working_directory

    def add_controller(self, controller_class, manager):
        """Creates and adds controller to rest application"""
        self._service_managers.append(manager)
        controller = controller_class(manager)
        self._rest_application.register_blueprint(
            controller.service_blueprint(), url_prefix=RestUrlPathUtilities.url_path_with_leading_path_separator(
                self._application_context_path, controller.service_url()))
        return controller
    
    def add_service_manager(self, manager):        
        """Creates and adds manager to rest application"""
        self._service_managers.append(manager)

    @abstractmethod
    def initialize(self):
        """Initializes the module"""

    def service_managers(self):
        """Returns the service managers of this module"""
        return self._service_managers

    def working_directory(self):
        """Returns the working directory of the server application"""
        return self._working_directory
