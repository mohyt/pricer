"""Implements prepare, validate start REST service operations"""

import logging

from quart import Quart
from werkzeug.utils import import_string

from common.exception import ExceptionUtilities
from common.main import Main
from common.safe_runners import run_all_and_raise_suppressed_exceptions_if_any, run_and_cleanup_on_error
from rest_server.error_handler import HttpErrorHandler

def rest_application(
        module_class_path, service_name, application_context_path=None, working_directory=None):
    """Return rest application to be run inside wsgi server"""
    module_class = import_string(module_class_path)
    return RestMain.initialize_and_return_rest_application(
        module_class, service_name, application_context_path=application_context_path,
        working_directory=working_directory)


class _RestMainImpl:

    """Stores the internal data members accessible via the 'RestMain' class"""

    def __init__(self):
        self._rest_application = Quart(__name__)
        self._service_managers = []

    def _prepare_service_managers(self):
        """Prepares service managers"""
        for service_manager in self._service_managers:
            service_manager.prepare()

    def _start_service_managers(self):
        """Starts service managers"""
        for service_manager in self._service_managers:
            service_manager.start()

    def _validate_service_managers(self):
        """Validates service managers"""
        validation_failures = []
        for service_manager in self._service_managers:
            validation_failures = validation_failures + service_manager.validate()
        if not validation_failures:
            return
        error_message = ExceptionUtilities.validation_failure_to_error_message(validation_failures)
        raise RuntimeError(error_message)

    def destroy_service_managers(self):
        """Destroys service managers"""
        destroy_service_tasks = []
        for service_manager in self._service_managers:
            destroy_service_tasks.append(service_manager.destroy)
        run_all_and_raise_suppressed_exceptions_if_any("failed to destroy service managers", destroy_service_tasks)

    def initialize_services(
            self, application_context_path, module_class, working_directory=None):
        """Initializes the services with the given names"""
        module = module_class(
            application_context_path, self._rest_application, working_directory)
        Main.logger().info("initializing the main module...")
        module.initialize()
        self._service_managers.extend(module.service_managers())
        Main.logger().info("preparing the service managers")
        self._prepare_service_managers()
        Main.logger().info("validating the service managers")
        self._validate_service_managers()
        Main.logger().info("starting the service managers")
        self._start_service_managers()
        Main.logger().info("initializing the error handlers")
        HttpErrorHandler.initialize_error_handlers(self._rest_application)

    def rest_application(self):
        """Returns the rest application"""
        return self._rest_application

    def stop_service_managers(self):
        """Stops service managers"""
        stop_service_tasks = []
        for service_manager in self._service_managers:
            stop_service_tasks.append(service_manager.stop)
        run_all_and_raise_suppressed_exceptions_if_any("failed to stop service managers", stop_service_tasks)


class RestMain:

    """Initializes and starts REST services"""

    _REST_MAIN_NOT_INITIALIZED_ERROR_MESSAGE = "the RestMain has not been initialized yet"

    _impl = None

    @staticmethod
    def finalize():
        """Finalizes the RestMain"""
        rest_main_impl = RestMain.rest_main_impl()
        finalize_tasks = [rest_main_impl.stop_service_managers, rest_main_impl.destroy_service_managers, Main.finalize]
        run_all_and_raise_suppressed_exceptions_if_any("failed to finalize the RestMain", finalize_tasks)

    @staticmethod
    def initialize_and_return_rest_application(
            module_class, service_name, application_context_path=None, working_directory=None):
        """Initializes and returns the rest application"""
        log_level = logging.DEBUG
        log_file_path = None
        if not Main.initialized():
            Main.initialize(
                f"{service_name}_rest_server", log_level=log_level, log_file_path=log_file_path)
        RestMain._impl = _RestMainImpl()
        run_and_cleanup_on_error(
            RestMain.finalize,
            f"failed to initialize services of '{service_name}' service",
            lambda: RestMain.rest_main_impl().initialize_services(
                application_context_path if application_context_path else service_name, module_class,
                working_directory=working_directory))
        return RestMain._impl.rest_application()

    @staticmethod
    def rest_main_impl():
        """Returns the global instance of RestMain"""
        if not RestMain._impl:
            raise RuntimeError(RestMain._REST_MAIN_NOT_INITIALIZED_ERROR_MESSAGE)
        return RestMain._impl