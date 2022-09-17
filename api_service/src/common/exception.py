"""Implements the exception related modules"""

import os
from http import HTTPStatus
import traceback

class NestedException(Exception):

    """Represents an exception with an optional cause and message"""

    def __init__(self, exception_message, cause=None):
        super().__init__(exception_message)
        self._exception_message = exception_message
        self._cause = cause

    def cause(self):
        """Returns the exception cause"""
        return self._cause

class OperationException(NestedException):

    """Represents an operation based exception"""

    def __init__(
            self, exception_message, cause=None, class_name_for_root_cause=None,
            response_status=HTTPStatus.INTERNAL_SERVER_ERROR):
        super().__init__(exception_message, cause)
        self._response_status = response_status
        self._class_name_for_root_cause = class_name_for_root_cause

    def response_status(self):
        """Returns response HTTP status"""
        return self._response_status

    def class_name_for_root_cause(self):
        """Returns the class name for the root cause"""
        return self._class_name_for_root_cause


class WebServiceException(NestedException):

    """Represents a web service exception"""

    _NEW_LINE = "\n"

    def __init__(
            self, operation_exception, cause=None, class_name_for_root_cause=None, exception_details=None,
            response_status=HTTPStatus.INTERNAL_SERVER_ERROR):
        super().__init__(WebServiceException._generate_exception_message(operation_exception, exception_details), cause)
        self._class_name_for_root_cause = class_name_for_root_cause
        self._exception_details = WebServiceException._prepare_exception_details(exception_details)
        self._operation_exception = operation_exception
        self._response_status = response_status

    @staticmethod
    def _all_exception_messages(exception):
        """Returns all exception messages of an exception"""
        exception_messages = [ExceptionUtilities.message(exception)]
        cause = getattr(exception, "cause", None)
        if cause and callable(cause) and exception.cause():
            exception_messages.extend(WebServiceException._all_exception_messages(exception.cause()))
        return exception_messages

    @staticmethod
    def _generate_exception_message(operation_exception, exception_details=None):
        """Constructs and returns the exception message"""
        if exception_details is None:
            return operation_exception
        formatted_exception_details = exception_details if isinstance(exception_details, str) else \
            WebServiceException._NEW_LINE.join(exception_details)
        return f"{operation_exception}, details: {formatted_exception_details}"

    @staticmethod
    def _prepare_exception_details(exception_details=None):
        """Prepares and returns the exception details"""
        if exception_details is None:
            return []
        return [exception_details] if isinstance(exception_details, str) else exception_details

    def all_exception_messages(self):
        """Returns the all exception messages"""
        return WebServiceException._all_exception_messages(self)

    def cause_message(self):
        """Returns the cause message"""
        return ExceptionUtilities.message(self._cause)

    def class_name_for_root_cause(self):
        """Returns the class name for the root cause"""
        return self._class_name_for_root_cause

    def exception_details(self):
        """Returns exception details"""
        return self._exception_details

    def operation_exception(self):
        """Returns operation exception"""
        return self._operation_exception

    def response_status(self):
        """Returns response HTTP status"""
        return self._response_status


class ExceptionUtilities:

    """Implements utilities for REST service exceptions"""

    @staticmethod
    def message(exception):
        """Returns the exception message"""
        return exception.message if hasattr(exception, "message") else str(exception)

    @staticmethod
    def validation_failure_to_error_message(validation_failures):
        """Returns combined error message form list of validate failures"""
        error_message = ""
        for validation_failure in validation_failures:
            validation_errors = validation_failure.validationErrors
            if not validation_errors:
                continue
            error_message += validation_failure.baseError + os.linesep
            error_message += os.linesep.join(
                "\t- %s" % validation_error_text for validation_error_text in validation_errors)
            error_message += os.linesep
        return error_message

    @staticmethod
    def stack_trace_as_string(exception):
        """Returns the full stack trace as a string"""
        return "".join(traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__))