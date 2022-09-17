import json
from http import HTTPStatus

from common.exception import ExceptionUtilities, WebServiceException


class HttpErrorHandler:
    """Implements HTTP error handlers"""

    _ERROR_DETAILS_SEPARATOR = "; "

    @staticmethod
    def initialize_error_handlers(rest_application):
        """Initializes HTTP error handlers"""

        @rest_application.errorhandler(WebServiceException)
        def handle_http_exceptions(exception):  # pylint: disable=unused-variable
            """Handles WebServiceException exceptions"""
            exception_details = exception.exception_details()
            end_user_message_detail = exception.cause_message() if not exception_details else \
                HttpErrorHandler._ERROR_DETAILS_SEPARATOR.join(exception_details)
            error_message = {
                "allMessages": exception.all_exception_messages(),
                "classNameForRootCause": exception.class_name_for_root_cause(),
                "endUserMessageBase":exception.operation_exception(),
                "endUserMessageDetail": end_user_message_detail
            }
            return json.dumps(error_message), exception.response_status()
        
        @rest_application.errorhandler(Exception)
        def handle_unknown_exceptions(exception):  # pylint: disable=unused-variable
            """Handles unknown exceptions"""
            return ExceptionUtilities.message(exception), HTTPStatus.INTERNAL_SERVER_ERROR
