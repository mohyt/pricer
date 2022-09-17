import asyncio
import inspect
import json
from http import HTTPStatus
from enum import Enum

from common.main import Main
from common.exception import ExceptionUtilities, OperationException, WebServiceException
from quart import Blueprint, Response, request

class RequestMethodType(Enum):
    """Defines request method types"""
    DELETE = "Delete"
    GET = "Get"
    POST = "Post"
    PUT = "Put"


def _decorate_controller_method(controller_method, request_method, url):
    """Implements a decorator to wrap a controller method and adds RouteProperties to it"""
    setattr(controller_method, RouteProperties.ROUTE_PROPERTIES_KEYWORD, RouteProperties(request_method, url))
    return controller_method


def body(parameter_name):
    """Implements a decorator to retrieve HTTP request body as an object in controller methods"""
    def decorate(controller_method):
        async def wrap_controller_method(*args, **kwargs):
            request_body_as_json = await request.get_json()
            body_as_object = json.loads(request_body_as_json) \
                if isinstance(request_body_as_json, str) else request_body_as_json
            kwargs[parameter_name] = body_as_object
            response = controller_method(*args, **kwargs)
            if asyncio.iscoroutine(response):
                return await response
            return response
        return wrap_controller_method
    return decorate


def delete(url):
    """Implements a decorator to wrap a controller method to be used by HTTP DELETE method"""
    def decorate(controller_method):
        async def wait_for_method(*args, **kwargs):
            return await controller_method(*args, **kwargs)
        return _decorate_controller_method(wait_for_method, RequestMethodType.DELETE.value, url)
    return decorate


def get(url):
    """Implements a decorator to wrap a controller method to be used by HTTP GET method"""
    def decorate(controller_method):
        async def wait_for_method(*args, **kwargs):
            return await controller_method(*args, **kwargs)
        return _decorate_controller_method(wait_for_method, RequestMethodType.GET.value, url)
    return decorate


def post(url):
    """Implements a decorator to wrap a controller method to be used by HTTP POST method"""
    def decorate(controller_method):
        async def wait_for_method(*args, **kwargs):
            return await controller_method(*args, **kwargs)
        return _decorate_controller_method(wait_for_method, RequestMethodType.POST.value, url)
    return decorate


def put(url):
    """Implements a decorator to wrap a controller method to be used by HTTP PUT method"""
    def decorate(controller_method):
        async def wait_for_method(*args, **kwargs):
            return await controller_method(*args, **kwargs)
        return _decorate_controller_method(wait_for_method, RequestMethodType.PUT.value, url)
    return decorate

class RestResponse:
    """Defines custom response class for REST service operations"""

    def __init__(self, response=None, content_type=None, response_status=None):
        self._response = response
        self._content_type = content_type
        self._response_status = response_status

    def content_type(self):
        """Returns the content type of the REST response"""
        return self._content_type

    def response(self):
        """Returns the actual response body of the REST response"""
        return self._response

    def response_status(self):
        """Returns the HTTP status of the REST response"""
        return self._response_status

class RestController:

    """Implements a base REST controller"""

    _DEFAULT_REQUEST_METHOD_2_RESPONSE_STATUS_CODE = {
        RequestMethodType.DELETE.value: HTTPStatus.OK,
        RequestMethodType.GET.value: HTTPStatus.OK,
        RequestMethodType.POST.value: HTTPStatus.CREATED,
        RequestMethodType.PUT.value: HTTPStatus.OK,
    }
    _OPERATION_ERROR_OCCURRED_OPERATION_ERROR_MESSAGE = "an operation error has occurred"
    _UNEXPECTED_ERROR_OCCURRED_OPERATION_ERROR_MESSAGE = "an unexpected error has occurred"

    def __init__(self, service_url):
        self._service_url = service_url
        self._service = Blueprint(service_url, __name__)
        self._register_endpoints()

    @staticmethod
    def _generate_rest_application_response(rest_response):
        """Generates and returns the response that will be returned from the REST application"""
        request_method = request.method
        default_response_status = \
            RestController._DEFAULT_REQUEST_METHOD_2_RESPONSE_STATUS_CODE.get(request_method, HTTPStatus.OK)
        if not rest_response:
            return Response(response="", status=default_response_status)
        response = rest_response.response()
        response_status = rest_response.response_status() or default_response_status
        if not response:
            return Response(response="", status=response_status)
        content_type = rest_response.content_type()
        return json.dumps(response), response_status, content_type

    def _register_endpoints(self):
        """Registers endpoints to the service blueprint"""
        method_list = inspect.getmembers(self, predicate=inspect.ismethod)
        route_key = RouteProperties.ROUTE_PROPERTIES_KEYWORD
        for (method_name, method) in method_list:
            if hasattr(method, route_key):
                route_properties = getattr(method, route_key)
                self._service.add_url_rule(
                    route_properties.url(), method_name, method, methods=[route_properties.method()])

    @staticmethod
    async def _run_task_and_return_response(task):
        """Runs a controller operation task and returns the generated response"""
        response = task()
        if asyncio.iscoroutine(response):
            response = await response
        rest_response = response if isinstance(response, RestResponse) else RestResponse(response)
        return RestController._generate_rest_application_response(rest_response)

    @staticmethod
    async def run(task, operation_exception_message, operation_exception_parameter_2_value=None):
        """Runs a controller operation and throws the web service exception if there is any"""
        try:
            return await RestController._run_task_and_return_response(task)
        except OperationException as exception:
            Main.logger().error(
                "%s in '%s' endpoint, details: %s", RestController._OPERATION_ERROR_OCCURRED_OPERATION_ERROR_MESSAGE,
                request.path, ExceptionUtilities.stack_trace_as_string(exception))
            raise WebServiceException(
                operation_exception=operation_exception_message if not operation_exception_parameter_2_value else
                operation_exception_message.format(**operation_exception_parameter_2_value),
                cause=exception,
                class_name_for_root_cause=exception.class_name_for_root_cause(),
                response_status=exception.response_status())
        except BaseException as exception:
            Main.logger().error(
                "%s in '%s' endpoint, details: %s", RestController._UNEXPECTED_ERROR_OCCURRED_OPERATION_ERROR_MESSAGE,
                request.path, ExceptionUtilities.stack_trace_as_string(exception))
            raise WebServiceException(
                operation_exception=RestController._UNEXPECTED_ERROR_OCCURRED_OPERATION_ERROR_MESSAGE,
                cause=exception)

    def service_blueprint(self):
        """Returns blueprint for the service endpoint"""
        return self._service

    def service_url(self):
        """Returns the service url"""
        return self._service_url


class RouteProperties:
    """Defines route properties class for controller methods"""

    ROUTE_PROPERTIES_KEYWORD = "_route"

    def __init__(self, method, url):
        self._method = method
        self._url = url

    def method(self):
        """Returns the HTTP method of the route"""
        return self._method

    def url(self):
        """Returns the URL of the route"""
        return self._url
