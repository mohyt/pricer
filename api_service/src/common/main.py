"""Implements the Main modules"""

import logging
import logging.handlers
import os
import sys
import traceback
from collections import OrderedDict

from common.file_system import resilient_makedirs

DATE_FORMAT = "%m-%d %H:%M:%S"
LOGGER_FORMAT = "%(asctime)s.%(msecs)03d|%(name)s|%(funcName)s|%(levelname)s|%(message)s"
logging.basicConfig(format=LOGGER_FORMAT, datefmt=DATE_FORMAT)

class _MainImpl:

    """Stores the internal data members accessible via the 'Main' class"""

    def __init__(self, application_name, *args, **kwargs):  # pylint: disable=unused-argument
        log_level = kwargs.get("log_level", None)
        log_file_path = kwargs.get("log_file_path", None)
        self._logger = Logger(application_name, level=log_level, log_file_path=log_file_path)

    def logger(self):
        """Returns the logger"""
        return self._logger

class Logger:

    """Defines a logger for an application"""

    LOG_FILE_MAX_BYTES = 10 * 1024 * 1024
    LOG_FILE_BACKUP_COUNT = 5
    VALID_LOG_LEVELS = OrderedDict(
        sorted({x: logging._levelToName[x] for x in logging._levelToName  # pylint: disable=protected-access
                if str(x).isdigit()}.items()))

    def __init__(self, application_name, level=None, log_file_path=None):
        self._application_name = application_name
        self._logger = logging.getLogger(self._application_name)
        self.set_properties(level, log_file_path)

    def set_properties(self, level, log_file_path):
        """Sets the logger properties"""
        if level is None:
            level = logging.WARNING
        formatter = logging.Formatter(LOGGER_FORMAT, datefmt=DATE_FORMAT)
        self._logger.setLevel(level)
        if log_file_path is None:
            return
        log_directory = os.path.dirname(log_file_path)
        resilient_makedirs(log_directory)
        file_logger_handler = logging.handlers.RotatingFileHandler(
            log_file_path, maxBytes=Logger.LOG_FILE_MAX_BYTES,
            backupCount=Logger.LOG_FILE_BACKUP_COUNT)
        file_logger_handler.setFormatter(formatter)
        self._logger.addHandler(file_logger_handler)
        self._logger.info(
            "the logs for the '%s' application are available at '%s'", self._application_name, log_directory)

    def logger(self):
        """Returns the logger instance"""
        return self._logger

class Main:

    """Defines the scaffolding Main class"""

    # Note: EX__BASE is not available in Python, so we are using the hack below to provide the base exit code since
    # EX__BASE == EX_USAGE
    BASE_EXIT_CODE = os.EX_USAGE

    FAILURE = os.EX_SOFTWARE        # in Linux: the numeric code is 70
    INVALID_USAGE = os.EX_USAGE     # in Linux: the numeric code is 64
    SUCCESS = os.EX_OK              # in Linux, the numeric code is 0
    # Note: EX__MAX is not available in Python, so we are using the hack below to avoid a clash (in Linux at least
    # EX__MAX == EX_CONFIG, and 22 is just an arbitrary value to make the UNEXPECTED_EXCEPTION_THROWN value 100)
    UNEXPECTED_EXCEPTION_THROWN = os.EX_CONFIG + 22     # this should be 100

    _impl = None

    @staticmethod
    def _assert_initialized():
        """Asserts that 'Main' is initialized"""
        assert Main._impl is not None, "'Main' has not been initialized yet"

    @classmethod
    def _run_it(cls, run_function, **kwargs):
        """Runs a top-level application function, catching all exceptions that might have bubbled up, providing a
        uniform error-handling message"""

        def _handle_exception(exception):
            """Handles an exception, providing diagnostics information"""
            traceback.print_exc(file=sys.stderr)
            details = str(exception)
            if len(details) == 0:
                details = str(type(exception))
            sys.stderr.write("\n")
            argv_as_str = " ".join(sys.argv)
            Main.print_error_ln(f"the execution of '{argv_as_str}' failed due to an exception, details: {details}")
            return exception
        try:
            exception = None
            return_value = None
            try:
                if not callable(run_function):
                    raise ValueError(
                        f"the 'run_function' parameter must be a callable, yet a parameter of the '{type(run_function)}' type was passed")
                return_value = run_function(**kwargs)
            except SystemExit as ex:
                return_value = ex.code
            except ImportError as ex:
                sys.stderr.write(
                    "\nfailed to import a module, running with the following Python path:\n\t - %s\n" %
                    "\n\t - ".join(sys.path))
                exception = _handle_exception(ex)
            except KeyboardInterrupt as ex:
                sys.stderr.write(
                    f"\n\nWARNING: the '{os.path.basename(sys.argv[0])}' process (pid={os.getpid()}) was interrupted\n")
                exception = ex
            # this is the final catchall for the application, so it must be general
            except BaseException as ex:
                exception = _handle_exception(ex)
            return return_value, exception
        finally:
            if cls.initialized():
                cls.finalize()

    @staticmethod
    def finalize(*args, **kwargs):  # pylint: disable=unused-argument
        """Finalizes all global singletons in this scope"""
        Main._assert_initialized()
        Main._impl = None

    @staticmethod
    def initialize(application_name, *args, **kwargs):
        """Initializes all global singletons in this scope"""
        assert Main._impl is None, "'Main' has already been initialized"
        Main._impl = _MainImpl(application_name, *args, **kwargs)

    @staticmethod
    def initialized():
        """Returns True if 'Main' has been initialized"""
        return Main._impl is not None

    @staticmethod
    def logger():
        """Returns logger object instance"""
        Main._assert_initialized()
        return Main._impl.logger().logger()

    @classmethod
    def run_and_exit(cls, run_function, failure_exit_code=FAILURE, **kwargs):
        """Runs a top-level application function and exits with proper return code"""
        return_value, exception = cls._run_it(run_function, **kwargs)
        if exception is not None:
            sys.exit(Main.UNEXPECTED_EXCEPTION_THROWN)
        else:
            if return_value is not None:
                sys.exit(return_value)
            sys.exit(failure_exit_code)

    @staticmethod
    def print_error(message):
        """Prints an error message to the standard error"""
        sys.stderr.write(f"ERROR: {message}")

    @staticmethod
    def print_error_ln(message):
        """Prints an error message to the standard error followed by a newline"""
        Main.print_error(f"{message}\n")

    @staticmethod
    def set_logger_properties(level, log_file_path):
        """Sets the logger properties"""
        Main._assert_initialized()
        Main._impl.logger().set_properties(level, log_file_path)
