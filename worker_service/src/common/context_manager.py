"""Implements context manager-related utility functions"""

from abc import ABC, abstractmethod

class SafeContextManager(ABC):

    """Implements a safer context handler where cascading exceptions are not swallowed"""

    def __enter__(self):
        try:
            return self._enter()
        except BaseException as exception_from__enter:
            try:
                self._exit()
            except BaseException as exception_from__exit:
                raise exception_from__enter from exception_from__exit
            raise

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self._exit()
        return False

    def _enter(self):
        """Implements the logic to be executed by the context manager's __enter__ method"""
        return self

    @abstractmethod
    def _exit(self):
        """Implements the logic to be executed by the context manager's __exit__ method"""


class DatabaseConnectionContextManager(SafeContextManager):
    """Creates database connection context"""

    def __init__(self, connection_pool):
        self._connection_pool = connection_pool
        self._connection = None

    def _enter(self):
        self._connection = self._connection_pool.connect()
        return self
    
    def _exit(self):
        if not self._connection:
            return
        self._connection.close()
    
    def connection(self):
        """Returns the connection"""
        return self._connection