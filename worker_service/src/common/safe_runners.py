"""Implements the Safe Runners method"""

import asyncio
import sys
import traceback

def _prepare_suppressed_exception_messages(exceptions):
    """Prepares the suppressed exception messages"""
    suppressed_exception_messages = []
    for exception in exceptions:
        suppressed_exception_messages += traceback.format_exception(*exception)
        suppressed_exception_messages += ["\n"]
    return suppressed_exception_messages

def run_all_and_raise_suppressed_exceptions_if_any(message, functions):
    """Runs all of the functions, handling their exceptions, and, subsequently, raises any exceptions that might have
    been suppressed"""
    exceptions = []
    for function in functions:
        try:
            function()
        except BaseException:
            exceptions.append(sys.exc_info())
    if len(exceptions) == 0:
        return
    suppressed_exception_messages = _prepare_suppressed_exception_messages(exceptions)
    raise RuntimeError(
        "%s, details: one or more exceptions were raised while executing the requested functions:\n\n%s" % (
            message, "".join(suppressed_exception_messages)))

def run_and_cleanup_on_error(cleanup_task, message, task):
    """Runs the task, handling its exceptions and running the cleanup task in that case, and, subsequently, raises any
    exceptions that might have been suppressed"""
    try:
        task()
    except BaseException:
        exceptions = [sys.exc_info()]
        try:
            cleanup_task()
        except BaseException:
            exceptions.append(sys.exc_info())
        suppressed_exception_messages = _prepare_suppressed_exception_messages(exceptions)
        raise RuntimeError(
            "%s, details: one or more exceptions were raised while executing the requested functions and the "
            "cleanup functions:\n\n%s" % (message, "".join(suppressed_exception_messages)))

async def run_and_cleanup_on_error_async(cleanup_task, message, task):
    """Runs the task, handling its exceptions and running the cleanup task in that case, and, subsequently, raises any
    exceptions that might have been suppressed"""
    try:
        result = task()
        if asyncio.iscoroutine(result):
            await result
    except BaseException:
        exceptions = [sys.exc_info()]
        try:
            result = cleanup_task()
            if asyncio.iscoroutine(result):
                await result
        except BaseException:
            exceptions.append(sys.exc_info())
        suppressed_exception_messages = _prepare_suppressed_exception_messages(exceptions)
        raise RuntimeError(
            "%s, details: one or more exceptions were raised while executing the requested functions and the "
            "cleanup functions:\n\n%s" % (message, "".join(suppressed_exception_messages)))