"""Implements the file system related modules"""

import os
import errno
import shutil


def resilient_makedirs(directory):
    """Creates a directory tree, irrespective of whether it already exists"""
    # accounting for a directory whose location is a symbolic link to an NFS mount
    actual_directory = os.path.realpath(directory)
    try:
        os.makedirs(actual_directory)
    except OSError as exception:
        if exception.errno != errno.EEXIST or not os.path.isdir(actual_directory):
            raise


def resilient_remove(path, keep_target_if_symlink=False):
    """Removes the given path (file, directory, or symlink) from the file system ignoring errors for non-existing
    paths"""
    if path is None:
        raise ValueError("None is not a valid path")
    is_symlink = os.path.islink(path)
    try:
        if os.path.isdir(path) and not is_symlink:
            shutil.rmtree(path, ignore_errors=True)
            # note: the 'path' may actually not be removed under certain circumstances (e.g., resource busy in NFS) and
            # such errors should propagate.
            #
            # Going over the removal again, ensures that this can be flagged, so rather than using ignore_errors below,
            # this will catch these cases and make them bubble up.
            try:
                shutil.rmtree(path)
            except OSError as exception:
                if exception.errno != errno.ENOTEMPTY:
                    raise
        else:
            # if the path is a symlink, both the symlink and its target are removed
            if is_symlink and not keep_target_if_symlink:
                resilient_remove(os.path.realpath(path))
            os.remove(path)
    except OSError as exception:
        if exception.errno != errno.ENOENT:
            raise