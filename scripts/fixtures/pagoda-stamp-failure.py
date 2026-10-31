#!/usr/bin/env python3
"""Run the real resolver with one failing stamp filesystem operation."""

import errno
import os
import runpy
import sys
from pathlib import Path
from unittest.mock import patch

_, driver, operation, *arguments = sys.argv
sys.argv = [driver, *arguments]
original_open = Path.open
original_fsync = os.fsync
original_utime = os.utime
stamp_fd = None


def fail():
    raise OSError(errno.ENOSPC, f"fixture stamp {operation} failure")


def open_file(path, *args, **kwargs):
    global stamp_fd
    is_stamp = path.name == "notify.stamp"
    if is_stamp and operation == "open":
        fail()
    handle = original_open(path, *args, **kwargs)
    if is_stamp:
        stamp_fd = handle.fileno()
    return handle


def fsync(fd):
    if fd == stamp_fd and operation == "fsync":
        fail()
    return original_fsync(fd)


def utime(path, *args, **kwargs):
    if Path(path).name == "notify.stamp" and operation == "utime":
        fail()
    return original_utime(path, *args, **kwargs)


with (
    patch.object(Path, "open", open_file),
    patch.object(os, "fsync", fsync),
    patch.object(os, "utime", utime),
):
    runpy.run_path(driver, run_name="__main__")
