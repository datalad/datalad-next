"""Common constants

COPY_BUFSIZE
  ``shutil`` buffer size default, with Windows platform default changes
  backported from Python 3.10.

PRE_INIT_COMMIT_SHA
  SHA value for ``git hash-object -t tree /dev/null``, i.e. for nothing.
  This corresponds to the state of a Git repository before the first commit
  is made.

on_linux
  ``True`` if executed on the Linux platform.

on_windows
  ``True`` if executed on the Windows platform.
"""

# import from "utils", but these really are constants
from datalad.utils import (
    on_linux,
    on_windows,
)

try:
    from shutil import COPY_BUFSIZE
except ImportError:  # pragma: no cover
    # too old
    # from PY3.10
    COPY_BUFSIZE = 1024 * 1024 if on_windows else 64 * 1024

from datalad.consts import PRE_INIT_COMMIT_SHA
