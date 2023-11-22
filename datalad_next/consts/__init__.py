"""Common constants
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
