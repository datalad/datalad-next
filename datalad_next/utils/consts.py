try:
    from shutil import COPY_BUFSIZE
except ImportError:  # pragma: no cover
    # too old
    from datalad_next.utils import on_windows
    # from PY3.10
    COPY_BUFSIZE = 1024 * 1024 if on_windows else 64 * 1024

from datalad.consts import PRE_INIT_COMMIT_SHA
