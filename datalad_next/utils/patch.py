import contextlib
from os import environ

# legacy import
from datalad_next.patches import apply_patch


@contextlib.contextmanager
def patched_env(**env):
    """Context manager for patching the process environment

    Any number of kwargs can be given. Keys represent environment variable
    names, and values their values. A value of ``None`` indicates that
    the respective variable should be unset, i.e., removed from the
    environment.
    """
    preserve = {}
    for name, val in env.items():
        preserve[name] = environ.get(name, None)
        if val is None:
            del environ[name]
        else:
            environ[name] = str(val)
    try:
        yield
    finally:
        for name, val in preserve.items():
            if val is None:
                del environ[name]
            else:
                environ[name] = val
