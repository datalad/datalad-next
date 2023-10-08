import os
import pytest
import sys

from ..protocols import StdOutCaptureGeneratorProtocol

from ..run import run


def test_run_timeout():
    with pytest.raises(TimeoutError):
        with run([
            sys.executable, '-c',
            'import time; time.sleep(3)'],
            StdOutCaptureGeneratorProtocol,
            timeout=1
        ) as sp:
            # must poll, or timeouts are not checked
            list(sp)


def test_run_kill_on_exit():
    with run([
        sys.executable, '-c',
        'import time; print("mike", flush=True); time.sleep(10)'],
        StdOutCaptureGeneratorProtocol,
    ) as sp:
        assert next(sp).rstrip(b'\r\n') == b'mike'
    # here the process must be killed be the exit of the contextmanager
    if os.name == 'posix':
        # on posix platforms a negative returncode of -X indicates
        # a "killed by signal X"
        assert sp.runner.process.returncode < 0
    # on any system the process must be dead now (indicated by a return code)
    assert sp.runner.process.returncode is not None
