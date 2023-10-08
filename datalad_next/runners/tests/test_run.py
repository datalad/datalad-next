import os
import pytest
from queue import Queue
import sys

from .. import (
    NoCapture,
    StdOutCapture,
    StdErrCapture,
    StdOutErrCapture,
)
from ..protocols import StdOutCaptureGeneratorProtocol
from ..run import run


def test_run_timeout():
    with pytest.raises(TimeoutError):
        with run([
            sys.executable, '-c',
            'import time; time.sleep(3)'],
            StdOutCaptureGeneratorProtocol,
            timeout=1
        ) as res:
            # must poll, or timeouts are not checked
            list(res)


def test_run_kill_on_exit():
    with run([
        sys.executable, '-c',
        'import time; print("mike", flush=True); time.sleep(10)'],
        StdOutCaptureGeneratorProtocol,
    ) as res:
        assert next(res).rstrip(b'\r\n') == b'mike'
    # here the process must be killed be the exit of the contextmanager
    if os.name == 'posix':
        # on posix platforms a negative returncode of -X indicates
        # a "killed by signal X"
        assert res.runner.process.returncode < 0
    # on any system the process must be dead now (indicated by a return code)
    assert res.runner.process.returncode is not None


def test_run_cwd(tmp_path):
    with run([
        sys.executable, '-c',
        'from pathlib import Path; print(Path.cwd(), end="")'],
        StdOutCapture,
        cwd=tmp_path,
    ) as res:
        assert res['stdout'] == str(tmp_path)


def test_run_input_bytes():
    with run([
        sys.executable, '-c',
        'import sys;'
        'print(sys.stdin.read(), end="")'],
        StdOutCapture,
        # it only takes bytes
        input=b'mybytes\nline',
    ) as res:
        # not that bytes went in, but str comes out -- it is up to
        # the protocol.
        # use splitlines to compensate for platform line ending
        # differences
        assert res['stdout'].splitlines() == ['mybytes', 'line']


def test_run_input_queue():
    input = Queue()
    with run([
        sys.executable, '-c',
        'from fileinput import input; import sys;'
        '[print(line, flush=True) if line.strip() else sys.exit(0)'
        ' for line in input()]'],
        StdOutCaptureGeneratorProtocol,
        input=input,
    ) as sp:
        input.put(b'one\n')
        assert next(sp).rstrip(b'\r\n') == b'one'
        input.put(b'two\n')
        assert next(sp).rstrip(b'\r\n') == b'two'
        # an empty line should cause process exit
        input.put(b'\n')
        # we can wait for that even before the context manager
        # does its thing and tears it down
        sp.runner.process.wait()


def test_run_nongenerator():
    # when executed with a non-generator protocol, the process
    # runs and returns whatever the specified protocol provides
    # as a result.
    # below we test the core protocols -- that all happen to
    # report a return `code`, `stdout`, `stderr` -- but this is
    # nowhow a given for any other protocol
    with run([sys.executable, '--version'], NoCapture) as res:
        assert res['code'] == 0
    with run([sys.executable, '-c', 'import sys; sys.exit(134)'],
             NoCapture) as res:
        assert res['code'] == 134
    with run([
        sys.executable, '-c',
        'import sys; print("print", end="", file=sys.stdout)'],
        StdOutCapture,
    ) as res:
        assert res['code'] == 0
        assert res['stdout'] == 'print'
    with run([
        sys.executable, '-c',
        'import sys; print("print", end="", file=sys.stderr)'],
        StdErrCapture,
    ) as res:
        assert res['code'] == 0
        assert res['stderr'] == 'print'
    with run([
        sys.executable, '-c',
        'import sys; print("outy", end="", file=sys.stdout); '
        'print("error", end="", file=sys.stderr)'],
        StdOutErrCapture,
    ) as res:
        assert res['code'] == 0
        assert res['stdout'] == 'outy'
        assert res['stderr'] == 'error'
