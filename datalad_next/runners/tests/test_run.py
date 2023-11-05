from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from queue import Queue
from random import randint
from typing import Generator

import pytest

from datalad.utils import (
    on_osx,
    on_windows,
)
from datalad.tests.utils_pytest import skip_if

from .. import (
    NoCapture,
    StdErrCapture,
    StdOutCapture,
    StdOutCaptureGeneratorProtocol,
    StdOutErrCapture,
)
from ..run import run

resources_dir = Path(__file__).parent / 'resources'


interruptible_prog = '''
import time

i = 0
while True:
    print(i, flush=True)
    i += 1
    time.sleep(1)
'''

uninterruptible_prog = '''
import signal

signal.signal(signal.SIGTERM, signal.SIG_IGN)
signal.signal(signal.SIGINT, signal.SIG_IGN)
''' + interruptible_prog

stdin_reading_prog = '''
import sys
import time

while True:
    data = sys.stdin.readline()
    if data == '':
        exit(0)
    print(f'entered: {data.strip()}', flush=True)
    time.sleep(.1)
'''

stdin_closing_prog = '''
import sys
import time

sys.stdin.close()
while True:
    print(f'stdin is closed {time.time()}', flush=True)
    time.sleep(.1)
'''

# A program that behaves similar to a remote shell, i.e. a single command
# leads to an unknown number of outputs. The program reads a single line,
# writes a random number of output lines, adds the line it read as end
# marker and exits.
shell_like_prog = '''
import random
import sys
import time


marker = sys.stdin.readline().strip()
output_line_count = 1 + random.randrange(8)
last_end = '\\n' if output_line_count % 2 == 1 else ''
for i in range(output_line_count):
    print(time.time(), end='\\n' if i < output_line_count - 1 else last_end)
print(marker)
if random.randrange(2) == 1:
    print(f'random additional output {time.time()}')
'''

# A program that takes exponentielly longer to respond. This is used to check
# for killing while waiting for a yield.
degrading_output_prog = '''
import time

i = 0
while True:
    print(i, flush=True)
    i += 1
    time.sleep(i**2 / 10)
'''


def test_sig_kill():
    with run(cmd=[sys.executable, '-u', '-c', uninterruptible_prog],
             protocol_class=StdOutCaptureGeneratorProtocol,
             terminate_time=1,
             kill_time=1) as r:
        # Fetch one data chunk to ensure that the process is running
        data = next(r)
        assert data[:1] == b'0'

    # Ensure that the return code was read and is not zero
    assert r.return_code not in (0, None)
    if os.name == 'posix':
        assert r.return_code == -signal.SIGKILL


def test_sig_terminate():
    with run(cmd=[sys.executable, '-u', '-c', interruptible_prog],
             protocol_class=StdOutCaptureGeneratorProtocol,
             terminate_time=1,
             kill_time=1) as r:
        # Fetch one data chunk to ensure that the process is running
        data = next(r)
        assert data[:1] == b'0'

    # Ensure that the return code was read
    assert r.return_code is not None
    if os.name == 'posix':
        assert r.return_code == -signal.SIGTERM


def test_external_close():
    stdin_queue = Queue()
    with run([sys.executable, '-u', '-c', stdin_reading_prog],
             protocol_class=StdOutCaptureGeneratorProtocol,
             stdin=stdin_queue) as r:
        while True:
            stdin_queue.put(f'{time.time()}{os.linesep}'.encode())
            try:
                result = next(r)
            except StopIteration:
                break
            r.runner.process.stdin.close()

    assert r.return_code == 0


@skip_if(on_osx or on_windows)   # On MacOS and Windows a write will block
def test_internal_close_file():
    # This test demonstrates pipe-writing behavior if the receiving side,
    # i.e. the sub-process, does not read from the pipe. It is not specifically
    # a test for the context-manager.
    with run([sys.executable, '-u', '-c', stdin_closing_prog],
             protocol_class=StdOutCaptureGeneratorProtocol,
             stdin=subprocess.PIPE,
             timeout=2.0,
             terminate_time=1,
             kill_time=1) as r:

        os.set_blocking(r.runner.process.stdin.fileno(), False)
        total = 0
        while True:
            try:
                written = r.runner.process.stdin.write(b'a' * 8000)
                if written is None:
                    print(f'Write failed after {total} bytes', flush=True)
                    # There are no proper STDIN-timeouts because we handle that
                    # ourselves. So for the purpose of this test, we can not
                    # rely on timeout. That means, we kill the process here
                    # and the let __exit__-method pick up the peaces, i.e. the
                    # return code.
                    r.runner.process.kill()
                    break
            except BrokenPipeError:
                print(f'Wrote less than {total + 8000} bytes', flush=True)
                break
            total += written
    assert r.return_code not in (0, None)


def test_kill_yield():
    # Check that a long next() on a result generator terminates the iteration
    with run(cmd=[sys.executable, '-u', '-c', degrading_output_prog],
             protocol_class=StdOutCaptureGeneratorProtocol,
             terminate_time=3,
             kill_time=2) as r:
        tuple(r)
    return_code = r.return_code
    if os.name == 'posix':
        assert return_code == -signal.SIGTERM


def _check_signal_blocking(program: str):
    with run(cmd=[sys.executable, '-u', '-c', program],
             protocol_class=StdOutCapture,
             terminate_time=1,
             kill_time=1) as r:
        pass

    # Check the content
    assert all([
        index == int(item)
        for index, item in enumerate(r['stdout'].splitlines())
    ])

    # Ensure that the return code was read
    return_code = r['code']
    assert return_code is not None
    return return_code


def test_kill_blocking():
    return_code = _check_signal_blocking(uninterruptible_prog)
    if os.name == 'posix':
        assert return_code == -signal.SIGKILL


def test_terminate_blocking():
    return_code = _check_signal_blocking(interruptible_prog)
    if os.name == 'posix':
        assert return_code == -signal.SIGTERM


def test_shell_like():
    #
    stdin_queue = Queue()
    with run(
        [sys.executable, '-u', '-c', shell_like_prog],
        protocol_class=StdOutCaptureGeneratorProtocol,
        stdin=stdin_queue
    ) as r:

        # Create a random marker and send it to the subprocess
        marker = f'mark-{randint(1000000, 2000000)}{os.linesep}'
        stdin_queue.put(marker.encode())

        # Read until the marker comes back
        unterminated_line = ''
        for line_index, line in enumerate(r):
            if line[-len(marker):] == marker:
                unterminated_line = line[:-len(marker)]
                break
        if unterminated_line:
            assert line_index % 2 == 1

    assert r.return_code == 0


def test_run_timeout():
    with pytest.raises(TimeoutError):
        with run([
            sys.executable, '-u', '-c',
            'import time; time.sleep(3)'],
                StdOutCaptureGeneratorProtocol,
                timeout=1
        ) as res:
            # must poll, or timeouts are not checked
            list(res)


def test_run_kill_on_exit():
    with run([
        sys.executable, '-u', '-c',
        'import time; print("mike", flush=True); time.sleep(10)'],
        StdOutCaptureGeneratorProtocol,
        terminate_time=1,
        kill_time=1,
    ) as res:
        assert next(res).rstrip(b'\r\n') == b'mike'
    # here the process must be killed be the exit of the contextmanager
    if os.name == 'posix':
        # on posix platforms a negative return code of -X indicates
        # a "killed by signal X"
        assert res.return_code < 0
    # on any system the process must be dead now (indicated by a return code)
    assert res.return_code is not None


def test_run_instant_kill():
    with run([
        sys.executable, '-u', '-c',
        'import time; time.sleep(3)'],
        StdOutCaptureGeneratorProtocol,
        terminate_time=1,
        kill_time=1,
    ) as sp:
        # we let it terminate instantly
        pass
    if os.name == 'posix':
        assert sp.return_code < 0
    assert sp.return_code is not None


def test_run_cwd(tmp_path):
    with run([
        sys.executable, '-u', '-c',
        'from pathlib import Path; print(Path.cwd(), end="")'],
            StdOutCapture,
            cwd=tmp_path,
    ) as res:
        assert res['stdout'] == str(tmp_path)


def test_run_input_bytes():
    with run([
        sys.executable, '-u', '-c',
        'import sys;'
        'print(sys.stdin.read(), end="")'],
            StdOutCapture,
            # it only takes bytes
            stdin=b'mybytes\nline',
    ) as res:
        # not that bytes went in, but str comes out -- it is up to
        # the protocol.
        # use splitlines to compensate for platform line ending
        # differences
        assert res['stdout'].splitlines() == ['mybytes', 'line']


def test_run_input_queue():
    stdin_queue = Queue()
    with run([
        sys.executable, '-u', '-c',
        'from fileinput import input; import sys;'
        '[print(line, end="", flush=True) if line.strip() else sys.exit(0)'
        ' for line in input()]'],
            StdOutCaptureGeneratorProtocol,
            stdin=stdin_queue,
    ) as sp:
        stdin_queue.put(f'one\n'.encode())
        response = next(sp)
        assert response.rstrip() == b'one'
        stdin_queue.put(f'two\n'.encode())
        response = next(sp)
        assert response.rstrip() == b'two'
        # an empty line should cause process exit
        stdin_queue.put(os.linesep.encode())
        # we can wait for that even before the context manager
        # does its thing and tears it down
        sp.runner.process.wait()


def test_run_nongenerator():
    # when executed with a non-generator protocol, the process
    # runs and returns whatever the specified protocol returns
    # from _prepare_result.
    # Below we test the core protocols -- that all happen to
    # report a return `code`, `stdout`, `stderr` -- but this is
    # nohow a given for any other protocol.
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


def test_run_exception_in_context(monkeypatch):
    # Check that an exception in the context is logged and re-raises:
    warnings = []
    monkeypatch.setattr(
        'datalad_next.runners.run.lgr.warning',
        lambda s: warnings.append(s)
    )
    with pytest.raises(ValueError):
        with run([
                sys.executable, '-u', '-c',
                'import time\n'
                'time.sleep(5)\n'
            ],
            StdOutCaptureGeneratorProtocol,
        ):
            raise ValueError('Something')
    assert warnings[0].startswith('Possible stall:')
