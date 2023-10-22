import os
import signal
import sys
import time
from queue import Queue

from .. import (
    GeneratorMixIn,
    StdOutErrCapture,
)
from ..batch import (
    BatchProcess,
    annexjson_batchcommand,
    batchcommand,
    stdout_batchcommand,
)


class PythonProtocol(StdOutErrCapture, GeneratorMixIn):
    """Parses interactive python output and enqueues complete output strings

    This is an example for a protocol that processes results of an a priori
    unknown structure and length.
    Instances of this class interpret the stdout- and stderr-output of a
    python interpreter. They assemble decoded stdout content until the python
    interpreter sends ``'>>> '`` on stderr. Then the assembled output is
    returned as result.

    This requires to start the python interpreter in unbuffered mode! If not,
    the ``stderr``-output can be processed too early, i.e. before all
    ``stdout``-output is processed. This is due to the fact that the runner is
    thread-based. The runner does not necessarily preserve the wall-clock-order
    of events that arrive from different streams.
    """
    def __init__(self):
        StdOutErrCapture.__init__(self)
        GeneratorMixIn.__init__(self)
        self.stdout = ''
        self.stderr = b''
        self.prompt_count = -1

    def pipe_data_received(self, fd: int, data: bytes) -> None:
        if fd == 1:
            # We known that no multibyte encoded strings are used in the
            # examples. Therefore, we don't have to care about encodings that
            # are split between consecutive data chunks, and we can always
            # successfully decode `data`.
            self.stdout += data.decode()
        elif fd == 2:
            self.stderr += data
            if len(self.stderr) >= 4 and b'>>> ' in self.stderr:
                self.prompt_count += 1
                self.stderr = b''
        if self.stdout and self.prompt_count > 0:
            self.send_result(self.stdout)
            self.stdout = ''
            self.prompt_count -= 1


def test_batch_simple(existing_dataset):
    # first with a simplistic protocol to test the basic mechanics
    with stdout_batchcommand(
            ['git', 'annex', 'examinekey',
             # the \n in the format is needed to produce an output that hits
             # the output queue after each input line
             '--format', '${bytesize}\n',
             '--batch'],
            cwd=existing_dataset.pathobj,
    ) as bp:
        res = bp(b'MD5E-s21032--2f4e22eb05d58c21663794876dc701aa\n')
        assert res.rstrip(b'\r\n') == b'21032'
        # to subprocess is still running
        assert bp.return_code is None
        # another batch
        res = bp(b'MD5E-s999--2f4e22eb05d58c21663794876dc701aa\n')
        assert res.rstrip(b'\r\n') == b'999'
        assert bp.return_code is None
        # we can bring the process down with stupid input, because it is
        # inside our context handlers, it will not raise CommandError. check exit instead
        res = bp(b'stupid\n')
        # process exit is detectable
        assert res is None
        assert bp.return_code == 1
        # continued use raises the same exception
        # (but stacktrace is obvs different)
        res = bp(b'MD5E-s999--2f4e22eb05d58c21663794876dc701aa\n')
        assert res is None
        assert bp.return_code == 1

    # now with a more complex protocol (decodes JSON-lines output)
    with annexjson_batchcommand(
            ['git', 'annex', 'examinekey', '--json', '--batch'],
            cwd=existing_dataset.pathobj,
    ) as bp:
        # output is a decoded JSON object
        res = bp(b'MD5E-s21032--2f4e22eb05d58c21663794876dc701aa\n')
        assert res['backend'] == "MD5E"
        assert res['bytesize'] == "21032"
        assert res['key'] == "MD5E-s21032--2f4e22eb05d58c21663794876dc701aa"
        res = bp(b'MD5E-s999--2f4e22eb05d58c21663794876dc701aa\n')
        assert res['bytesize'] == "999"
        res = bp(b'stupid\n')
        assert res is None
        assert bp.return_code == 1


def test_batch_killing(existing_dataset):
    # to test killing we have to circumvent the automatic stdin-closing by
    # BatchCommand. We do that by setting `closing_action` to an empty function.
    with stdout_batchcommand(
            ['git', 'annex', 'examinekey',
             # the \n in the format is needed to produce an output that hits
             # the output queue after each input line
             '--format', '${bytesize}\n',
             '--batch'],
            cwd=existing_dataset.pathobj,
            closing_action=lambda a, b: True,
            terminate_time=2,
            kill_time=2,
    ) as bp:
        leave_time = time.time()

    leave_time = time.time() - leave_time
    # at this point the process should have been terminated after about 3
    # seconds, because git-annex behaves well and terminates when it receives
    # a terminate signal
    assert 1.5 < leave_time < 2.5
    assert bp.return_code not in (0, None)
    if os.name == 'posix':
        assert bp.return_code == -signal.SIGTERM


def test_annexjsonbatch_killing(existing_dataset):
    # to test killing we have to circumvent the automatic stdin-closing by
    # BatchCommand. We do that by setting `closing_action` to an empty function.
    with annexjson_batchcommand(
            ['git', 'annex', 'examinekey', '--json', '--batch'],
            cwd=existing_dataset.pathobj,
            closing_action=lambda a, b: True,
            terminate_time=2,
            kill_time=2,
    ) as bp:
        leave_time = time.time()

    leave_time = time.time() - leave_time
    # at this point the process should have been terminated after about 2
    # seconds, because git-annex behaves well and terminates when it receives
    # a terminate signal
    assert 1.5 < leave_time < 2.5
    assert bp.return_code not in (0, None)
    if os.name == 'posix':
        assert bp.return_code == -signal.SIGTERM


def test_plain_batch_python_multiline():

    def close_stdin(batch_process: BatchProcess,
                    stdin_queue: Queue):
        assert isinstance(batch_process, BatchProcess)
        assert isinstance(stdin_queue, Queue)
        batch_process.close_stdin()
        stdin_queue.put(None)

    prog = '''
import time
def x(count):
    for i in range(count):
        print(i, flush=True)
    time.sleep(.2)
'''
    # We set a terminate and kill time here, because otherwise an exception
    # that is raised in the `batchcommand`-context will get the test to hang.
    # The reason is that the exception triggers an exit from the context,
    # but the python process will never stop since we did neither close its
    # stdin nor did we call the `exit()`-function.
    with batchcommand([sys.executable, '-i', '-u', '-c', prog],
                      protocol_class=PythonProtocol,
                      terminate_time=3,
                      kill_time=2,
                      ) as python_interactive:

        # multiline output should be handled by the protocol,
        for count in (5, 20):
            result_generator = python_interactive(f'x({count})\n'.encode())
            assert len(result_generator.splitlines()) == count
        python_interactive.close_stdin()
    assert python_interactive.return_code == 0

    # Test with unclosed stdin
    with batchcommand([sys.executable, '-i', '-u', '-c', prog],
                      protocol_class=PythonProtocol,
                      terminate_time=3,
                      kill_time=2,
                      ) as python_interactive:
        for count in (5, 20):
            result_generator = python_interactive(f'x({count})\n'.encode())
            assert len(result_generator.splitlines()) == count
        # Do not close stdin here, we let BatchCommand do that.
    assert python_interactive.return_code == 0

    # Test with closing action
    with batchcommand([sys.executable, '-i', '-u', '-c', prog],
                      protocol_class=PythonProtocol,
                      terminate_time=3,
                      kill_time=2,
                      closing_action=close_stdin
                      ) as python_interactive:
        for count in (5, 20):
            result_generator = python_interactive(f'x({count})\n'.encode())
            assert len(result_generator.splitlines()) == count
        # Do not close stdin here, we let the closing_action handle that.
    assert python_interactive.return_code == 0

    # Test with a "bad" closing action to ensure that only the closing action
    # is called and not the internal stdin-closing of `BatchCommand`.
    with batchcommand([sys.executable, '-i', '-u', '-c', prog],
                      protocol_class=PythonProtocol,
                      terminate_time=3,
                      kill_time=2,
                      closing_action=lambda a, b: None,
                      ) as python_interactive:
        for count in (5, 20):
            result_generator = python_interactive(f'x({count})\n'.encode())
            assert len(result_generator.splitlines()) == count
        # Do not close stdin here, we let the closing_action handle that.
    assert python_interactive.return_code not in (0, None)
    if os.name == 'posix':
        assert python_interactive.return_code == -signal.SIGTERM
