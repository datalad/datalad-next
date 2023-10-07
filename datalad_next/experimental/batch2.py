"""
This is a demo of a batch command that uses `outside of protocol` processing
of data, It uses nested generators that apply certain data-filter and
data-conversion functions.

There will be a second demo soon that uses `inside of protocol` processing
of data. The latter one might be useful in non-generator protocols as well.
"""
from __future__ import annotations

import sys
from asyncio.protocols import SubprocessProtocol
from contextlib import contextmanager
from pathlib import Path
from queue import Queue
from typing import (
    Any,
    Callable,
    Generator,
)

from datalad.runner.nonasyncrunner import (
    _ResultGenerator,
    ThreadedRunner,
    STDOUT_FILENO,
    STDERR_FILENO,
)
from datalad_next.runners.protocols import (
    GeneratorMixIn,
    StdOutCapture,
)

from .protocolmachine import (
    ProcessingPipeline,
    decode_processor,
    jsonline_processor,
    splitlines_processor,
)


class ProcessingGeneratorProtocol(GeneratorMixIn):
    """ A generator protocol that has "plugable" protocol handlers """

    proc_out = True
    proc_err = False

    def __init__(self,
                 stdout_processors: list[Callable],
                 stderr_processors: list[Callable],
                 ) -> None:
        GeneratorMixIn.__init__(self)
        self.processing_pipelines = {
            STDOUT_FILENO: ProcessingPipeline(stdout_processors),
            STDERR_FILENO: ProcessingPipeline(stderr_processors),
        }

    def pipe_data_received(self, fd: int, data: bytes):
        for result in self.processing_pipelines[fd].process(data):
            self.send_result((fd, result))


class TestProtocol(SubprocessProtocol, ProcessingGeneratorProtocol):
    def __init__(self,
                 stdout_processors: list[Callable],
                 ) -> None:
            SubprocessProtocol.__init__(self)
            ProcessingGeneratorProtocol.__init__(
                self,
                stdout_processors=stdout_processors,
                stderr_processors=[]
            )

    def pipe_data_received(self, fd: int, data: bytes):
        ProcessingGeneratorProtocol.pipe_data_received(self, fd, data)


class BatchProcess:
    """Representation of a (running) batch process

    An instance of this type is produced by any of the context manager variants
    in this module.

    A batch process instanced is used by passing ``bytes`` input to its
    ``__call__()`` method, and receiving the batch output as return value.

    While the process is still running, it ``return_code`` property will
    be ``None``. After the has terminated, the property will contain the
    respective exit code.
    """
    def __init__(self,
                 runner_generator: _ResultGenerator,
                 ):
        self.runner_generator = runner_generator

    def __call__(self, data: bytes) -> Any | None:
        self.runner_generator.runner.stdin_queue.put(data)
        try:
            return next(self.runner_generator)
        except StopIteration:
            return None

    @property
    def return_code(self) -> None | int:
        return self.runner_generator.return_code


@contextmanager
def batchcommand(cmd: list,
                 processors: list[Callable],
                 cwd: Path | None = None,
                 ) -> Generator:

    input_queue = Queue()

    base_generator = ThreadedRunner(
        cmd=cmd,
        protocol_class=TestProtocol,
        stdin=input_queue,
        cwd=cwd,
        # Do not raise exceptions on error, otherwise the call to
        # `tuple(base_generator)` in the finally-branch might raise an
        # exception.
        exception_on_error=False,
        protocol_kwargs={'stdout_processors': processors}
    ).run()
    try:
        yield BatchProcess(base_generator)
    finally:
        input_queue.put(None)
        # Exhaust the iterator to allow it to pick up process exit and the
        # return code.
        tuple(base_generator)


def stdout_batchcommand(
        cmd: list,
        cwd: Path | None = None,
) -> Generator[BatchProcess, None, None]:
    """Context manager for commands that produce arbitrary output on ``stdout``

    Internally this calls :func:`batchcommand` with the three processors:
    ``decode_processor``, and ``splitlines_processor``.
    """
    return batchcommand(
        cmd,
        processors=[splitlines_processor],
        cwd=cwd,
    )


def annexjson_batchcommand(
        cmd: list,
        cwd: Path | None = None,
) -> Generator[BatchProcess, None, None]:
    """Context manager for git-annex commands that support ``--batch --json``

    The given ``cmd``-list must be complete, i.e., include
    ``git annex ... --batch --json``, and any additional flags that may be
    needed.

    Internally this calls :func:`batchcommand` with the three processors:
    ``decode_processor``, ``splitlines_processor``, and ``jsonline_processpr``.
    """
    return batchcommand(
        cmd,
        processors=[splitlines_processor, jsonline_processor],
        cwd=cwd,
    )


if __name__ == '__main__':

    cmd_1 = [sys.executable, '-c', '''
import sys
while True:
    x = sys.stdin.readline()
    if x == '':
        exit(2)
    print('{"entered": "%s"}' % str(x.strip()), flush=True)
    if x.strip() == 'end':
        exit(3)
''']

    with annexjson_batchcommand(cmd=cmd_1) as bp:
        for command in ('sdsdasd\n', 'end\n'):
            res = bp(command.encode())
            print('received:', res)

    print('result code:', bp.return_code)

    with annexjson_batchcommand(cmd=cmd_1) as bp:
        pass
    print('result code:', bp.return_code)
