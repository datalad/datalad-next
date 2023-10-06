"""
This is a demo of a batch command that uses `outside of protocol` processing
of data, It uses nested generators that apply certain data-filter and
data-conversion functions.

There will be a second demo soon that uses `inside of protocol` processing
of data. The latter one might be useful in non-generator protocols as well.
"""
from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from queue import Queue
from typing import (
    Any,
    Callable,
    Generator,
)

from datalad.runner import Protocol
from datalad.runner.nonasyncrunner import (
    _ResultGenerator,
    ThreadedRunner,
)
from datalad_next.runners.protocols import (
    GeneratorMixIn,
    StdOutCapture,
)


class ProcessingGenerator(Generator):
    """ A generator that uses response-record detectors

    This generator allows a user to push back data that was yielded. The
    next call to send() (or next()) will yield the pushed data plus the
    next data from the underlying generator.
    """
    def __init__(self,
                 base_generator: Generator,
                 processor: Callable):
        self.base_generator = base_generator
        self.processor = processor
        self.remaining = None
        self.responses = []
        self.terminate = False

    def send(self, value):
        if self.responses:
            return self.responses.pop(0)

        if self.terminate:
            raise StopIteration()

        for data in self.base_generator:
            if self.remaining is not None:
                data, self.remaining = self.remaining + data, None
            self.responses, self.remaining, self.terminate = self.processor(data)
            if self.responses:
                return self.responses.pop(0)
        raise StopIteration

    def throw(self, exception_type, value=None, trace_back=None):
        return Generator.throw(self, exception_type, value, trace_back)


def decode_processor(data: bytes):
    try:
        text = data.decode()
    except UnicodeDecodeError:
        return [], data, False
    return [text], b'', False


def splitlines_processor(text: str | bytes):
    # Use the builtin linesplit-wisdom of Python
    parts_with_ends = text.splitlines(keepends=True)
    parts_without_ends = text.splitlines(keepends=False)
    if parts_with_ends[-1] == parts_without_ends[-1]:
        return parts_with_ends[:-1], parts_with_ends[-1], False
    # We use `text[0:0]` to get an empty value the proper type, i.e. either
    # the string `''` or the byte-string `b''`.
    return parts_with_ends, text[0:0], False


def jsonline_processor(data: str | bytes):
    assert len(data.splitlines()) == 1
    return [json.loads(data)], data[0:0], False


class TestProtocol(StdOutCapture, GeneratorMixIn):
    """ A minimal generator protocol that passes stdout """
    def pipe_data_received(self, fd: int, data: bytes):
        self.send_result(data)


def build_processing_generator(base_generator: Generator,
                               processors: list[Callable],
                               ) -> Generator:
    """ Build a generator the executes the processors in the given order"""
    for index, processor in enumerate(processors):
        base_generator = ProcessingGenerator(base_generator, processor)
    return base_generator


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
                 processors: list[Callable],
                 ):
        self.runner_generator = runner_generator
        self.result_generator = build_processing_generator(
            runner_generator,
            processors,
        )

    def __call__(self, data: bytes) -> Any | None:
        self.runner_generator.runner.stdin_queue.put(data)
        try:
            return next(self.result_generator)
        except StopIteration:
            return None

    @property
    def return_code(self) -> None | int:
        return self.runner_generator.return_code


@contextmanager
def batchcommand(cmd: list,
                 processors: list[Callable],
                 protocol_class: type[Protocol] = TestProtocol,
                 cwd: Path | None = None,
                 ) -> Generator:

    input_queue = Queue()

    base_generator = ThreadedRunner(
        cmd=cmd,
        protocol_class=protocol_class,
        stdin=input_queue,
        cwd=cwd,
        exception_on_error=False,
    ).run()
    try:
        yield BatchProcess(base_generator, processors)
    finally:
        input_queue.put(None)
        # Exhaust the iterator to allow it to pick up process exit
        # and the return code.
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
        protocol_class=TestProtocol,
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
        protocol_class=TestProtocol,
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
