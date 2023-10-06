"""
What does a data-parser that is there to detect responses do?
It reads data and identifies responses. To do this:
 - it decodes dota, if necessary (store unprocessed bytes),
 - it chops it up at response borders if necessary (store unprocessed bytes),
 - transforms it to a response structure, e.g. JSON-object
"""

from typing import Callable, Generator

from datalad_next.runners import Runner
from datalad_next.runners.protocols import GeneratorMixIn, StdOutCapture


class BaseGenerator(Generator):
    def throw(self, exception_type, value=None, trace_back=None):
        return Generator.throw(self, exception_type, value, trace_back)


class ProcessingGenerator(BaseGenerator):
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


def decode_processor(data: bytes):
    try:
        text = data.decode()
    except UnicodeDecodeError:
        return [], data, False
    return [text], b'', False


def splitlines_processor(text: str):
    parts_with_ends = text.splitlines(keepends=True)
    parts_without_ends = text.splitlines(keepends=False)
    if parts_with_ends[-1] == parts_without_ends[-1]:
        return parts_with_ends[:-1], parts_with_ends[-1], False
    return parts_with_ends, '', False


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


cmd1 = ['find', '/home/cristian/datalad/longnow-podcasts'],
cmd2 = ['python', '-c', '''
import time
for i in range(10):
    print(i, flush=True)
    time.sleep(1)
''']
cmd3 = ['python', '-c', '''exit(0)''']
cmd4 = ['python', '-c', '''
import sys
while True:
    x = sys.stdin.readline()
    print('--->: ', x, flush=True)
''']


g = Runner().run(cmd=cmd2, protocol=TestProtocol)
l = build_processing_generator(
    g,
    [decode_processor, splitlines_processor])

for line in l:
    print('Line:', repr(line))
print('return code:', g.return_code)
