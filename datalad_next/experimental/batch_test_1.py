"""
What does a data-parser that is there to detect responses do?
It reads data and identifies responses. To do this:
 - it decodes dota, if necessary (store unprocessed bytes),
 - it chops it up at response borders if necessary (store unprocessed bytes),
 - transforms it to a response structure, e.g. JSON-object
"""

from contextlib import contextmanager
from datalad_next.runners import Runner
from datalad_next.runners.protocols import GeneratorMixIn, StdOutCapture
from typing import Any, Generator


class BufferingGenerator(Generator):
    """ A buffering generator that allows to push back yielded data

    This generator allows a user to push back data that was yielded. The
    next call to send() (or next()) will yield the pushed data plus the
    next data from the underlying generator.
    """
    def __init__(self, base_generator: Generator):
        self.base_generator = base_generator
        self.store = b''

    def push(self, pushed_data: Any):
        print('A PUSHED: ', repr(pushed_data))
        self.store += pushed_data

    def send(self, value):
        result, self.store = self.store + self.base_generator.send(value)[1], b''
        return result

    def throw(self, exception_type, value=None, trace_back=None):
        return Generator.throw(self, exception_type, value, trace_back)


class DecodingGenerator(Generator):
    """ A generator that decodes incoming bytes before yielding them
    """
    def __init__(self,
                 base_generator: Generator,
                 encoding: str ='utf-8'):
        self.base_generator = BufferingGenerator(base_generator)
        self.encoding = encoding
        self.store = ''

    def send(self, value):
        for data in self.base_generator:
            try:
                text = data.decode(encoding=self.encoding)
            except UnicodeDecodeError:
                self.base_generator.push(data)
                continue
            result, self.store = self.store + text, ''
            return result

    def push(self, pushed_data: str):
        print('B PUSHED: ', repr(pushed_data))
        self.store += pushed_data

    def throw(self, exception_type, value=None, trace_back=None):
        return Generator.throw(self, exception_type, value, trace_back)


class TestProtocol(StdOutCapture, GeneratorMixIn):
    """ A minimal generator protocol that passes stdout """
    def pipe_data_received(self, fd: int, data: bytes):
        self.send_result((fd, data))



g = Runner().run(
    ['find', '/home/cristian/datalad/longnow-podcasts'],
    protocol=TestProtocol
)

b = BufferingGenerator(g)
for data in b:

    try:
        text = data.decode()
    except UnicodeDecodeError:
        b.push(data)
        continue

    # Use python's built in line split wisdom to split on any known line ending.
    parts_with_ends = text.splitlines(keepends=True)
    parts_without_ends = text.splitlines(keepends=False)
    if parts_with_ends[-1] == parts_without_ends[-1]:
        # Push back any non-terminated lines
        b.push(parts_without_ends[-1].encode())
        del parts_without_ends[-1]

    for part in parts_without_ends:
        print('FIND: ', part)
