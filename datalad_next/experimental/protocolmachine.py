"""
This module defines a protocol machine interface and provides basic machines.
"""
from __future__ import annotations

import json
from collections import defaultdict
from typing import (
    Any,
    Callable,
)


# Processors have the following signature
#
#    def process(self, data: list[T]) -> tuple[list[N] | None, list[T]]:
#       ...
#
# where N is tne type that is returned by processor. The return value is a
# consisting of optional results, i.e. list[N] | None, and a number of input
# elements that were not processed and should be presented again, when more
# data arrives from the "preceeding" element.




def jsonline_processor(data: list[str | bytes]) -> tuple[list, list[str | bytes]]:
    """
    A processor that converts lines into JSON objects, if possible
    """
    empty = data[0][0:0]
    result = []
    for line in data:
        assert len(line.splitlines()) == 1
        try:
            result.append((True, json.loads(line)))
        except json.decoder.JSONDecodeError:
            result.append((False, data))
    return result, empty


def decode_processor(data_chunks: list[bytes]):
    try:
        text = (b''.join(data_chunks)).decode()
    except UnicodeDecodeError:
        return None, data_chunks
    return [text], []


def splitlines_processor(data_chunks: list[str | bytes]):
    # We use `data_chunks[0][0:0]` to get an empty value the proper type, i.e.
    # either the string `''` or the byte-string `b''`.
    empty = data_chunks[0][0:0]
    text = empty.join(data_chunks)
    # Use the builtin line split-wisdom of Python
    parts_with_ends = text.splitlines(keepends=True)
    parts_without_ends = text.splitlines(keepends=False)
    if parts_with_ends[-1] == parts_without_ends[-1]:
        return parts_with_ends[:-1], [parts_with_ends[-1]]
    return parts_with_ends, []


class ProcessingPipeline:
    def __init__(self,
                 processors: list[Callable]
                 ) -> None:
        self.processors = processors
        self.waiting_data: dict[Callable, list] = defaultdict(list)
        self.remaining = None

    def process(self, data: bytes) -> list[Any]:
        output = [data]
        for processor in self.processors:
            if self.waiting_data[processor]:
                output = self.waiting_data[processor] + output
            output, self.waiting_data[processor] = processor(output)
            if not output:
                return []
        return output
