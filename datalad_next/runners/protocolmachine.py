"""
This module defines a protocol machine interface and provides basic machines.
"""
from __future__ import annotations

import json
from collections import defaultdict
from typing import (
    Any,
    Callable,
    List,
    Union,
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

StrList = List[str]
ByteList = List[bytes]
StrOrBytes = Union[str, bytes]
StrOrBytesList = List[StrOrBytes]


def jsonline_processor(lines: StrOrBytesList) -> tuple[list[tuple[bool, Any]], StrOrBytesList]:
    """
    A processor that converts lines into JSON objects, if possible.

    lines: StrOrBytesList
      A list containing strings or byte-strings that that hold JSON-serialized
      data.

    Returns: tuple[list[Tuple[bool, StrOrBytes]], StrOrByteList]
      The result, i.e. the first element of the result tuple, is a list that
      contains one tuple for each element of `lines`. The first element of the
      tuple is a bool that indicates whether the line could be converted. If it
      was successfully converted the value is `True`. The second element is the
      Python structure that resulted from the conversion if the first element
      was `True`. If the first element ist `False`, the second element contains
      the input that could not be converted.
    """
    result = []
    for line in lines:
        assert len(line.splitlines()) == 1
        try:
            result.append((True, json.loads(line)))
        except json.decoder.JSONDecodeError:
            result.append((False, lines))
    return result, []


def decode_processor(data_chunks: ByteList) -> tuple[StrList, ByteList]:
    try:
        text = (b''.join(data_chunks)).decode()
    except UnicodeDecodeError:
        return [], data_chunks
    return [text], []


def splitlines_processor(data_chunks: StrOrBytesList) -> tuple[StrOrBytesList, StrOrBytesList]:
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
    """
    Hold a list of processors and pushes data through them.

    Calls the processors in the specified order and feeds the output
    of a preceding processor into the following processor. If a processor
    has unprocessed data, either because it did not have enough data to
    successfully process it, or because not all data was processed, it returns
    the unprocessed data to the `process`-method and will receive it together
    with newly arriving data in the "next round".
    """
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
                break
        return output
