""" Data processor that ensure that a pattern odes not cross data chunk borders """

from __future__ import annotations

from functools import partial
from typing import Callable

from ..data_processor_pipeline import (
    StrOrBytes,
    StrOrBytesList,
)


__all__ = ['pattern_processor']


def pattern_processor(pattern: StrOrBytes) -> Callable:
    """ Create a pattern processor for the given ``pattern``.

    A pattern processor re-assembles data chunks in such a way, that a single
    data chunk could contain the complete pattern and will contain the complete
    pattern, if the complete pattern start in the data chunk. It guarantees:

    1. All chunks have at minimum the size of the pattern
    2. If a complete pattern exists, it will be contained completely within a
       single chunk, i.e. it will NOT be the case that a prefix of the pattern
       is at the end of a chunk, and the rest of the pattern in the beginning
       of the next chunk

    The pattern might be present multiple times in a data chunk.
    """
    assert len(pattern) > 0
    return partial(_pattern_processor, pattern)


def _pattern_processor(pattern: StrOrBytes,
                       data_chunks: StrOrBytesList,
                       final: bool = False,
                       ) -> tuple[StrOrBytesList, StrOrBytesList]:
    """ Ensure that ``pattern`` appears only completely contained within a chunk

    This processor ensures that a given data pattern (if it exists in the data
    chunks) is either completely contained in a chunk or not in the chunk. That
    means the processor ensures that all data chunks have at least the length of
    the data pattern and that they do not end with a prefix of the data pattern.

    As a result, a simple ``pattern in data_chunk`` test is sufficient to
    determine whether a pattern appears in the data stream.

    To use this function as a data processor, use partial to "fix" the first
    parameter.

    Parameters
    ----------
    pattern: str | bytes
        The pattern that should be contained in the chunks
    data_chunks: list[str | bytes]
        a list of strings or bytes
    final : bool
        the data chunks are the final data chunks of the source. A line is
        terminated by end of data.

    Returns
    -------
    list[str | bytes]
        data chunks that have at least the size of the pattern and do not end
        with a prefix of the pattern. Note that a data chunk might contain the
        pattern multiple times.
    """

    def ends_with_pattern_prefix(data: StrOrBytes, pattern: StrOrBytes) -> bool:
        """ Check whether the chunk ends with a prefix of the pattern """
        for index in range(len(pattern) - 1, 0, -1):
            if  data[-index:] == pattern[:index]:
                return True
        return False

    # Copy the list, because we might modify it and the caller might not expect that.
    data_chunks = data_chunks[:]

    # Join data chunks until they are sufficiently long to contain the pattern,
    # i.e. have a least the size: `len(pattern)`. Continue joining, if the chunk
    # ends with a prefix of the pattern.
    current_index = 0
    while current_index < len(data_chunks) - 1:
        current_chunk = data_chunks[current_index]
        while (len(data_chunks[current_index:]) > 1
               and (len(current_chunk) < len(pattern)
                    or ends_with_pattern_prefix(current_chunk, pattern))):
            data_chunks[current_index] += data_chunks[current_index + 1]
            del data_chunks[current_index + 1]
            current_chunk = data_chunks[current_index]
        current_index += 1

    # At this point we have joined whatever we can join. We still have to check
    # whether the last chunk ends with a pattern-prefix.
    if not final:
        if ends_with_pattern_prefix(data_chunks[-1], pattern):
            return data_chunks[:-1], data_chunks[-1:]
    return data_chunks, []
