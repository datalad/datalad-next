"""
 Function to ensure that a pattern is completely contained in single chunks,
 if it is present in the input chunks.
"""

from __future__ import annotations

from typing import (
    Generator,
    Iterable,
)


def align_pattern(iterable: Iterable,
                  pattern: str | bytes
                  ) -> Generator[str | bytes, None, None]:
    """ Yield data chunks that contain a complete pattern, if it is present

    This function ensures that a given data pattern (if it exists in the data
    chunks) is either completely contained in a chunk or not in the chunk. That
    means the processor ensures that all data chunks have at least the length of
    the data pattern and that they do not end with a prefix of the data pattern.

    In the context of this function, a ``pattern`` is either a string or a
    bytestring. The ``pattern`` is compared verbatim to the content in the data
    chunks, i.e. no parsing of the ``pattern`` is performed and no regular
    expressions or wildcards are supported.

    As a result, a simple `pattern in data_chunk` test is sufficient to
    determine whether a pattern appears in the data stream.

    ``align-pattern`` guarantees:

    1. All chunks have at minimum the size of the pattern (unless the complete
       input size is smaller than the size of the pattern).
    2. If a complete pattern exists, it will be contained completely within a
       single chunk, i.e. it will NOT be the case that a prefix of the pattern
       is at the end of a chunk, and the rest of the pattern in the beginning
       of the next chunk.

    The pattern might be present multiple times in a data chunk.

    Parameters
    ----------
    iterable: Iterable
        An iterable that yields data chunks
    pattern: str | bytes
        The pattern that should be contained in the chunks

    Yields
    -------
    str | bytes
        data chunks that have at least the size of the pattern and do not end
        with a prefix of the pattern. Note that a data chunk might contain the
        pattern multiple times.
    """

    def ends_with_pattern_prefix(data: str | bytes,
                                 pattern: str | bytes
                                 ) -> bool:
        """ Check whether the chunk ends with a prefix of the pattern """
        for index in range(len(pattern) - 1, 0, -1):
            if data[-index:] == pattern[:index]:
                return True
        return False

    # Join data chunks until they are sufficiently long to contain the pattern,
    # i.e. have at least size: `len(pattern)`. Continue joining, if the chunk
    # ends with a prefix of the pattern.
    current_chunk = None
    for data_chunk in iterable:
        # get the type of current_chunk from the type of this data_chunk
        if current_chunk is None:
            current_chunk = data_chunk
        else:
            current_chunk += data_chunk
        if len(current_chunk) >= len(pattern) \
                and not ends_with_pattern_prefix(current_chunk, pattern):
            yield current_chunk
            current_chunk = None

    if current_chunk is not None:
        yield current_chunk
