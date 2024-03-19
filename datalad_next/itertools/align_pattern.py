""" Function to ensure that a pattern is completely contained in single chunks
"""

from __future__ import annotations

import re
from typing import (
    Generator,
    Iterable,
)


def align_pattern(iterable: Iterable[str | bytes | bytearray],
                  pattern: str | bytes | bytearray
                  ) -> Generator[str | bytes | bytearray, None, None]:
    """ Yield data chunks that contain a complete pattern, if it is present

    ``align_pattern`` makes it easy to find a pattern (``str``, ``bytes``,
    or ``bytearray``) in data chunks. It joins data-chunks in such a way,
    that a simple containment-check (e.g. ``pattern in chunk``) on the chunks
    that ``align_pattern`` yields will suffice to determine whether the pattern
    is present in the stream yielded by the underlying iterable or not.

    To achieve this, ``align_pattern`` will join consecutive chunks to ensures
    that the following two assertions hold:

    1. Each chunk that is yielded by ``align_pattern`` has at least the length
       of the pattern (unless the underlying iterable is exhausted before the
       length of the pattern is reached).

    2. The pattern is not split between two chunks, i.e. no chunk that is
       yielded by ``align_pattern`` ends with a prefix of the pattern (unless
       it is the last chunk that the underlying iterable yield).

    The pattern might be present multiple times in a yielded data chunk.

    Note: the ``pattern`` is compared verbatim to the content in the data
    chunks, i.e. no parsing of the ``pattern`` is performed and no regular
    expressions or wildcards are supported.

    .. code-block:: python

        >>> from datalad_next.itertools import align_pattern
        >>> tuple(align_pattern([b'abcd', b'e', b'fghi'], pattern=b'def'))
        (b'abcdefghi',)
        >>> # The pattern can be present multiple times in a yielded chunk
        >>> tuple(align_pattern([b'abcd', b'e', b'fdefghi'], pattern=b'def'))
        (b'abcdefdefghi',)

    Use this function if you want to locate a pattern in an input stream. It
    allows to use a simple ``in``-check to determine whether the pattern is
    present in the yielded result chunks.

    The function always yields everything it has fetched from the underlying
    iterable. So after a yield it does not cache any data from the underlying
    iterable. That means, if the functionality of
    ``align_pattern`` is no longer required, the underlying iterator can be
    used, when ``align_pattern`` has yielded a data chunk.
    This allows more efficient  processing of the data that remains in the
    underlying iterable.

    Parameters
    ----------
    iterable: Iterable
        An iterable that yields data chunks.
    pattern: str | bytes | bytearray
        The pattern that should be contained in the chunks. Its type must be
        compatible to the type of the elements in ``iterable``.

    Yields
    -------
    str | bytes | bytearray
        data chunks that have at least the size of the pattern and do not end
        with a prefix of the pattern. Note that a data chunk might contain the
        pattern multiple times.
    """

    # Create pattern matcher for all
    if isinstance(pattern, str):
        regex: str | bytes | bytearray = '(' + '|'.join(
            '.' * (len(pattern) - index - 1) + re.escape(pattern[:index]) + '$'
            for index in range(1, len(pattern))
        ) + ')'
    else:
        regex = b'(' + b'|'.join(
            b'.' * (len(pattern) - index - 1) + re.escape(pattern[:index]) + b'$'
            for index in range(1, len(pattern))
        ) + b')'
    pattern_matcher = re.compile(regex, re.DOTALL)
    pattern_sub = len(pattern) - 1
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
                and not (
                    current_chunk[-1] in pattern
                    and pattern_matcher.match(current_chunk, len(current_chunk) - pattern_sub)):
            yield current_chunk
            current_chunk = None

    if current_chunk is not None:
        yield current_chunk
