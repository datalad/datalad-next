from __future__ import annotations

import pytest

from ..align_pattern import align_pattern


@pytest.mark.parametrize('data_chunks,pattern,expected', [
    (['a', 'b', 'c', 'd', 'e'], 'abc', ['abc', 'de']),
    (['a', 'b', 'c', 'a', 'b', 'c'], 'abc', ['abc', 'abc']),
    # Ensure that unaligned pattern prefixes are not keeping data chunks short.
    (['a', 'b', 'c', 'dddbbb', 'a', 'b', 'x'], 'abc', ['abc', 'dddbbb', 'abx']),
    # Expect that a trailing minimum length-chunk that ends with a pattern
    # prefix is not returned as data, but as remainder, if it is not the final
    # chunk.
    (['a', 'b', 'c', 'd', 'a'], 'abc', ['abc', 'da']),
    # Expect the last chunk to be returned as data, if final is True, although
    # it ends with a pattern prefix. If final is false, the last chunk will be
    # returned as a remainder, because it ends with a pattern prefix.
    (['a', 'b', 'c', 'dddbbb', 'a'], 'abc', ['abc', 'dddbbb', 'a']),
    (['a', 'b', 'c', '9', 'a'], 'abc', ['abc', '9a']),
])
def test_pattern_processor(data_chunks, pattern, expected):
    assert expected == list(align_pattern(data_chunks, pattern=pattern))
