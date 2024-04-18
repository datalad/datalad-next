from __future__ import annotations

import timeit

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


def test_performance():
    # Ensure that the performance of align_pattern is acceptable for large
    # data chunks and patterns.
    number = 10
    pattern = b'01234'
    data_chunks = [b'a' * 1000 for _ in range(100 * 1000)] + [pattern]

    result_base = timeit.timeit(
        lambda: tuple(data_chunks),
        number=number,
    )
    result_iter = timeit.timeit(
        lambda: tuple(align_pattern(data_chunks, pattern=pattern)),
        number=number,
    )

    print(result_base, result_iter, result_iter / result_base)


def test_newline_matches():
    pattern = b'----datalad-end-marker-3654137433-rekram-dne-dalatad----\n'
    chunk1 =  b'Have a lot of fun...\n----datalad-end-marker-3654137433-r'
    chunk2 =  b'e'
    chunk3 =  b'kram-dne-dalatad----\n'
    result = list(align_pattern([chunk1, chunk2, chunk3], pattern))
    assert result == [chunk1 + chunk2 + chunk3]
