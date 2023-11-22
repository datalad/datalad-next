from __future__ import annotations

import sys
import timeit

from ..decode_bytes import decode_bytes


def test_split_decoding():
    encoded = 'ö'.encode('utf-8')
    part_1, part_2 = encoded[:1], encoded[1:]

    # check that incomplete encodings are caught
    r = tuple(decode_bytes([b'abc' + part_1, part_2 + b'def']))
    assert ''.join(r) == 'abcödef'


def test_unfixable_error_decoding():
    encoded = 'ö'.encode('utf-8')
    part_1, part_2 = encoded[:1], encoded[1:]

    # check that incomplete encodings are caught
    r = tuple(decode_bytes([b'abc' + part_1 + b'def' + part_1, part_2 + b'ghi']))
    assert ''.join(r) == 'abc\\xc3deföghi'


def test_performance():
    encoded = 'ö'.encode('utf-8')
    part_1, part_2 = encoded[:1], encoded[1:]

    # check that incomplete encodings are caught
    iterable = [b'abc' + part_1 + b'def' + part_1, part_2 + b'ghi']

    d1 = timeit.timeit(lambda: tuple(decode_bytes(iterable)), number=1000000)
    print(d1, file=sys.stderr)
