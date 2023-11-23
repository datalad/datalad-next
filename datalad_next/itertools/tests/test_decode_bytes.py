from __future__ import annotations

import sys
import timeit

import pytest

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


def test_undecodable_byte():
    # check that a single undecodable byte is handled properly
    r = tuple(decode_bytes([b'\xc3']))
    assert ''.join(r) == '\\xc3'
    with pytest.raises(UnicodeDecodeError):
        tuple(decode_bytes([b'\xc3'], backslash_replace=False))


def test_performance():
    encoded = 'ö'.encode('utf-8')
    part_1, part_2 = encoded[:1], encoded[1:]

    # check that incomplete encodings are caught
    iterable = [b'abc' + part_1 + b'def' + part_1, part_2 + b'ghi']

    d1 = timeit.timeit(lambda: tuple(decode_bytes(iterable)), number=1000000)
    print(d1, file=sys.stderr)
