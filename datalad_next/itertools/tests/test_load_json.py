from __future__ import annotations

import json
from json.decoder import JSONDecodeError

import pytest

from ..load_json import (
    load_json,
    load_json_with_flag,
)
from ..decode_bytes import decode_bytes
from ..itemize import itemize


json_object = {
    'list1': [
        'a', 'bäöl', 1
    ],
    'dict1': {
        'x': 123,
        'y': 234,
        'z': 456,
    }
}


correct_json = b'\n'.join(
    json.dumps(x).encode()
    for x in [json_object] * 10
) + b'\n'

correct_chunks = [
    correct_json[i:i + 10]
    for i in range(0, len(correct_json) + 10, 10)
]

faulty_json = correct_json.replace(b'}\n', b'\n')
faulty_chunks = [
    faulty_json[i:i + 10]
    for i in range(0, len(correct_json) + 10, 10)
]


def test_load_json_on_decoded_bytes():
    assert all(x == json_object for x in load_json(
        decode_bytes(itemize(correct_chunks, b'\n'))))
    with pytest.raises(JSONDecodeError):
        list(load_json(decode_bytes(itemize(faulty_chunks, b'\n'))))


def test_load_json_with_flag():
    assert all(
        obj == json_object and success is True
        for (obj, success)
        in load_json_with_flag(decode_bytes(itemize(correct_chunks, b'\n')))
    )
    assert all(
        isinstance(exc, JSONDecodeError) and success is False
        for (exc, success)
        in load_json_with_flag(decode_bytes(itemize(faulty_chunks, b'\n')))
    )
