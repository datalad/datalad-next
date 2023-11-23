from __future__ import annotations

import json
import timeit

from ..load_json import load_json
from ..decode_bytes import decode_bytes
from ..itemize import itemize


o = {
    'list1': [
        'a', 'bäöl', 1
    ],
    'dict1': {
        'x': 123,
        'y': 234,
        'z': 456,
    }
}


b = b'\n'.join(json.dumps(x).encode() for x in [o] * 10)

c = [
    b[i:i+10]
    for i in range(0, len(b) + 10, 10)
]


def test_combi():
    all(x == 0 for x in load_json(decode_bytes(itemize(c))))


def test_combi_performance():

    def read_all(g):
        tuple(load_json((itemize(g))))

    def read_all_decoded(g):
        tuple(load_json(decode_bytes(itemize(g))))

    repeat = 33000
    d1 = timeit.timeit(lambda: read_all(c), number=repeat)
    print(f'read_all x {repeat}: {d1}')
    d2 = timeit.timeit(lambda: read_all_decoded(c), number=repeat)
    print(f'read_all_decoded x {repeat}: {d2}')
    print(f'read_all / read_all_decoded: {d1 / d2}')