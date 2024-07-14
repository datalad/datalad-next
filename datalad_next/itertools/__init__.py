"""Various iterators, e.g., for subprocess pipelining and output processing

.. deprecated:: 1.6

   This code has been moved to the datasalad library.
   Use it from ``datasalad.itertools`` instead.
"""

__all__ = [
    'align_pattern',
    'decode_bytes',
    'itemize',
    'load_json',
    'load_json_with_flag',
    'StoreOnly',
    'route_in',
    'route_out',
]

from datasalad.itertools import (
    align_pattern,
    decode_bytes,
    itemize,
    load_json,
    load_json_with_flag,
    route_in,
    route_out,
    StoreOnly,
)
