"""Various iterators, e.g., for subprocess pipelining and output processing

.. deprecated:: 1.6
   This module is deprecated. It has been migrated to the `datasalad library
   <https://pypi.org/project/datasalad>`__. Imports should be adjusted to
   ``datasalad.itertools``.
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

import warnings

from datasalad.itertools import (
    StoreOnly,
    align_pattern,
    decode_bytes,
    itemize,
    load_json,
    load_json_with_flag,
    route_in,
    route_out,
)

warnings.warn(
    '`datalad_next.itertools` has been migrated to the datasalad library, '
    'adjust imports to `datasalad.itertools`',
    DeprecationWarning,
    stacklevel=1,
)
