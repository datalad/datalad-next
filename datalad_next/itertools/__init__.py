"""Various iterators, e.g., for subprocess pipelining and output processing

.. currentmodule:: datalad_next.itertools
.. autosummary::
   :toctree: generated

    decode_bytes
    itemize
    load_json
    load_json_with_flag
    route_out
    route_in
"""


from .decode_bytes import decode_bytes
from .itemize import itemize
from .load_json import (
    load_json,
    load_json_with_flag,
)
from .reroute import (
    route_in,
    route_out,
    dont_process,
)
