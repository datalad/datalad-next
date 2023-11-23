"""Various iterators, e.g., for subprocess pipelining and output processing

This module provides iterators that are useful when combining
iterable_subprocesses and for processing of subprocess output.

.. currentmodule:: datalad_next.itertools
.. autosummary::
   :toctree: generated

    decode_bytes
    itemize
    load_json
    load_json_with_flag
    align_pattern
    route_out
    route_in
    join_with_list


Examples
========

Usage of ``route_out`` and ``route_in``
---------------------------------------

``route_out`` allows its user to:

 1. store data it receives from an iterable, and
 2. decide whether this data should be yielded to a consumer of ``route_out``.

``route_in`` allows its user to combine data it receives from an iterable with
data that was previously stored by ``route_out``. The user of ``route_in`` will
receive stored data in the same order as it was stored by ``route_out``,
including data that was not yielded by ``route_out`` but only stored.

The combination of the two functions can be used to "carry" additional data
along with data that is processed by iterators. And it can be used to route
data around iterators that cannot process certain data.

For example, a user has an iterator to divide all number in a list by 2. The
user wants the iterator tp process all number in a list, except from zeros,
In this case ``route_out`` and ``route_in`` can be used as follows:

.. code-block:: python

    from math import nan
    from datalad_next.itertools import route_out, route_in

    def splitter(data):
        # if data == 0, return None in the first element of the result tuple
        # to indicate that route_out should not yield anything to the consumer
        return (None, [data]) if data == 0 else (data / 2.0, [data])

    def joiner(processed_data, stored_data):
        return nan if processed_data is None else processed_data

    numbers = [0, 1, 0, 2, 0, 3, 0, 4]
    r = route_in(
        map(
            lambda x: x / 2.0,
            route_out(
                numbers,
                'zeros',
                splitter
            )
        ),
        'zeros',
        joiner
    )
    print(list(r))

The example about will print ``[nan, 0.25, nan, 0.5, nan, 0.75, nan, 1.0]``.

The names that are used to share data should are used inside a ``route_in``,
``route_out`` enclosure. For example, the following code snippet would not
lead to a collision of names:

.. code-block:: python

    route_in(
        iterator_1(route_out(input_iterable_1, 'storage', splitter),
        'storage'
        joiner
    )
    route_in(
        iterator_2(route_out(input_iterable_2, 'storage', splitter),
        'storage'
        joiner
    )

In contrast to that, the following code would lead to a collision of the name
``storage``, because it is used in nested ``route_out``- ``route_in``-calls

.. code-block:: python

    route_in(
        iterator_2(
            route_in(
                iterator_1(
                    route_out(
                        route_out(input_iterable, 'storage', splitter_1),
                        'storage',
                        splitter_2
                    )
                )
                'storage',
                joiner_1
            )
        )
        'storage',
        joiner_2
    )

"""


from .align_pattern import align_pattern
from .decode_bytes import decode_bytes
from .itemize import itemize
from .load_json import (
    load_json,
    load_json_with_flag,
)
from .decode_bytes import decode_bytes
from .reroute import (
    join_with_list,
    route_in,
    route_out,
)
