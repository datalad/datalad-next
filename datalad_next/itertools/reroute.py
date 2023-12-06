""" Functions that allow to route data around upstream iterator """

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Generator,
    Iterable,
)


__all__ = ['StoreOnly', 'route_in', 'route_out']


class StoreOnly:
    pass


def route_out(iterable: Iterable,
              data_store: list,
              splitter: Callable[[Any], tuple[Any, Any]],
              ) -> Generator:
    """ Route data around the consumer of this iterable

    :func:`route_out` allows its user to:

     1. store data that is received from an iterable,
     2. determine whether this data should be yielded to a consumer of
        ``route_out``, by calling :func:`splitter`.

    To determine which data is to be yielded to the consumer and which data
    should only be stored but not yielded, :func:`route_out` calls
    :func:`splitter`. :func:`splitter` is called for each item of the input
    iterable, with the item as sole argument. The function should return a
    tuple of two elements. The first element is the data that is to be
    yielded to the consumer. The second element is the data that is to be
    stored in the list ``data_store``. If the first element of the tuple is
    ``datalad_next.itertools.StoreOnly``, no data is yielded to the
    consumer.

    :func:`route_in` can be used to combine data that was previously
    stored by :func:`route_out` with the data that is yielded by
    :func:`route_out` and with the data the was not processed, i.e. not yielded
    by :func:`route_out`.

    The items yielded by :func:`route_in` will be in the same
    order in which they were passed into :func:`route_out`, including the
    items that were not yielded by :func:`route_out` because :func:`splitter`
    returned ``StoreOnly`` in the first element of the result-tuple.

    The combination of the two functions :func:`route_out` and :func:`route_in`
    can be used to "carry" additional data along with data that is processed by
    iterators. And it can be used to route data around iterators that cannot
    process certain data.

    For example, a user has an iterator to divide the number ``2`` by all
    numbers in a list. The user wants the iterator to process all numbers in a
    divisor list, except from zeros, In this case :func:`route_out` and
    :func:`route_in` can be used as follows:

    .. code-block:: python

        from math import nan
        from datalad_next.itertools import route_out, route_in, StoreOnly

        def splitter(divisor):
            # if divisor == 0, return `StoreOnly` in the first element of the
            # result tuple to indicate that route_out should not yield this
            # element to its consumer
            return (StoreOnly, divisor) if divisor == 0 else (divisor, divisor)

        def joiner(processed_data, stored_data):
            #
            return nan if processed_data is StoreOnly else processed_data

        divisors = [0, 1, 0, 2, 0, 3, 0, 4]
        store = list()
        r = route_in(
            map(
                lambda x: 2.0 / x,
                route_out(
                    divisors,
                    store,
                    splitter
                )
            ),
            store,
            joiner
        )
        print(list(r))

    The example about will print ``[nan, 2.0, nan, 1.0, nan, 0.6666666666666666, nan, 0.5]``.

    Parameters
    ----------
    iterable: Iterable
        The iterable that yields the input data
    data_store: list
        The list that is used to store the data that is routed out
    splitter: Callable[[Any], tuple[Any, Any | None]]
        The function that is used to determine which part of the input data,
        if any, is to be yielded to the consumer and which data is to
        be stored in the list ``data_store``.
        The function is called for each item of
        the input iterable with the item as sole argument. It should return a
        tuple of two elements. If the first element is not
        ``datalad_next.itertools.StoreOnly``, it is yielded to the consumer.
        If the first element is ``datalad_next.itertools.StoreOnly``,
        nothing is yielded to the consumer. The second element is stored in the
        list ``data_store``.
        The cardinality of ``data_store`` will be the same as the cardinality of
        the input iterable.
    """
    for item in iterable:
        data_to_process, data_to_store = splitter(item)
        data_store.append((data_to_process, data_to_store))
        if data_to_process is not StoreOnly:
            yield data_to_process


def route_in(iterable: Iterable,
             data_store: list,
             joiner: Callable[[Any, Any], Any]
             ) -> Generator:
    """ Yield previously rerouted data to the consumer

    This function is the counter-part to :func:`route_out`. It takes the iterable
    ``iterable`` and a data store given in ``data_store`` and yields items
    in the same order in which :func:`route_out` received them from its
    underlying iterable (using the same data store). This includes items that
    were not yielded by :func:`route_out`, but only stored.

    :func:`route_in` uses :func:`joiner`-function to determine how stored and
    optionally processed data should be joined into a single item, which is
    then yielded by :func:`route_in`.
    :func:`route_in` calls :func:`joiner` with a 2-tuple. The first
    element of the tuple is either ``datalad_next.itertools.StoreOnly`` or the
    next item from the underlying iterator. The second element is the data
    that was stored in the data store. The result of :func:`joiner` which will
    be yielded by :func:`route_in`.

    This module provides a standard joiner-function: :func:`join_with_list`
    that works with splitter-functions that return a list as second element of
    the result tuple.

    The cardinality of ``iterable`` must match the number of processed data
    elements in the data store. The output cardinality of :func:`route_in` will
    be the cardinality of the input iterable of the corresponding
    :func:`route_out`-call. Given the following code:

    .. code-block:: python

        store_1 = list()
        route_in(
            some_generator(
                route_out(input_iterable, store_1, splitter_1)
            ),
            store_1,
            joiner_1
        )

    :func:`route_in` will yield the same number of elements as ``input_iterable``.
    But, the number of elements processed by ``some_generator`` is determined by
    the :func:`splitter_1` in :func:`route_out`, i.e. by the number of
    :func:`splitter_1`-results that have don't have
    ``datalad_next.itertools.don_process`` as first element.

    Parameters
    ----------
    iterable: Iterable
        The iterable that yields the input data.
    data_store: list
        The list from which the data that is to be "routed in" is read.
    joiner: Callable[[Any, Any], Any]
        A function that determines how the items that are yielded by
        ``iterable`` should be combined with the corresponding data from
        ``data_store``, in order to yield the final result.
        The first argument to ``joiner`` is the item that is yielded by
        ``iterable``, or ``datalad_next.itertools.StoreOnly`` if no data
        was processed in the corresponding step. The second argument is the
        data that was stored in ``data_store`` in the corresponding step.
    """
    for element in iterable:
        processed, stored = data_store.pop(0)
        # yield stored-only content until we find an item that was processed
        while processed is StoreOnly:
            yield joiner(processed, stored)
            processed, stored = data_store.pop(0)
        yield joiner(element, stored)
    # we reached the end of the incoming iterable.
    # this means that we must not find any remaining items in `data_store`
    # that indicate that they would have a corresponding item in the
    # iterable (processed is not StoreOnly)
    for processed, stored in data_store:
        assert processed is StoreOnly, \
            "iterable did not yield matching item for route-in item, cardinality mismatch?"
        yield joiner(processed, stored)
    # rather than pop() in the last loop, we just yielded from the list
    # now this information is no longer needed
    del data_store[:]
