""" Functions that allow to route data around upstream iterator """

from __future__ import annotations

from collections import defaultdict
from typing import (
    Any,
    Callable,
    Generator,
    Iterable,
)


__all__ = ['route_in', 'route_out', 'join_with_list']

_active_fifos: dict[str, list] = defaultdict(list)


def route_out(iterable: Iterable,
              route_id: str,
              splitter: Callable[[Any], tuple[Any | None, Any | None]],
              ) -> Generator:
    """ Route data around the consumer of this generator

        This generator wraps another generator in order to route parts if the
        input data around upstream consumers, while other parts of the data are
        yielded to consumers.

        Data that is routed out is stored in a FIFO-element that is identified
        by ``route_id``.

        To determine which data is to be yielded to the consumer and which data
        is to be ignored, the ``splitter`` function is used. The ``splitter``
        function is called with each element of the input iterable. It returns a
        tuple of two elements. The first element is the data that is to be
        yielded to the consumer. The second element is the data that is to be
        stored in the FIFO-element defined by ``route_id``. If the first element
        of the tuple is ``None``, no data is yielded to the consumer.

        The counterpart to the ``splitter``-function is the ``joiner``-function
        in ``route_in``. This module provides a standard ``joiner``-function
        named ``join_with_list`` that works with splitters that return a list as
        second element of the result tuple.

        Parameters
        ----------
        iterable: Iterable
            The iterable that yields the input data
        route_id: str
            The identifier of the FIFO-element that is used to store the data
            that is routed out
        splitter: Callable[[Any], tuple[Any | None, Any | None]]
            The function that is used to determine which data is to be yielded
            to the consumer and which data is to be stored in the FIFO-element
            defined by ``route_id``. The function is called with each element of
            the input iterable. It returns a tuple of two elements. If the
            first element is not ``None``, it is yielded to the consumer. If the
            first element is ``None``, nothing is yielded to the consumer. The
            second element is stored in the FIFO-element defined by ``route_id``.
            The cardinality of the FIFO-element will be the same as the
            cardinality of the input iterable.
    """
    fifo = _active_fifos[route_id]
    for item in iterable:
        data_to_process, data_to_store = splitter(item)
        if data_to_process is not None:
            fifo.append(('process', data_to_store))
            yield data_to_process
        else:
            fifo.append(('ignore', data_to_store))


def route_in(iterable: Iterable,
             route_id: str,
             joiner: Callable[[Any | None, Any], Any]
             ) -> Generator:
    """ Insert previously rerouted data into the consumer of this generator

        This generator wraps the generator ``iterable`` and inserts data that was
        previously stored in the FIFO-element defined by ``route_id`` by the
        ``route_out``-generator.

        The cardinality of ``iterable`` must match the number of processed data
        elements in the FIFO-element defined by ``route_id``. The output
        cardinality of ``route_in`` will be the cardinality of the FIFO-elements,
        and thereby the cardinality of the input iterable that was given to
        ``route_out`` in order to create the FIFO-element. That means a
        combination of::

            route_in(
                some_generator(
                    route_out(input_iterable, 'id1', splitter_1)
                ),
                'id_1',
                joiner_1
            )

        will yield the same number of elements as ``input_iterable``. But, the
        number of elements processed by ``some_generator`` is determined by
        the ``splitter``-function in ``route_out``, i.e. by the number of
        ``splitter``-results that have a non-``None`` first element.

        ``route_in``, together with ``route_out``, allows to skip processing of
        elements like, for example, empty lines in ``some_generator``, while still
        yielding them from ``route_in``.

        ``route_in`` and ``route_out`` also allow to store additional information
        that "tags along" with the information that is processed by the
        generators.

        Parameters
        ----------
        iterable: Iterable
            The iterable that yields the input data
        route_id: str
            The identifier of the FIFO-element from the data should be read
            that is to be routed in
        joiner: Callable[[Any, Any], Any]
            A function that determines how the data that is yielded by
            ``iterable`` should be combined with the corresponding data that was
            stored in the FIFO-element, in order to yield the final result.
            The first argument to ``joiner`` is the data that is yielded by
            ``iterable``, or ``None`` if no data was processed in the corresponding
            step. The second argument is the data that was stored in the
            FIFO-element in the corresponding step.
    """
    fifo = _active_fifos[route_id]
    for element in iterable:
        process_info = fifo.pop(0)
        while process_info[0] == 'ignore':
            yield joiner(None, process_info[1])
            process_info = fifo.pop(0)
        yield joiner(element, process_info[1])
    assert len(fifo) == 0
    del _active_fifos[route_id]


def join_with_list(processed_data: Any | None,
                   stored_data: list
                   ) -> list:
    """ A standard joiner that works with splitter that store a list

        This joiner is used in combination with splitters that return a list as
        second element of the result tuple, i.e. splitters that will store a
        list in the FIFO-element. The joiner adds the corresponding element
        of ``iterable`` as first element to the list. The extended list is then
        yielded be the ``route_in``-generator.

        Parameters
        ----------
        processed_data: Any | None
            The data that is yielded by the ``iterable``-generator
        stored_data: list
            The data that was stored in the FIFO-element by the ``route_out``
            generator

        Returns
        -------
        list
            A list this is equivalent to ``[processed_data] + stored_data``
    """
    if not isinstance(processed_data, list):
        return [processed_data] + stored_data
    processed_data.extend(stored_data)
    return processed_data
