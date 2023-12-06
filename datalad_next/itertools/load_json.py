""" Functions that yield JSON objects converted from input items """

from __future__ import annotations

import json
from typing import (
    Any,
    Generator,
    Iterable,
)


__all__ = ['load_json', 'load_json_with_flag']


def load_json(iterable: Iterable[bytes | str],
              ) -> Generator[Any, None, None]:
    """ Convert items yielded by ``iterable`` into JSON objects and yield them

    This function fetches items from the underlying
    iterable. The items are expected to be ``bytes``, ``str``, or ``bytearry``,
    and contain one JSON-encoded object. Items
    are converted into a JSON-object, by feeding them into
    ``json.loads``.

    On successful conversion to a JSON-object, ``load_json`` will yield the
    resulting JSON-object. If the conversion to a JSON-object fails,
    ``load_json`` will raise a ``json.decoder.JSONDecodeError``:

    .. code-block:: python

        >>> from datalad_next.itertools import load_json, load_json_with_flag
        >>> tuple(load_json(['{"a": 1}']))
        ({'a': 1},)
        >>> tuple(load_json(['{"c": 3']))   # Faulty JSON-encoding, doctest: +SKIP
        Traceback (most recent call last):
            ...
        json.decoder.JSONDecodeError: Expecting ',' delimiter: line 1 column 8 (char 7)

    Using ``load_json`` together with ``itemize`` allows the processing of
    JSON-lines data. ``itemize`` will yield a single item for each line and
    ``load_json`` will convert it into a JSON-object.

    Note: JSON-decoding is slightly faster if the items of type ``str``. Items
    of type ``bytes`` or ``bytearray`` will work as well, but processing might
    be slower.

    Parameters
    ----------
    iterable: Iterable[bytes | str]
        The iterable that yields the JSON-strings or -bytestrings that should be
        parsed and converted into JSON-objects

    Yields
    ------
    Any
        The JSON-object that are generated from the data yielded by ``iterable``

    Raises
    ------
    json.decoder.JSONDecodeError
        If the data yielded by ``iterable`` is not a valid JSON-string
    """
    for json_string in iterable:
        yield json.loads(json_string)


def load_json_with_flag(
        iterable: Iterable[bytes | str],
) -> Generator[tuple[Any | json.decoder.JSONDecodeError, bool], None, None]:
    """ Convert items from ``iterable`` into JSON objects and a success flag

    ``load_json_with_flag`` works analogous to ``load_json``, but reports
    success and failure differently.

    On successful conversion to a JSON-object, ``load_json_with_flag`` will
    yield a tuple of two elements. The first element contains the JSON-object,
    the second element is ``True``.

    If the conversion to a JSON-object fails, ``load_json_with_flag`` will
    yield a tuple of two elements, where the first element contains the
    ``json.decoder.JSONDecodeError`` that was raised during conversion, and the
    second element is ``False``:

    .. code-block:: python

        >>> from datalad_next.itertools import load_json, load_json_with_flag
        >>> tuple(load_json_with_flag(['{"b": 2}']))
        (({'b': 2}, True),)
        >>> tuple(load_json_with_flag(['{"d": 4']))   # Faulty JSON-encoding
        ((JSONDecodeError("Expecting ',' delimiter: line 1 column 8 (char 7)"), False),)

    Parameters
    ----------
    iterable: Iterable[bytes | str]
        The iterable that yields the JSON-strings or -bytestrings that should be
        parsed and converted into JSON-objects

    Yields
    ------
    tuple[Any | json.decoder.JSONDecodeError, bool]
        A tuple containing of a decoded JSON-object and ``True``, if the JSON
        string could be decoded correctly. If the JSON string  could not be
        decoded correctly, the tuple will contain the
        ``json.decoder.JSONDecodeError`` that was raised during JSON-decoding
        and ``False``.
    """
    for json_string in iterable:
        try:
            yield json.loads(json_string), True
        except json.decoder.JSONDecodeError as e:
            yield e, False
