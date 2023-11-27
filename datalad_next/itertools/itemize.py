"""Get complete items from input chunks"""

from __future__ import annotations

from typing import (
    Generator,
    Iterable,
    TypeVar,
)


__all__ = ['itemize']


T = TypeVar('T', str, bytes, bytearray)


def itemize(
    iterable: Iterable[T],
    sep: T | None,
    *,
    keep_ends: bool = False,
) -> Generator[T, None, None]:
    """Yields complete items (only), assembled from an iterable

    This function consumes chunks from an iterable and yields items defined by
    a separator. An item might span multiple input chunks.  Input (chunks) can
    be ``bytes``, ``bytearray``, or ``str`` objects.  The result type is
    determined by the type of the first input chunk. During its runtime, the
    type of the elements in ``iterable`` must not change.

    Items are defined by a separator given via ``sep``. If ``sep`` is ``None``,
    the line-separators built into ``str.splitlines()`` are used, and each
    yielded item will be a line. If ``sep`` is not `None`, its type must be
    compatible to the type of the elements in ``iterable``.

    A separator could, for example, be ``b'\\n'``, in which case the items
    would be terminated by Unix line-endings, i.e. each yielded item is a
    single line. The separator could also be, ``b'\\x00'`` (or ``'\\x00'``),
    to split zero-byte delimited content, like the output of
    ``git ls-files -z``.

    Separators can be longer than one byte or character, e.g. ``b'\\r\\n'``, or
    ``b'\\n-------------------\\n'``.

    Content after the last separator, possibly merged across input chunks, is
    always yielded as the last item, even if it is not terminated by the
    separator.

    Performance notes:

    - Using ``None`` as a separator  (splitlines-mode) is slower than providing
      a specific separator.
    - If another separator than ``None`` is used, the runtime with ``keep_end=False`` is faster than with ``keep_end=True``.

    Parameters
    ----------
    iterable: Iterable[str | bytes | bytearray]
        The iterable that yields the input data
    sep: str | bytes | bytearray | None
        The separator that defines items. If ``None``, the items are
        determined by the line-separators that are built into
        ``str.splitlines()``.
    keep_ends: bool
        If `True`, the item-separator will remain at the end of a
        yielded item. If `False`, items will not contain the
        separator. Preserving separators implies a runtime cost, unless the separator is ``None``.

    Yields
    ------
    str | bytes | bytearray
        The items determined from the input iterable. The type of the yielded
        items depends on the type of the first element in ``iterable``.

    Examples
    --------

    .. code-block:: python

        >>> from datalad_next.itertools import itemize
        >>> with open('/etc/passwd', 'rt') as f:                            # doctest: +SKIP
        ...     print(tuple(itemize(iter(f.read, ''), sep=None))[0:2])      # doctest: +SKIP
        ('root:x:0:0:root:/root:/bin/bash',
         'systemd-timesync:x:497:497:systemd Time Synchronization:/:/usr/sbin/nologin')
        >>> with open('/etc/passwd', 'rt') as f:                            # doctest: +SKIP
        ...     print(tuple(itemize(iter(f.read, ''), sep=':'))[0:10])      # doctest: +SKIP
        ('root', 'x', '0', '0', 'root', '/root',
         '/bin/bash\\nsystemd-timesync', 'x', '497', '497')
        >>> with open('/etc/passwd', 'rt') as f:                                        # doctest: +SKIP
        ...     print(tuple(itemize(iter(f.read, ''), sep=':', keep_ends=True))[0:10])  # doctest: +SKIP
        ('root:', 'x:', '0:', '0:', 'root:', '/root:',
         '/bin/bash\\nsystemd-timesync:', 'x:', '497:', '497:')
    """
    if sep is None:
        yield from _split_lines(iterable, keep_ends=keep_ends)
    else:
        yield from _split_items_with_separator(
            iterable,
            sep=sep,
            keep_ends=keep_ends,
        )


def _split_items_with_separator(iterable: Iterable[T],
                                sep: T,
                                keep_ends: bool = False,
                                ) -> Generator[T, None, None]:
    assembled = None
    for chunk in iterable:
        if not assembled:
            assembled = chunk
        else:
            assembled += chunk
        items = assembled.split(sep=sep)
        if len(items) == 1:
            continue

        if assembled.endswith(sep):
            assembled = None
        else:
            assembled = items[-1]
        items.pop(-1)
        if keep_ends:
            for item in items:
                yield item + sep
        else:
            yield from items

    if assembled:
        yield assembled


def _split_lines(iterable: Iterable[T],
                 keep_ends: bool = False,
                 ) -> Generator[T, None, None]:
    assembled = None
    for chunk in iterable:
        if not assembled:
            assembled = chunk
        else:
            assembled += chunk
        # We don't know all elements on which python splits lines, therefore we
        # split once with ends and once without ends. Lines that differ have no
        # ending
        lines_with_end = assembled.splitlines(keepends=True)
        lines_without_end = assembled.splitlines(keepends=False)
        if lines_with_end[-1] == lines_without_end[-1]:
            assembled = lines_with_end[-1]
            lines_with_end.pop(-1)
            lines_without_end.pop(-1)
        else:
            assembled = None
        if keep_ends:
            yield from lines_with_end
        else:
            yield from lines_without_end

    if assembled:
        yield assembled
