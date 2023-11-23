""" Generator the emits only complete lines """

from __future__ import annotations

from typing import (
    Generator,
    Iterable,
)


__all__ = ['itemize']


def itemize(
    iterable: Iterable[bytes | str],
    separator: str | bytes | None = None,
    keep_ends: bool = False,
) -> Generator[bytes | str, None, None]:
    """ Generator that emits only complete items from chunks of an iterable

    This generator consumes chunks from an iterable and yields items defined by
    a separator. An item might span multiple input chunks.

    Items are defined by a ``separator``. If ``separator`` is ``None``, the
    line-separators built into `str.plitlines` are used.

    The generator works on string or byte chunks, depending on the type of the
    first element in ``iterable``. During its runtime, the type of the elements
    in ``iterable`` must not change. If ``separator`` is not `None`, its type
    must match the type of the elements in ``iterable``.

    The complexity of itemization without a defined separator is higher than
    the complexity of itemization with a defined separator (this is due to
    the externally unavailable set of line-separators that are built into
    `splitlines`).

    Runtime with ``keep_end=False`` is faster than otherwise, when a separator
    is defined.

    EOF ends all lines, but will never be present in the result, even if
    ``keep_ends`` is ``True``.

    Parameters
    ----------
    iterable: Iterable[bytes | str]
        The iterable that yields the input data
    separator: str | bytes | None
        The separator that defines items. If ``None``, the items are
        determined by the line-separators that are built into `splitlines`.
    keep_ends: bool
        If `True`, the item-separator will be present at the end of a
        yielded item line. If `False`, items will not contain the
        separator. Preserving separators an additional implies a runtime cost.

    Yields
    ------
    bytes | str
        The items determined from the input iterable. The type of the yielded
        lines depends on the type of the first element in ``iterable``.
    """
    if separator is None:
        yield from _split_lines(iterable, keep_ends=keep_ends)
    else:
        yield from _split_lines_with_separator(
            iterable,
            separator=separator,
            keep_ends=keep_ends,
        )


def _split_lines_with_separator(iterable: Iterable[bytes | str],
                                separator: str | bytes,
                                keep_ends: bool = False,
                                ) -> Generator[bytes | str, None, None]:
    assembled = None
    for chunk in iterable:
        if not assembled:
            assembled = chunk
        else:
            assembled += chunk
        lines = assembled.split(sep=separator)
        if len(lines) == 1:
            continue

        if assembled.endswith(separator):
            assembled = None
        else:
            assembled = lines[-1]
        lines.pop(-1)
        if keep_ends:
            for line in lines:
                yield line + separator
        else:
            yield from lines

    if assembled:
        yield assembled


def _split_lines(iterable: Iterable[bytes | str],
                 keep_ends: bool = False,
                 ) -> Generator[bytes | str, None, None]:
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