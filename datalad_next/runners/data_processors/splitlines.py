""" This module contains data processors for the data pipeline processor

The data processors contained here are:

  - splitlines_processor

"""
from __future__ import annotations

from functools import partial
from typing import Callable

from ..data_processor_pipeline import (
    StrOrBytes,
    StrOrBytesList,
)


# We don't use LineSplitter here because it has two "problems". Firstly, it does
# not support `bytes`. Secondly, it can not be properly re-used because it does
# not delete its internal storage when calling `LineSplitter.finish_processing`.
# The issue https://github.com/datalad/datalad/issues/7519 has been created to
# fix the problem upstream. Until then we use this code.
def _splitlines_processor(separator: StrOrBytes | None,
                          keep_ends: bool,
                          data_chunks: StrOrBytesList,
                          final: bool = False
                          ) -> tuple[StrOrBytesList, StrOrBytesList]:
    """ Implementation of character-strings or byte-strings line splitting

    This function implements the line-splitting data processor and is used
    by :func:`splitlines_processor` below.

    To use this function as a data processor, use partial to "fix" the first
    two parameter.

    Parameters
    ----------
    separator: Optional[str]
        If not None, the provided separator will be used to split lines.
    keep_ends: bool
        If True, the separator will be contained in the returned lines.
    data_chunks: list[str | bytes]
        a list of strings or bytes
    final : bool
        the data chunks are the final data chunks of the source. A line is
        terminated by end of data.

    Returns
    -------
    list[str | bytes]
        if the input data chunks contained bytes the result will be a list of
        byte-strings that end with byte-size line-delimiters. If the input data
        chunks contained strings, the result will be a list strings that end with
        string delimiters (see Python-documentation for a definition of string
        line delimiters).
    """
    # We use `data_chunks[0][0:0]` to get an empty value the proper type, i.e.
    # either the string `''` or the byte-string `b''`.
    empty = data_chunks[0][0:0]
    text = empty.join(data_chunks)
    if separator is None:
        # Use the builtin line split-wisdom of Python
        parts_with_ends = text.splitlines(keepends=True)
        parts_without_ends = text.splitlines(keepends=False)
        lines = parts_with_ends if keep_ends else parts_without_ends
        if parts_with_ends[-1] == parts_without_ends[-1] and not final:
            return lines[:-1], [parts_with_ends[-1]]
        return lines, []
    else:
        detected_lines = text.split(separator)
        remaining = detected_lines[-1] if text.endswith(separator) else None
        del detected_lines[-1]
        if keep_ends:
            result = [line + separator for line in detected_lines], [remaining]
        else:
            result = detected_lines, [remaining]
        if final:
            result = result[0].extend(result[1]), []
        return result


# A simple line-splitter on known line-endings that keeps line ends in the output
def splitlines_processor(separator: StrOrBytes | None = None,
                         keep_ends: bool = True
                         ) -> Callable:
    """ A data processor the splits character-strings or byte-strings into lines

    Split lines either on a given separator, if 'separator' is not `None`,
    or on one of the known line endings, if 'separator' is `None`. The line
    endings are determined by python

    Parameters
    ----------
    separator: Optional[str]
        If not None, the provided separator will be used to split lines.
    keep_ends: bool
        If True, the separator will be contained in the returned lines.

    Returns
    -------
    list[str | bytes]
        if the input data chunks contained bytes the result will be a list of
        byte-strings that end with byte-size line-delimiters. If the input data
        chunks contained strings, the result will be a list strings that end with
        string delimiters (see Python-documentation for a definition of string
        line delimiters).
    """
    return partial(_splitlines_processor, separator, keep_ends)
