"""Iterator that decodes bytes into strings"""

from __future__ import annotations

from typing import (
    Generator,
    Iterable,
)


__all__ = ['decode_bytes']


def decode_bytes(
    iterable: Iterable[bytes],
    encoding: str = 'utf-8',
    backslash_replace: bool = True,
) -> Generator[str, None, None]:
    """Decode bytes in an ``iterable`` into strings

    Parameters
    ----------
    iterable: Iterable[bytes]
        Iterable that yields bytes that should be decoded.
    encoding: str (default: ``'utf-8'``)
        Encoding to be used for decoding.
    backslash_replace: bool (default: ``True``)
        If ``True``, backslash-escapes are used for undecodable bytes. If
        ``False``, a ``UnicodeDecodeError`` is raised if a byte sequence cannot
        be decoded.

    Yields
    ------
    str
        Decoded strings that are generated by decoding the data yielded by
        ``iterable`` with the specified ``encoding``

    Raises
    ------
    UnicodeDecodeError
        If ``backslash_replace`` is ``False`` and the data yielded by
        ``iterable`` cannot be decoded with the specified ``encoding``
    """
    joined_data = b''
    position = 0
    for chunk in iterable:
        joined_data += chunk
        while position < len(joined_data):
            try:
                yield joined_data[position:].decode(encoding)
                joined_data = b''
            except UnicodeDecodeError as e:
                # If an encoding error occurs, we first check whether it was
                # in the middle of `joined_data` or whether it extends until the
                # end of `joined_data`.
                # If it occurred in the middle of
                # `joined_data`, we replace it with backslash encoding or
                # re-raise the decoding error.
                # If it occurred at the end of `joined_data`, we wait for the
                # next chunk, which might fix the problem.
                if position + e.end == len(joined_data):
                    # Wait for the next chunk, which might fix the problem
                    break
                else:
                    if not backslash_replace:
                        # Signal the error to the caller
                        raise
                    else:
                        yield (
                            joined_data[:position + e.start].decode(encoding)
                            + joined_data[position + e.start:position + e.end].decode(
                                encoding,
                                errors='backslashreplace'
                            )
                        )
                        position += e.end