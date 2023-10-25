""" Data processor that decodes bytes into strings """

from __future__ import annotations

from typing import Callable

from ..data_processor_pipeline import (
    BytesList,
    StrList,
)


__all__ = ['decode_processor']


def decode_processor(encoding: str = 'utf-8') -> Callable:
    """ create a data processor that decodes a byte-stream

    The created data processor will decode byte-streams, even if the encoding
    is split at chunk borders.
    If an encoding error occurs on the final data chunk, the un-decodable bytes
    will be replaced with their escaped hex-values, i.e. ``\\xHH``,
    for hex-value HH.

    Parameters
    ----------
    encoding: str
        The name of encoding that should be decoded.

    Returns
    -------
    Callable
        A data processor that can be used in a processing pipeline to decode
        chunks of bytes. The result are chunks of strings.
    """
    return _DecodeProcessor(encoding)


class _DecodeProcessor:
    """ Decode a byte-stream, even if the encoding is split at chunk borders

    Instances of this class can be used as data processors.
    """
    def __init__(self, encoding: str = 'utf-8') -> None:
        """

        Parameters
        ----------
        encoding: str
            The type of encoding that should be decoded.
        """
        self.encoding = encoding

    def __call__(self, data_chunks: BytesList,
                 final: bool = False
                 ) -> tuple[StrList, BytesList]:
        """ The data processor interface

        This allows instances of :class:``DecodeProcessor`` to be used as
        data processor in pipeline definitions.

        Parameters
        ----------
        data_chunks: list[bytes]
            a list of bytes (data chunks) that should be decoded
        final : bool
            the data chunks are the final data chunks of the source. If an
            encoding error happens, the offending bytes will be replaced with
            their escaped hex-values, i.e. ``\\xHH``, for hex-value HH.

        Returns
        -------
        list[str]
            the decoded data chunks, possibly joined
        """
        try:
            text = (b''.join(data_chunks)).decode(self.encoding)
        except UnicodeDecodeError:
            if final:
                text = (b''.join(data_chunks)).decode(
                    self.encoding,
                    errors='backslashreplace')
            else:
                return [], data_chunks
        return [text], []
