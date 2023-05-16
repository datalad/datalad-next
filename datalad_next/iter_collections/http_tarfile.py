"""Report on the content of TAR archives

The main functionality is provided by the :func:`iter_tar()` function.
"""

from __future__ import annotations

import logging
from io import (
    SEEK_SET,
    SEEK_CUR,
    SEEK_END,
)
from typing import (
    cast,
    Any,
    Generator,
    IO,
)
from urllib.parse import (
    urlunparse,
    ParseResult as URLParseResult,
)

import requests

from .tarfile import (
    iter_tar_on_file,
    TarfileItem,
)


lgr = logging.getLogger('datalad.iter_collections.http_tarfile')


class HttpFileObject:
    """Instances of this class map file-like access to HTTP-operations.

    This class uses the ``Range´´-header to request only bytes that the
    client reads. Therefore, there is no need to download the complete remote
    object in order to access only a small portion of it.
    """
    def __init__(self,
                 url: str | URLParseResult,
                 auth: Any,
                 ):
        self.url = url if isinstance(url, str) else urlunparse(url)
        self.auth = auth
        self.session = requests.Session()
        self.session.auth = auth
        self.offset = 0

    def tell(self):
        return self.offset

    def read(self, size: int) -> bytes:
        lgr.debug(f'read: %d bytes at %d', size, self.offset)
        r = self.session.get(
            self.url,
            headers={'Range': f'bytes={self.offset}-{self.offset + size-1}'},
        )
        self.offset += len(r.content)
        return r.content

    def seek(self, offset: int, whence: int = SEEK_SET):
        lgr.debug(
            'seek: %d bytes from %s',
            offset,
            {
                SEEK_CUR: 'SEEK_CUR',
                SEEK_SET: 'SEEK_SET',
                SEEK_END: 'SEEK_SET',
            }[whence]
        )
        if whence == SEEK_SET:
            self.offset = offset
        elif whence == SEEK_CUR:
            self.offset += offset
        else:
            raise Exception(f"Unsupported seek-whence: {whence}")


def iter_http_tar(
    url: str | URLParseResult,
    /,
    *,
    hash: list[str] | None = None,
    auth: Any | None = None,
) -> Generator[TarfileItem, None, None]:
    """

    Parameters
    ----------
    url: str
      A HTTP- or HTTPS-URL pointing to a tar-archive.
    hash: list(str), optional
      Any number of hash algorithm names (supported by the ``hashlib`` module
      of the Python standard library. If given, an item corresponding to the
      algorithm will be included in the ``hash`` property dict of each
      reported file-type item. Note that hashing in an iter_http_tar call will
      lead to the complete download of the hashed files (in this case of all
      files in the tar archive). There is no advantage time- and download-wise.
      There is still an advantage w.r.t. local storage because the downloaded
      bytes are immediately discarded and not locally stored.
    auth: Any
      A requests-compatible authentication object, e.g. ('user', 'password'), or
      HTTPBasicAuth('user', 'password')

    Yields
    ------
    :class:`TarfileItem`
    """
    http_file_object = HttpFileObject(url, auth)
    yield from iter_tar_on_file(cast(IO, http_file_object), hash=hash)
