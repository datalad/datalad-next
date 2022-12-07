"""Abstract base class for URL operation handlers"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

import datalad
from datalad_next.utils import log_progress

lgr = logging.getLogger('datalad.ext.next.url_operations')


class UrlOperations:
    """Abstraction for operations on URLs

    Support for specific URL schemes can be implemented via sub-classes.
    Such classes must comply with the following conditions:

    - Any configuration look-up must be performed with the `self.cfg`
      property, which is guaranteed to be a `ConfigManager` instance.

    - When downloads are to be supported, implement the `download()` method
      and comply with the behavior described in its documentation.

    This class provides a range of helper methods to aid computation of
    hashes and progress reporting.
    """
    def __init__(self, *, cfg=None):
        """
        Parameters
        ----------
        cfg: ConfigManager, optional
          A config manager instance that implementations will consult for
          any configuration items they may support.
        """
        self._cfg = cfg

    @property
    def cfg(self):

        if self._cfg is None:
            self._cfg = datalad.cfg
        return self._cfg

    def sniff(self,
              url: str,
              *,
              credential: str | None = None,
              timeout: float | None = None) -> Dict:
        """Gather information on a URL target, without downloading it

        Returns
        -------
        dict
          A mapping of property names to values of the URL target. The
          particular composition of properties depends on the specific
          URL. A standard property is 'content-length', indicating the
          size of a download.

        Raises
        ------
        UrlOperationsRemoteError
          This exception is raised on any access-related error on the remote
          side, with a summary of the underlying issues as its message.
          It may carry a status code (e.g. HTTP status code) as its
          ``status_code`` property.
          Any underlying exception must be linked via the `__cause__`
          property (e.g. `raise UrlOperationsRemoteError(...) from ...`).
        UrlOperationsInteractionError
        UrlOperationsAuthenticationError
        UrlOperationsAuthorizationError
        UrlOperationsResourceUnknown
          Implementations that can distinguish several remote error types
          beyond indication a general ``UrlOperationsRemoteError``:
          ``UrlOperationsInteractionError`` general issues in communicating
          with the remote side; ``UrlOperationsAuthenticationError`` for
          errors related to (failed) authentication at the remote;
          ``UrlOperationsAuthorizationError`` for (lack of) authorizating
          to access a particular resource of perform a particular operation;
          ``UrlOperationsResourceUnknown`` if the target of an operation
          does not exist.
        TimeoutError
          If `timeout` is given and the operation does not complete within the
          number of seconds that a specified by `timeout`.
        """
        raise NotImplementedError

    def download(self,
                 from_url: str,
                 to_path: Path | None,
                 *,
                 credential: str | None = None,
                 hash: list[str] | None = None,
                 timeout: float | None = None) -> Dict:
        """Download from a URL to a local file or stream to stdout

        Parameters
        ----------
        from_url: str
          Valid URL with any scheme supported by a particular implementation.
        to_path: Path or None
          A local platform-native path or `None`. If `None` the downloaded
          data is written to `stdout`, otherwise it is written to a file
          at the given path. The path is assumed to not exist. Any existing
          file will be overwritten.
        credential: str, optional
          The name of a dedicated credential to be used for authentication
          in order to perform the download. Particular implementations may
          or may not require or support authentication. They also may or
          may not support automatic credential lookup.
        hash: list(algorithm_names), optional
          If given, must be a list of hash algorithm names supported by the
          `hashlib` module. A corresponding hash will be computed simultaenous
          to the download (without reading the data twice), and included
          in the return value.
        timeout: float, optional
          If given, specifies a timeout in seconds. If the operation is not
          completed within this time, it will raise a `TimeoutError`-exception.
          If timeout is None, the operation will never timeout.

        Returns
        -------
        dict
          A mapping of property names to values for the completed download.
          If `hash` algorithm names are provided, a corresponding key for
          each algorithm is included in this mapping, with the hexdigest
          of the corresponding checksum as the value.

        Raises
        ------
        UrlOperationsRemoteError
          This exception is raised on any deletion-related error on the remote
          side, with a summary of the underlying issues as its message.
          It may carry a status code (e.g. HTTP status code) as its
          ``status_code`` property.
          Any underlying exception must be linked via the `__cause__`
          property (e.g. `raise UrlOperationsRemoteError(...) from ...`).
        UrlOperationsInteractionError
        UrlOperationsAuthenticationError
        UrlOperationsAuthorizationError
        UrlOperationsResourceUnknown
          Implementations that can distinguish several remote error types
          beyond indication a general ``UrlOperationsRemoteError``:
          ``UrlOperationsInteractionError`` general issues in communicating
          with the remote side; ``UrlOperationsAuthenticationError`` for
          errors related to (failed) authentication at the remote;
          ``UrlOperationsAuthorizationError`` for (lack of) authorizating
          to access a particular resource of perform a particular operation;
          ``UrlOperationsResourceUnknown`` if the target of an operation
          does not exist.
        TimeoutError
          If `timeout` is given and the operation does not complete within the
          number of seconds that a specified by `timeout`.
        """
        raise NotImplementedError

    def upload(self,
               from_path: Path | None,
               to_url: str,
               *,
               credential: str | None = None,
               hash: list[str] | None = None,
               timeout: float | None = None) -> Dict:
        """Upload from a local file or stream to a URL

        Parameters
        ----------
        from_path: Path or None
          A local platform-native path or `None`. If `None` the upload
          data is read from `stdin`, otherwise it is read from a file
          at the given path.
        to_url: str
          Valid URL with any scheme supported by a particular implementation.
          The target is assumed to not conflict with existing content, and
          may be overwritten.
        credential: str, optional
          The name of a dedicated credential to be used for authentication
          in order to perform the upload. Particular implementations may
          or may not require or support authentication. They also may or
          may not support automatic credential lookup.
        hash: list(algorithm_names), optional
          If given, must be a list of hash algorithm names supported by the
          `hashlib` module. A corresponding hash will be computed simultaenous
          to the upload (without reading the data twice), and included
          in the return value.
        timeout: float, optional
          If given, specifies a timeout in seconds. If the operation is not
          completed within this time, it will raise a `TimeoutError`-exception.
          If timeout is None, the operation will never timeout.

        Returns
        -------
        dict
          A mapping of property names to values for the completed upload.
          If `hash` algorithm names are provided, a corresponding key for
          each algorithm is included in this mapping, with the hexdigest
          of the corresponding checksum as the value.

        Raises
        ------
        FileNotFoundError
          If the source file cannot be found.
        UrlOperationsRemoteError
          This exception is raised on any deletion-related error on the remote
          side, with a summary of the underlying issues as its message.
          It may carry a status code (e.g. HTTP status code) as its
          ``status_code`` property.
          Any underlying exception must be linked via the `__cause__`
          property (e.g. `raise UrlOperationsRemoteError(...) from ...`).
        UrlOperationsInteractionError
        UrlOperationsAuthenticationError
        UrlOperationsAuthorizationError
        UrlOperationsResourceUnknown
          Implementations that can distinguish several remote error types
          beyond indication a general ``UrlOperationsRemoteError``:
          ``UrlOperationsInteractionError`` general issues in communicating
          with the remote side; ``UrlOperationsAuthenticationError`` for
          errors related to (failed) authentication at the remote;
          ``UrlOperationsAuthorizationError`` for (lack of) authorizating
          to access a particular resource of perform a particular operation;
          ``UrlOperationsResourceUnknown`` if the target of an operation
          does not exist.
        TimeoutError
          If `timeout` is given and the operation does not complete within the
          number of seconds that a specified by `timeout`.
        """
        raise NotImplementedError

    def delete(self,
               url: str,
               *,
               credential: str | None = None,
               timeout: float | None = None) -> Dict:
        """Delete a resource identified by a URL

        Parameters
        ----------
        url: str
          Valid URL with any scheme supported by a particular implementation.
        credential: str, optional
          The name of a dedicated credential to be used for authentication
          in order to perform the deletion. Particular implementations may
          or may not require or support authentication. They also may or
          may not support automatic credential lookup.
        timeout: float, optional
          If given, specifies a timeout in seconds. If the operation is not
          completed within this time, it will raise a `TimeoutError`-exception.
          If timeout is None, the operation will never timeout.

        Returns
        -------
        dict
          A mapping of property names to values for the deletion.

        Raises
        ------
        UrlOperationsRemoteError
          This exception is raised on any deletion-related error on the remote
          side, with a summary of the underlying issues as its message.
          It may carry a status code (e.g. HTTP status code) as its
          ``status_code`` property.
          Any underlying exception must be linked via the `__cause__`
          property (e.g. `raise UrlOperationsRemoteError(...) from ...`).
        UrlOperationsInteractionError
        UrlOperationsAuthenticationError
        UrlOperationsAuthorizationError
        UrlOperationsResourceUnknown
          Implementations that can distinguish several remote error types
          beyond indication a general ``UrlOperationsRemoteError``:
          ``UrlOperationsInteractionError`` general issues in communicating
          with the remote side; ``UrlOperationsAuthenticationError`` for
          errors related to (failed) authentication at the remote;
          ``UrlOperationsAuthorizationError`` for (lack of) authorizating
          to access a particular resource of perform a particular operation;
          ``UrlOperationsResourceUnknown`` if the target of an operation
          does not exist.
        TimeoutError
          If `timeout` is given and the operation does not complete within the
          number of seconds that a specified by `timeout`.
        """
        raise NotImplementedError


    def _get_progress_id(self, from_id: str, to_id: str):
        return f'progress_transport_{from_id}_{to_id}'

    def _progress_report_start(self,
                               pid: str,
                               log_msg: tuple,
                               label: str,
                               expected_size: int | None):
        log_progress(
            lgr.info,
            pid,
            *log_msg,
            unit=' Bytes',
            label=label,
            total=expected_size,
            noninteractive_level=logging.DEBUG,
        )

    def _progress_report_update(self, pid: str, log_msg: tuple, increment: int):
        log_progress(
            lgr.info, pid, *log_msg,
            update=increment,
            increment=True,
            noninteractive_level=logging.DEBUG,
        )

    def _progress_report_stop(self, pid: str, log_msg: tuple):
        log_progress(
            lgr.info, pid, *log_msg,
            noninteractive_level=logging.DEBUG,
        )

    def _get_hasher(self, hash: list[str] | None) -> list[callable]:
        if not hash:
            return []

        import hashlib
        # yes, this will crash, if an invalid hash algorithm name
        # is given
        _hasher = []
        for h in hash:
            hr = getattr(hashlib, h.lower(), None)
            if hr is None:
                raise ValueError(f'unsupported hash algorithm {h}')
            _hasher.append(hr())
        return _hasher

    def _get_hash_report(self,
                         hash_names: list[str] | None,
                         hashers: list) -> Dict:
        if not hash_names:
            return {}
        else:
            return dict(zip(hash_names, [h.hexdigest() for h in hashers]))

#
# Exceptions to be used by all handlers
#

class UrlOperationsRemoteError(Exception):
    def __init__(self, url, message=None, status_code: Any = None):
        # use base exception feature to store all arguments in a tuple
        # and have named properties to access them
        super().__init__(
            url,
            message,
            status_code,
        )

    @property
    def url(self):
        return self.args[0]

    @property
    def message(self):
        return self.args[1]

    @property
    def status_code(self):
        return self.args[2]


class UrlOperationsResourceUnknown(UrlOperationsRemoteError):
    """A connection request succeeded in principle, but target was not found

    Equivalent of an HTTP404 response.
    """
    pass


class UrlOperationsInteractionError(UrlOperationsRemoteError):
    pass


class UrlOperationsAuthenticationError(UrlOperationsInteractionError):
    def __init__(self,
                 url: str,
                 credential: dict | None = None,
                 message: str | None = None,
                 status_code: Any = None):
        super().__init__(url, message=message, status_code=status_code)
        self.credential = credential


class UrlOperationsAuthorizationError(UrlOperationsRemoteError):
    def __init__(self,
                 url: str,
                 credential: dict | None = None,
                 message: str | None = None,
                 status_code: Any = None):
        super().__init__(url, message=message, status_code=status_code)
        self.credential = credential
