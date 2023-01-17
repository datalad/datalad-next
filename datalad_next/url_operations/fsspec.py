"""Handler for interfacing with FSSPEC for URL operations"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from pathlib import Path
import sys
from typing import (
    Dict,
    Tuple,
)

from fsspec.core import (
    _un_chain,
    split_protocol,
    url_to_fs,
)

from datalad_next.utils.consts import COPY_BUFSIZE

from . import (
    UrlOperations,
)

lgr = logging.getLogger('datalad.ext.next.url_operations.fsspec')


def get_fs_generic(url, target_url, *, cfg, credential):
    """Helper to return an FSSPEC filesystem instance, if there is no better

    It performs no credential provisioning or customizations whatsoever.
    Alternative implementations of this interface could support particular
    protocol and means of authentication.

    Parameters
    ----------
    url: str
      The URL to access via a filesystem abstraction
    target_url: str
      If ``url`` is a chained URL, this URL points to the target containing
      the filesystem with the component to-be-accessed by ``url``.
      If ``url`` is not chained, this must be the same as ``url``.
    cfg: ConfigManager
      Instance to query for any configuration items
    credential: str
      Name of a credential to be used for authentication (if needed)

    Returns
    -------
    (Filesystem, str, dict)
      3-tuple with 1) the filesystem instance to access ``url``; 2) a
      location identifier (potentially different from ``url``) that
      matches the location of ``url`` within the context of the returned
      filesystem instance; and 3) a dict with information on the
      objects corresponding to the ``url``.
    """
    fs, urlpath = url_to_fs(url)
    stat = fs.stat(urlpath)
    return fs, urlpath, stat


def get_target_url(url: str) -> str:
    """Helper to determine the effective target from a chained URL

    The assumption is that in any chained URL the last component is actually
    pointing to the (remote) target.

    For any other (none-chained) URL, the full URL is returned as-is.

    Parameters
    ----------
    url: str
      URL to parse, and possible unchain.

    Returns
    -------
    str
      The target component of a chained URL or the full URL.
    """
    chain = _un_chain(url, {})
    if chain:
        # determine the effective transport. we assume that it is the
        # last one in any URL chain
        url, proto, _ = chain[-1]
        return f'{proto}://{url}'
    else:
        return url


class FsspecUrlOperations(UrlOperations):
    """URL IO using FSSPEC file system implementations

    The ``fsspec`` (https://github.com/fsspec/filesystem_spec) package provide
    a file system abstraction and implementation that uniform particular
    operations across several protocol and services. This handler maps this
    abstraction onto the ``UrlOperations`` interface.

    At present, only the `sniff()` and `download()` operations are implemented.

    ``fsspec`` provides a wide range of possibilities, not just different
    protocol support, but also transparent archive content access, or
    file-level and block-level caching. These features can be flexibly combined
    using "URL chaining". Here are a few examples of supported scenarios:

    A file on S3 storage::

      s3://mybucket/myfile.txt

    A file on an SSH-accessible server::

      sftp://host.example.net/home/user/file.txt

    A file in a particular version of a GitHub project::

      github://datalad:datalad@0.18.0/requirements-devel.txt

    A file in a TAR/ZIP archive::

      tar://somedir/somefile.txt::s3://mybucket/myarchive.tar.gz
      zip://somedir/somefile.txt::s3://mybucket/myarchive.zip

    The last example shows URL-chaining. Using this approach ``fsspec``
    can also be extracted to employ caching. The following URL will
    cause the containing archive to be downloaded first (and cached
    for the runtime of the process), and the target file (and any subsequent
    ones) to be extract from the local copy in the cache::

      tar://somedir/somefile.txt::filecache::s3://mybucket/myarchive.tar.gz

    This URL handler can also determine and provision DataLad-based
    credentials for ``fsspec``. At present this is implemented for `s3://`
    type URLs. Credentials are auto-determined, if possible, based on the
    accessed location (in the case of s3 this is a bucket), and can be
    entered manually if none can be found.
    """
    def sniff(self,
              url: str,
              *,
              credential: str | None = None,
              timeout: float | None = None) -> Dict:
        """Gather information on a URL target, without downloading it

        See :meth:`datalad_next.url_operations.UrlOperations.sniff`
        for parameter documentation and exception behavior.
        """
        _, _, props = self._get_fs(url, credential=credential)
        return self._stat2resultprops(props)

    def download(self,
                 from_url: str,
                 to_path: Path | None,
                 *,
                 credential: str | None = None,
                 hash: str | None = None,
                 timeout: float | None = None) -> Dict:
        """Download a file

        See :meth:`datalad_next.url_operations.UrlOperations.download`
        for parameter documentation and exception behavior.

        The ``timeout`` parameter is ignored by this implementation.
        """
        fs, urlpath, props = self._get_fs(from_url, credential=credential)

        # if we get here, access is working
        props = self._stat2resultprops(props)

        # this is pretty much shutil.copyfileobj() with the necessary
        # wrapping to perform hashing and progress reporting
        hasher = self._get_hasher(hash)
        progress_id = self._get_progress_id(from_url, to_path)

        dst_fp = None

        try:
            # we cannot always have an expected size
            expected_size = props.get('stat_size')
            dst_fp = sys.stdout.buffer if to_path is None \
                else open(to_path, 'wb')
            # download can start
            self._progress_report_start(
                progress_id,
                ('Download %s to %s', from_url, to_path),
                'downloading',
                expected_size,
            )
            with fs.open(urlpath) as src_fp:
                # not every file abstraction supports all features
                # switch by capabilities
                if hasattr(src_fp, '__next__'):
                    # iterate full-auto if we can
                    self._download_via_iterable(
                        src_fp, dst_fp,
                        hasher, progress_id)
                elif expected_size is not None:
                    # read chunks until target size, if we know the size
                    # (e.g. the Tar filesystem would simply read beyond
                    # file boundaries otherwise
                    self._download_chunks_to_maxsize(
                        src_fp, dst_fp, expected_size,
                        hasher, progress_id)
                else:
                    # this needs a fallback that simply calls read()
                    raise NotImplementedError(
                        f"No reader for FSSPEC implementation {fs}")

            props.update(self._get_hash_report(hash, hasher))
            return props
        finally:
            if dst_fp and to_path is not None:
                dst_fp.close()
            self._progress_report_stop(progress_id, ('Finished download',))

    def upload(self,
               from_path: Path | None,
               to_url: str,
               *,
               credential: str | None = None,
               hash: list[str] | None = None,
               timeout: float | None = None) -> Dict:
        """Upload a file

        See :meth:`datalad_next.url_operations.UrlOperations.upload`
        for parameter documentation and exception behavior.

        The ``timeout`` parameter is ignored by this implementation.
        """
        raise NotImplementedError

    def delete(self,
               url: str,
               *,
               credential: str | None = None,
               timeout: float | None = None) -> Dict:
        """Delete a resource identified by a URL

        See :meth:`datalad_next.url_operations.UrlOperations.delete`
        for parameter documentation and exception behavior.

        The ``timeout`` parameter is ignored by this implementation.
        """
        raise NotImplementedError

    def _get_fs(self, url, *, credential) -> Tuple:
        """Helper to get a FSSPEC filesystem instance from a URL

        In order to ensure proper functioning (i.e. a valid authentication
        a ``stat()`` is performed on the URL target as a test. The outcome
        of this ``stat()`` is returned (whatever a particular filesystem
        will report) in order to facilitate efficient reuse.

        Returns
        -------
        (fsspec.spec.AbstractFileSystem, str, dict)
          Three items are returned in a tuple: 1) the filesystem instance,
          2) a URL or path string applicable in the context of that filesystem
          instance. This may or may not be the same as the original ``url``
          parameter. For example, with URL-chaining this might become an
          address within an archive, rather than the original chain. 3) a dict
          with all properties reported by the internal ``stat()`` call.
        """
        # NOTE this is an instance method to be (potentially) able to skip
        # repeated FS object creation via some kind of cache.
        # However, it is presently unclear what an optimal cache retrieval
        # condition would be (considering the many possibilities with
        # chained URLs)
        # TODO maybe cache `fs` under a `target_url` key
        # if that URL is not identical to `url`
        # TODO or maybe even better: let a `get_fs()` itself return a
        # key for caching, and provide that cache to any `get_fs()`.
        # This would expose the logic what makes sense to cache and
        # in the domain where such decisions can be made best
        # (protocol/service specific)
        # TODO with archive filesystems, even a re-used FS from a cache
        # would required the generation and use of `urlpath` (returned
        # by ``get_fs()`` to stat the target. We could simply support that
        # for a few scenarios (zip, tar)
        target_url = get_target_url(url)
        fstype, _ = split_protocol(target_url)

        fstype = fstype.lower()

        # defer to some more specialized implementations if we can
        if fstype == 's3':
            from .fsspec_s3 import get_fs
        else:
            get_fs = get_fs_generic

        return get_fs(
            url,
            target_url,
            cfg=self.cfg,
            credential=credential,
        )

    def _stat2resultprops(self, props: Dict) -> Dict:
        props = {
            f'stat_{k}': v for k, v in props.items()
        }
        # normalize size property to what is expected across UrlOperations
        # implementations, but keep original too for internal consistency
        if 'stat_size' in props:
            props['content-length'] = props['stat_size']
        return props

    def _download_via_iterable(self, src_fp, dst_fp, hasher, progress_id):
        """Download from a file object that supports iteration"""
        # Localize variable access to minimize overhead
        dst_fp_write = dst_fp.write
        for chunk in src_fp:
            # write data
            dst_fp_write(chunk)
            # compute hash simultaneously
            for h in hasher:
                h.update(chunk)
            self._progress_report_update(
                progress_id, ('Downloaded chunk',), len(chunk))

    def _download_chunks_to_maxsize(self, src_fp, dst_fp, size_to_copy,
                                    hasher, progress_id):
        """Download from a file object that does not support iteration"""
        # Localize variable access to minimize overhead
        src_fp_read = src_fp.read
        dst_fp_write = dst_fp.write
        while True:
            # make sure to not read beyond the target size
            # some archive filesystem abstractions do not necessarily
            # stop at the end of an archive member otherwise
            chunk = src_fp_read(min(COPY_BUFSIZE, size_to_copy))
            if not chunk:
                break
            # write data
            dst_fp_write(chunk)
            chunk_size = len(chunk)
            # compute hash simultaneously
            for h in hasher:
                h.update(chunk)
            self._progress_report_update(
                progress_id, ('Downloaded chunk',), chunk_size)
            size_to_copy -= chunk_size
