"""Handler for interfacing with FSSPEC for URL operations"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from pathlib import (
    Path,
    PurePosixPath,
)
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


def get_fs_generic(url, target_url, *, cfg, credential, **kwargs):
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
    **kwargs:
      Will be passed to ``fsspec.core.url_to_fs()``

    Returns
    -------
    (Filesystem, str, dict)
      3-tuple with 1) the filesystem instance to access ``url``; 2) a
      location identifier (potentially different from ``url``) that
      matches the location of ``url`` within the context of the returned
      filesystem instance; and 3) a dict with information on the
      objects corresponding to the ``url``.
    """
    fs, urlpath = url_to_fs(url, **kwargs)
    try:
        stat = fs.stat(urlpath)
    except FileNotFoundError:
        # TODO this could happen on upload, but may be FS-specific
        # it could be that this needs to be a best-effort thing
        # and returning `stat != None` can be used as an indicator
        # for things working, but is not always present
        # TODO maybe add a switch to prevent stat'ing right away
        # to avoid wasting cycles when it is known that the target
        # does not exist
        stat = None
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

    S3 versioning specifics:
    Users can parametrize the ``fs_kwargs` dictionary with a boolean
    ``version_aware`` key to explicitly set whether versioned S3 URLs are
    parsed. It is not recommended to set this key, though: The underlying
    S3-specific libraries detect versioned URLs automatically and set the flag
    if necessary. If a user sets ``fs_kwargs={'version_aware': False}`` but
    supplies versioned URLs nevertheless, internal errors occur.

    """
    def __init__(self,
                 cfg=None,
                 block_size: int | None = None,
                 fs_kwargs: Dict | None = None,
    ):
        """
        Parameters
        ----------
        cfg: ConfigManager, optional
          A config manager instance that is consulted for any configuration
          filesystem configuration individual handlers may support.
        block_size: int, optional
          Number of bytes to process at once. This determines the chunk size
          for hashing, progress reporting and reading from FSSPEC file objects
          (when the underlying filesystem can report the total size to read).
          If not given, a platform-specific default is used. Depending
          on the type of caching used by FSSPEC, this parameter might have
          less impact that specifying a filesystem-specific readahead cache
          size, or similar parameters. Without caching, this parameter will
          more directly influence how data are transported.
        fs_kwargs: dict, optional
          Will be passed to ``fsspec.core.url_to_fs()`` as ``kwargs``.
        """
        super().__init__(cfg=cfg)
        self._fs_kwargs = fs_kwargs
        self._block_size = block_size

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
        return props

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
        dst_fp = None

        try:
            # we cannot always have an expected size
            expected_size = props.get('stat_size')
            dst_fp = sys.stdout.buffer if to_path is None \
                else open(to_path, 'wb')

            with fs.open(urlpath) as src_fp:
                # not every file abstraction supports all features
                # switch by capabilities
                props.update(self._copyfp(
                    src_fp,
                    dst_fp,
                    expected_size,
                    hash,
                    start_log=('Download %s to %s', from_url, to_path),
                    update_log=('Downloaded chunk',),
                    finish_log=('Finished download',),
                    progress_label='downloading',
                ))
                return props
        finally:
            if dst_fp and to_path is not None:
                dst_fp.close()

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
        props = {}
        if from_path:
            expected_size = from_path.stat().st_size
            props['content-length'] = expected_size
        else:
            expected_size = None

        fs, urlpath, target_stat_ = self._get_fs(to_url, credential=credential)
        # TODO target_stat would be None for a non-existing target (ok here)
        # but if it is not None, we might want to consider being vocal about
        # that
        src_fp = None
        dst_fp = None

        try:
            src_fp = sys.stdin.buffer if from_path is None \
                else open(from_path, 'rb')

            try:
                dst_fp = fs.open(urlpath, 'wb')
            except FileNotFoundError:
                # TODO other supported FS might have different ways of saying
                # "I need a parent to exist first"
                fs.mkdir(str(PurePosixPath(urlpath).parent),
                         create_parents=True)
                dst_fp = fs.open(urlpath, 'wb')

            # not every file abstraction supports all features
            # switch by capabilities
            props.update(self._copyfp(
                src_fp,
                dst_fp,
                expected_size,
                hash,
                start_log=('Upload %s to %s', from_path, to_url),
                update_log=('Uploaded chunk',),
                finish_log=('Finished upload',),
                progress_label='uploading',
            ))
            return props
        finally:
            if src_fp and from_path is not None:
                src_fp.close()
            if dst_fp is not None:
                dst_fp.close()

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
        fs, urlpath, props = self._get_fs(url, credential=credential)
        fs.rm_file(urlpath)
        return props

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

        fs, urlpath, props = get_fs(
            url,
            target_url,
            cfg=self.cfg,
            credential=credential,
            **(self._fs_kwargs or {})
        )
        if props is not None:
            # if we get here, access is working, normalize the stat properties
            props = self._stat2resultprops(props)
        return fs, urlpath, props

    def _stat2resultprops(self, props: Dict) -> Dict:
        props = {
            f'stat_{k}': v for k, v in props.items()
        }
        # normalize size property to what is expected across UrlOperations
        # implementations, but keep original too for internal consistency
        if 'stat_size' in props:
            props['content-length'] = props['stat_size']
        return props

    def _copyfp(self,
                src_fp,
                dst_fp,
                expected_size: int,
                hash: list[str] | None,
                start_log: tuple,
                update_log: tuple,
                finish_log: tuple,
                progress_label: str) -> dict:
        # this is pretty much shutil.copyfileobj() with the necessary
        # wrapping to perform hashing and progress reporting
        hasher = self._get_hasher(hash)
        progress_id = self._get_progress_id(id(src_fp), id(src_fp))

        # Localize variable access to minimize overhead
        src_fp_read = src_fp.read
        dst_fp_write = dst_fp.write

        props = {}
        self._progress_report_start(
            progress_id, start_log, progress_label, expected_size)
        copy_size = 0
        try:
            # not every file abstraction supports all features
            # switch by capabilities
            if expected_size is not None:
                # read chunks until target size, if we know the size
                # (e.g. the Tar filesystem would simply read beyond
                # file boundaries otherwise.
                # but this method can be substantially faster than
                # alternative methods
                self._copy_chunks_to_maxsize(
                    src_fp_read, dst_fp_write, expected_size,
                    hasher, progress_id, update_log)
            elif hasattr(src_fp, '__next__'):
                # iterate full-auto if we can
                self._copy_via_iterable(
                    src_fp, dst_fp_write,
                    hasher, progress_id, update_log)
            else:
                # this needs a fallback that simply calls read()
                raise NotImplementedError(
                    f"No reader for FSSPEC implementation {src_fp}")
            props.update(self._get_hash_report(hash, hasher))
            # return how much was copied. we could compare with
            # `expected_size` and error on mismatch, but not all
            # sources can provide that (e.g. stdin)
            props['content-length'] = copy_size
            return props
        finally:
            self._progress_report_stop(progress_id, finish_log)

    def _copy_via_iterable(self, src_fp, dst_fp_write, hasher,
                           progress_id, update_log):
        """Copy from a file object that supports iteration"""
        for chunk in src_fp:
            # write data
            dst_fp_write(chunk)
            # compute hash simultaneously
            for h in hasher:
                h.update(chunk)
            self._progress_report_update(
                progress_id, update_log, len(chunk))

    def _copy_chunks_to_maxsize(self, src_fp_read, dst_fp_write, size_to_copy,
                                hasher, progress_id, update_log):
        """Download from a file object that does not support iteration"""
        # use a specific block size, if one was set and go with a
        # platform default if not.
        # this is also the granularity with which progress reporting
        # is made.
        block_size = self._block_size or COPY_BUFSIZE
        while True:
            # make sure to not read beyond the target size
            # some archive filesystem abstractions do not necessarily
            # stop at the end of an archive member otherwise
            chunk = src_fp_read(min(block_size, size_to_copy))
            if not chunk:
                break
            # write data
            dst_fp_write(chunk)
            chunk_size = len(chunk)
            # compute hash simultaneously
            for h in hasher:
                h.update(chunk)
            self._progress_report_update(
                progress_id, update_log, chunk_size)
            size_to_copy -= chunk_size
