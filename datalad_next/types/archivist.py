"""``dl+archive:`` archive member locator"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
import re

from .annexkey import AnnexKey
from .enums import ArchiveType


# be relatively permissive
_recognized_urls = re.compile(r'^dl\+archive:(?P<key>.*)#(?P<props>.*)')
# each archive member is identified by a (relative) path inside
# the archive.
_archive_member_props = re.compile(
    # a path may contain any char but '&'
    # TODO check that something in the machinery ensures proper
    # quoting
    'path=(?P<path>[^&]+)'
    # size info (in bytes) is optional
    '(&size=(?P<size>[0-9]+)|)'
    # archive type label is optional
    '(&atype=(?P<atype>[a-z0-9]+)|)'
)


@dataclass
class ArchivistLocator:
    """Representation of a ``dl+archive:`` archive member locator

    These locators are used by the ``datalad-archives`` and ``archivist``
    git-annex special remotes. They identify a member of a archive that is
    itself identified by an annex key.

    Each member is annotated with its size (in bytes). Optionally,
    the file format type of the archive can be annotated too.

    Syntax of ``dl+archives:`` locators
    -----------------------------------

    The locators the following minimal form::

        dl+archive:<archive-key>#path=<path-in-archive>

    where ``<archive-key>`` is a regular git-annex key of an archive file,
    and ``<path-in-archive>`` is a POSIX-style relative path pointing to
    a member within the archive.

    Two optional, additional attributes ``size`` and ``atype`` are recognized
    (only ``size`` is also understood by the ``datalad-archives``
    special remote).

    ``size`` declares the size of the (extracted) archive member in bytes::

        dl+archive:<archive-key>#path=<path-in-archive>&size=<size-in-bytes>

    ``atype`` declares the type of the containing archive using a label.
    Currently recognized labels are ``tar`` (a TAR archive, compressed or not),
    and ``zip`` (a ZIP archive). See
    :class:`~datalad_next.types.enums.ArchiveType` for all recognized labels.

    If no type information is given, :func:`ArchivistLocator.from_str()` will
    try to determine the archive type from the archive key (via ``*E``-type
    git-annex backends, such as DataLad's default ``MD5E``).

    The order in the fragment part of the URL (after ``#``) is significant.
    ``path`` must come first, followed by ``size`` or ``atype``. If both
    ``size`` and ``atype`` are present, ``size`` must be declared first. A
    complete example of a URL is::

        dl+archive:MD5-s389--e9f624eb778e6f945771c543b6e9c7b2#path=dir/file.csv&size=234&atype=tar
    """
    akey: AnnexKey
    member: PurePosixPath
    size: int
    # datalad-archives did not have the type info, we want to be
    # able to handle those too, make optional
    atype: ArchiveType | None = None

    def __str__(self) -> str:
        return 'dl+archive:{akey}#path={member}&size={size}{atype}'.format(
            akey=self.akey,
            # TODO needs quoting?
            member=self.member,
            size=self.size,
            atype=f'&atype={self.atype.value}' if self.atype else '',
        )

    @classmethod
    def from_str(cls, url: str):
        """Return ``ArchivistLocator`` from ``str`` form"""
        url_matched = _recognized_urls.match(url)
        if not url_matched:
            raise ValueError('Unrecognized dl+archives locator syntax')
        url_matched = url_matched.groupdict()
        # convert to desired type
        akey = AnnexKey.from_str(url_matched['key'])

        # archive member properties
        props_matched = _archive_member_props.match(url_matched['props'])
        if not props_matched:
            # without at least a 'path' there is nothing we can do here
            raise ValueError(
                'dl+archives locator contains invalid archive member '
                f'specification: {url_matched["props"]!r}')
        props_matched = props_matched.groupdict()
        amember_path = PurePosixPath(props_matched['path'])
        if amember_path.is_absolute():
            raise ValueError(
                'dl+archives locator contains absolute archive member path')
        if '..' in amember_path.parts:
            raise ValueError(
                'dl+archives locator archive member path contains ".."')

        # size is optional, regex ensure that it is an int
        size = props_matched.get('size')

        # archive type, could be None
        atype = props_matched.get('atype')
        if atype is not None:
            # if given, most be known type
            try:
                atype = getattr(ArchiveType, atype)
            except AttributeError as e:
                raise ValueError(
                    'dl+archives locator archive type unrecognized') from e

        if atype is None and akey.backend.endswith('E'):
            # try by key name extension
            suf = PurePosixPath(akey.name).suffixes
            if '.zip' == suf[-1]:
                atype = ArchiveType.zip
            elif '.tar' in suf:
                atype = ArchiveType.tar

        return cls(
            akey=akey,
            member=amember_path,
            size=size,
            atype=atype,
        )
