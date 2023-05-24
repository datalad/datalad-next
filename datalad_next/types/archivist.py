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
    def from_str(cls, url):
        url_matched = _recognized_urls.match(url)
        if not url_matched:
            raise ValueError('Unrecognized dl+archives URL syntax')
        url_matched = url_matched.groupdict()
        # we must have a key, and we must have at least a path property
        # pointing to an archive member
        if any(p not in url_matched for p in ('key', 'props')):
            raise ValueError('Unrecognized dl+archives URL syntax')
        # convert to desired type
        akey = AnnexKey.from_str(url_matched['key'])

        # archive member properties
        props_matched = _archive_member_props.match(url_matched['props'])
        if not props_matched:
            # without at least a 'path' there is nothing we can do here
            raise ValueError(
                'dl+archives URL contains invalid archive member '
                f'specification: {url_matched["props"]!r}')
        props_matched = props_matched.groupdict()
        try:
            amember_path = PurePosixPath(props_matched['path'])
        except Exception as e:
            raise ValueError(
                'dl+archives URL contains invalid archive member path'
            ) from e

        atype = dict(
            tar=ArchiveType.tar,
            zip=ArchiveType.zip).get(
                props_matched.get('atype')
        )

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
            size=props_matched['size'],
            atype=atype,
        )
