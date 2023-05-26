import pytest

from ..annexkey import AnnexKey
from ..archivist import ArchivistLocator
from ..enums import ArchiveType

some_key = 'MD5-s389--e9f624eb778e6f945771c543b6e9c7b2'


def test_archivistlocator():
    test_locator = \
        f'dl+archive:{some_key}#path=dir/file.csv&size=234&atype=tar'

    al = ArchivistLocator.from_str(test_locator)

    assert al.akey == AnnexKey.from_str(some_key)
    assert al.atype == ArchiveType.tar

    # round trip
    assert str(al) == test_locator

    # type determination from key
    assert ArchivistLocator.from_str(
        'dl+archive:MD5E-s1--e9f624eb778e6f945771c543b6e9c7b2.tar#path=f.txt'
    ).atype == ArchiveType.tar
    assert ArchivistLocator.from_str(
        'dl+archive:MD5E-s1--e9f624eb778e6f945771c543b6e9c7b2.zip#path=f.txt'
    ).atype == ArchiveType.zip


def test_archivistlocatori_errors():
    for wrong in (
        # no chance without prefix
        'bogus',
        # not just a prefix or some bogus properties
        'dl+archive:',
        'dl+archive:#',
        'dl+archive:keything',
        'dl+archive:#props',
        'dl+archive:keything#props',
        # a real key is required, but not sufficient
        f'dl+archive:{some_key}#props',
        # we require a member path, the rest is optional
        f'dl+archive:{some_key}#size=123',
        f'dl+archive:{some_key}#size=123&atype=tar',
        # must be a proper POSIX path, relative, no ..
        f'dl+archive:{some_key}#path=/dummy',
        f'dl+archive:{some_key}#path=../dd',
        # cannot work with unknown archive type
        f'dl+archive:{some_key}#path=good&size=123&atype=eh!',
    ):
        with pytest.raises(ValueError):
            ArchivistLocator.from_str(wrong)
