from ..annexkey import AnnexKey
from ..archivist import ArchivistLocator
from ..enums import ArchiveType


def test_archivistlocator():
    test_locator = \
        'dl+archive:MD5-s389--e9f624eb778e6f945771c543b6e9c7b2#path=dir/file.csv&size=234&atype=tar'

    al = ArchivistLocator.from_str(test_locator)

    assert al.akey == AnnexKey.from_str(
        'MD5-s389--e9f624eb778e6f945771c543b6e9c7b2')
    assert al.atype == ArchiveType.tar

    # round trip
    assert str(al) == test_locator
