import pytest

from ..multihash import (
    MultiHash,
    NoOpHash,
)


def test_multihash():
    mh = MultiHash(['sha1', 'MD5'])
    mh.update(b'')
    hd = mh.get_hexdigest()
    assert len(hd) == 2
    # algorithm label preserves original casing
    assert hd['MD5'] == 'd41d8cd98f00b204e9800998ecf8427e'
    assert hd['sha1'] == 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

    with pytest.raises(ValueError):
        MultiHash(['bogus'])



def test_noophash():
    mh = NoOpHash()
    mh.update(b'')
    assert mh.get_hexdigest() == {}
