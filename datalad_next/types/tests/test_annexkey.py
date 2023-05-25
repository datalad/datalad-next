import pytest

from ..annexkey import AnnexKey


def test_annexkey():
    for key in (
        'MD5E-s792207360--985e680a221e47db05063a12b91d7d89.tar',
        'SHA256E-s31390--f50d7ac4c6b9031379986bc362fcefb65f1e52621ce1708d537e740fefc59cc0.mp3',
        'URL-s1899248--http&c%%ai.stanford.edu%,126nilsson%MLBOOK.pdf/URL-s1899248--http&c%%ai.stanford.edu%,126nilsson%MLBOOK.pdf',
    ):
        # round-tripping for any key must give same outcome
        assert key == str(AnnexKey.from_str(key))

    # check that it can be used as a dict-key, i.e. is hashable
    key = AnnexKey.from_str('MD5-s9--985e680a221e47db05063a12b91d7d89')
    d = {key: 'some'}


def test_annexkey_errors():
    for wrong in (
        'MD5E-985e680a221e47db05063a12b91d7d89.tar',
        'MD5E-SUPRISE--985e680a221e47db05063a12b91d7d89.tar',
    ):
        with pytest.raises(ValueError):
            AnnexKey.from_str(wrong)
