from ..annexkey import AnnexKey


def test_annexkey():
    for key in (
        'MD5E-s792207360--985e680a221e47db05063a12b91d7d89.tar',
    ):
        # round-tripping for any key must give same outcome
        assert key == str(AnnexKey.from_str(key))
