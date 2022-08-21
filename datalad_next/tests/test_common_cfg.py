def test_annexretry():
    from datalad.interface.common_cfg import definitions
    assert definitions['datalad.annex.retry']['default'] == 1
