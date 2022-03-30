
def test_register():
    import datalad.api as da
    assert hasattr(da, 'credentials')
