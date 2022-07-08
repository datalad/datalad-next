try:
    from datalad.conftest import setup_package
except ImportError:
    # assume old datalad without pytest support introduced in
    # https://github.com/datalad/datalad/pull/6273
    import pytest
    from datalad import setup_package as _setup_package
    from datalad import teardown_package as _teardown_package


    @pytest.fixture(autouse=True, scope="session")
    def setup_package():
        _setup_package()
        yield
        _teardown_package()

