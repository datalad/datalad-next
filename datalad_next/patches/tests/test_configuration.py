from datalad.tests.utils_pytest import (
    assert_in_results,
    chpwd,
    with_tempfile,
)
from datalad.api import (
    Dataset,
    configuration,
)

# run all -core tests
from datalad.local.tests.test_configuration import *


@with_tempfile(mkdir=True)
def test_config_get_global(path=None):
    """Make sure `get` does not require a dataset to be present"""
    # enter a tempdir to be confident that there is no dataset around
    with chpwd(path):
        res = configuration('get', 'user.name', result_renderer='disabled')
        assert_in_results(
            res,
            name='user.name',
            status='ok',
        )
    # verify that the dataset method was replaced too
    ds = Dataset(path).create()
    assert "'get' action can be constrained" in ds.configuration.__doc__
