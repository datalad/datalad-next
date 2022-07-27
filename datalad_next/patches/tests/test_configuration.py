from datalad.tests.utils_pytest import (
    assert_in_results,
    assert_raises,
    chpwd,
    with_tempfile,
)
from datalad.api import (
    Dataset,
    configuration,
)
from datalad.support.exceptions import IncompleteResultsError

# run all -core tests
from datalad.local.tests.test_configuration import *

ckwa = dict(
    result_renderer='disabled',
)


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


@with_tempfile(mkdir=True)
def test_getset_None(path=None):
    # enter a tempdir to be confident that there is no dataset around
    with chpwd(path):
        # set an empty string, this is not the same as `None`
        configuration('set', 'some.item=', scope='global', **ckwa)
        assert_in_results(
            configuration('get', 'some.item', **ckwa),
            value='',
        )
        # an unset config item is equivalent to `None`
        configuration('unset', 'some.item', scope='global', **ckwa),
        # retrieving an unset item triggers an exception ...
        assert_raises(
            IncompleteResultsError,
            configuration, 'get', 'some.item', **ckwa)
        # ... because the status of the respective result is "impossible"
        assert_in_results(
            configuration('get', 'some.item',
                          on_failure='ignore', **ckwa),
            value=None,
            status='impossible',
        )
