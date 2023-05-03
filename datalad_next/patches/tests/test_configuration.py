from datalad_next.tests.utils import (
    assert_in_results,
    assert_raises,
    chpwd,
)
from datalad.api import configuration
from datalad_next.exceptions import IncompleteResultsError

# run all -core tests
from datalad.local.tests.test_configuration import *

ckwa = dict(
    result_renderer='disabled',
)


def test_config_get_global(existing_dataset, tmp_path):
    """Make sure `get` does not require a dataset to be present"""
    # enter a tempdir to be confident that there is no dataset around
    with chpwd(str(tmp_path)):
        res = configuration('get', 'user.name', result_renderer='disabled')
        assert_in_results(
            res,
            name='user.name',
            status='ok',
        )
    # verify that the dataset method was replaced too
    assert "'get' action can be constrained" \
        in existing_dataset.configuration.__doc__


def test_getset_None(tmp_path):
    # enter a tempdir to be confident that there is no dataset around
    with chpwd(str(tmp_path)):
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
