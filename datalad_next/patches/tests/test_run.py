import pytest

from datalad_next.exceptions import IncompleteResultsError
from datalad_next.tests.utils import (
    SkipTest,
    assert_result_count,
)


def test_substitution_config_default(existing_dataset):
    ds = existing_dataset

    if ds.config.get('datalad.run.substitutions.python') is not None:
        # we want to test default handling when no config is set
        raise SkipTest(
            'Test assumptions conflict with effective configuration')

    # the {python} placeholder is not explicitly defined, but it has
    # a default, which run() should discover and use
    res = ds.run('{python} -c "True"', result_renderer='disabled')
    assert_result_count(res, 1, action='run', status='ok')

    # make sure we could actually detect breakage with the check above
    with pytest.raises(IncompleteResultsError):
        ds.run('{python} -c "breakage"', result_renderer='disabled')
