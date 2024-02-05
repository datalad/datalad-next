import pytest

from datalad_next.exceptions import IncompleteResultsError
from datalad_next.tests import assert_result_count


def test_substitution_config_default(existing_dataset, no_result_rendering):
    ds = existing_dataset

    if ds.config.get('datalad.run.substitutions.python') is not None:
        # we want to test default handling when no config is set
        pytest.skip(
            'Test assumptions conflict with effective configuration')

    # the {python} placeholder is not explicitly defined, but it has
    # a default, which run() should discover and use
    res = ds.run('{python} -c "True"')
    assert_result_count(res, 1, action='run', status='ok')

    # make sure we could actually detect breakage with the check above
    with pytest.raises(IncompleteResultsError):
        ds.run('{python} -c "breakage"')
