import pytest

from datalad.api import next_status

from datalad_next.constraints import (
    CommandParametrizationError,
    ParameterConstraintContext,
)
from datalad_next.utils import chpwd

from ..status import (
    opt_eval_subdataset_state_values,
    opt_recursive_values,
    opt_untracked_values,
)


def test_status_invalid(tmp_path, datalad_cfg):
    # we want exhaustive parameter validation (i.e., continue after
    # first failure), saves some code here
    datalad_cfg.set('datalad.runtime.parameter-violation',
                    'raise-at-end',
                    scope='global')
    with chpwd(tmp_path):
        with pytest.raises(CommandParametrizationError) as e:
            next_status(
                untracked='weird',
                recursive='upsidedown',
                eval_subdataset_state='moonphase',
            )
        errors = e.value.errors
        assert 'no dataset found' in \
            errors[ParameterConstraintContext(('dataset',))].msg.casefold()
        for opt in ('untracked', 'recursive', 'eval_subdataset_state'):
            assert 'is not one of' in \
                errors[ParameterConstraintContext((opt,))].msg.casefold()


def test_status_renderer_smoke(existing_dataset):
    ds = existing_dataset
    assert ds.next_status() == []
    (ds.pathobj / 'untracked').touch()
    st = ds.next_status()
    assert len(st) == 1


def test_status_clean(existing_dataset, no_result_rendering):
    ds = existing_dataset
    ds.create('subds')
    for recmode in opt_recursive_values:
        assert [] == ds.next_status(recursive=recmode)
    for untracked in opt_untracked_values:
        assert [] == ds.next_status(untracked=untracked)
    for eval_sm in opt_eval_subdataset_state_values:
        assert [] == ds.next_status(eval_subdataset_state=eval_sm)
