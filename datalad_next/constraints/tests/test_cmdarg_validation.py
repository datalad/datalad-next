import pytest

from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import patch
from uuid import UUID

from datalad_next.commands import (
    ValidatedInterface,
    Parameter,
    eval_results,
)
from datalad_next.utils import on_windows
from .. import (
    ConstraintError,
    EnsureGeneratorFromFileLike,
    EnsureInt,
    EnsureJSON,
    EnsureListOf,
    EnsureMapping,
    EnsurePath,
    EnsureRange,
    EnsureStr,
    EnsureURL,
    EnsureValue,
)
from ..base import (
    AnyOf,
    Constraint,
)
from ..dataset import EnsureDataset
from ..parameter import EnsureCommandParameterization
from ..exceptions import ParameterConstraintContext


class EnsureAllUnique(Constraint):
    def __call__(self, value):
        if len(set(value)) < len(value):
            self.raise_for(value, 'not all values are unique')
        return value


class BasicCmdValidator(EnsureCommandParameterization):
    url_constraint = EnsureURL(required=['scheme'])
    url2path_constraint = EnsureMapping(
        key=url_constraint, value=EnsurePath(),
        delimiter='\t'
    )
    spec_item_constraint = url2path_constraint | url_constraint \
        | (EnsureJSON() & url2path_constraint)

    spec_constraint = AnyOf(
        EnsureListOf(spec_item_constraint),
        EnsureGeneratorFromFileLike(spec_item_constraint),
        spec_item_constraint,
    )

    def __init__(self, **kwargs):
        # this is the key bit: a mapping of parameter names to validators
        super().__init__(
            dict(
                spec=self.spec_constraint,
                p1=EnsureInt() | EnsureStr(),
            ),
            **kwargs
        )


class SophisticatedCmdValidator(BasicCmdValidator):
    def _check_unique_values(self, **kwargs):
        try:
            EnsureAllUnique()(kwargs.values())
        except ConstraintError as e:
            self.raise_for(
                kwargs,
                e.msg,
            )

    def _check_sum_range(self, p1, p2):
        try:
            EnsureRange(min=3)(p1 + p2)
        except ConstraintError:
            self.raise_for(
                dict(p1=p1, p2=p2),
                "it's too small"
            )

    def _limit_sum_range(self, p1, p2):
        # random example of a joint constraint that modifies the parameter
        # set it is given
        return dict(p1=p1, p2=min(p2, 100 - p1 - p2))

    def __init__(self):
        # this is the key bit: a mapping of parameter names to validators
        super().__init__(
            # implementation example of a higher-order constraint
            joint_constraints={
                ParameterConstraintContext(('p1', 'p2'), 'identity'):
                    self._check_unique_values,
                ParameterConstraintContext(('p1', 'p2'), 'sum'):
                    self._check_sum_range,
                ParameterConstraintContext(('p1', 'p2'), 'sum-limit'):
                    self._limit_sum_range,
            },
        )


class BrokenJointValidation(SophisticatedCmdValidator):
    def joint_validation(self, params, on_error):
        res = super().joint_validation(params, on_error)
        # remove any report, and it should trigger a RuntimeError on return
        res.popitem()
        return res


class CmdWithValidation(ValidatedInterface):
    # this is of little relevance, no validation configured here
    _params_ = dict(spec=Parameter(args=('spec',), nargs='+'))

    _validator_ = BasicCmdValidator()

    # command implementation that only validates and returns the outcome
    @staticmethod
    @eval_results
    def __call__(spec, p1='one', p2='two'):
        yield dict(
            action='cmd_with_validation',
            # list() consumes any potential generator
            spec=list(spec),
            status='ok',
        )


def test_multi_validation():
    val = BasicCmdValidator()
    # we break the parameter specification, and get a ConstraintError
    with pytest.raises(ConstraintError) as e:
        val(dict(spec='5'))
    # but actually, it is a ConstraintErrors instance, and we get the
    # violation exceptions within the context in which they occurred.
    # here this is a parameter name
    errors = e.value.errors
    assert len(errors) == 1
    ctx = ParameterConstraintContext(('spec',))
    assert ctx in errors
    assert errors[ctx].constraint == BasicCmdValidator.spec_constraint
    assert errors[ctx].value == '5'

    # now we trigger a higher-order error, and receive multiple reports
    val = SophisticatedCmdValidator()
    # but first a quick check if it behaves will with valid input
    valid_input = dict(spec='http://example.com', p1=1, p2=2)
    assert val(valid_input) == valid_input
    with pytest.raises(ConstraintError) as e:
        val(dict(spec='5', p1=1, p2=1), on_error='raise-at-end')
    errors = e.value.errors
    assert len(errors) == 3
    # the spec-param-only error
    assert errors.messages[0].startswith('does not match any of')
    # higher-order issue traces (their order is deterministic)
    assert 'not all values are unique' == errors.messages[1]
    assert 'p1, p2 (identity)' == errors.context_labels[1]
    assert 'p1, p2 (sum)' in errors.context_labels
    # and now against, but with stop-on-first-error
    with pytest.raises(ConstraintError) as e:
        val(dict(spec='5', p1=1, p2=1), on_error='raise-early')
    errors = e.value.errors
    # and we only get one!
    assert len(errors) == 1
    # the spec-param-only error
    assert errors.messages[0].startswith('does not match any of')
    assert 'not all values are unique' not in errors.messages
    # now we do it again, but with a valid spec, such that the first
    # and only error is a higher order error
    with pytest.raises(ConstraintError) as e:
        val(dict(spec=5, p1=1, p2=1), on_error='raise-early')
    errors = e.value.errors
    assert len(errors) == 1
    assert 'not all values are unique' == errors.messages[0]
    assert 'p1, p2 (identity)' == errors.context_labels[0]

    # a single-parameter validation error does not lead to a crash
    # in higher-order validation, instead the latter is performed
    # when a require argument could not be provided
    with pytest.raises(ConstraintError) as e:
        # p1 must be int|str
        val(dict(spec=5, p1=None, p2=1), on_error='raise-at-end')


def test_invalid_multi_validation():
    val = BrokenJointValidation()
    # this works for the underlying validator, but BrokenJointValidation
    # butchers the result, which must be detected
    valid_input = dict(spec='http://example.com', p1=1, p2=2)
    with pytest.raises(RuntimeError):
        val(valid_input)


def test_cmd_with_validation():
    target_urls = ['http://example.com', 'file:///dev/null']
    target_url_path_maps = [
        {'http://example.com': Path('some/dir/file')},
        {'file:///dev/null': Path('/dev/null')},
    ]
    json_lines = '{"http://example.com":"some/dir/file"}\n' \
                 '{"file:///dev/null":"/dev/null"}'

    for input, target in (
        # perfect input
        (target_urls, target_urls),
        (target_url_path_maps, target_url_path_maps),
        # actual invput conversion
        ([{'http://example.com': 'some/dir/file'},
          {'file:///dev/null': '/dev/null'}],
         target_url_path_maps),
        # custom mapping syntax
        (['http://example.com\tsome/dir/file',
          'file:///dev/null\t/dev/null'],
         target_url_path_maps),
        # JSON lines
        (['{"http://example.com":"some/dir/file"}',
          '{"file:///dev/null":"/dev/null"}'],
         target_url_path_maps),
        # from file with JSON lines
        (StringIO(json_lines), target_url_path_maps),
    ):
        res = CmdWithValidation.__call__(
            input,
            return_type='item-or-list', result_renderer='disabled',
        )
        assert 'spec' in res
        assert res['spec'] == target

    # read from file
    if not on_windows:
        # on windows the write-rewind-test logic is not possible
        # (PermissionError) -- too lazy to implement a workaround
        with NamedTemporaryFile('w+') as f:
            f.write(json_lines)
            f.seek(0)
            res = CmdWithValidation.__call__(
                f.name,
                return_type='item-or-list', result_renderer='disabled',
            )
            assert res['spec'] == target_url_path_maps

    with patch("sys.stdin", StringIO(json_lines)):
        res = CmdWithValidation.__call__(
            '-',
            return_type='item-or-list', result_renderer='disabled',
        )
        assert res['spec'] == target_url_path_maps

    # and now something that fails
    # TODO error reporting should be standardized (likely) on an explicit
    # and dedicated exception type
    # https://github.com/datalad/datalad/issues/7167
    with pytest.raises(ValueError):
        CmdWithValidation.__call__(
            'unsupported',
            return_type='item-or-list', result_renderer='disabled',
        )

    # no call with a required argument missing
    with pytest.raises(ValueError) as e:
        CmdWithValidation.__call__()
    exc_rendering = str(e.value)
    # must label the issue correctly
    assert 'missing required argument' in exc_rendering
    # must identify the missing argument
    assert 'spec=<no value>' in exc_rendering


#
# test dataset tailoring
#

class EnsureUUID(Constraint):
    def __call__(self, value):
        return UUID(value)


class EnsureDatasetID(EnsureUUID):
    """Makes sure that something is a dataset ID (UUID), or the dataset UUID
    of a particular dataset when tailored"""
    def for_dataset(self, dsarg):
        return EnsureValue(UUID(dsarg.ds.id))


class DsTailoringValidator(EnsureCommandParameterization):
    def __init__(self, **kwargs):
        # this is the key bit: a mapping of parameter names to validators
        super().__init__(
            dict(
                dataset=EnsureDataset(),
                id=EnsureDatasetID(),
            ),
            **kwargs
        )


def test_constraint_dataset_tailoring(existing_dataset):
    proper_uuid = '152f4fc0-b444-11ed-a9cb-701ab88b716c'
    no_uuid =     '152f4fc0------11ed-a9cb-701ab88b716c'
    # no tailoring works as expected
    val = DsTailoringValidator()
    assert val(dict(id=proper_uuid)) == dict(id=UUID(proper_uuid))
    with pytest.raises(ValueError):
        val(dict(id=no_uuid))

    # adding a dataset to the mix does not change a thing re the uuid
    ds = existing_dataset
    res = val(dict(dataset=ds.pathobj, id=proper_uuid))
    assert res['id'] == UUID(proper_uuid)
    assert res['dataset'].ds == ds

    # and we can still break it
    with pytest.raises(ValueError):
        val(dict(dataset=ds.pathobj, id=no_uuid))

    # now with tailoring the UUID checking to a particular dataset.
    # it is enabled via parameter, because it is a use case specific
    # choice, not a mandate
    target_uuid = ds.id
    tailoring_val = DsTailoringValidator(
        tailor_for_dataset=dict(id='dataset'),
    )
    # no uuid is still an issue
    with pytest.raises(ValueError):
        tailoring_val(dict(dataset=ds.pathobj, id=no_uuid))
    # what was good enough above (any UUID), no longer is
    with pytest.raises(ValueError):
        tailoring_val(dict(dataset=ds.pathobj, id=proper_uuid))

    # only the actual dataset's UUID makes it past the gates
    res = val(dict(dataset=ds.pathobj, id=target_uuid))
    assert res['id'] == UUID(target_uuid)
    assert res['dataset'].ds == ds
    # the order in the kwargs does not matter
    assert val(dict(id=target_uuid, dataset=ds.pathobj))['id'] == \
        UUID(target_uuid)

    # but when no dataset is being provided (and the dataset-constraint
    # allows for that), no tailoring is performed
    assert tailoring_val(dict(id=proper_uuid))['id'] == UUID(proper_uuid)
    # but still no luck with invalid args
    with pytest.raises(ValueError):
        val(dict(dataset=ds.pathobj, id=no_uuid))
