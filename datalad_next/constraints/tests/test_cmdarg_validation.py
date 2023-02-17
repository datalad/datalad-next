import pytest

from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import patch

from datalad_next.commands import (
    ValidatedInterface,
    Parameter,
    eval_results,
)
from datalad_next.utils import on_windows
from datalad_next.constraints import (
    ConstraintError,
    EnsureGeneratorFromFileLike,
    EnsureJSON,
    EnsureListOf,
    EnsureMapping,
    EnsurePath,
    EnsureRange,
    EnsureURL,
)
from datalad_next.constraints.base import (
    AltConstraints,
    Constraint,
)
from datalad_next.constraints.parameter import EnsureCommandParameterization
from datalad_next.constraints.exceptions import ParameterConstraintContext


class EnsureAllUnique(Constraint):
    def short_description(self):
        return 'all values must be unique'

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

    # Must not OR: https://github.com/datalad/datalad/issues/7164
    #spec_constraint = \
    #    spec_item_constraint | EnsureListOf(spec_item_constraint)
    spec_constraint = AltConstraints(
        EnsureListOf(spec_item_constraint),
        EnsureGeneratorFromFileLike(spec_item_constraint),
        spec_item_constraint,
    )

    def __init__(self, **kwargs):
        # this is the key bit: a mapping of parameter names to validators
        super().__init__(
            dict(spec=self.spec_constraint),
            **kwargs
        )


class SophisticatedCmdValidator(BasicCmdValidator):
    def _check_unique_values(self, **kwargs):
        EnsureAllUnique()(kwargs.values())

    def _check_sum_range(self, p1, p2):
        EnsureRange(min=3)(p1 + p2)

    def __init__(self):
        # this is the key bit: a mapping of parameter names to validators
        super().__init__(
            # implementation example of a higher-order constraint
            joint_constraints={
                ParameterConstraintContext(('p1', 'p2'), 'identity'):
                    self._check_unique_values,
                ParameterConstraintContext(('p1', 'p2'), 'sum'):
                    self._check_sum_range,
            },
        )


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
    with pytest.raises(ConstraintError) as e:
        val(dict(spec='5', p1=1, p2=1), on_error='raise-at-end')
    errors = e.value.errors
    assert len(errors) == 3
    # the spec-param-only error
    assert errors.messages[0].startswith('not any of')
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
    assert errors.messages[0].startswith('not any of')
    assert 'not all values are unique' not in errors.messages
    # now we do it again, but with a valid spec, such that the first
    # and only error is a higher order error
    with pytest.raises(ConstraintError) as e:
        val(dict(spec=5, p1=1, p2=1), on_error='raise-early')
    errors = e.value.errors
    assert len(errors) == 1
    assert 'not all values are unique' == errors.messages[0]
    assert 'p1, p2 (identity)' == errors.context_labels[0]


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
