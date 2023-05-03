"""Build a complete (minimal) command that implements batch-mode

But without any batch-mode code inside the command implementation
"""

from io import StringIO

from datalad_next.commands import (
    EnsureCommandParameterization,
    ValidatedInterface,
    Parameter,
    build_doc,
    eval_results,
    get_status_dict,
)
from datalad_next.exceptions import CapturedException
from datalad_next.constraints import (
    EnsureGeneratorFromFileLike,
    EnsureJSON,
)


@build_doc
class DoBatch(ValidatedInterface):
    """Explainer!"""
    _validator_ = EnsureCommandParameterization(dict(
        # TODO add constraint that checks composition
        # of each JSON-line
        source=EnsureGeneratorFromFileLike(
            EnsureJSON(),
            exc_mode='yield',
        ),
    ))

    _params_ = dict(
        source=Parameter(args=('source',)),
    )

    @staticmethod
    @eval_results
    def __call__(source):
        for item in source:
            if isinstance(item, CapturedException):
                yield get_status_dict(
                    action='dobatch',
                    status='error',
                    exception=item,
                )
                continue
            yield get_status_dict(
                action='dobatch',
                status='ok',
                selected=item.get('this'),
            )


def test_dobatch(monkeypatch):
    data_in = '{"this":[1,2,3],"noise":"some"}\n{"this":true}'
    monkeypatch.setattr('sys.stdin', StringIO(data_in))
    res = DoBatch.__call__('-', result_renderer='disabled')
    assert len(res) == 2
    assert res[0]['selected'] == [1, 2, 3]
    assert res[1]['selected'] is True

    # now we have an intermediate error
    monkeypatch.setattr('sys.stdin', StringIO('bug\n' + data_in))
    res = DoBatch.__call__(
        '-', on_failure='ignore', result_renderer='disabled')
    assert len(res) == 3
    assert res[0]['status'] == 'error'
    assert 'Expecting value' in res[0]['error_message']
    # second one has the data now
    assert res[1]['selected'] == [1, 2, 3]
    assert res[2]['selected'] is True
