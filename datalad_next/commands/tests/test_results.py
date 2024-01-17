from pathlib import (
    Path,
    PurePath,
)
import pytest

from datalad_next.datasets import Dataset

from ..results import (
    CommandResult,
    CommandResultStatus,
)


def test_commandresult():
    # CommandResult is a plain data class, so there is no much to test
    # only the partial dict API that is implemented as a compatibility
    # shim for the datalad core result loop
    #
    # we need action, status, and path unconditionally
    with pytest.raises(TypeError):
        CommandResult()
    with pytest.raises(TypeError):
        CommandResult(action='some')
    with pytest.raises(TypeError):
        CommandResult(action='some', status='ok')
    # no something that works
    st = CommandResult(
        action='actionlabel',
        status=CommandResultStatus.ok,
        path=PurePath('mypath'),
        refds=Dataset('myds'),
    )
    # we can get a dict with stringified values (for some types)
    assert dict(st.items()) == {
        'action': 'actionlabel',
        'status': 'ok',
        'path': 'mypath',
        'message': None,
        'exception': None,
        'error_message': None,
        'type': None,
        'logger': None,
        'refds': str(Path.cwd() / 'myds'),
    }
    # 'in' works
    assert 'action' in st
    assert 'weird' not in st
    # getitem works, and gives strings
    assert st['path'] == 'mypath'
    # same for get
    assert st.get('path') == 'mypath'
    assert st.get('weird', 'normal') == 'normal'
    # 'pop' is emulated by setting to None
    assert st.pop('path') == 'mypath'
    assert st.path is None
    # popping something unknown doesn't blow
    assert st.pop('weird', 'default') == 'default'
    # and does not add cruft to the dataclass instance
    assert not hasattr(st, 'weird')

