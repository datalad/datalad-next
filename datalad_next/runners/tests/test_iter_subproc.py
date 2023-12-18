import pytest
import sys

from ..iter_subproc import (
    iter_subproc,
    CommandError,
)


def test_iter_subproc_cwd(tmp_path):
    test_content = 'some'
    test_file_name = 'testfile'
    test_file = tmp_path / test_file_name
    test_file.write_text(test_content)

    check_fx = \
        "import sys\n" \
        "if open('{input}').read() == '{content}':\n" \
        "    print('okidoki')".format(
            input=test_file_name,
            content=test_content,
        )
    # we cannot read the test file without a full path, because
    # CWD is not `tmp_path`
    with pytest.raises(CommandError) as e:
        with iter_subproc([sys.executable, '-c', check_fx]):
            pass
        assert 'FileNotFoundError' in e.value

    # but if we make it change to CWD, the same code runs
    with iter_subproc([sys.executable, '-c', check_fx], cwd=tmp_path) as proc:
        out = b''.join(proc)
        assert b'okidoki' in out
