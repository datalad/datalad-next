import pytest

from datalad.runner.exception import CommandError

from ..batch_test_3 import (
    annexjson_batchcommand,
    stdout_batchcommand,
)


def test_batch_simple(existing_dataset):
    # first with a simplistic protocol to test the basic mechanics
    with stdout_batchcommand(
            ['git', 'annex', 'examinekey',
             # the \n in the format is needed to produce an output that hits
             # the output queue after each input line
             '--format', '${bytesize}\n',
             '--batch'],
            cwd=existing_dataset.pathobj,
    ) as bp:
        res = bp(b'MD5E-s21032--2f4e22eb05d58c21663794876dc701aa\n')
        assert res.rstrip('\r\n') == '21032'
        # to subprocess is still running
        assert bp.return_code is None
        # another batch
        res = bp(b'MD5E-s999--2f4e22eb05d58c21663794876dc701aa\n')
        assert res.rstrip('\r\n') == '999'
        assert bp.return_code is None
        # we can bring the process down with stupid input
        with pytest.raises(CommandError) as deadexc:
            bp(b'stupid\n')
        # process exit is detectable
        assert bp.return_code == 1
        # continued use raises the same exception
        # (but stacktrace is obvs different)
        with pytest.raises(CommandError) as stilldeadexc:
            bp(b'MD5E-s999--2f4e22eb05d58c21663794876dc701aa\n')
        assert deadexc.value.to_str() == stilldeadexc.value.to_str()

    # now with a more complex protocol (decodes JSON-lines output)
    with annexjson_batchcommand(
            ['git', 'annex', 'examinekey', '--json', '--batch'],
            cwd=existing_dataset.pathobj,
    ) as bp:
        # output is a decoded JSON object
        res = bp(b'MD5E-s21032--2f4e22eb05d58c21663794876dc701aa\n')
        assert res['backend'] == "MD5E"
        assert res['bytesize'] == "21032"
        assert res['key'] == "MD5E-s21032--2f4e22eb05d58c21663794876dc701aa"
        res = bp(b'MD5E-s999--2f4e22eb05d58c21663794876dc701aa\n')
        assert res['bytesize'] == "999"
        with pytest.raises(CommandError) as deadexc:
            bp(b'stupid\n')
        assert bp.return_code == 1