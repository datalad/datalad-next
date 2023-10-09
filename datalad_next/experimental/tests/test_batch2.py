from __future__ import annotations

import sys

import pytest

from ..batch2 import (
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
        _, res = bp(b'MD5E-s21032--2f4e22eb05d58c21663794876dc701aa\n')
        assert res.rstrip(b'\r\n') == b'21032'
        # to subprocess is still running
        assert bp.return_code is None
        # another batch
        _, res = bp(b'MD5E-s999--2f4e22eb05d58c21663794876dc701aa\n')
        assert res.rstrip(b'\r\n') == b'999'
        assert bp.return_code is None
        # we can bring the process down with stupid input
        bp(b'stupid\n')
        # process exit is detectable
        return_code =  bp.return_code
        assert return_code not in (None, 0)
        # continued use leads to same error
        bp(b'MD5E-s999--2f4e22eb05d58c21663794876dc701aa\n')
        assert bp.return_code == return_code

    # now with a more complex protocol (decodes JSON-lines output)
    with annexjson_batchcommand(
            ['git', 'annex', 'examinekey', '--json', '--batch'],
            cwd=existing_dataset.pathobj,
    ) as bp:
        # output is a decoded JSON object
        success, (_, res) = bp(b'MD5E-s21032--2f4e22eb05d58c21663794876dc701aa\n')
        assert success
        assert res['backend'] == "MD5E"
        assert res['bytesize'] == "21032"
        assert res['key'] == "MD5E-s21032--2f4e22eb05d58c21663794876dc701aa"
        success, (_, res) = bp(b'MD5E-s999--2f4e22eb05d58c21663794876dc701aa\n')
        assert success
        assert res['bytesize'] == "999"
        bp(b'stupid\n')
        assert bp.return_code not in (0, None)


def test_batch_basic():
    cmd_1 = [sys.executable, '-c',
'''
import sys
while True:
    x = sys.stdin.readline()
    if x == '':
        exit(2)
    print('{"entered": "%s"}' % str(x.strip()), flush=True)
    if x.strip() == 'end':
        exit(3)
''']

    with annexjson_batchcommand(cmd=cmd_1) as bp:
        for command in ('sdsdasd\n', 'end\n'):
            res = bp(command.encode())
            print('received:', res)

    print('result code:', bp.return_code)

    with annexjson_batchcommand(cmd=cmd_1) as bp:
        pass
    print('result code:', bp.return_code)


def test_batch2_exits():
    cmd = [sys.executable, '-c', '''
import sys
while True:
    x = sys.stdin.readline()
    if x == '':
        exit(2)
    print('{"entered": "%s"}' % str(x.strip()), flush=True)
    if x.strip() == 'end':
        exit(3)
''']

    with annexjson_batchcommand(cmd=cmd) as bp:
        for command in ('sdsdasd\n', 'end\n'):
            res = bp(command.encode())
            print('received:', res)
    # We should get the return code from the `end`-path
    assert bp.return_code == 3

    with annexjson_batchcommand(cmd=cmd) as bp:
        # Do nothing here to check that context manager exit works
        # properly
        pass
    # We should get the return code from the "stdin-closed" path
    assert bp.return_code == 2
