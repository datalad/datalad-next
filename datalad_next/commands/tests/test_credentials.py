# ex: set sts=4 ts=4 sw=4 noet:
# -*- coding: utf-8 -*-
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""

"""
import logging
from unittest.mock import patch

from ..credentials import (
    Credentials,
    normalize_specs,
)
from datalad_next.exceptions import IncompleteResultsError
from datalad_next.utils import external_versions
from datalad_next.tests.utils import (
    assert_in,
    assert_in_results,
    assert_raises,
    eq_,
    run_main,
    swallow_logs,
)


# helper to test error handling
class BrokenCredentialManager(object):
    def __init__(*args, **kwargs):
        pass

    def set(self, *args, **kwargs):
        raise RuntimeError('INTENTIONAL FAILURE')

    def remove(self, *args, **kwargs):
        raise RuntimeError('INTENTIONAL FAILURE')

    def get(self, *args, **kwargs):
        # if there is no secret
        return None


def test_normalize_specs():
    for i, o in (
            ('', {}),
            ('{}', {}),
            ([], {}),
            # indicate removal
            ([':mike'], {'mike': None}),
            (['this=that'], {'this': 'that'}),
            # not removal without : prefix
            (['empty='], {'empty': ''}),
            # multiple works
            (['this=that', ':mike'], {'this': 'that', 'mike': None}),
            # complete specs dict needs no removal marker
            ('{"this":"that","mike":null}', {'this': 'that', 'mike': None}),
            # list-spec, but as JSON
            ('["this=that", ":mike"]', {'this': 'that', 'mike': None}),
    ):
        eq_(normalize_specs(i), o)

    for error in (
            # no removal marker
            ['mike'],
            # any string would be JSON and must be valid
            'brokenjson'
    ):
        assert_raises(ValueError, normalize_specs, error)


def test_errorhandling_smoketest():
    callcfg = dict(on_failure='ignore', result_renderer='disabled')

    with patch('datalad_next.commands.credentials.CredentialManager',
               BrokenCredentialManager):
        cred = Credentials()
        assert_in_results(
            cred('set', name='dummy', **callcfg),
            status='error', name='dummy')
        assert_in_results(
            cred('remove', name='dummy', **callcfg),
            status='error', name='dummy')


def test_credentials_cli(tmp_keyring):
    # usable command
    cred = Credentials()
    # unknown action
    assert_raises(ValueError, cred, 'levitate')
    with swallow_logs(new_level=logging.ERROR) as cml:
        # it is a shame that the error is not coming out on
        # stderr
        run_main(['credentials', 'remove'], exit_code=1)
        if external_versions['datalad'] > '0.17.9':
            # depends on (yet unreleased)
            # https://github.com/datalad/datalad/pull/7210
            assert '  no credential name provided' in cml.lines
    # catch missing `name` via Python call too
    assert_raises(IncompleteResultsError, cred, 'set', spec=[':mike'])
    # no name and no property
    assert_raises(ValueError, cred, 'get')

    assert_in_results(
        cred('get', name='donotexiststest', on_failure='ignore',
             result_renderer='disabled'),
        status='error',
        name='donotexiststest',
    )

    # we are not getting a non-existing credential, and it is also
    # not blocking for input with a non-interactive session
    assert_in(
        'credential not found',
        run_main(['credentials', 'get', 'donotexiststest'], exit_code=1)[0]
    )

    # set a credential CLI
    run_main(
        ['credentials', 'set', 'mycred', 'secret=some', 'user=mike'],
        exit_code=0)
    # which we can retrieve
    run_main(['credentials', 'get', 'mycred'], exit_code=0)
    # query runs without asking for input, and comes back clean
    assert_in(
        'some',
        run_main(['-f', 'json', 'credentials', 'query'], exit_code=0)[0])
    # and remove
    run_main(['credentials', 'remove', 'mycred'], exit_code=0)
    # nothing bad on second attempt
    run_main(['credentials', 'remove', 'mycred'], exit_code=0)
    # query runs without asking for input, and comes back clean
    run_main(['credentials', 'query'], exit_code=0)


def test_interactive_entry_get(tmp_keyring, datalad_interactive_ui):
    ui = datalad_interactive_ui
    ui.staged_responses.extend([
        'attr1', 'attr2', 'secret'])
    # should ask all properties in order and the secret last
    cred = Credentials()
    assert_in_results(
        cred('get',
             name='myinteractive_get',
             # use CLI notation
             spec=[':attr1', ':attr2'],
             prompt='dummyquestion',
             result_renderer='disabled'),
        cred_attr1='attr1',
        cred_attr2='attr2',
        cred_secret='secret',
    )
    assert ui.operation_sequence == ['question', 'response'] * 3


def test_interactive_entry_set(tmp_keyring, datalad_interactive_ui):
    ui = datalad_interactive_ui
    ui.staged_responses.append('secretish')
    # should ask all properties in order and the secret last
    cred = Credentials()
    assert_in_results(
        cred('set',
             name='myinteractive_set',
             prompt='dummyquestion',
             result_renderer='disabled'),
        cred_secret='secretish',
    )
    assert ui.operation_sequence == ['question', 'response']


def test_result_renderer():
    # it must survive a result that is not from the command itself
    Credentials.custom_result_renderer(dict(
        action='weird',
        status='broken',
    ))


def test_extreme_credential_name(tmp_keyring, datalad_cfg):
    cred = Credentials()
    extreme = 'ΔЙקم๗あ |/;&%b5{}"'
    assert_in_results(
        cred(
            'set',
            name=extreme,
            # use CLI style spec to exercise more code
            spec=[f'someprop={extreme}', f'secret={extreme}'],
        ),
        cred_someprop=extreme,
        cred_secret=extreme,
    )
