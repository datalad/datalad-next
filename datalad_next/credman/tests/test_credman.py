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
import pytest
from unittest.mock import patch

from datalad.config import ConfigManager
from ..manager import (
    CredentialManager,
    _get_cred_cfg_var,
)
from datalad_next.tests.utils import (
    MemoryKeyring,
    assert_in,
    assert_not_in,
    assert_raises,
    eq_,
    neq_,
    with_tempfile,
    with_testsui,
)
from datalad_next.datasets import Dataset


def test_credmanager():
    # we want all tests to bypass the actual system keyring
    with patch('datalad.support.keyring_.keyring', MemoryKeyring()):
        check_credmanager()


def check_credmanager():
    cfg = ConfigManager()
    credman = CredentialManager(cfg)
    # doesn't work with thing air
    assert_raises(ValueError, credman.get)
    eq_(credman.get('donotexiststest'), None)
    # but if there is anything, report it
    # this makes it possible to discover credential fragments, if only to
    # expose them for clean-up
    eq_(credman.get(crazy='empty'), {'crazy': 'empty'})
    # smoke test for legacy credential retrieval code
    # reporting back a credential, even if empty, exposes the legacy
    # credentials (by name), and enables discovery and (re)setting them
    # using this newer credential system
    eq_(credman.get('donotexiststest', type='user_password'),
        {'type': 'user_password'})
    # does not fiddle with a secret that is readily provided
    eq_(credman.get('dummy', secret='mike', _type_hint='token'),
        dict(type='token', secret='mike'))

    # remove a secret that has not yet been set
    eq_(credman.remove('elusive', type_hint='user_password'), False)

    # but the secret was written to the keystore
    with pytest.raises(ValueError):
        credman.set('mycred', forbidden_propname='some')

    # no instructions what to do, no legacy entry, nothing was changed
    # but the secret was written to the keystore
    eq_(credman.set('mycred', secret='some'), dict(secret='some'))
    # redo but with timestep
    setprops = credman.set('lastusedcred', _lastused=True, secret='some')
    assert_in('last-used', setprops)
    # now re-set, based on the retrieved info, but update the timestamp
    setprops_new = credman.set('lastusedcred', _lastused=True,
                               **credman.get('lastusedcred'))
    # must have updated 'last-used'
    neq_(setprops['last-used'], setprops_new['last-used'])
    # first property store attempt
    eq_(credman.set('changed', secret='some', prop='val'),
        dict(secret='some', prop='val'))
    # second, no changing the secret, but changing the prop, albeit with
    # the same value, change report should be empty
    eq_(credman.set('changed', prop='val'), dict())
    # change secret, with value pulled from config
    try:
        cfg.set('datalad.credential.changed.secret', 'envsec',
                scope='override')
        eq_(credman.set('changed', secret=None), dict(secret='envsec'))
    finally:
        cfg.unset('datalad.credential.changed.secret', scope='override')

    # remove non-existing property, secret not report, because unchanged
    eq_(credman.set('mycred', dummy=None), dict(dummy=None))
    assert_not_in(_get_cred_cfg_var("mycred", "dummy"), cfg)

    # set property
    eq_(credman.set('mycred', dummy='good', this='that'),
        dict(dummy='good', this='that'))
    # ensure set
    eq_(credman.get('mycred'), dict(dummy='good', this='that', secret='some'))
    # remove individual property
    eq_(credman.set('mycred', dummy=None), dict(dummy=None))
    # remove individual property that is not actually present
    eq_(credman.set('mycred', imaginary=None), dict(imaginary=None))
    # ensure removal
    eq_(credman.get('mycred'), dict(this='that', secret='some'))

    # test full query and constrained query
    q = list(credman.query_())
    # 3 defined here, plus any number of legacy credentials
    assert len(q) > 3
    # now query for one of the creds created above
    q = list(credman.query_(prop='val'))
    eq_(len(q), 1)
    eq_(q[0][0], 'changed')
    eq_(q[0][1]['prop'], 'val')
    # and now a query with no match
    q = list(credman.query_(prop='val', funky='town'))
    eq_(len(q), 0)

    # remove complete credential
    credman.remove('mycred')
    eq_(credman.get('mycred'), None)

    # test prompting for a secret when none is given
    res = with_testsui(responses=['mysecret'])(credman.set)(
        'mycred', other='prop')
    assert res == {'other': 'prop', 'secret': 'mysecret'}

    # test prompting for a name when None is given
    res = with_testsui(responses=['mycustomname'])(credman.set)(
        None, secret='dummy', other='prop')
    assert res == {'name': 'mycustomname', 'other': 'prop', 'secret': 'dummy'}

    # test name prompt loop in case of a name collision
    res = with_testsui(
        responses=['mycustomname', 'mycustomname2'])(
            credman.set)(
        None, secret='dummy2', other='prop2')
    assert res == {'name': 'mycustomname2', 'other': 'prop2',
                   'secret': 'dummy2'}

    # test skipping at prompt, smoke test _context arg
    res = with_testsui(responses=['skip'])(credman.set)(
        None, _context='for me', secret='dummy', other='prop')
    assert res is None

    # if no name is provided and none _can_ be entered -> raise
    with pytest.raises(ValueError):
        credman.set(None, secret='dummy', other='prop')

    # accept suggested name
    res = with_testsui(responses=[''])(credman.set)(
        None, _suggested_name='auto1', secret='dummy', other='prop')
    assert res == {'name': 'auto1', 'other': 'prop', 'secret': 'dummy'}

    # a suggestion conflicting with an existing credential is like
    # not making a suggestion at all
    res = with_testsui(responses=['', 'auto2'])(credman.set)(
        None, _suggested_name='auto1', secret='dummy', other='prop')
    assert res == {'name': 'auto2', 'other': 'prop', 'secret': 'dummy'}


@with_tempfile
def test_credman_local(path=None):
    ds = Dataset(path).create(result_renderer='disabled')
    credman = CredentialManager(ds.config)

    # deposit a credential into the dataset's config, and die trying to
    # remove it
    ds.config.set('datalad.credential.stupid.secret', 'really', scope='branch')
    assert_raises(RuntimeError, credman.remove, 'stupid')

    # but it manages for the local scope
    ds.config.set('datalad.credential.notstupid.secret', 'really', scope='local')
    credman.remove('notstupid')


def test_query():
    # we want all tests to bypass the actual system keyring
    with patch('datalad.support.keyring_.keyring', MemoryKeyring()):
        check_query()


def check_query():
    cfg = ConfigManager()
    credman = CredentialManager(cfg)
    # set a bunch of credentials with a common realm AND timestamp
    for i in range(3):
        credman.set(
            f'cred.{i}',
            _lastused=True,
            secret=f'diff{i}',
            realm='http://ex.com/login',
        )
    # now a credential with the common realm, but without a timestamp
    credman.set(
        'cred.no.time',
        _lastused=False,
        secret='notime',
        realm='http://ex.com/login',
    )
    # and the most recent one (with timestamp) is an unrelated one
    credman.set('unrelated', _lastused=True, secret='unrelated')

    # smoke test for an unsorted report
    assert len(credman.query()) > 1
    # now we want all credentials that match the realm, sorted by
    # last-used timestamp -- most recent first
    slist = credman.query(realm='http://ex.com/login', _sortby='last-used')
    eq_(['cred.2', 'cred.1', 'cred.0', 'cred.no.time'],
        [i[0] for i in slist])
    # same now, but least recent first, importantly no timestamp stays last
    slist = credman.query(realm='http://ex.com/login', _sortby='last-used',
                          _reverse=False)
    eq_(['cred.0', 'cred.1', 'cred.2', 'cred.no.time'],
        [i[0] for i in slist])


def test_credman_get():
    # we are not making any writes, any config must work
    credman = CredentialManager(ConfigManager())
    # must be prompting for missing properties
    res = with_testsui(responses=['myuser'])(credman.get)(
        None, _type_hint='user_password', _prompt='myprompt',
        secret='dummy')
    assert 'myuser' == res['user']
    # same for the secret
    res = with_testsui(responses=['mysecret'])(credman.get)(
        None, _type_hint='user_password', _prompt='myprompt',
        user='dummy')
    assert 'mysecret' == res['secret']


def test_credman_obtain():
    # we want all tests to bypass the actual system keyring
    with patch('datalad.support.keyring_.keyring', MemoryKeyring()):
        check_obtain()


def check_obtain():
    credman = CredentialManager(ConfigManager())
    # senseless, but valid call
    # could not possibly report a credential without any info
    with pytest.raises(ValueError):
        credman.obtain()
    # a type_hint is not enough, if no prompt is provided
    with pytest.raises(ValueError):
        credman.obtain(type_hint='token')
    # also a prompt alone is not enough
    with pytest.raises(ValueError):
        credman.obtain(prompt='myprompt')
    # minimal condition prompt and type-hint for manual entry
    res = with_testsui(responses=['mytoken'])(credman.obtain)(
        type_hint='token', prompt='myprompt')
    assert res == (None,
                   {'type': 'token', 'secret': 'mytoken', '_edited': True})

    # no place a credential we could discover
    cred1_props = dict(secret='sec1', type='token', realm='myrealm')
    credman.set('cred1', _lastused=True, **cred1_props)

    # one matching property is all that is needed
    res = credman.obtain(query_props={'realm': 'myrealm'})
    assert res == ('cred1', credman.get('cred1'))

    # will report the last-used one
    credman.set('cred2', _lastused=True, **cred1_props)
    res = credman.obtain(query_props={'realm': 'myrealm'})
    assert res == ('cred2', credman.get('cred2'))
    credman.set('cred1', _lastused=True, **cred1_props)
    res = credman.obtain(query_props={'realm': 'myrealm'})
    assert res == ('cred1', credman.get('cred1'))

    # built-in test for additional property expectations
    with pytest.raises(ValueError):
        credman.obtain(query_props={'realm': 'myrealm'},
                       expected_props=['funky'])

    res = credman.obtain(query_props={'realm': 'myrealm'})
    # if we are looking for a realm, we get it back even if a credential
    # had to be entered
    res = with_testsui(responses=['mynewtoken'])(credman.obtain)(
        type_hint='token', prompt='myprompt',
        query_props={'realm': 'mytotallynewrealm'})
    assert res == (None,
                   {'type': 'token', 'secret': 'mynewtoken', '_edited': True,
                    'realm': 'mytotallynewrealm'})
