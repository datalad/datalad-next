import pytest

from .. import (
    InvalidCredential,
    NoSuitableCredentialAvailable,
)


class CredTester:
    """Helper to provide callables for CredentialManager.call_with_credential

    Each passed credential is captured in the ``creds`` member for inspection.
    The length of that list also captured how often the callable has been
    executed.
    """
    def __init__(self):
        self.reset()

    def reset(self):
        self.creds = []

    def never_called(self, cred):
        raise RuntimeError(
            "TEST FAILURE, should have never been called")  # pragma nocover

    def always_invalid(self, cred):
        """Not credentials will ever work"""
        self.creds.append(cred)
        raise InvalidCredential('NOT VALID')

    def works_1sttry(self, cred):
        """The first credential will always work"""
        self.creds.append(cred)
        return '1st-is-success'

    def works_2ndtry(self, cred):
        """After one failure, the second credential will work"""
        self.creds.append(cred)
        if len(self.creds) == 2:
            return '2nd-is-success'
        else:
            raise InvalidCredential(f"NOT VALID {len(self.creds)}")


@pytest.fixture(autouse=False, scope="function")
def credtester():
    """Fixture to provision a fresh tester on each function call"""
    yield CredTester()


def test_cred_workflow_noexec(credman, credtester):
    # no credential, must not call fx
    # proper signaling
    with pytest.raises(NoSuitableCredentialAvailable):
        credman.call_with_credential(
            # if called, would raise RuntimeError
            credtester.never_called,
            # we must provide some identifying info, arbitrary
            type_hint='token',
        )
    # verify never been called
    assert not len(credtester.creds)


def test_cred_workflow_existing_named_credential(credman, credtester):
    # deposit a credential that is then provisioned based on its name
    credman.set('c1', site='this', secret='c1')
    res = credman.call_with_credential(
        # if called, would raise RuntimeError
        credtester.works_1sttry,
        name='c1'
    )
    assert res == '1st-is-success'
    # verify that called twice with both credentials once each
    assert [c['secret'] for c in credtester.creds] == ['c1']


def test_cred_workflow_existing_named_credential_not_working(
        credman, credtester, datalad_interactive_ui):
    ui = datalad_interactive_ui
    ui.staged_responses.extend([
        # this token will be accepted; there is no repeated entry required!
        'myothersecret',
        # saved under new name
        'mysecret',
    ])
    # deposit a credential that is then provisioned based on its name
    credman.set('c1', site='this', secret='c1')
    # we need to path testui in order to be able to actually verify the
    # pattern of interaction with a user
    # https://github.com/datalad/datalad-next/issues/423
    res = credman.call_with_credential(
        # if called, would raise RuntimeError
        credtester.works_2ndtry,
        name='c1',
        prompt='TELL ME',
        type_hint='token',
    )
    assert ui.operations == [
        ('message', 'NOT VALID 1'),
        ('question', 'token'),
        ('response', 'myothersecret'),
        ('question', 'name'),
        ('response', 'mysecret'),
    ]
    assert res == '2nd-is-success'
    assert [c['secret'] for c in credtester.creds] == ['c1', 'myothersecret']
    mycred = credman.get('mysecret')
    assert mycred['type'] == 'token'
    assert mycred['secret'] == 'myothersecret'


def test_cred_workflow_existing_named_credential_not_existing(
        credman, credtester, datalad_interactive_ui):
    ui = datalad_interactive_ui
    ui.staged_responses.append(
        # this is the secret for the credential with a name but no
        # definition yet
        'myothersecret'
    )
    res = credman.call_with_credential(
        # if called, would raise RuntimeError
        credtester.works_1sttry,
        name='c1',
        prompt='TELL ME',
        type_hint='token',
    )
    assert ui.operations == [
        # ask for a new one
        ('question', 'token'),
        ('response', 'myothersecret'),
        # there is no prompt for saving, a credential name was already provided
        # TODO document that this implication of given a credential name
        # is clear
    ]
    assert res == '1st-is-success'
    assert [c['secret'] for c in credtester.creds] == ['myothersecret']
    mycred = credman.get('c1')
    assert mycred['type'] == 'token'
    assert mycred['secret'] == 'myothersecret'


def test_cred_workflow_nomatch(credman, credtester):
    # we deposit two matching credentials
    credman.set('c1', _lastused=True, site='this', secret='c1')
    credman.set('c2', _lastused=True, site='this', secret='c2')
    # we arrange it such that neither credential will work
    with pytest.raises(NoSuitableCredentialAvailable):
        credman.call_with_credential(
            # if called, would raise RuntimeError
            credtester.always_invalid,
            # query such that the deposited credentials will match
            query_props=[{'site': 'this'}],
            type_hint='token',
        )
    # verify that called twice with both credentials once each
    assert [c['secret'] for c in credtester.creds] == ['c2', 'c1']


def test_cred_workflow_querymatch(credman, credtester):
    # we place two credentials that always match together, lastused=True
    # will make a query sort the last deposited first
    credman.set('c1', _lastused=True, site='this', secret='c1')
    credman.set('c2', _lastused=True, site='this', secret='c2')
    # succeed on 2nd try
    res = credman.call_with_credential(
        credtester.works_2ndtry, query_props=[{'site': 'this'}],
    )
    assert res == '2nd-is-success'
    # creds tried with last used/created first
    assert [c['secret'] for c in credtester.creds] == ['c2', 'c1']

    credtester.reset()
    # now we have c1 as the last successfully used credential in the manager
    # it will be tried first, but we will make it fail and must succeed
    # with c2 instead
    res = credman.call_with_credential(
        credtester.works_2ndtry, query_props=[{'site': 'this'}],
    )
    assert res == '2nd-is-success'
    # creds tried with last used/created first
    assert [c['secret'] for c in credtester.creds] == ['c1', 'c2']

    # if we now make the first credential pass, we should get c2, as the last
    # working one
    credtester.reset()
    res = credman.call_with_credential(
        credtester.works_1sttry, query_props=[{'site': 'this'}],
    )
    assert res == '1st-is-success'
    # creds tried with last used/created first
    assert [c['secret'] for c in credtester.creds] == ['c2']

    # double-check that even with credentials present, nothing is called
    # when nothing matches
    credtester.reset()
    with pytest.raises(NoSuitableCredentialAvailable):
        credman.call_with_credential(
            credtester.works_1sttry,
            type_hint='token',
            query_props=[{'site': 'other'}],
        )
    # verify never been called
    assert not len(credtester.creds)


def test_cred_workflow_querymatch_2ndset(credman, credtester):
    # we deposit one matching and one non-matching credential
    credman.set('c1', _lastused=True, site='this', secret='c1',
                essential='yes')
    credman.set('c2', _lastused=True, site='this', secret='c2')
    credman.set('c3', _lastused=True, site='that', secret='c3')
    # we run with two subsequent query sets, the first does not
    # match, but the second matches one of the deposited credentials
    res = credman.call_with_credential(
        credtester.works_1sttry,
        query_props=[
            {'site': 'wedontknow'},
            # include an empty set as a smoke test, does not help,
            # but also does not hurt
            {},
            {'site': 'this'},
        ],
        required_props=['essential'],
    )
    assert res == '1st-is-success'
    # creds tried with last used/created first
    assert len(credtester.creds) == 1
    assert credtester.creds[0]['secret'] == 'c1'


def test_cred_workflow_manual_entry(
        credman, credtester, datalad_interactive_ui):
    ui = datalad_interactive_ui
    ui.staged_responses.extend([
        # intentionally make the tokens alpha-num-sort backwards
        # this token will not be accepted; there is no repeated entry required!
        'tokenB1',
        # but this will
        'tokenA2',
        # it will be saved under this name
        'mycredential',
    ])
    res = credman.call_with_credential(
        credtester.works_2ndtry,
        prompt='TELL ME',
        # shoehorn in a query that will not match, but when something is
        # entered, we also get this realm annotation out
        query_props=[{'realm': 'somerealm'}],
        type_hint='token',
    )
    assert ui.operations == [
        ('question', 'token'),
        ('response', 'tokenB1'),
        ('message', 'NOT VALID 1'),
        ('question', 'token'),
        ('response', 'tokenA2'),
        ('question', 'name'),
        ('response', 'mycredential'),
    ]
    assert res == '2nd-is-success'
    assert [c['secret'] for c in credtester.creds] == ['tokenB1', 'tokenA2']
    mycred = credman.get('mycredential')
    assert mycred['type'] == 'token'
    assert mycred['secret'] == 'tokenA2'
    # we get the realm ID that we only put into the query and did not enter too
    assert mycred['realm'] == 'somerealm'


def test_cred_workflow_manual_entry_nosave(
        credman, credtester, monkeypatch, datalad_interactive_ui):
    ui = datalad_interactive_ui
    ui.staged_responses.extend([
        # this token will be accepted; there is no repeated entry required!
        'mytoken',
        # used, but not unsafe
        'skip',
    ])
    # if anything inside credman sets any config, we want to fail
    # 'skip' should cause no modification whatsoever
    with monkeypatch.context() as m:
        def _error(*args, **kwargs):   # pragma nocover
            raise RuntimeError('TEST FAILURE')

        m.setattr(credman._cfg, 'set', _error)
        res = credman.call_with_credential(
            credtester.works_1sttry,
            prompt='TELL ME',
            type_hint='token',
        )
    assert res == '1st-is-success'
    assert [c['secret'] for c in credtester.creds] == ['mytoken']
    # check that nothing was safed. we check for no secret being 'mytoken'
    # in this TMP credman instance.
    assert not any(
        cprops.get('secret') == 'mytoken' in cprops
        for cname, cprops in credman.query()), \
        credman.query()
