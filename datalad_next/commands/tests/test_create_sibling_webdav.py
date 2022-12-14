from pathlib import Path
from unittest.mock import (
    call,
    patch,
)
from urllib.parse import quote as urlquote

from datalad_next.tests.utils import (
    assert_equal,
    assert_in,
    assert_in_results,
    assert_raises,
    assert_result_count,
    assert_status,
    eq_,
    ok_,
    run_main,
    serve_path_via_webdav,
    with_credential,
    with_tempfile,
    with_tree
)
import pytest

from datalad.api import (
    clone,
    create_sibling_webdav,
)
from datalad_next.datasets import Dataset

from ..create_sibling_webdav import _get_url_credential


webdav_cred = ('dltest-my&=webdav', 'datalad', 'secure')


def test_common_workflow_implicit_cred():
    check_common_workflow(False, 'annex')


def test_common_workflow_explicit_cred():
    check_common_workflow(True, 'annex')


def test_common_workflow_export():
    check_common_workflow(False, 'filetree')


@with_credential(
    webdav_cred[0], user=webdav_cred[1], secret=webdav_cred[2],
    type='user_password')
@with_tempfile
@with_tempfile
@with_tempfile
@serve_path_via_webdav(auth=webdav_cred[1:])
def check_common_workflow(
        declare_credential, mode,
        clonepath, localpath, remotepath, url):
    ca = dict(result_renderer='disabled')
    ds = Dataset(localpath).create(**ca)
    # need to amend the test credential, can only do after we know the URL
    ds.credentials(
        'set',
        name=webdav_cred[0],
        # the test webdav webserver uses a realm label '/'
        spec=dict(realm=url + '/'),
        **ca)

    # we use a nasty target directory that has the potential to ruin the
    # git-remote URL handling
    targetdir_name = 'tar&get=mike'
    targetdir = Path(remotepath) / targetdir_name
    url = f'{url}/{targetdir_name}'

    res = ds.create_sibling_webdav(
        url,
        credential=webdav_cred[0] if declare_credential else None,
        mode=mode,
        **ca)
    assert_in_results(
        res,
        action='create_sibling_webdav.storage',
        status='ok',
        type='sibling',
        path=ds.path,
        url=url,
        name='127.0.0.1-storage',
        # TODO: name=???
        #  Parse url for host or depend on serve_path always
        #  delivering 127.0.0.1? (Think IPv6 or a literal `localhost` or
        #  anything like that) Same applies to hardcoded `dlaurl`.
    )
    # where it should be accessible
    # needs to be quoted
    dlaurl = (
        'datalad-annex::?type=webdav&encryption=none&exporttree={exp}&'
        'url=http%3A//127.0.0.1%3A43612/tar%26get%3Dmike').format(
        exp='yes' if 'filetree' in mode else 'no',
    )
    if declare_credential:
        dlaurl += f'&dlacredential={urlquote(webdav_cred[0])}'

    assert_in_results(
        res,
        action='create_sibling_webdav',
        status='ok',
        path=ds.path,
        name='127.0.0.1',
        url=dlaurl,
        type='sibling',
    )
    ok_(targetdir.exists())
    # add some annex payload
    (ds.pathobj / 'testfile.dat').write_text('dummy')
    ds.save(**ca)
    res = ds.push(to='127.0.0.1', **ca)
    assert_in_results(
        res,
        action='copy',
        path=str(ds.pathobj / 'testfile.dat'),
        status='ok',
    )
    assert_in_results(res, action='publish', status='ok')

    cloneurl = dlaurl
    if not declare_credential and 'filetree' in mode:
        # we can use a simplified URL
        cloneurl = 'webdav://{url}'.format(
            # strip http://
            url=url[7:],
        )
    dsclone = clone(cloneurl, clonepath, **ca)
    # we get the same thing
    eq_(ds.repo.get_hexsha(ds.repo.get_corresponding_branch()),
        dsclone.repo.get_hexsha(dsclone.repo.get_corresponding_branch()))

    # check that it auto-deploys webdav credentials
    # at some point, clone should be able to do this internally
    # https://github.com/datalad/datalad/issues/6634
    dsclone.siblings('enable', name='127.0.0.1-storage', **ca)
    # verify that we can get testfile.dat
    # just get the whole damn thing
    assert_status('ok', dsclone.get('.', **ca))
    # verify testfile content
    eq_('dummy', (dsclone.pathobj / 'testfile.dat').read_text())


@with_tempfile
def test_bad_url_catching(path=None):
    # Ensure that bad URLs are detected and handled

    ds = Dataset(path).create()
    check_pairs = [
        (
            "http://localhost:33322/abc?a",
            "URL has forbidden 'query' component"
        ),
        (
            "https://netloc/has-a-fragment#sdsd",
            "URL has forbidden 'fragment' component"
        ),
        (
            "https:///has-no-net-location",
            "URL is missing 'netloc' component"
        ),
        (
            "xxx://localhost:33322/abc",
            "URL does not match expression '^(http|https)://'"
        ),
    ]

    for bad_url, expected_message in check_pairs:
        with pytest.raises(ValueError) as e:
            create_sibling_webdav(dataset=ds, url=bad_url)
        assert expected_message.format(url=bad_url) in str(e.value.__cause__)


@with_tempfile
def test_http_warning(path=None):
    # Check that usage of http: triggers a warning.

    ds = Dataset(path).create()
    url = "http://localhost:33322/abc"

    with patch("datalad_next.commands.create_sibling_webdav._get_url_credential") as gur_mock, \
         patch("datalad_next.commands.create_sibling_webdav._create_sibling_webdav") as csw_mock, \
         patch("datalad_next.commands.create_sibling_webdav.lgr") as lgr_mock:

        csw_mock.return_value = iter([])
        gur_mock.return_value = None

        # We set up the mocks to generate the following exception. This allows
        # us to limit the test to the logic in 'create_sibling_wabdav'.
        with pytest.raises(
                ValueError,
                match=f"No suitable credential for {url} found or specified"):
            create_sibling_webdav(dataset=ds, url=url)

        eq_(lgr_mock.warning.call_count, 1)
        assert_in(
            call(
                f"Using 'http:' ({url!r}) means that WebDAV credentials are "
                f"sent unencrypted over network links. Consider using "
                f"'https:'."),
            lgr_mock.warning.mock_calls)


@with_tempfile
def test_constraints_checking(path=None):
    # Ensure that constraints are checked internally

    ds = Dataset(path).create()
    url = "http://localhost:22334/abc"
    for key in ("existing", "mode"):
        with pytest.raises(ValueError) as e:
            create_sibling_webdav(
                dataset=ds, url=url, **{key: "illegal-value"})
        assert "is not one of" in str(e.value.__cause__)


@with_tempfile
def test_credential_handling(path=None):

    ds = Dataset(path).create()
    url = "https://localhost:22334/abc"
    with patch("datalad_next.commands.create_sibling_webdav._get_url_credential") as gur_mock, \
         patch("datalad_next.commands.create_sibling_webdav._create_sibling_webdav") as csw_mock, \
         patch("datalad_next.commands.create_sibling_webdav.lgr") as lgr_mock:

        csw_mock.return_value = iter([])

        gur_mock.return_value = None
        with pytest.raises(
                ValueError,
                match=f"No suitable credential for {url} found or specified"):
            create_sibling_webdav(
                dataset=ds, url=url, name="some_name", existing="error")

        gur_mock.reset_mock()
        gur_mock.return_value = [None, {"some_key": "some_value"}]
        with pytest.raises(
                ValueError,
                match=f"No suitable credential for {url} found or specified"):
            create_sibling_webdav(
                dataset=ds, url=url, name="some_name", existing="error")

        # Ensure that failed credential storing is handled and logged
        gur_mock.reset_mock()
        gur_mock.return_value = [None, {"user": "u", "secret": "s"}]
        create_sibling_webdav(
            dataset=ds,
            url=url,
            name="some_name",
            existing="error")


@with_tempfile
def test_name_clash_detection(path=None):
    # Ensure that constraints are checked internally

    ds = Dataset(path).create()
    url = "http://localhost:22334/abc"
    for mode in ("annex", 'filetree', 'annex-only', 'filetree-only'):
        with pytest.raises(ValueError) as e:
            create_sibling_webdav(
                dataset=ds, url=url, name="abc", storage_name="abc", mode=mode)
        assert "sibling names must not be equal" in str(e.value.__cause__)


@with_tempfile
def test_unused_storage_name_warning(path=None):
    # Ensure that constraints are checked internally

    ds = Dataset(path).create()
    url = "https://localhost:22334/abc"

    with patch("datalad_next.commands.create_sibling_webdav._get_url_credential") as gur_mock, \
         patch("datalad_next.commands.create_sibling_webdav._create_sibling_webdav") as csw_mock, \
         patch("datalad_next.commands.create_sibling_webdav.lgr") as lgr_mock:

        csw_mock.return_value = iter([])
        gur_mock.return_value = None

        mode_values = ("git-only", "annex-only", "filetree-only")
        for mode in mode_values:
            # We set up the mocks to generate the following exception. This allows
            # us to limit the test to the logic in 'create_sibling_wabdav'.
            assert_raises(
                ValueError,
                create_sibling_webdav,
                dataset=ds,
                url=url,
                name="abc",
                storage_name="abc",
                mode=mode)
        eq_(lgr_mock.warning.call_count, len(mode_values))


def test_get_url_credential():
    url = "https://localhost:22334/abc"

    class MockCredentialManager:
        def __init__(self, name=None, credentials=None):
            self.name = name
            self.credentials = credentials

        def query(self, *args, **kwargs):
            if self.name or self.credentials:
                return [(self.name, kwargs.copy())]
            return None

        get = query

    with patch("datalad_next.commands.create_sibling_webdav."
               "get_specialremote_credential_properties") as gscp_mock:

        # Expect credentials to be derived from WebDAV-url if no credential
        # name is provided
        gscp_mock.return_value = {"some": "value"}
        result = _get_url_credential(
            name=None,
            url=url,
            credman=MockCredentialManager("n1", "c1"))
        eq_(result, ("n1", {'_sortby': 'last-used', 'some': 'value'}))

        # Expect the credential name to be used, if provided
        result = _get_url_credential(
            name="some-name",
            url=url,
            credman=MockCredentialManager("x", "y"))
        eq_(result[0], "some-name")
        eq_(result[1][0][1]["name"], "some-name")

        # Expect the credentials to be derived from realm,
        # if no name is provided, and if the URL is not known.
        gscp_mock.reset_mock()
        gscp_mock.return_value = dict()
        result = _get_url_credential(
            name=None,
            url=url,
            credman=MockCredentialManager("u", "v"))
        eq_(result[0], None)


@with_credential(
    'dltest-mywebdav', user=webdav_cred[1], secret=webdav_cred[2],
    type='user_password')
@with_tree(tree={'sub': {'f0': '0'},
                 'sub2': {'subsub': {'f1': '1'},
                          'f2': '2'},
                 'f3': '3'})
@with_tempfile
@serve_path_via_webdav(auth=webdav_cred[1:])
def test_existing_switch(localpath=None, remotepath=None, url=None):
    ca = dict(result_renderer='disabled')
    ds = Dataset(localpath).create(force=True, **ca)
    # use a tricky name: '3f7' will be the hashdir of the XDLRA
    # key containing the superdataset's datalad-annex archive after a push
    sub = ds.create('3f7', force=True, **ca)
    sub2 = ds.create('sub2', force=True, **ca)
    subsub = sub2.create('subsub', force=True, **ca)
    ds.save(recursive=True, **ca)

    # need to amend the test credential, can only do after we know the URL
    ds.credentials(
        'set',
        name='dltest-mywebdav',
        # the test webdav webserver uses a realm label '/'
        spec=dict(realm=url + '/'),
        **ca)

    subsub.create_sibling_webdav(f'{url}/sub2/subsub', mode='annex',
                                 **ca)
    sub2.create_sibling_webdav(f'{url}/sub2', mode='annex-only', **ca)
    sub.create_sibling_webdav(f'{url}/3f7', mode='git-only', **ca)

    res = ds.create_sibling_webdav(url, mode='annex',
                                   existing='skip',
                                   recursive=True, **ca)
    dlaurl='datalad-annex::?type=webdav&encryption=none&exporttree=no&' \
           'url=http%3A//127.0.0.1%3A43612/'

    # results per dataset:
    # super:
    assert_in_results(
        res,
        action='create_sibling_webdav.storage',
        status='ok',
        type='sibling',
        name='127.0.0.1-storage',
        path=ds.path,
        url=url,
    )
    assert_in_results(
        res,
        action='create_sibling_webdav',
        status='ok',
        type='sibling',
        path=ds.path,
        name='127.0.0.1',
        url=dlaurl[:-1],
    )
    # sub
    assert_in_results(
        res,
        action='create_sibling_webdav.storage',
        status='ok',
        type='sibling',
        name='127.0.0.1-storage',
        path=sub.path,
        url=f'{url}/3f7',
    )
    assert_in_results(
        res,
        action='create_sibling_webdav',
        status='notneeded',
        type='sibling',
        name='127.0.0.1',
        path=sub.path,
    )
    # sub2
    assert_in_results(
        res,
        action='create_sibling_webdav.storage',
        status='notneeded',
        type='sibling',
        name='127.0.0.1-storage',
        path=sub2.path
    )
    assert_in_results(
        res,
        action='create_sibling_webdav',
        status='ok',
        type='sibling',
        path=sub2.path,
        name='127.0.0.1',
        url=f'{dlaurl}sub2',
    )
    # subsub
    assert_in_results(
        res,
        action='create_sibling_webdav.storage',
        status='notneeded',
        type='sibling',
        name='127.0.0.1-storage',
        path=subsub.path
    )
    assert_in_results(
        res,
        action='create_sibling_webdav',
        status='notneeded',
        type='sibling',
        name='127.0.0.1',
        path=subsub.path,
    )

    # should fail upfront with first discovered remote that already exist
    res = ds.create_sibling_webdav(
        url, mode='annex', existing='error', recursive=True,
        on_failure='ignore', **ca)
    assert_result_count(res, 8, status='error', type='sibling')
    # Note: 'message' is expected to be a tuple (and always present).
    assert all("is already configured" in r['message'][0] for r in res)
    assert all(r['action'].startswith('create_sibling_webdav') for r in res)

    srv_rt = Path(remotepath)
    (srv_rt / '3f7').rmdir()
    (srv_rt / 'sub2' / 'subsub').rmdir()
    (srv_rt / 'sub2').rmdir()

    # existing=skip actually doesn't do anything (other than yielding notneeded)
    res = ds.create_sibling_webdav(url, mode='annex',
                                   existing='skip',
                                   recursive=True, **ca)
    assert_result_count(res, 8, status='notneeded')
    remote_content = list(srv_rt.glob('**'))
    assert_equal(len(remote_content), 1)  # nothing but root dir

    # reconfigure to move target one directory level:
    dlaurl += 'reconfigure'
    url += '/reconfigure'
    new_root = srv_rt / 'reconfigure'
    res = ds.create_sibling_webdav(url, mode='annex',
                                   existing='reconfigure',
                                   recursive=True, **ca)
    assert_result_count(res, 8, status='ok')
    assert all(r['action'].startswith('reconfigure_sibling_webdav')
               for r in res)
    remote_content = list(new_root.glob('**'))
    assert_in(new_root / '3f7', remote_content)
    assert_in(new_root / 'sub2', remote_content)
    assert_in(new_root / 'sub2' / 'subsub', remote_content)


@with_credential(
    webdav_cred[0], user=webdav_cred[1], secret=webdav_cred[2],
    type='user_password')
@with_tempfile
@with_tempfile
@serve_path_via_webdav(auth=webdav_cred[1:])
def test_result_renderer(localpath=None, remotepath=None, url=None):
    ca = dict(result_renderer='disabled')
    ds = Dataset(localpath).create(**ca)
    # need to amend the test credential, can only do after we know the URL
    ds.credentials(
        'set',
        name=webdav_cred[0],
        # the test webdav webserver uses a realm label '/'
        spec=dict(realm=url + '/'),
        **ca)

    out, err = run_main(['create-sibling-webdav', '-d', localpath, url])
    # url is somehow reported
    assert_in('datalad-annex::?type=webdav', out)
    # and the two custom result renderings
    assert_in('create_sibling_webdav(ok)', out)
    assert_in('create_sibling_webdav.storage(ok)', out)
