from pathlib import Path
from unittest.mock import patch

from datalad.tests.utils import (
    assert_in,
    assert_in_results,
    assert_raises,
    assert_status,
    eq_,
    ok_,
)
# TODO find a replacement for this in anticipation of nose->pytest
from nose.tools import (
    assert_raises_regexp,
)

from datalad.api import (
    clone,
    create_sibling_webdav,
)
from datalad.distribution.dataset import (
    Dataset,
    require_dataset,
)
from datalad.support.exceptions import IncompleteResultsError
from datalad.tests.utils import (
    with_tempfile,
)
from datalad_next.tests.utils import (
    serve_path_via_webdav,
    with_credential,
)


webdav_cred = ('datalad', 'secure')


@with_credential(
    'dltest-mywebdav', user=webdav_cred[0], secret=webdav_cred[1],
    type='user_password')
@with_tempfile
@with_tempfile
@with_tempfile
@serve_path_via_webdav(auth=webdav_cred)
def test_common_workflow(clonepath, localpath, remotepath, url):
    ca = dict(result_renderer='disabled')
    ds = Dataset(localpath).create(**ca)
    # need to amend the test credential, can only do after we know the URL
    ds.credentials(
        'set',
        name='dltest-mywebdav',
        # the test webdav webserver uses a realm label '/'
        spec=dict(realm=url + '/'),
        **ca)

    # we use a nasty target directory that has the potential to ruin the
    # git-remote URL handling
    targetdir_name = 'tar&get=mike'
    targetdir = Path(remotepath) / targetdir_name
    url = f'{url}/{targetdir_name}'

    res = ds.create_sibling_webdav(url, storage_sibling='yes', **ca)
    assert_in_results(
        res,
        action='create_sibling_webdav.storage',
        status='ok',
        type='dataset',
        path=ds.path,
    )
    # where it should be accessible
    # needs to be quoted
    dlaurl='datalad-annex::?type=webdav&encryption=none&exporttree=no&' \
           'dlacredential=dltest-mywebdav&' \
           'url=http%3A//127.0.0.1%3A43612/tar%26get%3Dmike'
    assert_in_results(
        res,
        action='add-sibling',
        status='ok',
        path=ds.path,
        name='127.0.0.1',
        url=dlaurl,
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

    dsclone = clone(dlaurl, clonepath, **ca)
    # we get the same thing
    eq_(ds.repo.get_hexsha(ds.repo.get_corresponding_branch()),
        dsclone.repo.get_hexsha(dsclone.repo.get_corresponding_branch()))

    # check that it auto-deploys webdav credentials
    # at some point, clone should be able to do this internally
    # https://github.com/datalad/datalad/issues/6634
    dsclone.siblings('enable', name='127.0.0.1-storage')
    # verify that we can get testfile.dat
    # just get the whole damn thing
    assert_status('ok', dsclone.get('.'))
    # verify testfile content
    eq_('dummy', (dsclone.pathobj / 'testfile.dat').read_text())


def test_bad_url_catching():
    # Ensure that bad URLs are detected and handled
    bad_url = "this:is-not-an-expected-url"
    assert_raises_regexp(
        ValueError, f"no sibling name given.*{bad_url}",
        create_sibling_webdav, url=bad_url)

    bad_url = "http://localhost:33322/abc?a=b"
    assert_raises_regexp(
        ValueError, "URLs with a query component are not supported",
        create_sibling_webdav, url=bad_url)


def test_constraints_checking():
    # Ensure that constraints are checked internally
    url = "http://localhost:22334/abc"
    for key in ("existing", "storage_sibling"):
        assert_raises_regexp(
            ValueError, "value is not one of",
            create_sibling_webdav,
            url=url,
            **{key: "illegal-value"})


def test_credential_handling():
    url = "http://localhost:22334/abc"
    with patch("datalad_next.create_sibling_webdav._get_url_credential") as gur_mock, \
         patch("datalad_next.create_sibling_webdav._create_sibling_webdav") as csw_mock, \
         patch("datalad_next.create_sibling_webdav.lgr") as lgr_mock:

        csw_mock.return_value = iter([])

        gur_mock.return_value = None
        assert_raises_regexp(
            ValueError, "No suitable credential for http://localhost:22334/abc found or specified",
            create_sibling_webdav,
            url=url,
            name="some_name",
            existing="error")

        gur_mock.reset_mock()
        gur_mock.return_value = [None, {"some_key": "some_value"}]
        assert_raises_regexp(
            ValueError, "No suitable credential for http://localhost:22334/abc found or specified",
            create_sibling_webdav,
            url=url,
            name="some_name",
            existing="error")

        # Ensure that failed credential storing is handled and logged
        gur_mock.reset_mock()
        gur_mock.return_value = [None, {"user": "u", "secret": "s"}]
        create_sibling_webdav(
            url=url,
            name="some_name",
            existing="error")


def test_name_clash_detection():
    # Ensure that constraints are checked internally
    url = "http://localhost:22334/abc"
    for storage_sibling in ("yes", 'export', 'only', 'only-export'):
        assert_raises_regexp(
            ValueError, "sibling names must not be equal",
            create_sibling_webdav,
            url=url,
            name="abc",
            storage_name="abc",
            storage_sibling=storage_sibling)


def test_unused_storage_name_warning():
    # Ensure that constraints are checked internally

    # We use a URL with query for an early exit form 'create_sibling_webdav'.
    # This is NOT nice, instead we should either mock out all other calls in
    # 'create_sibling_webdav', or factor out the parameter check and test it in
    # isolation.
    url = "http://localhost:22334/abc?x=1"

    with patch("datalad_next.create_sibling_webdav.lgr") as lgr_mock:
        storage_sibling_values = ("no", "only", "only-export")
        for storage_sibling in storage_sibling_values:
            assert_raises(
                ValueError,
                create_sibling_webdav,
                url=url,
                name="abc",
                storage_name="abc",
                storage_sibling=storage_sibling)
        eq_(lgr_mock.warning.call_count, len(storage_sibling_values))


def test_check_existing_siblings():
    # Ensure that constraints are checked internally
    url = "http://localhost:22334/abc"

    ds = require_dataset(
        None,
        check_installed=True,
        purpose='create WebDAV sibling(s)')

    with patch("datalad_next.create_sibling_webdav."
               "_yield_ds_w_matching_siblings") as ms_mock:

        ms_mock.return_value = [
            ("some_path", "some_name1"),
            ("some_path", "some_name2")]
        try:
            create_sibling_webdav(
                url=url,
                name="some_name",
                existing="error")
        except IncompleteResultsError as ire:
            for existing_name in ("some_name1", "some_name2"):
                assert_in(
                    {
                        'action': 'create_sibling_webdav',
                        'refds': ds.path,
                        'status': 'error',
                        'message': (
                            'a sibling %r is already configured in dataset %r',
                            existing_name,
                            'some_path'
                        )
                    },
                    ire.failed)
        else:
            raise ValueError(
                "expected exception not raised: IncompleteResultsError")
