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

from pathlib import Path
from stat import S_IREAD, S_IRGRP, S_IROTH
from unittest.mock import patch

from datalad.api import (
    Dataset,
    clone,
)
from datalad.runner import CommandError
from datalad.tests.utils_pytest import (
    DEFAULT_BRANCH,
    DEFAULT_REMOTE,
    assert_raises,
    assert_status,
    eq_,
    neq_,
    rmtree,
    serve_path_via_http,
    with_tempfile,
)
from datalad.utils import on_windows
from datalad_next.tests.utils import (
    serve_path_via_webdav,
    with_credential,
)
from ..datalad_annex import get_initremote_params_from_url


webdav_cred = ('datalad', 'secure')


def eq_dla_branch_state(state, path, branch=DEFAULT_BRANCH):
    """Confirm that the reported branch hexsha at a remote matches
    a given value"""
    refsfile = Path(path) / '3f7' / '4a3' / 'XDLRA--refs' / 'XDLRA--refs'
    if not refsfile.exists():
        # this may be an export remote
        refsfile = Path(path) / '.datalad' / 'dotgit' / 'refs'
    if not refsfile.exists():
        assert None, f'Could not find refs at {path}'

    for line in refsfile.read_text().splitlines():
        if line.strip().endswith(f'heads/{branch}'):
            eq_(state, line.split(maxsplit=1)[0])
            return
    assert None, f'Could not find state for branch {branch} at {path}'


@with_tempfile
@with_tempfile(mkdir=True)
def test_annex_remote(dspath=None, remotepath=None):
    # bypass the complications of folding a windows path into a file URL
    dlaurl = \
        f'datalad-annex::?type=directory&directory={remotepath}&encryption=none' \
        if on_windows else \
        f'datalad-annex::file://{remotepath}?type=directory&directory={{path}}&encryption=none'
    ds = Dataset(dspath).create(annex=False, result_renderer='disabled')
    _check_push_fetch_cycle(ds, dlaurl, remotepath)


@with_tempfile
@with_tempfile(mkdir=True)
def test_export_remote(dspath=None, remotepath=None):
    # bypass the complications of folding a windows path into a file URL
    dlaurl = \
        f'datalad-annex::?type=directory&directory={remotepath}&encryption=none&exporttree=yes' \
        if on_windows else \
        f'datalad-annex::file://{remotepath}?type=directory&directory={{path}}&encryption=none&exporttree=yes'
    ds = Dataset(dspath).create(annex=False, result_renderer='disabled')
    _check_push_fetch_cycle(ds, dlaurl, remotepath)


@with_tempfile
@with_tempfile
def _check_push_fetch_cycle(ds, remoteurl, remotepath, localtargetpath, probepath):
    """Test helper

    - add a dla remote to the dataset
    - push the ds to it
    - clone from it to a tmp location
    - check error handling when post-git-update upload fails
    - update cycle starting from the original ds
    - repeated supposed-to-be-noop push/fetch calls
    - update cycle starting from the clone
    """
    dsrepo = ds.repo
    dsrepo.call_git(['remote', 'add', 'dla', remoteurl])
    remotepath = Path(remotepath)

    # basic push/clone roundtrip on clean locations
    # Since some version of git > 2.30.2 and <= 2.35.1
    # it would work without specifying branch.
    dsrepo.call_git(['push', '-u', 'dla', DEFAULT_BRANCH])
    eq_dla_branch_state(dsrepo.get_hexsha(DEFAULT_BRANCH), remotepath)
    dsclone = clone(remoteurl, localtargetpath)
    dsclonerepo = dsclone.repo
    eq_(dsrepo.get_hexsha(DEFAULT_BRANCH), dsclonerepo.get_hexsha(DEFAULT_BRANCH))

    # update round
    (ds.pathobj / 'file1').write_text('file1text')
    assert_status('ok', ds.save())

    # but first make destination read-only to test error recovery
    # verify starting point, we are one step ahead of the remote
    eq_(dsrepo.get_hexsha(DEFAULT_BRANCH + '~1'),
        dsrepo.get_hexsha(f'dla/{DEFAULT_BRANCH}'))

    # if we are on a sane system, also test recovery from (temporary)
    # push failure. MIH cannot force himself to figure out how to do
    # this on windows/crippledFS, sorry
    probeds = Dataset(probepath).create()
    if not probeds.repo.is_managed_branch():
        # preserve stat-info for later restore
        stat_records = {}
        # must go reverse to not block chmod'ing of children
        for p in sorted(remotepath.glob('**/*'), reverse=True):
            stat_records[p] = p.stat().st_mode
            p.chmod(S_IREAD | S_IRGRP | S_IROTH)
        # push must fail
        assert_raises(CommandError, dsrepo.call_git, ['push', 'dla'])
        # really bad that we cannot roll-back the remote branch state
        # from within the helper (see code), but we leave an indicator
        eq_(dsrepo.get_hexsha(DEFAULT_BRANCH),
            dsrepo.get_hexsha(f'refs/dlra-upload-failed/dla/{DEFAULT_BRANCH}'))

        # revert read-only permission on the remote side
        for p in sorted(stat_records):
            p.chmod(stat_records[p])

    # now a push can work (it should internally see that refs need
    # pushing that previously were reported as pushed, no need for
    # --force)
    dsrepo.call_git(['push', 'dla'])
    # and it has removed the marker
    assert_raises(
        ValueError,
        dsrepo.get_hexsha,
        f'refs/dlra-upload-failed/dla/{DEFAULT_BRANCH}')

    # the remote has received the new state
    eq_dla_branch_state(dsrepo.get_hexsha(DEFAULT_BRANCH), remotepath)
    # verify that there is something to update
    neq_(dsrepo.get_hexsha(DEFAULT_BRANCH), dsclonerepo.get_hexsha(DEFAULT_BRANCH))
    # pull
    dsclonerepo.call_git(['pull', DEFAULT_REMOTE, DEFAULT_BRANCH])
    # source and clone are now equal
    eq_(dsrepo.get_hexsha(DEFAULT_BRANCH), dsclonerepo.get_hexsha(DEFAULT_BRANCH))

    # push no update
    dsrepo.call_git(['push', 'dla'])
    # twice
    dsrepo.call_git(['push', 'dla'])

    # fetch no update
    dsclonerepo.call_git(['fetch', DEFAULT_REMOTE])
    # twice
    dsclonerepo.call_git(['fetch', DEFAULT_REMOTE])

    # push/pull in reverse from clone to source
    (dsclone.pathobj / 'file2').write_text('file2text')
    assert_status('ok', dsclone.save())
    neq_(dsrepo.get_hexsha(DEFAULT_BRANCH), dsclonerepo.get_hexsha(DEFAULT_BRANCH))
    dsclonerepo.call_git(['push', DEFAULT_REMOTE])
    eq_dla_branch_state(dsclonerepo.get_hexsha(DEFAULT_BRANCH), remotepath)
    dsrepo.call_git(['pull', 'dla', DEFAULT_BRANCH])
    eq_(dsrepo.get_hexsha(DEFAULT_BRANCH), dsclonerepo.get_hexsha(DEFAULT_BRANCH))

    # now create a non-heads ref and roundtrip that
    # this is what metalad needs to push metadata refs
    dsrepo.call_git([
        'update-ref', 'refs/datalad/dummy', dsrepo.get_hexsha(DEFAULT_BRANCH)])
    dsrepo.call_git(['push', 'dla', 'refs/datalad/dummy'])
    dsclonerepo.call_git([
        'fetch', DEFAULT_REMOTE, 'refs/datalad/dummy:refs/datalad/dummy'])
    eq_(dsrepo.get_hexsha('refs/datalad/dummy'),
        dsclonerepo.get_hexsha('refs/datalad/dummy'))


@with_tempfile
@with_tempfile(mkdir=True)
def test_annex_remote_autorepush(dspath=None, remotepath=None):
    # bypass the complications of folding a windows path into a file URL
    dlaurl = \
        f'datalad-annex::?type=directory&directory={remotepath}&encryption=none' \
        if on_windows else \
        f'datalad-annex::file://{remotepath}?type=directory&directory={{path}}&encryption=none'
    ds = Dataset(dspath).create(annex=False, result_renderer='disabled')
    _check_repush_after_vanish(ds, dlaurl, remotepath)


@with_tempfile
@with_tempfile(mkdir=True)
def test_export_remote_autorepush(dspath=None, remotepath=None):
    # bypass the complications of folding a windows path into a file URL
    dlaurl = \
        f'datalad-annex::?type=directory&directory={remotepath}&encryption=none&exporttree=yes' \
        if on_windows else \
        f'datalad-annex::file://{remotepath}?type=directory&directory={{path}}&encryption=none&exporttree=yes'
    ds = Dataset(dspath).create(annex=False, result_renderer='disabled')
    _check_repush_after_vanish(ds, dlaurl, remotepath)


def _check_repush_after_vanish(ds, remoteurl, remotepath):
    dsrepo = ds.repo
    dsrepo.call_git(['remote', 'add', 'dla', remoteurl])
    remotepath = Path(remotepath)

    dsrepo.call_git(['push', '-u', 'dla', DEFAULT_BRANCH])
    eq_dla_branch_state(dsrepo.get_hexsha(DEFAULT_BRANCH), remotepath)

    # wipe out the remote
    rmtree(remotepath)
    assert not remotepath.exists()
    remotepath.mkdir(parents=True)

    # helper must detect the discrepancy and re-push, despite the local mirror
    # repo already being uptodate
    dsrepo.call_git(['push', 'dla'])
    eq_dla_branch_state(dsrepo.get_hexsha(DEFAULT_BRANCH), remotepath)


def test_params_from_url():
    f = get_initremote_params_from_url
    # just the query part being used
    eq_(f('datalad-annex::?encryption=none&type=directory&directory=/this/h'),
        ['encryption=none', 'type=directory', 'directory=/this/h'])
    # some url prperty expansion
    eq_(f('datalad-annex::file:///this/h?type=directory&directory={path}'),
        ['type=directory', 'directory=/this/h'])
    # original URL, but query stripped
    eq_(f('https://ex.com/dav/proj/ds?type=webdav&url={noquery}&keyid=id@ex'),
        ['type=webdav', 'url=https://ex.com/dav/proj/ds', 'keyid=id@ex'])
    # proper unquoting
    eq_(f('http://ex.com?weirdparam=some%26amp'),
        ['type=web', 'exporttree=yes',
         'url=http://ex.com?weirdparam=some%26amp'])
    # nothing is not valid
    assert_raises(ValueError, f, '')
    assert_raises(ValueError, f, 'datalad-annex::')
    # URL without annotation is type=web export remote
    eq_(f('http://example.com/path/to/something'),
        ['type=web', 'exporttree=yes',
         'url=http://example.com/path/to/something'])


def test_typeweb_annex():
    _check_typeweb(
        # bypass the complications of folding a windows path into a file URL
        'datalad-annex::?type=directory&directory={export}&encryption=none' \
        if on_windows else
        'datalad-annex::file://{export}?type=directory&directory={{path}}&encryption=none',
        'datalad-annex::{url}?type=web&url={{noquery}}',
    )


# just to exercise the code path leading to an uncompressed ZIP
def test_typeweb_annex_uncompressed():
    _check_typeweb(
        # bypass the complications of folding a windows path into a file URL
        'datalad-annex::?type=directory&directory={export}&encryption=none&dladotgit=uncompressed' \
        if on_windows else
        'datalad-annex::file://{export}?type=directory&directory={{path}}&encryption=none&dladotgit=uncompressed',
        'datalad-annex::{url}?type=web&url={{noquery}}',
    )


def test_typeweb_export():
    _check_typeweb(
        # bypass the complications of folding a windows path into a file URL
        'datalad-annex::?type=directory&directory={export}&encryption=none&exporttree=yes' \
        if on_windows else
        'datalad-annex::file://{export}?type=directory&directory={{path}}&encryption=none&exporttree=yes',
        # when nothing is given type=web&exporttree=yes is the default
        'datalad-annex::{url}',
    )


@with_tempfile(mkdir=True)
@serve_path_via_http
@with_tempfile
@with_tempfile
def _check_typeweb(pushtmpl, clonetmpl, export, url, preppath, clonepath):
    ds = Dataset(preppath).create(annex=False, result_renderer='disabled')
    ds.repo.call_git([
        'remote', 'add',
        'dla',
        pushtmpl.format(**locals()),
    ])
    ds.repo.call_git(['push', '-u', 'dla', DEFAULT_BRANCH])
    # must override git-annex security setting for localhost
    with patch.dict(
            "os.environ", {
                "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": "annex.security.allowed-ip-addresses",
                "GIT_CONFIG_VALUE_0": "127.0.0.1"}):
        dsclone = clone(
            clonetmpl.format(**locals()),
            clonepath)
    eq_(ds.repo.get_hexsha(DEFAULT_BRANCH),
        dsclone.repo.get_hexsha(DEFAULT_BRANCH))


@with_tempfile(mkdir=True)
@serve_path_via_http
@with_tempfile
def test_submodule_url(servepath=None, url=None, workdir=None):
    workdir = Path(workdir)
    # a future subdataset that we want to register under a complex URL
    tobesubds = Dataset(workdir / 'subdsprep').create(annex=False, result_renderer='disabled')
    # push to test web server, this URL doesn't matter yet
    tobesubds.repo.call_git([
        'remote', 'add', 'dla',
        # bypass the complications of folding a windows path into a file URL
        f'datalad-annex::?type=directory&directory={servepath}&encryption=none&exporttree=yes'
        if on_windows else
        f'datalad-annex::file://{servepath}?type=directory&directory={{path}}&encryption=none&exporttree=yes',
    ])
    tobesubds.repo.call_git(['push', '-u', 'dla', DEFAULT_BRANCH])
    # create a superdataset to register the subds to
    super = Dataset(workdir / 'super').create()
    with patch.dict(
            "os.environ", {
                "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": "annex.security.allowed-ip-addresses",
                "GIT_CONFIG_VALUE_0": "127.0.0.1"}):
        # this is the URL that matters
        # we intentionally use something that leaves a placeholder behind
        # in the submodule record
        super.clone(
            f'datalad-annex::{url}?type=web&url={{noquery}}&exporttree=yes',
            'subds')
    # no clone the entire super
    superclone = clone(super.path, workdir / 'superclone')
    # and auto-fetch the sub via the datalad-annex remote helper
    superclone.get('subds', get_data=False, recursive=True)
    # we got the original subds
    subdsclone = Dataset(superclone.pathobj / 'subds')
    eq_(tobesubds.id, subdsclone.id)


@with_credential(
    'dltest-mystuff', user=webdav_cred[0], secret=webdav_cred[1],
    type='user_password')
@with_tempfile
@with_tempfile
@with_tempfile
@serve_path_via_webdav(auth=webdav_cred)
def test_webdav_auth(preppath=None, clnpath=None, remotepath=None, webdavurl=None):
    # this is the dataset we want to roundtrip through webdav
    ds = Dataset(preppath).create(annex=False, result_renderer='disabled')

    remoteurl = \
        f'datalad-annex::{webdavurl}' \
        '?type=webdav&url={noquery}&encryption=none&' \
        'dlacredential=dltest-mystuff'

    ds.repo.call_git(['remote', 'add', 'dla', remoteurl])

    # roundtrip
    ds.repo.call_git(['push', '-u', 'dla', DEFAULT_BRANCH])
    cln = clone(remoteurl, clnpath)
    # must give the same thing
    eq_(ds.repo.get_hexsha(DEFAULT_BRANCH),
        cln.repo.get_hexsha(DEFAULT_BRANCH))
