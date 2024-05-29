"""Patch ria-, ora-, ria_utils-, and clone-tests to work with modified ria_utils

The ria-utils-patches use an abstract path representation for RIA-store elements.
This patch adapts the tests that use `ria_utils.create_store` and
`ria_utils.create_ds_in_store` to these modifications.
"""
from __future__ import annotations

import logging
import shutil
import stat
from pathlib import (
    Path,
    PurePosixPath,
)
from urllib.request import pathname2url

from datalad.api import (
    Dataset,
    clone,
    create_sibling_ria,
)
from datalad.cmd import (
    WitlessRunner as Runner,
    NoCapture,
)
from datalad.customremotes.ria_utils import (
    UnknownLayoutVersion,
    create_ds_in_store,
    create_store,
    get_layout_locations,
)
from datalad.distributed.ora_remote import (
    LocalIO,
    SSHRemoteIO,
)
from datalad.distributed.tests.ria_utils import (
    common_init_opts,
    get_all_files,
    populate_dataset,
)
from datalad.support.exceptions import (
    CommandError,
    IncompleteResultsError,
)
from datalad.support.network import get_local_file_url
from datalad.tests.utils_pytest import (
    SkipTest,
    assert_equal,
    assert_false,
    assert_in,
    assert_not_in,
    assert_raises,
    assert_repo_status,
    assert_result_count,
    assert_status,
    assert_true,
    create_tree,
    has_symlink_capability,
    known_failure_githubci_win,
    known_failure_windows,
    rmtree,
    serve_path_via_http,
    skip_if_adjusted_branch,
    swallow_logs,
    with_tempfile,
)

from . import apply_patch


def local_path2pure_posix_path(path: Path | str):
    return PurePosixPath(pathname2url(str(path)))


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@with_tempfile
def patched__postclonetest_prepare(lcl, storepath, storepath2, link):

    from datalad.customremotes.ria_utils import (
        create_ds_in_store,
        create_store,
        get_layout_locations,
    )
    from datalad.distributed.ora_remote import LocalIO

    create_tree(lcl,
                tree={
                        'ds': {
                            'test.txt': 'some',
                            'subdir': {
                                'subds': {'testsub.txt': 'somemore'},
                                'subgit': {'testgit.txt': 'even more'}
                            },
                        },
                      })

    lcl = Path(lcl)
    storepath = Path(storepath)
    storepath2 = Path(storepath2)
    # PATCH: introduce `ppp_storepath` and `ppp_storepath2` and use them instead
    # of `storepath` and `storepath2`.
    ppp_storepath = local_path2pure_posix_path(storepath)
    ppp_storepath2 = local_path2pure_posix_path(storepath2)
    link = Path(link)
    link.symlink_to(storepath)

    # create a local dataset with a subdataset
    subds = Dataset(lcl / 'ds' / 'subdir' / 'subds').create(force=True)
    subds.save()
    # add a plain git dataset as well
    subgit = Dataset(lcl / 'ds' / 'subdir' / 'subgit').create(force=True,
                                                              annex=False)
    subgit.save()
    ds = Dataset(lcl / 'ds').create(force=True)
    ds.save(version_tag='original')
    assert_repo_status(ds.path)

    io = LocalIO()

    # Have a second store with valid ORA remote. This should not interfere with
    # reconfiguration of the first one, when that second store is not the one we
    # clone from. However, don't push data into it for easier get-based testing
    # later on.
    # Doing this first, so datasets in "first"/primary store know about this.
    create_store(io, ppp_storepath2, '1')
    url2 = "ria+{}".format(get_local_file_url(str(storepath2)))
    for d in (ds, subds, subgit):
        create_ds_in_store(io, ppp_storepath2, d.id, '2', '1')
        d.create_sibling_ria(url2, "anotherstore", new_store_ok=True)
        d.push('.', to='anotherstore', data='nothing')
        store2_loc, _, _ = get_layout_locations(1, ppp_storepath2, d.id)
        Runner(cwd=str(store2_loc)).run(['git', 'update-server-info'])

    # Now the store to clone from:
    create_store(io, ppp_storepath, '1')

    # URL to use for upload. Point is, that this should be invalid for the clone
    # so that autoenable would fail. Therefore let it be based on a to be
    # deleted symlink
    upl_url = "ria+{}".format(get_local_file_url(str(link)))

    for d in (ds, subds, subgit):

        # TODO: create-sibling-ria required for config! => adapt to RF'd
        #       creation (missed on rebase?)
        create_ds_in_store(io, ppp_storepath, d.id, '2', '1')
        d.create_sibling_ria(upl_url, "store", new_store_ok=True)

        if d is not subgit:
            # Now, simulate the problem by reconfiguring the special remote to
            # not be autoenabled.
            # Note, however, that the actual intention is a URL, that isn't
            # valid from the point of view of the clone (doesn't resolve, no
            # credentials, etc.) and therefore autoenabling on git-annex-init
            # when datalad-cloning would fail to succeed.
            Runner(cwd=d.path).run(['git', 'annex', 'enableremote',
                                    'store-storage',
                                    'autoenable=false'])
        d.push('.', to='store')
        store_loc, _, _ = get_layout_locations(1, ppp_storepath, d.id)
        Runner(cwd=str(store_loc)).run(['git', 'update-server-info'])

    link.unlink()
    # We should now have a store with datasets that have an autoenabled ORA
    # remote relying on an inaccessible URL.
    # datalad-clone is supposed to reconfigure based on the URL we cloned from.
    # Test this feature for cloning via HTTP, SSH and FILE URLs.

    return ds.id


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@known_failure_githubci_win  # in datalad/git-annex as e.g. of 20201218
@with_tempfile(mkdir=True)
@with_tempfile
@with_tempfile
def patched_test_ria_postclone_noannex(dspath=None, storepath=None, clonepath=None):

    # Test for gh-5186: Cloning from local FS, shouldn't lead to annex
    # initializing origin.

    dspath = Path(dspath)
    storepath = Path(storepath)
    clonepath = Path(clonepath)
    # PATCH: introduce `ppp_storepath` and use it instead of `storepath`.
    ppp_storepath = local_path2pure_posix_path(storepath)

    from datalad.customremotes.ria_utils import (
        create_ds_in_store,
        create_store,
        get_layout_locations,
    )
    from datalad.distributed.ora_remote import LocalIO

    # First create a dataset in a RIA store the standard way
    somefile = dspath / 'a_file.txt'
    somefile.write_text('irrelevant')
    ds = Dataset(dspath).create(force=True)

    io = LocalIO()
    create_store(io, ppp_storepath, '1')
    lcl_url = "ria+{}".format(get_local_file_url(str(storepath)))
    create_ds_in_store(io, ppp_storepath, ds.id, '2', '1')
    ds.create_sibling_ria(lcl_url, "store", new_store_ok=True)
    ds.push('.', to='store')


    # now, remove annex/ tree from store in order to see, that clone
    # doesn't cause annex to recreate it.
    store_loc, _, _ = get_layout_locations(1, storepath, ds.id)
    annex = store_loc / 'annex'
    rmtree(str(annex))
    assert_false(annex.exists())

    clone_url = get_local_file_url(str(storepath), compatibility='git') + \
                '#{}'.format(ds.id)
    clone("ria+{}".format(clone_url), clonepath)

    # no need to test the cloning itself - we do that over and over in here

    # bare repo in store still has no local annex:
    assert_false(annex.exists())


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@with_tempfile
def patched_test_setup_store(io_cls, io_args, store=None):
    io = io_cls(*io_args)
    store = Path(store)
    # PATCH: introduce `ppp_store` and use it instead of `store`
    ppp_store = local_path2pure_posix_path(store)
    version_file = store / 'ria-layout-version'
    error_logs = store / 'error_logs'

    # invalid version raises:
    assert_raises(UnknownLayoutVersion, create_store, io, ppp_store, '2')

    # non-existing path should work:
    create_store(io, ppp_store, '1')
    assert_true(version_file.exists())
    assert_true(error_logs.exists())
    assert_true(error_logs.is_dir())
    assert_equal([f for f in error_logs.iterdir()], [])

    # empty target directory should work as well:
    rmtree(str(store))
    store.mkdir(exist_ok=False)
    create_store(io, ppp_store, '1')
    assert_true(version_file.exists())
    assert_true(error_logs.exists())
    assert_true(error_logs.is_dir())
    assert_equal([f for f in error_logs.iterdir()], [])

    # re-execution also fine:
    create_store(io, ppp_store, '1')

    # but version conflict with existing target isn't:
    version_file.write_text("2|unknownflags\n")
    assert_raises(ValueError, create_store, io, ppp_store, '1')
    # TODO: check output reporting conflicting version "2"


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@with_tempfile
def patched_test_setup_ds_in_store(io_cls, io_args, store=None):
    io = io_cls(*io_args)
    store = Path(store)
    # PATCH: introduce `ppp_store` and use it instead of `store`
    ppp_store = local_path2pure_posix_path(store)
    # ATM create_ds_in_store doesn't care what kind of ID is provided
    dsid = "abc123456"

    ds_path = store / dsid[:3] / dsid[3:]  # store layout version 1
    version_file = ds_path / 'ria-layout-version'
    archives = ds_path / 'archives'
    objects = ds_path / 'annex' / 'objects'
    git_config = ds_path / 'config'

    # invalid store version:
    assert_raises(UnknownLayoutVersion,
                  create_ds_in_store, io, ppp_store, dsid, '1', 'abc')

    # invalid obj version:
    assert_raises(UnknownLayoutVersion,
                  create_ds_in_store, io, ppp_store, dsid, 'abc', '1')

    # version 1
    create_store(io, ppp_store, '1')
    create_ds_in_store(io, ppp_store, dsid, '1', '1')
    for p in [ds_path, archives, objects]:
        assert_true(p.is_dir(), msg="Not a directory: %s" % str(p))
    for p in [version_file]:
        assert_true(p.is_file(), msg="Not a file: %s" % str(p))
    assert_equal(version_file.read_text(), "1\n")

    # conflicting version exists at target:
    assert_raises(ValueError, create_ds_in_store, io, ppp_store, dsid, '2', '1')

    # version 2
    # Note: The only difference between version 1 and 2 are supposed to be the
    #       key paths (dirhashlower vs mixed), which has nothing to do with
    #       setup routine.
    rmtree(str(store))
    create_store(io, ppp_store, '1')
    create_ds_in_store(io, ppp_store, dsid, '2', '1')
    for p in [ds_path, archives, objects]:
        assert_true(p.is_dir(), msg="Not a directory: %s" % str(p))
    for p in [version_file]:
        assert_true(p.is_file(), msg="Not a file: %s" % str(p))
    assert_equal(version_file.read_text(), "2\n")


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@with_tempfile(mkdir=True)
@serve_path_via_http
@with_tempfile
def patched_test_initremote(store_path=None, store_url=None, ds_path=None):
    ds = Dataset(ds_path).create()
    store_path = Path(store_path)
    # PATCH: introduce `ppp_store_path` and use it instead of `store_path`
    ppp_store_path = local_path2pure_posix_path(store_path)
    url = "ria+" + store_url
    init_opts = common_init_opts + ['url={}'.format(url)]

    # fail when there's no RIA store at the destination
    assert_raises(CommandError, ds.repo.init_remote, 'ora-remote',
                  options=init_opts)
    # Doesn't actually create a remote if it fails
    assert_not_in('ora-remote',
                  [cfg['name']
                   for uuid, cfg in ds.repo.get_special_remotes().items()]
                  )

    # now make it a store
    io = LocalIO()
    create_store(io, ppp_store_path, '1')
    create_ds_in_store(io, ppp_store_path, ds.id, '2', '1')

    # fails on non-RIA URL
    assert_raises(CommandError, ds.repo.init_remote, 'ora-remote',
                  options=common_init_opts + ['url={}'
                                              ''.format(store_path.as_uri())]
                  )
    # Doesn't actually create a remote if it fails
    assert_not_in('ora-remote',
                  [cfg['name']
                   for uuid, cfg in ds.repo.get_special_remotes().items()]
                  )

    ds.repo.init_remote('ora-remote', options=init_opts)
    assert_in('ora-remote',
              [cfg['name']
               for uuid, cfg in ds.repo.get_special_remotes().items()]
              )
    assert_repo_status(ds.path)
    # git-annex:remote.log should have:
    #   - url
    #   - common_init_opts
    #   - archive_id (which equals ds id)
    remote_log = ds.repo.call_git(['cat-file', 'blob', 'git-annex:remote.log'],
                                  read_only=True)
    assert_in("url={}".format(url), remote_log)
    [assert_in(c, remote_log) for c in common_init_opts]
    assert_in("archive-id={}".format(ds.id), remote_log)


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
# TODO: on crippled FS copytree to populate store doesn't seem to work.
#       Or may be it's just the serving via HTTP that doesn't work.
#       Either way, after copytree and fsck, whereis doesn't report
#       the store as an available source.
@skip_if_adjusted_branch
@known_failure_windows  # see gh-4469
@with_tempfile(mkdir=True)
@serve_path_via_http
@with_tempfile
def patched_test_read_access(store_path=None, store_url=None, ds_path=None):

    ds = Dataset(ds_path).create()
    populate_dataset(ds)

    files = [Path('one.txt'), Path('subdir') / 'two']
    store_path = Path(store_path)
    # PATCH: introduce `ppp_store_path` and use it instead of `store_path`
    ppp_store_path = local_path2pure_posix_path(store_path)
    url = "ria+" + store_url
    init_opts = common_init_opts + ['url={}'.format(url)]

    io = LocalIO()
    create_store(io, ppp_store_path, '1')
    create_ds_in_store(io, ppp_store_path, ds.id, '2', '1')
    ds.repo.init_remote('ora-remote', options=init_opts)
    fsck_results = ds.repo.fsck(remote='ora-remote', fast=True)
    # Note: Failures in the special remote will show up as a success=False
    # result for fsck -> the call itself would not fail.
    for r in fsck_results:
        if "note" in r:
            # we could simply assert "note" to not be in r, but we want proper
            # error reporting - content of note, not just its unexpected
            # existence.
            assert_equal(r["success"], "true",
                         msg="git-annex-fsck failed with ORA over HTTP: %s" % r)
        assert_equal(r["error-messages"], [])
    store_uuid = ds.siblings(name='ora-remote',
                             return_type='item-or-list',
                             result_renderer='disabled')['annex-uuid']
    here_uuid = ds.siblings(name='here',
                            return_type='item-or-list',
                            result_renderer='disabled')['annex-uuid']

    # nothing in store yet:
    for f in files:
        known_sources = ds.repo.whereis(str(f))
        assert_in(here_uuid, known_sources)
        assert_not_in(store_uuid, known_sources)

    annex_obj_target = str(store_path / ds.id[:3] / ds.id[3:]
                           / 'annex' / 'objects')
    shutil.rmtree(annex_obj_target)
    shutil.copytree(src=str(ds.repo.dot_git / 'annex' / 'objects'),
                    dst=annex_obj_target)

    ds.repo.fsck(remote='ora-remote', fast=True)
    # all in store now:
    for f in files:
        known_sources = ds.repo.whereis(str(f))
        assert_in(here_uuid, known_sources)
        assert_in(store_uuid, known_sources)

    ds.drop('.')
    res = ds.get('.')
    assert_equal(len(res), 4)
    assert_result_count(res, 4, status='ok', type='file', action='get',
                        message="from ora-remote...")

    # try whether the reported access URL is correct
    one_url = ds.repo.whereis('one.txt', output='full'
        )[store_uuid]['urls'].pop()
    assert_status('ok', ds.download_url(urls=[one_url], path=str(ds.pathobj / 'dummy')))


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@with_tempfile
@with_tempfile
def patched_test_initremote_basic(url, io, store, ds_path, link):

    ds_path = Path(ds_path)
    store = Path(store)
    # PATCH: introduce `ppp_store` and use it instead of `store`
    ppp_store = local_path2pure_posix_path(store)
    link = Path(link)
    ds = Dataset(ds_path).create()
    populate_dataset(ds)

    init_opts = common_init_opts + ['url={}'.format(url)]

    # fails on non-existing storage location
    assert_raises(CommandError,
                  ds.repo.init_remote, 'ria-remote', options=init_opts)
    # Doesn't actually create a remote if it fails
    assert_not_in('ria-remote',
                  [cfg['name']
                   for uuid, cfg in ds.repo.get_special_remotes().items()]
                  )

    # fails on non-RIA URL
    assert_raises(CommandError, ds.repo.init_remote, 'ria-remote',
                  options=common_init_opts + ['url={}'.format(store.as_uri())]
                  )
    # Doesn't actually create a remote if it fails
    assert_not_in('ria-remote',
                  [cfg['name']
                   for uuid, cfg in ds.repo.get_special_remotes().items()]
                  )

    # set up store:
    create_store(io, ppp_store, '1')
    # still fails, since ds isn't setup in the store
    assert_raises(CommandError,
                  ds.repo.init_remote, 'ria-remote', options=init_opts)
    # Doesn't actually create a remote if it fails
    assert_not_in('ria-remote',
                  [cfg['name']
                   for uuid, cfg in ds.repo.get_special_remotes().items()]
                  )
    # set up the dataset as well
    create_ds_in_store(io, ppp_store, ds.id, '2', '1')
    # now should work
    ds.repo.init_remote('ria-remote', options=init_opts)
    assert_in('ria-remote',
              [cfg['name']
               for uuid, cfg in ds.repo.get_special_remotes().items()]
              )
    assert_repo_status(ds.path)
    # git-annex:remote.log should have:
    #   - url
    #   - common_init_opts
    #   - archive_id (which equals ds id)
    remote_log = ds.repo.call_git(['cat-file', 'blob', 'git-annex:remote.log'],
                                  read_only=True)
    assert_in("url={}".format(url), remote_log)
    [assert_in(c, remote_log) for c in common_init_opts]
    assert_in("archive-id={}".format(ds.id), remote_log)

    # re-configure with invalid URL should fail:
    assert_raises(
        CommandError,
        ds.repo.call_annex,
        ['enableremote', 'ria-remote'] + common_init_opts + [
            'url=ria+file:///non-existing'])
    # but re-configure with valid URL should work
    if has_symlink_capability():
        link.symlink_to(store)
        new_url = 'ria+{}'.format(link.as_uri())
        ds.repo.call_annex(
            ['enableremote', 'ria-remote'] + common_init_opts + [
                'url={}'.format(new_url)])
        # git-annex:remote.log should have:
        #   - url
        #   - common_init_opts
        #   - archive_id (which equals ds id)
        remote_log = ds.repo.call_git(['cat-file', 'blob',
                                       'git-annex:remote.log'],
                                      read_only=True)
        assert_in("url={}".format(new_url), remote_log)
        [assert_in(c, remote_log) for c in common_init_opts]
        assert_in("archive-id={}".format(ds.id), remote_log)

    # we can deal with --sameas, which leads to a special remote not having a
    # 'name' property, but only a 'sameas-name'. See gh-4259
    try:
        ds.repo.init_remote('ora2',
                            options=init_opts + ['--sameas', 'ria-remote'])
    except CommandError as e:
        if 'Invalid option `--sameas' in e.stderr:
            # annex too old - doesn't know --sameas
            pass
        else:
            raise
    # TODO: - check output of failures to verify it's failing the right way
    #       - might require to run initremote directly to get the output


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@known_failure_windows  # see gh-4469
@with_tempfile
@with_tempfile
@with_tempfile
def patched_test_remote_layout(host, dspath, store, archiv_store):

    dspath = Path(dspath)
    store = Path(store)
    archiv_store = Path(archiv_store)
    # PATCH: introduce `ppp_store` and use it instead of `store`
    ppp_store = local_path2pure_posix_path(store)
    ppp_archiv_store = local_path2pure_posix_path(archiv_store)
    ds = Dataset(dspath).create()
    populate_dataset(ds)
    assert_repo_status(ds.path)

    # set up store:
    io = SSHRemoteIO(host) if host else LocalIO()
    if host:
        store_url = "ria+ssh://{host}{path}".format(host=host,
                                                    path=store)
        arch_url = "ria+ssh://{host}{path}".format(host=host,
                                                   path=archiv_store)
    else:
        store_url = "ria+{}".format(store.as_uri())
        arch_url = "ria+{}".format(archiv_store.as_uri())

    create_store(io, ppp_store, '1')

    # TODO: Re-establish test for version 1
    # version 2: dirhash
    create_ds_in_store(io, ppp_store, ds.id, '2', '1')

    # add special remote
    init_opts = common_init_opts + ['url={}'.format(store_url)]
    ds.repo.init_remote('store', options=init_opts)

    # copy files into the RIA store
    ds.push('.', to='store')

    # we should see the exact same annex object tree
    dsgit_dir, archive_dir, dsobj_dir = \
        get_layout_locations(1, store, ds.id)
    store_objects = get_all_files(dsobj_dir)
    local_objects = get_all_files(ds.pathobj / '.git' / 'annex' / 'objects')
    assert_equal(len(store_objects), 4)

    if not ds.repo.is_managed_branch():
        # with managed branches the local repo uses hashdirlower instead
        # TODO: However, with dataset layout version 1 this should therefore
        #       work on adjusted branch the same way
        # TODO: Wonder whether export-archive-ora should account for that and
        #       rehash according to target layout.
        assert_equal(sorted([p for p in store_objects]),
                     sorted([p for p in local_objects])
                     )

        if not io.get_7z():
            raise SkipTest("No 7z available in RIA store")

        # we can simply pack up the content of the remote into a
        # 7z archive and place it in the right location to get a functional
        # archive remote

        create_store(io, ppp_archiv_store, '1')
        create_ds_in_store(io, ppp_archiv_store, ds.id, '2', '1')

        whereis = ds.repo.whereis('one.txt')
        dsgit_dir, archive_dir, dsobj_dir = \
            get_layout_locations(1, archiv_store, ds.id)
        ds.export_archive_ora(archive_dir / 'archive.7z')
        init_opts = common_init_opts + ['url={}'.format(arch_url)]
        ds.repo.init_remote('archive', options=init_opts)
        # now fsck the new remote to get the new special remote indexed
        ds.repo.fsck(remote='archive', fast=True)
        assert_equal(len(ds.repo.whereis('one.txt')), len(whereis) + 1)
        # test creating an archive with filters on files
        ds.export_archive_ora(archive_dir / 'archive2.7z', annex_wanted='(include=*.txt)')
        # test with wanted expression of a specific remote
        ds.repo.set_preferred_content("wanted", "include=subdir/*", remote="store")
        ds.export_archive_ora(archive_dir / 'archive3.7z', remote="store")
        # test with the current sha
        ds.export_archive_ora(
            archive_dir / 'archive4.7z',
            froms=ds.repo.get_revisions()[1],
            )


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@known_failure_windows  # see gh-4469
@with_tempfile
@with_tempfile
def patched_test_version_check(host, dspath, store):

    dspath = Path(dspath)
    store = Path(store)
    # PATCH: introduce `ppp_store` and use it instead of `store`
    ppp_store = local_path2pure_posix_path(store)

    ds = Dataset(dspath).create()
    populate_dataset(ds)
    assert_repo_status(ds.path)

    # set up store:
    io = SSHRemoteIO(host) if host else LocalIO()
    if host:
        store_url = "ria+ssh://{host}{path}".format(host=host,
                                                    path=store)
    else:
        store_url = "ria+{}".format(store.as_uri())

    create_store(io, ppp_store, '1')

    # TODO: Re-establish test for version 1
    # version 2: dirhash
    create_ds_in_store(io, ppp_store, ds.id, '2', '1')

    # add special remote
    init_opts = common_init_opts + ['url={}'.format(store_url)]
    ds.repo.init_remote('store', options=init_opts)
    ds.push('.', to='store')

    # check version files
    remote_ds_tree_version_file = store / 'ria-layout-version'
    dsgit_dir, archive_dir, dsobj_dir = \
        get_layout_locations(1, store, ds.id)
    remote_obj_tree_version_file = dsgit_dir / 'ria-layout-version'

    assert_true(remote_ds_tree_version_file.exists())
    assert_true(remote_obj_tree_version_file.exists())

    with open(str(remote_ds_tree_version_file), 'r') as f:
        assert_equal(f.read().strip(), '1')
    with open(str(remote_obj_tree_version_file), 'r') as f:
        assert_equal(f.read().strip(), '2')

    # Accessing the remote should not yield any output regarding versioning,
    # since it's the "correct" version. Note that "fsck" is an arbitrary choice.
    # We need just something to talk to the special remote.
    with swallow_logs(new_level=logging.INFO) as cml:
        ds.repo.fsck(remote='store', fast=True)
        # TODO: For some reason didn't get cml.assert_logged to assert
        #       "nothing was logged"
        assert not cml.out

    # Now fake-change the version
    with open(str(remote_obj_tree_version_file), 'w') as f:
        f.write('X\n')

    # Now we should see a message about it
    with swallow_logs(new_level=logging.INFO) as cml:
        ds.repo.fsck(remote='store', fast=True)
        cml.assert_logged(level="INFO",
                          msg="Remote object tree reports version X",
                          regex=False)

    # reading still works:
    ds.drop('.')
    assert_status('ok', ds.get('.'))

    # but writing doesn't:
    with open(str(Path(ds.path) / 'new_file'), 'w') as f:
        f.write("arbitrary addition")
    ds.save(message="Add a new_file")

    with assert_raises((CommandError, IncompleteResultsError)):
        ds.push('new_file', to='store')

    # However, we can force it by configuration
    ds.config.add("annex.ora-remote.store.force-write", "true", scope='local')
    ds.push('new_file', to='store')


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
# git-annex-testremote is way too slow on crippled FS.
# Use is_managed_branch() as a proxy and skip only here
# instead of in a decorator
@skip_if_adjusted_branch
@known_failure_windows  # see gh-4469
@with_tempfile
@with_tempfile
def patched_test_gitannex(host, store, dspath):
    dspath = Path(dspath)
    store = Path(store)
    # PATCH: introduce `ppp_store` and use it instead of `store`
    ppp_store = local_path2pure_posix_path(store)

    ds = Dataset(dspath).create()

    populate_dataset(ds)
    assert_repo_status(ds.path)

    # set up store:
    io = SSHRemoteIO(host) if host else LocalIO()
    if host:
        store_url = "ria+ssh://{host}{path}".format(host=host,
                                                    path=store)
    else:
        store_url = "ria+{}".format(store.as_uri())

    create_store(io, ppp_store, '1')

    # TODO: Re-establish test for version 1
    # version 2: dirhash
    create_ds_in_store(io, ppp_store, ds.id, '2', '1')

    # add special remote
    init_opts = common_init_opts + ['url={}'.format(store_url)]
    ds.repo.init_remote('store', options=init_opts)

    from datalad.support.external_versions import external_versions
    if '8.20200330' < external_versions['cmd:annex'] < '8.20200624':
        # https://git-annex.branchable.com/bugs/testremote_breeds_way_too_many_instances_of_the_externals_remote/?updated
        raise SkipTest(
            "git-annex might lead to overwhelming number of external "
            "special remote instances")

    # run git-annex-testremote
    # note, that we don't want to capture output. If something goes wrong we
    # want to see it in test build's output log.
    ds.repo._call_annex(['testremote', 'store'], protocol=NoCapture)


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@known_failure_windows
@with_tempfile
@with_tempfile
@with_tempfile
def patched_test_push_url(storepath=None, dspath=None, blockfile=None):

    dspath = Path(dspath)
    store = Path(storepath)
    # PATCH: introduce `ppp_store` and use it instead of `store`
    ppp_store = local_path2pure_posix_path(store)
    blockfile = Path(blockfile)
    blockfile.touch()

    ds = Dataset(dspath).create()
    populate_dataset(ds)
    assert_repo_status(ds.path)
    repo = ds.repo

    # set up store:
    io = LocalIO()
    store_url = "ria+{}".format(store.as_uri())
    create_store(io, ppp_store, '1')
    create_ds_in_store(io, ppp_store, ds.id, '2', '1')

    # initremote fails with invalid url (not a ria+ URL):
    invalid_url = (store.parent / "non-existent").as_uri()
    init_opts = common_init_opts + ['url={}'.format(store_url),
                                    'push-url={}'.format(invalid_url)]
    assert_raises(CommandError, ds.repo.init_remote, 'store', options=init_opts)

    # initremote succeeds with valid but inaccessible URL (pointing to a file
    # instead of a store):
    block_url = "ria+" + blockfile.as_uri()
    init_opts = common_init_opts + ['url={}'.format(store_url),
                                    'push-url={}'.format(block_url)]
    repo.init_remote('store', options=init_opts)

    store_uuid = ds.siblings(name='store',
                             return_type='item-or-list')['annex-uuid']
    here_uuid = ds.siblings(name='here',
                            return_type='item-or-list')['annex-uuid']

    # but a push will fail:
    assert_raises(CommandError, ds.repo.call_annex,
                  ['copy', 'one.txt', '--to', 'store'])

    # reconfigure w/ local overwrite:
    repo.config.add("remote.store.ora-push-url", store_url, scope='local')
    # push works now:
    repo.call_annex(['copy', 'one.txt', '--to', 'store'])

    # remove again (config and file from store)
    repo.call_annex(['move', 'one.txt', '--from', 'store'])
    repo.config.unset("remote.store.ora-push-url", scope='local')
    repo.call_annex(['fsck', '-f', 'store'])
    known_sources = repo.whereis('one.txt')
    assert_in(here_uuid, known_sources)
    assert_not_in(store_uuid, known_sources)

    # reconfigure (this time committed)
    init_opts = common_init_opts + ['url={}'.format(store_url),
                                    'push-url={}'.format(store_url)]
    repo.enable_remote('store', options=init_opts)

    # push works now:
    repo.call_annex(['copy', 'one.txt', '--to', 'store'])
    known_sources = repo.whereis('one.txt')
    assert_in(here_uuid, known_sources)
    assert_in(store_uuid, known_sources)


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
# Skipping on adjusted branch as a proxy for crippledFS. Write permissions of
# the owner on a directory can't be revoked on VFAT. "adjusted branch" is a
# bit broad but covers the CI cases. And everything RIA/ORA doesn't currently
# properly run on crippled/windows anyway. Needs to be more precise when
# RF'ing will hopefully lead to support on windows in principle.
@skip_if_adjusted_branch
@known_failure_windows
@with_tempfile
@with_tempfile
def patched_test_permission(host, storepath, dspath):

    # Test whether ORA correctly revokes and obtains write permissions within
    # the annex object tree. That is: Revoke after ORA pushed a key to store
    # in order to allow the object tree to safely be used with an ephemeral
    # clone. And on removal obtain write permissions, like annex would
    # internally on a drop (but be sure to restore if something went wrong).

    dspath = Path(dspath)
    storepath = Path(storepath)
    # PATCH: introduce `ppp_storepath` and use it instead of `storepath`
    ppp_storepath = local_path2pure_posix_path(storepath)
    ds = Dataset(dspath).create()
    populate_dataset(ds)
    ds.save()
    assert_repo_status(ds.path)
    testfile = 'one.txt'

    # set up store:
    io = SSHRemoteIO(host) if host else LocalIO()
    if host:
        store_url = "ria+ssh://{host}{path}".format(host=host,
                                                    path=storepath)
    else:
        store_url = "ria+{}".format(storepath.as_uri())

    create_store(io, ppp_storepath, '1')
    create_ds_in_store(io, ppp_storepath, ds.id, '2', '1')
    _, _, obj_tree = get_layout_locations(1, storepath, ds.id)
    assert_true(obj_tree.is_dir())
    file_key_in_store = obj_tree / 'X9' / '6J' / 'MD5E-s8--7e55db001d319a94b0b713529a756623.txt' / 'MD5E-s8--7e55db001d319a94b0b713529a756623.txt'

    init_opts = common_init_opts + ['url={}'.format(store_url)]
    ds.repo.init_remote('store', options=init_opts)

    store_uuid = ds.siblings(name='store',
                             return_type='item-or-list')['annex-uuid']
    here_uuid = ds.siblings(name='here',
                            return_type='item-or-list')['annex-uuid']

    known_sources = ds.repo.whereis(testfile)
    assert_in(here_uuid, known_sources)
    assert_not_in(store_uuid, known_sources)
    assert_false(file_key_in_store.exists())

    ds.repo.call_annex(['copy', testfile, '--to', 'store'])
    known_sources = ds.repo.whereis(testfile)
    assert_in(here_uuid, known_sources)
    assert_in(store_uuid, known_sources)
    assert_true(file_key_in_store.exists())

    # Revoke write permissions from parent dir in-store to test whether we
    # still can drop (if we can obtain the permissions). Note, that this has
    # no effect on VFAT.
    file_key_in_store.parent.chmod(file_key_in_store.parent.stat().st_mode &
                                   ~stat.S_IWUSR)
    # we can't directly delete; key in store should be protected
    assert_raises(PermissionError, file_key_in_store.unlink)

    # ORA can still drop, since it obtains permission to:
    ds.repo.call_annex(['drop', testfile, '--from', 'store'])
    known_sources = ds.repo.whereis(testfile)
    assert_in(here_uuid, known_sources)
    assert_not_in(store_uuid, known_sources)
    assert_false(file_key_in_store.exists())


# Overwrite `_postclonetest_prepare` to handle paths properly
apply_patch(
    'datalad.core.distributed.tests.test_clone',
    None,
    '_postclonetest_prepare',
    patched__postclonetest_prepare,
    'modify _postclonetest_prepare to use PurePosixPath-arguments '
    'in RIA-methodes'
)


apply_patch(
    'datalad.core.distributed.tests.test_clone',
    None,
    'test_ria_postclone_noannex',
    patched_test_ria_postclone_noannex,
    'modify test_ria_postclone_noannex to use PurePosixPath-arguments '
    'in RIA-methods'
)


apply_patch(
    'datalad.customremotes.tests.test_ria_utils',
    None,
    '_test_setup_store',
    patched_test_setup_store,
    'modify _test_setup_store to use PurePosixPath-arguments in RIA-methods'
)


apply_patch(
    'datalad.customremotes.tests.test_ria_utils',
    None,
    '_test_setup_ds_in_store',
    patched_test_setup_ds_in_store,
    'modify _test_setup_ds_in_store to use PurePosixPath-arguments '
    'in RIA-methods'
)


apply_patch(
    'datalad.distributed.tests.test_ora_http',
    None,
    'test_initremote',
    patched_test_initremote,
    'modify test_initremote to use PurePosixPath-arguments in RIA-methods'
)


apply_patch(
    'datalad.distributed.tests.test_ora_http',
    None,
    'test_read_access',
    patched_test_read_access,
    'modify test_read_access to use PurePosixPath-arguments in RIA-methods'
)


apply_patch(
    'datalad.distributed.tests.test_ria_basics',
    None,
    '_test_initremote_basic',
    patched_test_initremote_basic,
    'modify _test_initremote_basic to use PurePosixPath-arguments '
    'in RIA-methods'
)


apply_patch(
    'datalad.distributed.tests.test_ria_basics',
    None,
    '_test_remote_layout',
    patched_test_remote_layout,
    'modify _test_remote_layout to use PurePosixPath-arguments in RIA-methods'
)


apply_patch(
    'datalad.distributed.tests.test_ria_basics',
    None,
    '_test_version_check',
    patched_test_version_check,
    'modify _test_version_check to use PurePosixPath-arguments in RIA-methods'
)


apply_patch(
    'datalad.distributed.tests.test_ria_basics',
    None,
    '_test_gitannex',
    patched_test_gitannex,
    'modify _test_gitannex to use PurePosixPath-arguments in RIA-methods'
)


apply_patch(
    'datalad.distributed.tests.test_ria_basics',
    None,
    'test_push_url',
    patched_test_push_url,
    'modify test_push_url to use PurePosixPath-arguments in RIA-methods'
)


apply_patch(
    'datalad.distributed.tests.test_ria_basics',
    None,
    '_test_permission',
    patched_test_permission,
    'modify _test_permission to use PurePosixPath-arguments in RIA-methods'
)
