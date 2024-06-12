"""ria-, ora-, ria_utils-, and clone-tests for patched ria/ora code

This are patched tests that work with the patched ria/ora code.

The ria-utils-patches use an abstract path representation for RIA-store elements.
This patch adapts the tests that use `ria_utils.create_store` and
`ria_utils.create_ds_in_store` to these modifications.
"""
from __future__ import annotations

import logging
import os.path as op
import random
import shutil
import stat
import string
from pathlib import (
    Path,
    PurePosixPath,
)
from urllib.parse import urlparse
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
    DEFAULT_REMOTE,
    SkipTest,
    assert_equal,
    assert_false,
    assert_not_in,
    assert_not_is_instance,
    assert_repo_status,
    assert_result_count,
    assert_status,
    assert_true,
    create_tree,
    has_symlink_capability,
    known_failure_windows,
    ok_,
    rmtree,
    serve_path_via_http,
    skip_if_adjusted_branch,
    skip_if_root,
    skip_ssh,
    slow,
    turtle,
    with_tempfile,
)

from datalad_next.tests import (
    assert_in,
    assert_raises,
    eq_,
    swallow_logs,
)


def _local_path2pure_posix_path(path: Path | str):
    return PurePosixPath(pathname2url(str(path)))


def _random_name(prefix: str = '') -> str:
    return prefix + ''.join(
        random.choices(string.ascii_letters + string.digits, k=8)
    )


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@with_tempfile
def _postclonetest_prepare(lcl, storepath, storepath2, link):

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
    ppp_storepath = _local_path2pure_posix_path(storepath)
    ppp_storepath2 = _local_path2pure_posix_path(storepath2)
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
        store2_loc, _, _ = get_layout_locations(1, storepath2, d.id)
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
        store_loc, _, _ = get_layout_locations(1, storepath, d.id)
        Runner(cwd=str(store_loc)).run(['git', 'update-server-info'])

    link.unlink()
    # We should now have a store with datasets that have an autoenabled ORA
    # remote relying on an inaccessible URL.
    # datalad-clone is supposed to reconfigure based on the URL we cloned from.
    # Test this feature for cloning via HTTP, SSH and FILE URLs.

    return ds.id


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732 and patched
# and refactored
def test_ria_postclone_noannex(existing_dataset, tmp_path):
    # Test for gh-5186: Cloning from local FS, shouldn't lead to annex
    # initializing origin.
    from datalad.customremotes.ria_utils import (
        create_ds_in_store,
        create_store,
        get_layout_locations,
    )
    from datalad.distributed.ora_remote import LocalIO

    ds = existing_dataset
    some_file = ds.pathobj / 'a_file.txt'
    some_file.write_text('irrelevant')

    ria_store_path = tmp_path / 'ria_store'
    url_ria_store_path = _local_path2pure_posix_path(ria_store_path)

    io = LocalIO()
    create_store(io, url_ria_store_path, '1')
    lcl_url = "ria+{}".format(get_local_file_url(str(ria_store_path)))
    create_ds_in_store(io, url_ria_store_path, ds.id, '2', '1')
    ds.create_sibling_ria(lcl_url, "store", new_store_ok=True)
    ds.push('.', to='store')

    # Remove annex-tree from store to check that clone doesn't cause annex to
    # recreate it.
    store_loc = get_layout_locations(1, ria_store_path, ds.id)[0]
    annex = store_loc / 'annex'
    rmtree(str(annex))
    assert_false(annex.exists())

    clone(
        f"ria+{get_local_file_url(str(ria_store_path), compatibility='git')}#{ds.id}",
        tmp_path / 'cloned_ds'
    )
    assert_false(annex.exists())


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
def _test_setup_store(io, store_url, local_store_path):

    store_url_path = PurePosixPath(urlparse(store_url).path)

    # invalid version raises:
    assert_raises(
        UnknownLayoutVersion,
        create_store, io, store_url_path, '2')

    # non-existing path should work:
    create_store(io, store_url_path, '1')

    version_file = local_store_path / 'ria-layout-version'
    error_logs = local_store_path / 'error_logs'
    assert_true(version_file.exists())
    assert_true(error_logs.exists())
    assert_true(error_logs.is_dir())
    assert_equal([f for f in error_logs.iterdir()], [])

    # empty target directory should work as well:
    rmtree(str(local_store_path))
    local_store_path.mkdir(exist_ok=False)
    create_store(io, store_url_path, '1')
    assert_true(version_file.exists())
    assert_true(error_logs.exists())
    assert_true(error_logs.is_dir())
    assert_equal([f for f in error_logs.iterdir()], [])

    # re-execution also fine:
    create_store(io, store_url_path, '1')

    # but version conflict with existing target isn't:
    version_file.write_text("2|unknownflags\n")
    assert_raises(ValueError, create_store, io, store_url_path, '1')
    # TODO: check output reporting conflicting version "2"


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
def _test_setup_ds_in_store(io, store_url, local_store_path):

    store_url_path = PurePosixPath(urlparse(store_url).path)

    # ATM create_ds_in_store doesn't care what kind of ID is provided
    dsid = "abc123456"

    ds_path = local_store_path / dsid[:3] / dsid[3:]  # store layout version 1
    version_file = ds_path / 'ria-layout-version'
    archives = ds_path / 'archives'
    objects = ds_path / 'annex' / 'objects'
    git_config = ds_path / 'config'

    # invalid store version:
    assert_raises(
        UnknownLayoutVersion,
        create_ds_in_store, io, store_url_path, dsid, '1', 'abc'
    )

    # invalid obj version:
    assert_raises(
        UnknownLayoutVersion,
        create_ds_in_store, io, store_url_path, dsid, 'abc', '1'
    )

    # version 1
    create_store(io, store_url_path, '1')
    create_ds_in_store(io, store_url_path, dsid, '1', '1')
    for p in [ds_path, archives, objects]:
        assert_true(p.is_dir(), msg="Not a directory: %s" % str(p))
    for p in [version_file]:
        assert_true(p.is_file(), msg="Not a file: %s" % str(p))
    assert_equal(version_file.read_text().strip(), "1")

    # conflicting version exists at target:
    assert_raises(
        ValueError,
        create_ds_in_store, io, store_url_path, dsid, '2', '1'
    )

    # version 2
    # Note: The only difference between version 1 and 2 are supposed to be the
    #       key paths (dirhashlower vs mixed), which has nothing to do with
    #       setup routine.
    rmtree(local_store_path)
    create_store(io, store_url_path, '1')
    create_ds_in_store(io, store_url_path, dsid, '2', '1')
    for p in [ds_path, archives, objects]:
        assert_true(p.is_dir(), msg="Not a directory: %s" % str(p))
    for p in [version_file]:
        assert_true(p.is_file(), msg="Not a file: %s" % str(p))
    assert_equal(version_file.read_text().strip(), "2")


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@with_tempfile(mkdir=True)
@serve_path_via_http
@with_tempfile
def test_initremote(store_path=None, store_url=None, ds_path=None):
    ds = Dataset(ds_path).create()
    store_path = Path(store_path)
    # PATCH: introduce `ppp_store_path` and use it instead of `store_path`
    ppp_store_path = _local_path2pure_posix_path(store_path)
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
def test_read_access(store_path=None, store_url=None, ds_path=None):

    ds = Dataset(ds_path).create()
    populate_dataset(ds)

    files = [Path('one.txt'), Path('subdir') / 'two']
    store_path = Path(store_path)
    # PATCH: introduce `ppp_store_path` and use it instead of `store_path`
    ppp_store_path = _local_path2pure_posix_path(store_path)
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
def _test_initremote_basic(io, store_url, local_store_path, ds, link, overwrite = None):

    populate_dataset(ds)

    store_url_path = overwrite or PurePosixPath(urlparse(store_url).path)
    link = Path(link)

    init_opts = common_init_opts + [f'url=ria+{store_url}']

    # fails on non-existing storage location
    assert_raises(
        CommandError,
        ds.repo.init_remote, 'ria-remote', options=init_opts
    )
    # Doesn't actually create a remote if it fails
    assert_not_in(
        'ria-remote',
        [
            cfg['name']
            for uuid, cfg in ds.repo.get_special_remotes().items()
        ]
    )

    # fails on non-RIA URL
    assert_raises(
        CommandError,
        ds.repo.init_remote, 'ria-remote',
        options=common_init_opts + [f'url={store_url}']
    )

    # Doesn't actually create a remote if it fails
    assert_not_in(
        'ria-remote',
        [
            cfg['name']
            for uuid, cfg in ds.repo.get_special_remotes().items()
        ]
    )

    # set up store:
    create_store(io, store_url_path, '1')
    # still fails, since ds isn't setup in the store
    assert_raises(
        CommandError,
  ds.repo.init_remote, 'ria-remote', options=init_opts
    )
    # Doesn't actually create a remote if it fails
    assert_not_in(
        'ria-remote',
        [
            cfg['name']
            for uuid, cfg in ds.repo.get_special_remotes().items()
        ]
    )
    # set up the dataset as well
    create_ds_in_store(io, store_url_path, ds.id, '2', '1')
    # now should work
    ds.repo.init_remote('ria-remote', options=init_opts)
    assert_in(
        'ria-remote',
        [
            cfg['name']
            for uuid, cfg in ds.repo.get_special_remotes().items()
        ]
    )
    assert_repo_status(ds.path)
    # git-annex:remote.log should have:
    #   - url
    #   - common_init_opts
    #   - archive_id (which equals ds id)
    remote_log = ds.repo.call_git(
        ['cat-file', 'blob', 'git-annex:remote.log'], read_only=True
    )
    assert_in(f'url=ria+{store_url}', remote_log)
    [assert_in(c, remote_log) for c in common_init_opts]
    assert_in("archive-id={}".format(ds.id), remote_log)

    # re-configure with invalid URL should fail:
    assert_raises(
        CommandError,
        ds.repo.call_annex,
        ['enableremote', 'ria-remote'] + common_init_opts + [
            'url=ria+file:///non-existing'
        ]
    )
    # but re-configure with valid URL should work
    if has_symlink_capability():
        link.symlink_to(local_store_path)
        new_url = 'ria+{}'.format(link.as_uri())
        ds.repo.call_annex(
            ['enableremote', 'ria-remote'] + common_init_opts + [
                'url={}'.format(new_url)
            ]
        )
        # git-annex:remote.log should have:
        #   - url
        #   - common_init_opts
        #   - archive_id (which equals ds id)
        remote_log = ds.repo.call_git(
            ['cat-file', 'blob', 'git-annex:remote.log'], read_only=True
        )
        assert_in("url={}".format(new_url), remote_log)
        [assert_in(c, remote_log) for c in common_init_opts]
        assert_in("archive-id={}".format(ds.id), remote_log)

    # we can deal with --sameas, which leads to a special remote not having a
    # 'name' property, but only a 'sameas-name'. See gh-4259
    try:
        ds.repo.init_remote(
            'ora2',
            options=init_opts + ['--sameas', 'ria-remote']
        )
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
def _test_remote_layout(io,
                        store_url,
                        archive_store_url,
                        local_store_path,
                        local_archive_store_path,
                        ds
                        ):

    populate_dataset(ds)
    assert_repo_status(ds.path)

    store_url_path = PurePosixPath(urlparse(store_url).path)
    archive_store_url_path = PurePosixPath(urlparse(archive_store_url).path)

    # set up store:
    create_store(io, store_url_path, '1')

    # TODO: Re-establish test for version 1
    # version 2: dirhash
    create_ds_in_store(io, store_url_path, ds.id, '2', '1')

    # add special remote
    init_opts = common_init_opts + [f'url=ria+{store_url}']
    ds.repo.init_remote('store', options=init_opts)

    # copy files into the RIA store
    ds.push('.', to='store')

    # we should see the exact same annex object tree
    dsgit_dir, archive_dir, dsobj_dir = get_layout_locations(
        1, local_store_path, ds.id
    )
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

        create_store(io, archive_store_url_path, '1')
        create_ds_in_store(io, archive_store_url_path, ds.id, '2', '1')

        whereis = ds.repo.whereis('one.txt')
        dsgit_dir, archive_dir, dsobj_dir = get_layout_locations(
            1, local_archive_store_path, ds.id)
        ds.export_archive_ora(archive_dir / 'archive.7z')
        init_opts = common_init_opts + [f'url=ria+{archive_store_url}']
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
def _test_version_check(io, store_url, local_store_path, ds):

    store_url_path = PurePosixPath(urlparse(store_url).path)

    populate_dataset(ds)
    assert_repo_status(ds.path)

    create_store(io, store_url_path, '1')

    # TODO: Re-establish test for version 1
    # version 2: dirhash
    create_ds_in_store(io, store_url_path, ds.id, '2', '1')

    # add special remote
    init_opts = common_init_opts + [f'url={"ria+" + store_url}']
    ds.repo.init_remote('store', options=init_opts)
    ds.push('.', to='store')

    # check version files
    remote_ds_tree_version_file = local_store_path / 'ria-layout-version'
    dsgit_dir, archive_dir, dsobj_dir = get_layout_locations(
        1,
        local_store_path,
        ds.id
    )
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
    with swallow_logs(new_level=logging.INFO, name='datalad.annex') as cml:
        ds.repo.fsck(remote='store', fast=True)
        # TODO: For some reason didn't get cml.assert_logged to assert
        #       "nothing was logged"
        assert not cml.out

    # Now fake-change the version
    with open(str(remote_obj_tree_version_file), 'w') as f:
        f.write('X\n')

    # Now we should see a message about it
    with swallow_logs(new_level=logging.INFO, name='datalad.annex') as cml:
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
def _test_gitannex(host, store, dspath):
    dspath = Path(dspath)
    store = Path(store)
    # PATCH: introduce `ppp_store` and use it instead of `store`
    ppp_store = _local_path2pure_posix_path(store)

    ds = Dataset(dspath).create()

    populate_dataset(ds)
    assert_repo_status(ds.path)

    # set up store:
    io = SSHRemoteIO(host) if host else LocalIO()
    if host:
        store_url = "ria+{host}{path}".format(
            host=host[:-1],
            path=store
        )
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
def test_push_url(storepath=None, dspath=None, blockfile=None):

    dspath = Path(dspath)
    store = Path(storepath)
    # PATCH: introduce `ppp_store` and use it instead of `store`
    ppp_store = _local_path2pure_posix_path(store)
    blockfile = Path(blockfile)
    blockfile.touch()

    ds = Dataset(dspath).create()
    populate_dataset(ds)
    assert_repo_status(ds.path)
    repo = ds.repo

    # set up store:
    io = LocalIO()
    store_url = "ria+{}".format(ppp_store.as_uri())
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


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732, refactored,
# and patched.
# Skipping on adjusted branch as a proxy for crippledFS. Write permissions of
# the owner on a directory can't be revoked on VFAT. "adjusted branch" is a
# bit broad but covers the CI cases. And everything RIA/ORA doesn't currently
# properly run on crippled/windows anyway. Needs to be more precise when
# RF'ing will hopefully lead to support on windows in principle.
@skip_if_adjusted_branch
def _test_permission(io, store_url, local_ria_store_path, ds):

    # Test whether ORA correctly revokes and obtains write permissions within
    # the annex object tree. That is: Revoke after ORA pushed a key to store
    # in order to allow the object tree to safely be used with an ephemeral
    # clone. And on removal obtain write permissions, like annex would
    # internally on a drop (but be sure to restore if something went wrong).

    populate_dataset(ds)
    ds.save()
    assert_repo_status(ds.path)
    testfile = 'one.txt'

    store_url_path = PurePosixPath(urlparse(store_url).path)

    create_store(io, store_url_path, '1')
    create_ds_in_store(io, store_url_path, ds.id, '2', '1')

    _, _, obj_tree = get_layout_locations(1, local_ria_store_path, ds.id)
    assert_true(obj_tree.is_dir())
    file_key_in_store = obj_tree / 'X9' / '6J' / 'MD5E-s8--7e55db001d319a94b0b713529a756623.txt' / 'MD5E-s8--7e55db001d319a94b0b713529a756623.txt'

    init_opts = common_init_opts + [f'url={"ria+" + store_url}']
    ds.repo.init_remote('store', options=init_opts)

    store_uuid = ds.siblings(
        name='store',
        return_type='item-or-list'
    )['annex-uuid']
    here_uuid = ds.siblings(
        name='here',
        return_type='item-or-list'
    )['annex-uuid']

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
    file_key_in_store.parent.chmod(
        file_key_in_store.parent.stat().st_mode & ~stat.S_IWUSR
    )
    # we can't directly delete; key in store should be protected
    assert_raises(PermissionError, file_key_in_store.unlink)

    # ORA can still drop, since it obtains permission to:
    ds.repo.call_annex(['drop', testfile, '--from', 'store'])
    known_sources = ds.repo.whereis(testfile)
    assert_in(here_uuid, known_sources)
    assert_not_in(store_uuid, known_sources)
    assert_false(file_key_in_store.exists())


def _get_host_from_ssh_server(ssh_server):
    url_parts = urlparse(ssh_server[0])
    return (
        'ssh://'
        + ((url_parts.username + '@') if url_parts.username else '')
        + url_parts.hostname
        + ((':' + str(url_parts.port)) if url_parts.port else '')
        + '/'
    )


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@slow  # 17sec + ? on travis
@skip_ssh
def test_version_check_ssh(sshserver, existing_dataset):
    store_dir = _random_name('ria-store-')
    store_url = sshserver[0] + '/' + store_dir
    _test_version_check(
        SSHRemoteIO(store_url),
        store_url,
        sshserver[1] / store_dir,
        existing_dataset
    )


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
def test_version_check(tmp_path, existing_dataset):
    _test_version_check(
        LocalIO(),
        'file://' + (tmp_path / 'ria-store').as_uri(),
        tmp_path / 'ria-store',
        existing_dataset
    )


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@skip_ssh
def test_obtain_permission_ssh(sshserver, existing_dataset):
    store_dir = _random_name('ria-store-')
    store_url = sshserver[0] + '/' + store_dir
    _test_permission(
        SSHRemoteIO(store_url),
        store_url,
        sshserver[1] / store_dir,
        existing_dataset
    )


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@skip_if_root
def test_obtain_permission_root(tmp_path, existing_dataset):
    _test_permission(
        LocalIO(),
        'file://' + (tmp_path / 'ria-store').as_uri(),
        tmp_path / 'ria-store',
        existing_dataset
    )


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
def test_setup_store_local(tmp_path):
    _test_setup_store(
        LocalIO(),
        'file://' + (tmp_path / 'ria-store').as_uri(),
        tmp_path / 'ria-store',
    )


@skip_ssh
def test_setup_store_ssh(sshserver):
    store_dir = _random_name('ria-store-')
    store_url = sshserver[0] + '/' + store_dir
    _test_setup_store(
        SSHRemoteIO(store_url),
        store_url,
        sshserver[1] / store_dir,
    )


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
def test_setup_ds_in_store_local(tmp_path):
    _test_setup_ds_in_store(
        LocalIO(),
        'file://' + (tmp_path / 'ria-store').as_uri(),
        tmp_path / 'ria-store',
    )


@skip_ssh
def test_setup_ds_in_store_ssh(sshserver):
    store_dir = _random_name('ria-store-')
    store_url = sshserver[0] + '/' + store_dir
    _test_setup_ds_in_store(
        SSHRemoteIO(store_url),
        store_url,
        sshserver[1] / store_dir,
    )


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@slow  # 12sec + ? on travis
@skip_ssh
def test_remote_layout_ssh(sshserver, existing_dataset):
    store_dir = _random_name('ria-store-')
    store_url = sshserver[0] + '/' + store_dir
    archive_store_dir = _random_name('ria-archive-store-')
    archive_store_url = sshserver[0] + '/' + archive_store_dir
    _test_remote_layout(
        SSHRemoteIO(store_url),
        store_url,
        archive_store_url,
        sshserver[1] / store_dir,
        sshserver[1] / archive_store_dir,
        existing_dataset,
    )


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
def test_remote_layout(tmp_path, existing_dataset):
    _test_remote_layout(
        LocalIO(),
        'file://' + (tmp_path / 'ria-store').as_uri(),
        'file://' + (tmp_path / 'ria-archive-store').as_uri(),
        tmp_path / 'ria-store',
        tmp_path / 'ria-archive-store',
        existing_dataset,
    )


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
# PATCH remove @know_failure_windows
@skip_ssh
def test_initremote_basic_sshurl(sshserver, tmp_path, existing_dataset):
    store_dir = _random_name('ria-store-')
    store_url = sshserver[0] + '/' + store_dir
    _test_initremote_basic(
        SSHRemoteIO(store_url),
        store_url,
        sshserver[1] / store_dir,
        existing_dataset,
        tmp_path / _random_name('link-'),
    )


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
# PATCH remove @know_failure_windows
def test_initremote_basic_fileurl(tmp_path, existing_dataset):
    store_dir = _random_name('ria-store-')
    _test_initremote_basic(
        LocalIO(),
        'file://' + (tmp_path / store_dir).as_uri(),
        tmp_path / store_dir,
        existing_dataset,
        tmp_path / 'link',
    )


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
# https://github.com/datalad/datalad/issues/6160
@known_failure_windows
def test_initremote_basic_httpurl(http_server, tmp_path, existing_dataset):
    # TODO: add a test for https
    _test_initremote_basic(
        LocalIO(),
        http_server.url,
        http_server.path,
        existing_dataset,
        tmp_path / _random_name('link-'),
        _local_path2pure_posix_path(http_server.path),
    )


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@turtle
# PATCH remove @know_failure_windows
@skip_ssh
def test_gitannex_ssh(sshserver):
    _test_gitannex(_get_host_from_ssh_server(sshserver))


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@slow  # 41sec on travis
def test_gitannex_local():
    _test_gitannex(None)


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
# TODO?: make parametric again on _test_ria_postclonecfg
# PATCH remove @know_failure_windows
@slow  # 14 sec on travis
@known_failure_windows  # https://github.com/datalad/datalad/issues/5134
def test_ria_postclonecfg(sshserver):

    if not has_symlink_capability():
        # This is needed to create an ORA remote using an URL for upload,
        # that is then invalidated later on (delete the symlink it's based on).
        raise SkipTest("Can't create symlinks")

    lcl_dir = _random_name('lcl-')
    store_dir = _random_name('ria-store-')
    store2_dir = _random_name('ria-store-2-')

    lcl = sshserver[1] / lcl_dir
    store = sshserver[1] / store_dir
    store2 = sshserver[1] / store2_dir

    lcl.mkdir(parents=True)
    store.mkdir(parents=True)
    store2.mkdir(parents=True)

    #lcl_url = sshserver[0] + '/' + lcl_dir
    store_url = sshserver[0] + '/' + store_dir
    #store2_url = sshserver[0] + '/' + store2_dir

    id = _postclonetest_prepare(lcl, store, store2)

    # test cloning via ria+file://
    _test_ria_postclonecfg(
        get_local_file_url(store, compatibility='git'), id
    )

    # Note: HTTP disabled for now. Requires proper implementation in ORA
    #       remote. See
    # https://github.com/datalad/datalad/pull/4203#discussion_r410284649

    # # test cloning via ria+http://
    # with HTTPPath(store) as url:
    #     yield _test_ria_postclonecfg, url, id

    # test cloning via ria+ssh://
    skip_ssh(_test_ria_postclonecfg)(store_url, id)


# taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
@with_tempfile
@with_tempfile
def _test_ria_postclonecfg(url, dsid, clone_path, superds):
    # Test cloning from RIA store while ORA special remote autoenabling failed
    # due to an invalid URL from the POV of the cloner.
    # Origin's git-config-file should contain the UUID to enable. This needs to
    # work via HTTP, SSH and local cloning.

    # Autoenabling should fail initially by git-annex-init and we would report
    # on INFO level. Only postclone routine would deal with it.
    with swallow_logs(
            new_level=logging.INFO,
            name='datalad.core.distributed.clone',
    ) as cml:
        # First, the super ds:
        riaclone = clone('ria+{}#{}'.format(url, dsid), clone_path)
        cml.assert_logged(msg="access to 1 dataset sibling store-storage not "
                              "auto-enabled",
                          level="INFO",
                          regex=False)

    # However, we now can retrieve content since clone should have enabled the
    # special remote with new URL (or origin in case of HTTP).
    res = riaclone.get('test.txt')
    assert_result_count(res, 1,
                        status='ok',
                        path=str(riaclone.pathobj / 'test.txt'),
                        message="from {}...".format(DEFAULT_REMOTE
                                                    if url.startswith('http')
                                                    else "store-storage"))

    # Second ORA remote is enabled and not reconfigured:
    untouched_remote = riaclone.siblings(name='anotherstore-storage',
                                         return_type='item-or-list')
    assert_not_is_instance(untouched_remote, list)
    untouched_url = riaclone.repo.get_special_remotes()[
        untouched_remote['annex-uuid']]['url']
    ok_(untouched_url.startswith("ria+file://"))
    ok_(not untouched_url.startswith("ria+{}".format(url)))

    # publication dependency was set for store-storage but not for
    # anotherstore-storage:
    eq_(riaclone.config.get(f"remote.{DEFAULT_REMOTE}.datalad-publish-depends",
                            get_all=True),
        "store-storage")

    # same thing for the sub ds (we don't need a store-url and id - get should
    # figure those itself):
    with swallow_logs(
            new_level=logging.INFO,
            name='datalad.core.distributed.clone',
    ) as cml:
        riaclonesub = riaclone.get(
            op.join('subdir', 'subds'), get_data=False,
            result_xfm='datasets', return_type='item-or-list')
        cml.assert_logged(msg="access to 1 dataset sibling store-storage not "
                              "auto-enabled",
                          level="INFO",
                          regex=False)
    res = riaclonesub.get('testsub.txt')
    assert_result_count(res, 1,
                        status='ok',
                        path=str(riaclonesub.pathobj / 'testsub.txt'),
                        message="from {}...".format(DEFAULT_REMOTE
                                                    if url.startswith('http')
                                                    else "store-storage"))

    # publication dependency was set for store-storage but not for
    # anotherstore-storage:
    eq_(riaclonesub.config.get(f"remote.{DEFAULT_REMOTE}.datalad-publish-depends",
                               get_all=True),
        "store-storage")

    # finally get the plain git subdataset.
    # Clone should figure to also clone it from a ria+ URL
    # (subdataset-source-candidate), notice that there wasn't an autoenabled ORA
    # remote, but shouldn't stumble upon it, since it's a plain git.
    res = riaclone.get(op.join('subdir', 'subgit', 'testgit.txt'))
    assert_result_count(res, 1, status='ok', type='dataset', action='install')
    assert_result_count(res, 1, status='notneeded', type='file')
    assert_result_count(res, 2)
    # no ORA remote, no publication dependency:
    riaclonesubgit = Dataset(riaclone.pathobj / 'subdir' / 'subgit')
    eq_(riaclonesubgit.config.get(f"remote.{DEFAULT_REMOTE}.datalad-publish-depends",
                                  get_all=True),
        None)

    # Now, test that if cloning into a dataset, ria-URL is preserved and
    # post-clone configuration is triggered again, when we remove the subds and
    # retrieve it again via `get`:
    ds = Dataset(superds).create()
    ria_url = 'ria+{}#{}'.format(url, dsid)
    ds.clone(ria_url, 'sub')
    sds = ds.subdatasets('sub')
    eq_(len(sds), 1)
    eq_(sds[0]['gitmodule_datalad-url'], ria_url)
    assert_repo_status(ds.path)
    ds.drop('sub', what='all', reckless='kill', recursive=True)
    assert_repo_status(ds.path)

    # .gitmodules still there:
    sds = ds.subdatasets('sub')
    eq_(len(sds), 1)
    eq_(sds[0]['gitmodule_datalad-url'], ria_url)
    # get it again:

    # Autoenabling should fail initially by git-annex-init and we would report
    # on INFO level. Only postclone routine would deal with it.
    with swallow_logs(
            new_level=logging.INFO,
            name='datalad.core.distributed.clone',
    ) as cml:
        ds.get('sub', get_data=False)
        cml.assert_logged(msg="access to 1 dataset sibling store-storage not "
                              "auto-enabled",
                          level="INFO",
                          regex=False)

    subds = Dataset(ds.pathobj / 'sub')
    # special remote is fine:
    res = subds.get('test.txt')
    assert_result_count(res, 1,
                        status='ok',
                        path=str(subds.pathobj / 'test.txt'),
                        message="from {}...".format(DEFAULT_REMOTE
                                                    if url.startswith('http')
                                                    else "store-storage"))
