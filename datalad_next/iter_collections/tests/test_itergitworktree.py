from pathlib import (
    PurePath,
    PurePosixPath,
)

import pytest

from datalad_next.tests.utils import rmtree

from ..gitworktree import (
    GitWorktreeItem,
    GitWorktreeFileSystemItem,
    iter_gitworktree,
)


def test_iter_gitworktree(existing_dataset):
    ds = existing_dataset

    (ds.pathobj / 'emptydir').mkdir()
    untracked = ds.pathobj / 'subdir' / 'untracked'
    untracked.parent.mkdir()
    untracked.write_text('untracked')

    tracked_items = list(iter_gitworktree(ds.pathobj, untracked=None))
    # without untracked's and no link resolution this is plain and fast
    assert all(
        isinstance(i, GitWorktreeItem) and i.gitsha and i.gittype
        for i in tracked_items
    )

    all_items = list(iter_gitworktree(ds.pathobj, untracked='all'))
    # empty-dir is not reported, only untracked files
    assert len(all_items) == len(tracked_items) + 1
    assert any(
        i.name == PurePath('subdir', 'untracked')
        and i.gitsha is None and i.gittype is None
        for i in all_items
    )
    # same again, but with a different untracked reporting
    all_items = list(iter_gitworktree(ds.pathobj, untracked='whole-dir'))
    # emptydir is reported too
    assert len(all_items) == len(tracked_items) + 2
    assert any(
        i.name == PurePath('subdir')
        and i.gitsha is None and i.gittype is None
        for i in all_items
    )
    # and again for the last variant
    all_items = list(iter_gitworktree(ds.pathobj, untracked='no-empty-dir'))
    # and again no emptydir
    assert len(all_items) == len(tracked_items) + 1
    assert any(
        i.name == PurePath('subdir')
        and i.gitsha is None and i.gittype is None
        for i in all_items
    )

    # if we ask for file objects or link_targets, we get a different item type,
    # but including the same
    for kwargs in (
            dict(link_target=True, fp=False, untracked=None),
            dict(link_target=False, fp=True, untracked=None),
            dict(link_target=True, fp=True, untracked=None),
    ):
        assert all(
            isinstance(i, GitWorktreeFileSystemItem) and i.gitsha and i.gittype
            for i in iter_gitworktree(ds.pathobj, **kwargs)
        )

    # check that file pointers work for tracked and untracked content
    checked_tracked = False
    checked_untracked = False
    for item in iter_gitworktree(ds.pathobj, fp=True):
        if item.name == PurePath('.datalad', 'config'):
            assert ds.id in (ds.pathobj / item.name).read_text()
            checked_tracked = True
        elif item.name == PurePath('subdir', 'untracked'):
            assert 'untracked' == (ds.pathobj / item.name).read_text()
            checked_untracked = True
    assert checked_tracked
    assert checked_untracked


def test_name_starting_with_tab(existing_dataset, no_result_rendering):
    ds = existing_dataset
    if ds.repo.is_crippled_fs():
        pytest.skip("not applicable on crippled filesystems")
    tabbed_file_name = "\ttab.txt"
    tabbed_name = ds.pathobj / tabbed_file_name
    tabbed_name.write_text('name of this file starts with a tab')
    ds.save()

    iter_names = [item.name for item in iter_gitworktree(ds.pathobj)]
    assert PurePosixPath(tabbed_file_name) in iter_names


def test_iter_gitworktree_recursive(existing_dataset):
    # actually, this tests non-recursive, because within-repo
    # recursion is the default.
    # later, we might also test subdataset recursion here
    ds = existing_dataset
    # some tracked content
    tracked1 = ds.pathobj / 'tracked1'
    tracked2 = ds.pathobj / 'subdir' / 'tracked2'
    tracked3 = ds.pathobj / 'subdir' / 'tracked3'
    for p in (tracked1, tracked2, tracked3):
        p.parent.mkdir(exist_ok=True)
        p.write_text(p.name)
    ds.save()

    # an "invisible" directory (no content)
    (ds.pathobj / 'emptydir').mkdir()
    # untracked file in subdir
    untracked = ds.pathobj / 'subdir_u' / 'untracked'
    untracked.parent.mkdir()
    untracked.write_text('untracked')

    # matches git report with untracked=all
    all_content = set((
        PurePath('.datalad'),
        PurePath('subdir'),
        PurePath('.gitattributes'),
        PurePath('subdir_u'),
        PurePath('tracked1'),
    ))
    # without any recursion, we see all top-level content, except for
    # the empty directory with no content
    all_items = list(iter_gitworktree(ds.pathobj, recursive='no'))
    assert set(i.name for i in all_items) == all_content

    # no we test a query that gooey would want to make,
    # give me all content in a single directory, and also include any
    # untracked files and even untracked/empty directories
    all_items = list(iter_gitworktree(ds.pathobj, recursive='no',
                                      untracked='whole-dir'))
    assert set(i.name for i in all_items) == \
        all_content.union((PurePath('emptydir'),))


def test_iter_gitworktree_empty(existing_dataset, no_result_rendering):
    ds = existing_dataset
    rmtree(ds.pathobj / '.datalad')
    (ds.pathobj / '.gitattributes').unlink()
    ds.save()
    assert len(ds.status()) == 0
    all_items = list(iter_gitworktree(ds.pathobj))
    assert len(all_items) == 0
