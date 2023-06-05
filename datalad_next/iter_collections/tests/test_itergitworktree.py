from pathlib import PurePath
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
