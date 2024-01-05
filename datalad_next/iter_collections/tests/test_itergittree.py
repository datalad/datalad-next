from pathlib import (
    PurePosixPath,
)

import pytest

from datalad_next.tests.utils import rmtree

from ..gittree import (
    GitTreeItem,
    GitTreeItemType,
    iter_gittree,
)


def test_iter_gittree(existing_dataset, no_result_rendering):
    ds = existing_dataset

    tracked_items = list(iter_gittree(ds.pathobj, 'HEAD'))
    # without untracked's and no link resolution this is plain and fast
    assert all(
        isinstance(i, GitTreeItem) and i.gitsha and i.gittype
        for i in tracked_items
    )
    # we add a new file and test its expected properties
    probe_name = 'probe.txt'
    probe = ds.pathobj / 'subdir' / probe_name
    probe.parent.mkdir()
    probe.write_text('probe')
    ds.save()
    assert any(
        i.name == PurePosixPath(f'subdir/{probe_name}')
        and i.gitsha == '7c38bf0378c31f8696e5869e7828a32c9dc2684e'
        and i.gittype == GitTreeItemType.symlink
        for i in iter_gittree(ds.pathobj, 'HEAD')
    )
    # if we check the prior version, we do not see it (hence the
    # tree-ish passing is working
    assert not any(
        i.name == PurePosixPath(f'subdir/{probe_name}')
        for i in iter_gittree(ds.pathobj, 'HEAD~1')
    )

    # if we disable recursion, the probe is not listed, but its
    # parent dir is
    tracked_toplevel_items = list(
        iter_gittree(ds.pathobj, 'HEAD', recursive='no'))
    assert not any(
        i.name == PurePosixPath(f'subdir/{probe_name}')
        for i in tracked_toplevel_items
    )
    assert any(
        i.name == PurePosixPath('subdir')
        and i.gitsha == 'eb4aa65f42b90178837350571a227445b996cf90'
        and i.gittype == GitTreeItemType.directory
        for i in tracked_toplevel_items
    )
    # iterating on a subdir does constrain the report
    tracked_subdir_items = list(iter_gittree(probe.parent, 'HEAD'))
    assert len(tracked_subdir_items) == 1
    probe_item = tracked_subdir_items[0]
    assert probe_item.name == PurePosixPath(probe_name)
    assert probe_item.gitsha == '7c38bf0378c31f8696e5869e7828a32c9dc2684e'


def test_name_starting_with_tab(existing_dataset, no_result_rendering):
    ds = existing_dataset
    if ds.repo.is_crippled_fs():
        pytest.skip("not applicable on crippled filesystems")
    tabbed_file_name = "\ttab.txt"
    tabbed_name = ds.pathobj / tabbed_file_name
    tabbed_name.write_text('name of this file starts with a tab')
    ds.save()
    iter_names = [item.name for item in iter_gittree(ds.pathobj, 'HEAD')]
    assert PurePosixPath(tabbed_file_name) in iter_names


def test_iter_gittree_empty(existing_dataset, no_result_rendering):
    ds = existing_dataset
    rmtree(ds.pathobj / '.datalad')
    (ds.pathobj / '.gitattributes').unlink()
    ds.save()
    assert len(ds.status()) == 0
    all_items = list(iter_gittree(ds.pathobj, 'HEAD'))
    assert len(all_items) == 0
