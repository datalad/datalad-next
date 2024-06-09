from pathlib import (
    PurePath,
    PurePosixPath,
)

import pytest

from datalad_next.utils import (
    check_symlink_capability,
    rmtree,
)

from ..gittree import (
    GitTreeItemType,
)
from ..gitworktree import (
    GitWorktreeItem,
    GitWorktreeFileSystemItem,
    iter_gitworktree,
    iter_submodules,
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


def test_iter_gitworktree_recursive(existing_dataset, no_result_rendering):
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


def test_iter_gitworktree_deadsymlinks(existing_dataset, no_result_rendering):
    ds = existing_dataset
    dpath = ds.pathobj / 'subdir'
    dpath.mkdir()
    fpath = dpath / 'file1'
    test_content = 'content'
    fpath.write_text(test_content)
    ds.save()
    ds.drop(fpath, reckless='availability')
    try:
        # if there is a file we can open, it should not have the content
        # (annex pointer file)
        assert fpath.read_text() != test_content
    except FileNotFoundError:
        # with dead symlinks, we end up here and that is normal
        pass
    # next one must not crash
    all_items = list(iter_gitworktree(dpath))
    # we get our "dead symlink" -- but depending on the p[latform
    # it may take a different form, hence not checking for type
    assert len(all_items) == 1
    assert all_items[0].name == PurePath('file1')


def prep_fp_tester(ds):
    # we expect to process an exact number of files below
    # 3 annexed files, 1 untracked, 1 in git,
    # and possibly 1 symlink in git, 1 symlink untracked
    # we count them up on creation, and then down on test
    fcount = 0

    content_tmpl = 'content: #ö file_{}'
    for i in ('annex1', 'annex2', 'annex3'):
        (ds.pathobj / f'file_{i}').write_text(
            content_tmpl.format(i), encoding='utf-8')
        fcount += 1
    ds.save()
    ds.drop(
        ds.pathobj / 'file_annex1',
        reckless='availability',
    )
    # and also add a file to git directly and a have one untracked too
    for i in ('untracked', 'ingit', 'deleted'):
        (ds.pathobj / f'file_{i}').write_text(
            content_tmpl.format(i), encoding='utf-8')
        fcount += 1
    ds.save(['file_ingit', 'file_deleted'], to_git=True)
    # and add symlinks (untracked and in git)
    if check_symlink_capability(
        ds.pathobj / '_dummy', ds.pathobj / '_dummy_target'
    ):
        for i in ('symlinkuntracked', 'symlinkingit'):
            tpath = ds.pathobj / f'target_{i}'
            lpath = ds.pathobj / f'file_{i}'
            tpath.write_text(
                content_tmpl.format(i), encoding='utf-8')
            lpath.symlink_to(tpath)
            fcount += 1
    ds.save('file_symlinkingit', to_git=True)
    (ds.pathobj / 'file_deleted').unlink()
    return fcount, content_tmpl


def test_iter_gitworktree_basic_fp(existing_dataset, no_result_rendering):
    ds = existing_dataset
    fcount, content_tmpl = prep_fp_tester(ds)

    for ai in filter(
        lambda i: i.name.name.startswith('file_'),
        iter_gitworktree(ds.pathobj, fp=True)
    ):
        fcount -= 1
        if getattr(ai, 'fp', False):
            # for annexed files the fp can be an annex pointer file.
            # in the context of `iter_gitworktree` this is not a
            # recognized construct
            assert content_tmpl.format(
                ai.name.name[5:]) == ai.fp.read().decode() \
                or ai.name.name.startswith('file_annex')
        else:
            assert (ds.pathobj / ai.name).exists() is False
    assert not fcount


def test_iter_gitworktree_untracked_only(modified_dataset):
    p = modified_dataset.pathobj
    # only untracked files
    repo_items = list(iter_gitworktree(p, untracked='only'))
    assert all(f.name.name == 'file_u' for f in repo_items)
    # same report, but compressed to immediate directory children
    dir_items = list(iter_gitworktree(p, untracked='only', recursive='no'))
    assert set(f.name.parts[0] for f in repo_items) == \
        set(f.name.name for f in dir_items)
    # no wholly untracked directories in standard report
    assert not any(f.name.name == 'dir_u'
                   for f in iter_gitworktree(p, untracked='only'))
    # but this can be requested
    wholedir_items = list(iter_gitworktree(p, untracked='only-whole-dir'))
    assert any(f.name.name == 'dir_u' for f in wholedir_items)
    # smoke test remaining mode, test case doesn't cause difference
    assert any(f.name.name == 'dirempty_u' for f in wholedir_items)
    assert not any(f.name.name == 'dirempty_u'
                   for f in iter_gitworktree(p, untracked='only-no-empty-dir'))



def test_iter_gitworktree_pathspec(modified_dataset):
    p = modified_dataset.pathobj
    # query for any files that are set to go straight to Git. these are just
    # dotfiles in the default config
    items = list(iter_gitworktree(
        p,
        pathspecs=[':(attr:annex.largefiles=nothing)']))
    assert items
    assert all(str(i.name).startswith('.') for i in items)
    # glob-styles
    # first some that only give a top-level match
    assert len(list(iter_gitworktree(p, pathspecs=['file_a']))) == 1
    assert len(list(iter_gitworktree(p, pathspecs=[':(glob)*file_a']))) == 1
    # now some that match at any depth
    assert len(list(iter_gitworktree(p, pathspecs=['*file_a']))) == 2
    assert len(list(iter_gitworktree(p, pathspecs=[':(glob)**/file_a']))) == 2


def test_iter_submodules(modified_dataset):
    p = modified_dataset.pathobj
    all_sm = list(iter_submodules(p))
    assert all_sm
    assert all(sm.gittype == GitTreeItemType.submodule for sm in all_sm)
    assert all(str(sm.path.parent) == 'dir_sm' for sm in all_sm)
    assert sorted([str(sm.path.name) for sm in all_sm]) \
        == ['droppedsm_c', 'sm_c', 'sm_d', 'sm_m', 'sm_mu', 'sm_n',
            'sm_nm', 'sm_nmu', 'sm_u']
    # constrain by pathspec
    res = list(iter_submodules(p, pathspecs=['*/sm_c']))
    assert len(res) == 1
    assert res[0].name == PurePath('dir_sm', 'sm_c')
    # test negative condition
    res = list(iter_submodules(p, pathspecs=[':(exclude)*/sm_c']))
    assert len(res) == len(all_sm) - 1
    assert not any(r.name == PurePath('dir_sm', 'sm_c') for r in res)

    # test pathspecs matching inside submodules
    # baseline, pointing inside a submodule gives no matching results
    assert not list(iter_submodules(p, pathspecs=['dir_sm/sm_c/.datalad']))
    # we can discover the submodule that could have content that matches
    # the pathspec
    res = list(iter_submodules(p, pathspecs=['dir_sm/sm_c/.datalad'],
                               match_containing=True))
    assert len(res) == 1
    assert res[0].name == PurePath('dir_sm', 'sm_c')
    # if we use a wildcard that matches any submodule, we also get all of them
    # and this includes the dropped submodule, because iter_submodules()
    # make no assumptions on what this information will be used for
    res = list(iter_submodules(p, pathspecs=['*/.datalad'],
                               match_containing=True))
    assert len(res) == len(all_sm)
