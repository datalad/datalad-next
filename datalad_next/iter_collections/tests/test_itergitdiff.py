from pathlib import PurePosixPath
import pytest
import shutil

from datalad_next.utils import rmtree

from ..gitdiff import (
    GitTreeItemType,
    GitDiffStatus,
    iter_gitdiff,
)


def test_iter_gitdiff_invalid():
    with pytest.raises(ValueError):
        # no meaningful comparison
        list(iter_gitdiff('.', None, None))
    with pytest.raises(ValueError):
        # unsupported eval mode
        list(iter_gitdiff('.', None, None, eval_submodule_state='weird'))


def test_iter_gitdiff_basic(existing_dataset, no_result_rendering):
    ds = existing_dataset
    dsp = ds.pathobj
    # we compare based on the last state of the corresponding
    # branch if there is any, or the HEAD of the current
    # branch
    comp_base = ds.repo.get_corresponding_branch() or 'HEAD'
    # we use two distinct content blobs below, hardcode sha here
    # for readability
    empty_sha = 'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391'
    content = '123'
    content_sha = 'd800886d9c86731ae5c4a62b0b77c437015e00d2'
    status_args = (
        # we always test against the root of the dataset
        dsp,
        comp_base,
        # we always compare to the worktree
        None,
    )
    diff_args = (
        # we always test against the root of the dataset
        dsp,
        # we always compare to last committed state
        f'{comp_base}~1', comp_base,
    )
    # clean dataset, no items
    assert list(iter_gitdiff(*status_args)) == []
    testpath = dsp / 'sub' / 'test'
    testpath.parent.mkdir()
    testpath.touch()
    # dataset with untracked file, no items
    assert list(iter_gitdiff(*status_args)) == []
    ds.save(to_git=True)
    # clean dataset again, no items
    assert list(iter_gitdiff(*status_args)) == []
    # added file
    diff = list(iter_gitdiff(*diff_args))
    assert len(diff) == 1
    di = diff[0]
    assert di.status == GitDiffStatus.addition
    assert di.name == 'sub/test'
    assert di.prev_name is di.prev_gitsha is di.prev_gittype is None
    assert di.gitsha == empty_sha
    assert di.gittype == GitTreeItemType.file
    # modified file
    testpath.write_text(content)
    diff = list(iter_gitdiff(*status_args))
    assert len(diff) == 1
    di = diff[0]
    # labeled as modified
    assert di.status == GitDiffStatus.modification
    # the name is plain str in POSIX
    assert di.name == di.prev_name == 'sub/test'
    # path conversion yield POSIX relpath
    assert di.path == di.prev_path == PurePosixPath(testpath.relative_to(dsp))
    # unstaged modification reports no shasum
    assert di.gitsha is None
    assert di.prev_gitsha == empty_sha
    assert di.gittype == di.prev_gittype == GitTreeItemType.file
    # make clean
    ds.save(to_git=True)
    moved_testpath = testpath.parent / 'moved_test'
    testpath.rename(moved_testpath)
    # renamed file, unstaged, reported as deletion, we do not see the addition
    # yet (untracked)
    diff = list(iter_gitdiff(*status_args))
    assert len(diff) == 1
    di = diff[0]
    assert di.status == GitDiffStatus.deletion
    assert di.name == di.prev_name == 'sub/test'
    assert di.prev_gitsha == content_sha
    assert di.prev_gittype == GitTreeItemType.file
    assert di.gitsha is di.gittype is None
    # make clean
    ds.save(to_git=True)
    # now we can look at the rename
    diff = list(iter_gitdiff(*diff_args, find_renames=100))
    assert len(diff) == 1
    di = diff[0]
    assert di.status == GitDiffStatus.rename
    assert di.name == 'sub/moved_test'
    assert di.prev_name == 'sub/test'
    assert di.gitsha == di.prev_gitsha == content_sha
    assert di.prev_gittype is di.gittype is GitTreeItemType.file
    assert di.percentage == 100
    # now a copy
    shutil.copyfile(moved_testpath, testpath)
    ds.save(to_git=True)
    diff = list(iter_gitdiff(*diff_args, find_copies=100))
    assert len(diff) == 1
    di = diff[0]
    assert di.status == GitDiffStatus.copy
    assert di.name == 'sub/test'
    assert di.prev_name == 'sub/moved_test'
    assert di.gitsha == di.prev_gitsha == content_sha
    assert di.percentage == 100
    # now replace file with submodule
    testpath.unlink()
    # we must safe to appease datalad's content collision detection
    ds.save(to_git=True)
    # intermediate smoke test for describing a single tree (diff from parents)
    diff = list(iter_gitdiff(dsp, None, comp_base))
    assert len(diff) == 1
    assert diff[0].status == GitDiffStatus.deletion
    # now cause typechange
    ds.create(testpath)
    diff = list(iter_gitdiff(
        dsp,
        # because we have an intermediate safe, compare to two states
        # back
        f'{comp_base}~2', comp_base,
    ))
    assert len(diff) == 2
    # let's ignore the uninteresting .gitmodules addition for further tests
    di = [i for i in diff if i.name != '.gitmodules'][0]
    assert di.status == GitDiffStatus.typechange
    assert di.name == di.prev_name == 'sub/test'
    assert di.gitsha != di.prev_gitsha
    assert di.prev_gitsha == content_sha
    assert di.prev_gittype == GitTreeItemType.file
    assert di.gittype == GitTreeItemType.submodule


def test_iter_gitdiff_nonroot(existing_dataset, no_result_rendering):
    ds = existing_dataset
    comp_base = ds.repo.get_corresponding_branch() or 'HEAD'
    # all tests are concerned with running not in the dataset root
    root = ds.pathobj
    nonroot = root / 'sub'
    nonroot.mkdir()
    status_args = (nonroot, comp_base, None)
    diff_args = (nonroot, f'{comp_base}~1', comp_base)

    # nothing to report, no problem
    assert list(iter_gitdiff(*status_args)) == []
    # change above CWD is not reported
    (root / 'rootfile').touch()
    ds.save(to_git=True)
    assert list(iter_gitdiff(*diff_args)) == []
    # check worktree modification detection too
    (root / 'rootfile').write_text('some')
    assert list(iter_gitdiff(*status_args)) == []
    # and now test that reporting is relative to
    # CWD
    (nonroot / 'nonrootfile').touch()
    ds.save(to_git=True)
    assert list(iter_gitdiff(*diff_args))[0].name == 'nonrootfile'
    (nonroot / 'nonrootfile').write_text('other')
    assert list(iter_gitdiff(*diff_args))[0].name == 'nonrootfile'


def test_iter_gitdiff_nonrec(existing_dataset, no_result_rendering):
    ds = existing_dataset
    dsp = ds.pathobj
    comp_base = ds.repo.get_corresponding_branch() or 'HEAD'
    subdir = dsp / 'sub'
    subdir.mkdir()
    for fn in ('f1.txt', 'f2.txt'):
        (subdir / fn).touch()
    ds.save(to_git=True)
    diff = list(iter_gitdiff(dsp, f'{comp_base}~1', comp_base, recursive='no'))
    assert len(diff) == 1
    di = diff[0]
    assert di.name == 'sub'
    assert di.gittype == GitTreeItemType.directory
    assert di.status == GitDiffStatus.addition
    di_tree = di
    # same behavior for a worktree modification
    for fn in ('f1.txt', 'f2.txt'):
        (subdir / fn).write_text('modified')
    diff = list(iter_gitdiff(dsp, f'{comp_base}~1', None, recursive='no'))
    assert len(diff) == 1
    di = diff[0]
    # these are identical to the diff-tree based report
    for p in ('name', 'gittype', 'prev_gitsha', 'prev_gittype'):
        assert getattr(di, p) == getattr(di_tree, p)
    # and there are different
    # not staged, no gitsha
    assert di.gitsha is None
    # it does no type inference for the previous state (expensive)
    assert di.prev_gittype is None

    # when the directory existed in the from-state it becomes a
    # modification
    diff = list(iter_gitdiff(dsp, f'{comp_base}~1', None, recursive='no'))
    assert len(diff) == 1
    diff[0].status == GitDiffStatus.modification

    # now remove the subdir
    rmtree(subdir)
    diff = list(iter_gitdiff(dsp, comp_base, None, recursive='no'))
    assert len(diff) == 1
    # it still reports a modification, even though the directory is empty/gone.
    # it would require a filesystem STAT to detect a deletion, and a further
    # type investigation in `from_treeish` to detect a type change.
    # This is not done until there is evidence for a real use case
    diff[0].status == GitDiffStatus.modification


def test_iter_gitdiff_typechange_issue6791(
        existing_dataset, no_result_rendering):
    # verify that we can handle to problem described in
    # https://github.com/datalad/datalad/issues/6791
    #
    # a subdataset is wiped out (uncommitted) and replaced by a file
    ds = existing_dataset
    ds.create('subds')
    rmtree(ds.pathobj / 'subds')
    (ds.pathobj / 'subds').touch()
    diff = list(iter_gitdiff(
        ds.pathobj,
        ds.repo.get_corresponding_branch() or 'HEAD', None,
    ))
    assert len(diff) == 1
    di = diff[0]
    assert di.status == GitDiffStatus.typechange
    assert di.name == di.prev_name == 'subds'
    # unstaged change
    assert di.gitsha is None
    assert di.prev_gittype == GitTreeItemType.submodule
    assert di.gittype == GitTreeItemType.file


def test_iter_gitdiff_rec(existing_dataset, no_result_rendering):
    ds = existing_dataset
    subds = ds.create('subds')
    dsp = ds.pathobj
    comp_base = ds.repo.get_corresponding_branch() or 'HEAD'
    status_args = (dsp, comp_base, None)
    diff_args = (dsp, f'{comp_base}~1', comp_base)

    diff = list(iter_gitdiff(*diff_args, recursive='submodules'))
    # we get more than just .gitmodules and a submodule record
    assert len(diff) > 2
    # the entire submodule is new and the first one, so everything
    # is an addition
    assert all(i.status == GitDiffStatus.addition for i in diff)
    # only files, no submodule record, by default
    assert all(i.gittype == GitTreeItemType.file for i in diff)

    # when we ask for it, we get the submodule item too
    diff_w_sm = list(iter_gitdiff(*diff_args,
                                  recursive='submodules',
                                  yield_tree_items='submodules'))
    assert len(diff) + 1 == len(diff_w_sm)
    assert any(i.name == 'subds' and i.gittype == GitTreeItemType.submodule
               for i in diff_w_sm)

    # smoke test for an all-clean diff against the worktrees
    assert list(iter_gitdiff(*status_args, recursive='submodules')) == []

    # make subdataset record modified
    (subds.pathobj / 'file').touch()
    subds.save(to_git=True)
    diff = list(iter_gitdiff(*status_args, recursive='submodules'))
    assert len(diff) == 1
    di = diff[0]
    assert di.name == 'subds/file'
    assert di.status == GitDiffStatus.addition
    # now with submodule item
    diff_w_sm = list(iter_gitdiff(*status_args,
                                  recursive='submodules',
                                  yield_tree_items='all'))
    assert len(diff_w_sm) == 2
    di = diff_w_sm[0]
    # the submodule item is always first
    assert di.name == 'subds'
    assert di.gittype == GitTreeItemType.submodule
    assert di.status == GitDiffStatus.modification
    assert diff_w_sm[1] == diff[0]

    # safe the whole hierarchy
    ds.save(recursive=True)
    # we get the exact same change report via the diff to HEAD~1:HEAD
    assert diff == list(iter_gitdiff(*diff_args, recursive='submodules'))

    # modify a tracked file in the subdataset
    (subds.pathobj / 'file').write_text('123')
    diff_w_sm = list(iter_gitdiff(*status_args,
                                  recursive='submodules',
                                  yield_tree_items='all'))
    # same report for the submodule (and it is first again)
    assert diff_w_sm[0].name == 'subds'
    assert diff_w_sm[0].gittype == GitTreeItemType.submodule
    assert diff_w_sm[0].status == GitDiffStatus.modification
    # but this time the file is not an addition but a modification
    assert diff_w_sm[1].name == 'subds/file'
    assert diff_w_sm[1].status == GitDiffStatus.modification

    # force-wipe the subdataset, and create a condition where the subdatasets
    # is expected but missing
    rmtree(subds.pathobj)
    diff = list(iter_gitdiff(*status_args))
    assert len(diff) == 1
    di = diff[0]
    assert di.name == 'subds'
    assert di.status == GitDiffStatus.deletion
    # if we now run with recursion, we get the exact same result, the absent
    # submodule is a subtree that we do not recurse into, hence the report
    # is only on the tree itself
    assert diff == list(iter_gitdiff(*status_args, recursive='submodules'))
    # use the opportunity to check equality of recursive='all' for this case
    assert diff == list(iter_gitdiff(*status_args, recursive='all'))
