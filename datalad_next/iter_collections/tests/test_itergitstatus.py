from itertools import chain
import pytest

from datalad_next.datasets import Dataset
from datalad_next.runners import call_git_success
from datalad_next.tests.utils import rmtree

from ..gitstatus import (
    GitDiffStatus,
    GitContainerModificationType,
    iter_gitstatus,
)


# we make this module-scope, because we use the same complex test case for all
# tests here and we trust that nothing in here changes that test case
@pytest.fixture(scope="module")
def status_playground(tmp_path_factory):
    """Produces a dataset with various modifications

    ``git status`` will report::

        ❯ git status -uall
        On branch dl-test-branch
        Changes to be committed:
          (use "git restore --staged <file>..." to unstage)
                new file:   dir_m/file_a
                new file:   file_a

        Changes not staged for commit:
          (use "git add/rm <file>..." to update what will be committed)
          (use "git restore <file>..." to discard changes in working directory)
          (commit or discard the untracked or modified content in submodules)
                deleted:    dir_d/file_d
                deleted:    dir_m/file_d
                modified:   dir_m/file_m
                deleted:    dir_sm/sm_d
                modified:   dir_sm/sm_m (modified content)
                modified:   dir_sm/sm_mu (modified content, untracked content)
                modified:   dir_sm/sm_n (new commits)
                modified:   dir_sm/sm_nm (new commits, modified content)
                modified:   dir_sm/sm_nmu (new commits, modified content, untracked content)
                modified:   dir_sm/sm_u (untracked content)
                deleted:    file_d
                modified:   file_m

        Untracked files:
          (use "git add <file>..." to include in what will be committed)
                dir_m/dir_u/file_u
                dir_m/file_u
                dir_u/file_u
                file_u

    Suffix indicates the ought-to state (multiple possible):

    a - added
    c - clean
    d - deleted
    n - new commits
    m - modified
    u - untracked content

    Prefix indicated the item type:

    file - file
    sm - submodule
    dir - directory
    """
    ds = Dataset(tmp_path_factory.mktemp("status_playground"))
    ds.create(result_renderer='disabled')
    ds_dir = ds.pathobj / 'dir_m'
    ds_dir.mkdir()
    ds_dir_d = ds.pathobj / 'dir_d'
    ds_dir_d.mkdir()
    (ds_dir / 'file_m').touch()
    (ds.pathobj / 'file_m').touch()
    dirsm = ds.pathobj / 'dir_sm'
    dss = {}
    for smname in (
        'sm_d', 'sm_c', 'sm_n', 'sm_m', 'sm_nm', 'sm_u', 'sm_mu', 'sm_nmu',
        'droppedsm_c',
    ):
        sds = Dataset(dirsm / smname).create(result_renderer='disabled')
        # for the plain modification, commit the reference right here
        if smname in ('sm_m', 'sm_nm', 'sm_mu', 'sm_nmu'):
            (sds.pathobj / 'file_m').touch()
        sds.save(to_git=True, result_renderer='disabled')
        dss[smname] = sds
    # files in superdataset to be deleted
    for d in (ds_dir_d, ds_dir, ds.pathobj):
        (d / 'file_d').touch()
    dss['.'] = ds
    dss['dir'] = ds_dir
    ds.save(to_git=True, result_renderer='disabled')
    ds.drop(dirsm / 'droppedsm_c', what='datasets', reckless='availability',
            result_renderer='disabled')
    # a new commit
    for smname in ('.', 'sm_n', 'sm_nm', 'sm_nmu'):
        sds = dss[smname]
        (sds.pathobj / 'file_c').touch()
        sds.save(to_git=True, result_renderer='disabled')
    # modified file
    for smname in ('.', 'dir', 'sm_m', 'sm_nm', 'sm_mu', 'sm_nmu'):
        obj = dss[smname]
        pobj = obj.pathobj if isinstance(obj, Dataset) else obj
        (pobj / 'file_m').write_text('modify!')
    # untracked
    for smname in ('.', 'dir', 'sm_u', 'sm_mu', 'sm_nmu'):
        obj = dss[smname]
        pobj = obj.pathobj if isinstance(obj, Dataset) else obj
        (pobj / 'file_u').touch()
        (pobj / 'dirempty_u').mkdir()
        (pobj / 'dir_u').mkdir()
        (pobj / 'dir_u' / 'file_u').touch()
    # delete items
    rmtree(dss['sm_d'].pathobj)
    rmtree(ds_dir_d)
    (ds_dir / 'file_d').unlink()
    (ds.pathobj / 'file_d').unlink()
    # added items
    for smname in ('.', 'dir', 'sm_m', 'sm_nm', 'sm_mu', 'sm_nmu'):
        obj = dss[smname]
        pobj = obj.pathobj if isinstance(obj, Dataset) else obj
        (pobj / 'file_a').write_text('added')
        assert call_git_success(['add', 'file_a'], cwd=pobj)

    yield ds


def test_status_homogeneity(status_playground):
    """Test things that should always be true, no matter the precise
    parameterization

    A main purpose of this test is also to exercise all (main) code paths.
    """
    ds = status_playground
    for kwargs in (
        # default
        dict(path=ds.pathobj),
        dict(path=ds.pathobj, recursive='no'),
        dict(path=ds.pathobj, recursive='repository'),
        dict(path=ds.pathobj, recursive='submodules'),
        # same as above, but with the submodules in the root
        dict(path=ds.pathobj / 'dir_sm', recursive='no'),
        dict(path=ds.pathobj / 'dir_sm', recursive='repository'),
        dict(path=ds.pathobj / 'dir_sm', recursive='submodules'),
        # no submodule state
        dict(path=ds.pathobj, eval_submodule_state='no', recursive='no'),
        dict(path=ds.pathobj, eval_submodule_state='no', recursive='repository'),
        dict(path=ds.pathobj, eval_submodule_state='no', recursive='submodules'),
        # just the commit
        dict(path=ds.pathobj, eval_submodule_state='commit', recursive='no'),
        dict(path=ds.pathobj, eval_submodule_state='commit', recursive='repository'),
        dict(path=ds.pathobj, eval_submodule_state='commit', recursive='submodules'),
        # without untracked
        dict(path=ds.pathobj, untracked='no', recursive='no'),
        dict(path=ds.pathobj, untracked='no', recursive='repository'),
        dict(path=ds.pathobj, untracked='no', recursive='submodules'),
        # special untracked modes
        dict(path=ds.pathobj, untracked='whole-dir', recursive='no'),
        dict(path=ds.pathobj, untracked='whole-dir', recursive='repository'),
        dict(path=ds.pathobj, untracked='whole-dir', recursive='submodules'),
        dict(path=ds.pathobj, untracked='no-empty-dir', recursive='no'),
        dict(path=ds.pathobj, untracked='no-empty-dir', recursive='repository'),
        dict(path=ds.pathobj, untracked='no-empty-dir', recursive='submodules'),
        # call in the mountpoint of a dropped submodule
        dict(path=ds.pathobj / 'dir_sm' / 'droppedsm_c'),
    ):
        st = {item.name: item for item in iter_gitstatus(**kwargs)}
        # we get no report on anything clean (implicitly also tests
        # whether all item names are plain strings
        assert all(not i.name.endswith('_c') for i in st.values())

        # anything untracked is labeled as such
        assert all(
            i.status == GitDiffStatus.other
            # we would not see a submodule modification qualifier when instructed
            # not to evaluate a submodule
            or kwargs.get('eval_submodule_state') in ('no', 'commit')
            or GitContainerModificationType.untracked_content in i.modification_types
            for i in st.values()
            if 'u' in i.path.name.split('_')[1]
        )

        # anything modified is labeled as such
        assert all(
            i.status == GitDiffStatus.modification
            for i in st.values()
            if 'm' in i.path.name.split('_')[1]
        )

        # anything deleted is labeled as such
        assert all(
            i.status == GitDiffStatus.deletion
            for i in st.values()
            if 'd' in i.path.name.split('_')[1]
        )


def test_status_invalid_params(existing_dataset):
    ds = existing_dataset
    with pytest.raises(ValueError):
        list(iter_gitstatus(ds.pathobj, recursive='fromspace'))


test_cases_repository_recursion = [
    {'name': 'file_a', 'status': GitDiffStatus.addition},
    {'name': 'dir_m/file_a', 'status': GitDiffStatus.addition},
    {'name': 'file_u', 'status': GitDiffStatus.other},
    {'name': 'dir_u/file_u', 'status': GitDiffStatus.other},
    {'name': 'dir_m/file_u', 'status': GitDiffStatus.other},
    {'name': 'dir_m/dir_u/file_u', 'status': GitDiffStatus.other},
    {'name': 'file_d', 'status': GitDiffStatus.deletion},
    {'name': 'dir_d/file_d', 'status': GitDiffStatus.deletion},
    {'name': 'dir_m/file_d', 'status': GitDiffStatus.deletion},
    {'name': 'file_m', 'status': GitDiffStatus.modification},
    {'name': 'dir_m/file_m', 'status': GitDiffStatus.modification},
    {'name': 'dir_sm/sm_d', 'status': GitDiffStatus.deletion},
    {'name': 'dir_sm/sm_n', 'status': GitDiffStatus.modification,
     'qual': (GitContainerModificationType.new_commits,)},
    {'name': 'dir_sm/sm_m', 'status': GitDiffStatus.modification,
     'qual': (GitContainerModificationType.modified_content,)},
    {'name': 'dir_sm/sm_nm', 'status': GitDiffStatus.modification,
     'qual': (GitContainerModificationType.modified_content,
              GitContainerModificationType.new_commits)},
    {'name': 'dir_sm/sm_nmu', 'status': GitDiffStatus.modification,
     'qual': (GitContainerModificationType.modified_content,
              GitContainerModificationType.untracked_content,
              GitContainerModificationType.new_commits)},
    {'name': 'dir_sm/sm_u', 'status': GitDiffStatus.modification,
     'qual': (GitContainerModificationType.untracked_content,)},
    {'name': 'dir_sm/sm_mu', 'status': GitDiffStatus.modification,
     'qual': (GitContainerModificationType.modified_content,
              GitContainerModificationType.untracked_content)},
]

test_cases_submodule_recursion = [
    {'name': 'dir_sm/sm_m/file_a', 'status': GitDiffStatus.addition},
    {'name': 'dir_sm/sm_nm/file_a', 'status': GitDiffStatus.addition},
    {'name': 'dir_sm/sm_mu/file_a', 'status': GitDiffStatus.addition},
    {'name': 'dir_sm/sm_nmu/file_a', 'status': GitDiffStatus.addition},
    {'name': 'dir_sm/sm_m/file_m', 'status': GitDiffStatus.modification},
    {'name': 'dir_sm/sm_mu/file_m', 'status': GitDiffStatus.modification},
    {'name': 'dir_sm/sm_nmu/file_m', 'status': GitDiffStatus.modification},
    {'name': 'dir_sm/sm_u/file_u', 'status': GitDiffStatus.other},
    {'name': 'dir_sm/sm_mu/file_u', 'status': GitDiffStatus.other},
    {'name': 'dir_sm/sm_nmu/file_u', 'status': GitDiffStatus.other},
    {'name': 'dir_sm/sm_u/dir_u/file_u', 'status': GitDiffStatus.other},
    {'name': 'dir_sm/sm_mu/dir_u/file_u', 'status': GitDiffStatus.other},
    {'name': 'dir_sm/sm_nmu/dir_u/file_u', 'status': GitDiffStatus.other},
]


def _assert_testcases(st, tc):
    for c in tc:
        assert st[c['name']].status == c['status']
        mod_types = st[c['name']].modification_types
        if 'qual' in c:
            assert set(mod_types) == set(c['qual'])
        else:
            assert mod_types is None


def test_status_vs_git(status_playground):
    """Implements a comparison against how git-status behaved when
    the test was written  (see fixture docstring)
    """
    st = {
        item.name: item
        for item in iter_gitstatus(
            path=status_playground.pathobj, recursive='repository',
            eval_submodule_state='full', untracked='all',
        )
    }
    _assert_testcases(st, test_cases_repository_recursion)


def test_status_norec(status_playground):
    st = {
        item.name: item
        for item in iter_gitstatus(
            path=status_playground.pathobj, recursive='no',
            eval_submodule_state='full', untracked='all',
        )
    }
    test_cases = [
        {'name': 'file_a', 'status': GitDiffStatus.addition},
        {'name': 'dir_d', 'status': GitDiffStatus.deletion},
        {'name': 'dir_m', 'status': GitDiffStatus.modification,
         'qual': (GitContainerModificationType.modified_content,
                  GitContainerModificationType.untracked_content)},
        {'name': 'dir_sm', 'status': GitDiffStatus.modification,
         'qual': (GitContainerModificationType.modified_content,
                  GitContainerModificationType.untracked_content)},
        {'name': 'file_d', 'status': GitDiffStatus.deletion},
        {'name': 'file_m', 'status': GitDiffStatus.modification},
        {'name': 'dir_u', 'status': GitDiffStatus.other},
        {'name': 'file_u', 'status': GitDiffStatus.other},
    ]
    _assert_testcases(st, test_cases)


def test_status_smrec(status_playground):
    st = {
        item.name: item
        for item in iter_gitstatus(
            path=status_playground.pathobj, recursive='submodules',
            eval_submodule_state='full', untracked='all',
        )
    }
    # in this mode we expect ALL results of a 'repository' mode recursion,
    # including the submodule-type items, plus additional ones from within
    # the submodules
    _assert_testcases(st, chain(test_cases_repository_recursion,
                                test_cases_submodule_recursion))


def test_status_monorec(status_playground):
    st = {
        item.name: item
        for item in iter_gitstatus(
            path=status_playground.pathobj, recursive='monolithic',
            eval_submodule_state='full', untracked='all',
        )
    }
    # in this mode we expect ALL results of a 'repository' mode recursion,
    # including the submodule-type items, plus additional ones from within
    # the submodules
    _assert_testcases(
        st,
        # repository and recursive test cases, minus any direct submodule
        # items
        [c for c in chain(test_cases_repository_recursion,
                          test_cases_submodule_recursion)
         if not c['name'].split('/')[-1].split('_')[0] == 'sm'])


def test_status_gitinit(tmp_path):
    # initialize a fresh git repo, but make no commits
    assert call_git_success(['init'], cwd=tmp_path)
    for recmode in ('no', 'repository', 'submodules'):
        assert [] == list(iter_gitstatus(tmp_path, recursive=recmode))
    # untracked reporting must be working normal
    (tmp_path / 'untracked').touch()
    for recmode in ('no', 'repository', 'submodules'):
        res = list(iter_gitstatus(tmp_path, recursive=recmode))
        assert len(res) == 1
        assert res[0].name == 'untracked'
        assert res[0].status == GitDiffStatus.other
