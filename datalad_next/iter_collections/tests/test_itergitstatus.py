from itertools import chain
import pytest

from datalad_next.runners import (
    call_git_success,
)

from ..gitstatus import (
    GitDiffStatus,
    GitContainerModificationType,
    iter_gitstatus,
)


def test_status_homogeneity(modified_dataset):
    """Test things that should always be true, no matter the precise
    parameterization

    A main purpose of this test is also to exercise all (main) code paths.
    """
    ds = modified_dataset
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
            # either directly
            i.status == GitDiffStatus.modification
            # or as an addition with a modification on top
            or (i.status == GitDiffStatus.addition
                and GitContainerModificationType.modified_content
                    in i.modification_types)
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


def test_status_vs_git(modified_dataset):
    """Implements a comparison against how git-status behaved when
    the test was written  (see fixture docstring)
    """
    st = {
        item.name: item
        for item in iter_gitstatus(
            path=modified_dataset.pathobj, recursive='repository',
            eval_submodule_state='full', untracked='all',
        )
    }
    _assert_testcases(st, test_cases_repository_recursion)


def test_status_norec(modified_dataset):
    st = {
        item.name: item
        for item in iter_gitstatus(
            path=modified_dataset.pathobj, recursive='no',
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


def test_status_smrec(modified_dataset):
    st = {
        item.name: item
        for item in iter_gitstatus(
            path=modified_dataset.pathobj, recursive='submodules',
            eval_submodule_state='full', untracked='all',
        )
    }
    # in this mode we expect ALL results of a 'repository' mode recursion,
    # including the submodule-type items, plus additional ones from within
    # the submodules
    _assert_testcases(st, chain(test_cases_repository_recursion,
                                test_cases_submodule_recursion))


def test_status_monorec(modified_dataset):
    st = {
        item.name: item
        for item in iter_gitstatus(
            path=modified_dataset.pathobj, recursive='monolithic',
            eval_submodule_state='full', untracked='all',
        )
    }
    # in this mode we expect ALL results of a 'repository' mode recursion,
    # including the submodule-type items, plus additional ones from within
    # the submodules
    _assert_testcases(
        st,
        # repository and recursive test cases
        [c for c in chain(test_cases_repository_recursion,
                          test_cases_submodule_recursion)
         # minus any submodule that have no new commits
         # (this only thing that is not attributable to individual
         # content changes)
         if not c['name'].split('/')[-1] in (
             'sm_m', 'sm_mu', 'sm_u',
         )])


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


def test_status_nohead_staged(tmp_path):
    # initialize a fresh git repo, but make no commits
    assert call_git_success(['init'], cwd=tmp_path)
    # stage a file
    (tmp_path / 'probe').write_text('tostage')
    assert call_git_success(['add', 'probe'], cwd=tmp_path)
    _assert_testcases(
        {i.name: i for i in iter_gitstatus(tmp_path)},
        [{'name': 'probe', 'status': GitDiffStatus.addition}],
    )
