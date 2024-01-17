import pytest

from datalad_next.runners import call_git

from .. import get_worktree_head


def test_get_worktree_head(tmp_path, existing_dataset):
    ds = existing_dataset

    with pytest.raises(ValueError) as e:
        get_worktree_head(tmp_path / 'IDONOTEXISTONTHEFILESYSTEM')
    assert str(e.value) == 'path not found'

    norepo = tmp_path / 'norepo'
    norepo.mkdir()
    with pytest.raises(ValueError) as e:
        get_worktree_head(norepo)
    assert str(e.value) == f'no Git repository at {norepo!r}'

    reponohead = tmp_path / 'reponohead'
    reponohead.mkdir()
    call_git(['init'], cwd=reponohead)
    assert (None, None) == get_worktree_head(reponohead)

    # and actual repo with a commit
    head, chead = get_worktree_head(ds.pathobj)
    # we always get a HEAD
    # we always get fullname symbolic info
    assert head.startswith('refs/heads/')
    if chead is not None:
        # there is a corresponding head, and we get it as the
        # git-annex 'basis' ref
        assert head.startswith('refs/heads/adjusted/')
        assert chead.startswith('refs/basis/')
