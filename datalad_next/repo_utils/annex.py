from pathlib import Path

from datalad_next.runners import call_git_success


def has_initialized_annex(
    path: Path,
) -> bool:
    """Return whether there is an initialized annex for ``path``

    The given ``path`` can be any directory, inside or outside a Git
    repository. ``True`` is returned when the path is found to be
    within a (locally) initialized git-annex repository.

    When this test returns ``True`` it can be expected that no subsequent
    call to an annex command fails with

    `git-annex: First run: git-annex init`

    for this ``path``.
    """
    # this test is about 3ms in MIH's test system.
    # datalad-core tests for a git repo and then for .git/annex, this
    # achieves both in one step (although the test in datalad-core is
    # likely still faster, because it only inspects the filesystem
    # for a few key members of a Git repo. In order for that test to
    # work, though, it has to traverse the filesystem to find a repo root
    # -- if there even is any).
    # also ee https://git-annex.branchable.com/forum/Cheapest_test_for_an_initialized_annex__63__/
    return call_git_success(
        ['config', '--local', 'annex.uuid'],
        cwd=path,
        capture_output=True,
    )
