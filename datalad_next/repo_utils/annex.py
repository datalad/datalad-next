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
    return call_git_success(
        ['annex', 'info', '--fast', '-q'],
        cwd=path,
        capture_output=True,
    )
