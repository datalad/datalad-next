from __future__ import annotations

from pathlib import Path
import subprocess

from datalad_next.exceptions import CapturedException

from .iter_subproc import (
    CommandError,
    iter_subproc,
)


def _call_git(
    args: list[str],
    *,
    capture_output: bool = False,
    cwd: Path | None = None,
    check: bool = False,
    text: bool | None = None,
    input: str | bytes | None = None,
    # TODO
    #patch_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    """Wrapper around ``subprocess.run`` for calling Git command

    ``args`` is a list of argument for the Git command. This list must not
    contain the Git executable itself. It will be prepended (unconditionally)
    to the arguments before passing them on.

    All other argument are pass on to ``subprocess.run()`` verbatim.
    """
    # make configurable
    git_executable = 'git'
    cmd = [git_executable, *args]
    try:
        return subprocess.run(
            cmd,
            capture_output=capture_output,
            cwd=cwd,
            check=check,
            text=text,
            input=input,
        )
    except subprocess.CalledProcessError as e:
        # TODO we could support post-error forensics, but some client
        # might call this knowing that it could fail, and may not
        # appreciate the slow-down. Add option `expect_fail=False`?
        #
        # normalize exception to datalad-wide standard
        raise CommandError(
            cmd=cmd,
            code=e.returncode,
            stdout=e.stdout,
            stderr=e.stderr,
            cwd=cwd,
        ) from e


def call_git(
    args: list[str],
    *,
    cwd: Path | None = None,
) -> None:
    """Call git with no output capture, raises on non-zero exit.

    If ``cwd`` is not None, the function changes the working directory to
    ``cwd`` before executing the command.
    """
    _call_git(
        args,
        capture_output=False,
        cwd=cwd,
        check=True,
    )


def call_git_success(
    args: list[str],
    *,
    cwd: Path | None = None,
    capture_output: bool = False,
) -> bool:
    """Call Git for a single line of output.

    ``args`` is a list of arguments for the Git command. This list must not
    contain the Git executable itself. It will be prepended (unconditionally)
    to the arguments before passing them on.

    If ``cwd`` is not None, the function changes the working directory to
    ``cwd`` before executing the command.

    If ``capture_output`` is ``True``, process output is captured, but not
    returned. By default process output is not captured.
    """
    try:
        _call_git(
            args,
            capture_output=capture_output,
            cwd=cwd,
            check=True,
        )
    except CommandError as e:
        CapturedException(e)
        return False
    return True


def call_git_lines(
    args: list[str],
    *,
    cwd: Path | None = None,
    input: str | None = None,
) -> list[str]:
    """Call Git for any (small) number of lines of output.

    ``args`` is a list of arguments for the Git command. This list must not
    contain the Git executable itself. It will be prepended (unconditionally)
    to the arguments before passing them on.

    If ``cwd`` is not None, the function changes the working directory to
    ``cwd`` before executing the command.

    If ``input`` is not None, the argument becomes the subprocess’s stdin.
    This is intended for small-scale inputs. For call that require processing
    large inputs, ``iter_git_subproc()`` is to be preferred.

    Raises
    ------
    CommandError if the call exits with a non-zero status.
    """
    res = _call_git(
        args,
        capture_output=True,
        cwd=cwd,
        check=True,
        text=True,
        input=input,
    )
    return res.stdout.splitlines()


def call_git_oneline(
    args: list[str],
    *,
    cwd: Path | None = None,
    input: str | None = None,
) -> str:
    """Call git for a single line of output.

    If ``cwd`` is not None, the function changes the working directory to
    ``cwd`` before executing the command.

    Raises
    ------
    CommandError if the call exits with a non-zero status.
    AssertionError if there is more than one line of output.
    """
    lines = call_git_lines(args, cwd=cwd, input=input)
    if len(lines) > 1:
        raise AssertionError(
            f"Expected Git {args} to return a single line, but got {lines}"
        )
    return lines[0]


def iter_git_subproc(
    args: list[str],
    **kwargs
):
    """``iter_subproc()`` wrapper for calling Git commands

    All argument semantics are identical to those of ``iter_subproc()``,
    except that ``args`` must not contain the Git binary, but need to be
    exclusively arguments to it. The respective `git` command/binary is
    automatically added internally.
    """
    cmd = ['git']
    cmd.extend(args)

    return iter_subproc(cmd, **kwargs)
