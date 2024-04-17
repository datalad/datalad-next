"""Fix remote platform detection of ORA's SSHRemoteIO

The implementation assumed that local and remote platform are
identical. This patch introduced an actual remote platform/system
determination.
"""
from contextlib import contextmanager
from datalad.distributed.ora_remote import (
    # we import from the patch target to make breaking changes
    # more visible
    functools,
    sh_quote,
    stat,
)
from . import apply_patch


# this function is taken from datalad-core@84b2bd574e9edf6a8edf37e22021a0ebbc744e66
# datalad/distributed/ora_remote.py
# it was modified in the spirit of https://github.com/datalad/datalad/pull/7549
@contextmanager
def ensure_writeable(self, path):
    """Context manager to get write permission on `path` and restore
    original mode afterwards.

    If git-annex ever touched the key store, the keys will be in mode 444
    directories, and we need to obtain permission first.

    Parameters
    ----------
    path: Path
      path to the target file
    """

    remote_system = getattr(self, '_remote_system', None)
    if remote_system is None:
        self._remote_system = self._run(
            "uname -s",
            no_output=False,
            check=True
        ).strip().casefold()
        remote_system = self._remote_system

    path = sh_quote(str(path))
    # remember original mode -- better than to prescribe a fixed mode

    if remote_system == 'darwin':
        format_option = "-f%Dp"
        # on macOS this would return decimal representation of mode (same
        # as python's stat().st_mode
        conversion = int
    else:  # win is currently ignored anyway
        format_option = "--format=\"%f\""
        # in opposition to the above form for macOS, on debian this would
        # yield the hexadecimal representation of the mode; hence conversion
        # needed.
        conversion = functools.partial(int, base=16)

    output = self._run(f"stat {format_option} {path}",
                       no_output=False, check=True)
    mode = conversion(output)
    if not mode & stat.S_IWRITE:
        new_mode = oct(mode | stat.S_IWRITE)[-3:]
        self._run(f"chmod {new_mode} {path}")
        changed = True
    else:
        changed = False
    try:
        yield
    finally:
        if changed:
            # restore original mode
            self._run("chmod {mode} {file}".format(mode=oct(mode)[-3:],
                                                   file=path),
                      check=False)  # don't fail if path doesn't exist
                                    # anymore


apply_patch(
    'datalad.distributed.ora_remote', 'SSHRemoteIO', 'ensure_writeable',
    ensure_writeable,
)
