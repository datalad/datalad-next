"""Enable SSH-based remote command execution on Windows

This change introduces a replacement for core's
``datalad/support/sshconnector.py:BaseSSHConnection._exec_ssh()``
with a dedicated handling of ``stdin`` for Windows.

The OpenSSH client in Windows modifies its ``stdin``-descriptor in such a way, that it becomes unusable for the python process, if the ``stdin``-descriptor is shared between the ``python``-process and the ``ssh``-process. As a result, all read-operations that the ``python``-process performs on ``stdin`` will block and leave the python-process "hanging".

This change passes an explicit, empty, byte-string as ``stdin`` to the
SSH client call, in order to avoid any interaction of SSH with the
parent process's ``stdin`` descriptor.
"""

import logging

from datalad.support.sshconnector import (
    StdOutErrCapture,
    NoCapture,
)

from datalad_next.patches import apply_patch
from datalad_next.utils import on_windows

# use same logger as -core
lgr = logging.getLogger('datalad.support.sshconnector')


# This method interface/original implementation is taken from
# datalad-core@58b8e06317fe1a03290aed80526bff1e2d5b7797
# datalad/support/sshconnector.py:BaseSSHConnection
def _exec_ssh(self, ssh_cmd, cmd, options=None, stdin=None, log_output=True):
    cmd = self._adjust_cmd_for_bundle_execution(cmd)

    for opt in options or []:
        ssh_cmd.extend(["-o", opt])

    # THIS IS THE PATCH
    if on_windows and stdin is None:
        # SSH on windows requires a special stdin handling. If we'd let
        # stdin=None do its normal thing, the Python process would hang,
        # because it looses touch with its own file descriptor.
        # See https://github.com/datalad/datalad-ria/issues/68
        stdin = b''

    # build SSH call, feed remote command as a single last argument
    # whatever it contains will go to the remote machine for execution
    # we cannot perform any sort of escaping, because it will limit
    # what we can do on the remote, e.g. concatenate commands with '&&'
    ssh_cmd += [self.sshri.as_str()] + [cmd]

    lgr.debug("%s is used to run %s", self, ssh_cmd)

    # TODO: pass expect parameters from above?
    # Hard to explain to toplevel users ... So for now, just set True
    out = self.runner.run(
        ssh_cmd,
        protocol=StdOutErrCapture if log_output else NoCapture,
        stdin=stdin)
    return out['stdout'], out['stderr']


apply_patch(
    'datalad.support.sshconnector', 'BaseSSHConnection', '_exec_ssh',
    _exec_ssh,
)
