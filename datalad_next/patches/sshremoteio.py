"""Correct flaws in `SSHRemoteIO` operation

The original code has a number of problems.

1. The ``cmd``-argument for the shell ssh-process, which is created by:
   ``self.shell = subprocess.Popen(cmd, ...)`` is not correct, if ``self.ssh``i
   is an instance of ``NoMultiplexSSHConnection``.

   The changes in this patch build the correct ``cmd``-argument by adding
   additional arguments to ``cmd``, if `self.ssh` is an instance of
   ``NoMultiplexSSHConnection``. More precisely, the arguments that are
   required to open a "shell" in a ``NoMultiplexSSHConnection`` are stored in
   ``NoMultiplexSSHConnection._ssh_open_args`` and not in
   ``NoMultiplexSSHConnection._ssh_args``. This patch therefore provides
   arguments from both lists, i.e. from ``_ssh_args`` and ``_ssh_open_args`` in
   the call that opens a "shell", if ``self.ssh`` is an instance of
   ``NoMultiplexSSHConnection``.

2. The while-loop that waits to read ``b"RIA-REMOTE-LOGIN-END\\n"`` from the
   shell ssh-process did not contain any error handling. That led to an
   infinite loop in case that the shell ssh-process terminates without writing
   ``b"RIA-REMOTE-LOGIN-END\\n"`` to its stdout, or in the case that the
   stdout-pipeline of the shell ssh-process is closed.

   This patch introduces two checks into the while loop. One check for
   termination of the ssh shell-process, and one check for a closed
   stdout-pipeline of the ssh shell-process, i.e. reading an EOF from the
   stdout-pipeline. If any of those two cases appears, an exception is raised.

3. The output endmarker handling in ``SSHRemoteIO._run()`` could not reliably
   handle commands that would yield output without a final newline (e.g.,
   ``cat`` of a file without a trailing newline). This patch changes to
   endmarker handling to guarantee that they appear on a dedicated line,
   by prefixing the marker itself with a newline (which is withheld form the
   actual output).

4. ``SSHRemoteIO.remove_dir()`` would not fail (unlike ``FileIO.remove_dir()``)
   when ran on a non-empty directory. Despite not failing, it would also not
   remove that directory.

In addition, this patch modifies two comments. It adds a missing description of
the ``buffer_size``-parameter of ``SSHRemoteIO.__init__``to the doc-string, and
fixes the description of the condition in the comment on the use of
``DEFAULT_BUFFER_SIZE``.
"""

import logging
import subprocess

from datalad.distributed.ora_remote import (
    ssh_manager,
    sh_quote,
    RemoteCommandFailedError,
    RIARemoteError,
)
# we need to get this from elsewhere, the orginal code does local imports
from datalad.support.exceptions import CommandError
# we need this for a conditional that is not part of the original code
from datalad.support.sshconnector import NoMultiplexSSHConnection

from datalad_next.utils.consts import COPY_BUFSIZE
from datalad_next.patches import apply_patch

# use same logger as -core
lgr = logging.getLogger('datalad.customremotes.ria_remote')


DEFAULT_BUFFER_SIZE = COPY_BUFSIZE


# The method 'SSHRemoteIO__init__' is a patched version of
# 'datalad/distributed/ora-remote.py:SSHRemoteIO.__init___'
# from datalad@8a145bf432ae8931be7039c97ff602e53813d238
def SSHRemoteIO__init__(self, host, buffer_size=DEFAULT_BUFFER_SIZE):
    """
    Parameters
    ----------
    host : str
      SSH-accessible host(name) to perform remote IO operations
      on.
    buffer_size: int or None
      The preferred buffer size
    """

    # the connection to the remote
    # we don't open it yet, not yet clear if needed
    self.ssh = ssh_manager.get_connection(
        host,
        use_remote_annex_bundle=False,
    )
    self.ssh.open()

    # This is a PATCH: it extends ssh_args to contain all
    # necessary parameters
    ssh_args = self.ssh._ssh_args
    if isinstance(self.ssh, NoMultiplexSSHConnection):
        ssh_args.extend(self.ssh._ssh_open_args)
    cmd = ['ssh'] + ssh_args + [self.ssh.sshri.as_str()]

    # open a remote shell
    self.shell = subprocess.Popen(cmd,
                                  stderr=subprocess.DEVNULL,
                                  stdout=subprocess.PIPE,
                                  stdin=subprocess.PIPE)
    # swallow login message(s):
    self.shell.stdin.write(b"echo RIA-REMOTE-LOGIN-END\n")
    self.shell.stdin.flush()
    while True:
        # This is a PATCH: detect a terminated shell-process
        status = self.shell.poll()
        if status not in (0, None):
            raise CommandError(f'ssh shell process exited with {status}')

        line = self.shell.stdout.readline()
        if line == b"RIA-REMOTE-LOGIN-END\n":
            break

        # This is a PATCH: detect closing of stdout of the shell-process
        if line == '':
            raise RuntimeError(f'ssh shell process close stdout unexpectedly')
    # TODO: Same for stderr?

    # make sure default is used if 0 or None was passed, too.
    self.buffer_size = buffer_size if buffer_size else DEFAULT_BUFFER_SIZE


# The method 'SSHRemoteIO_append_end_markers' is a patched version of
# 'datalad/distributed/ora-remote.py:SSHRemoteIO._append_end_markers'
# from datalad@58b8e06317fe1a03290aed80526bff1e2d5b7797
def SSHRemoteIO_append_end_markers(self, cmd):
    """Append end markers to remote command"""

    # THE PATCH: the addition of the leading newline char
    return "{} && printf '\\n%s\\n' {} || printf '\\n%s\\n' {}\n".format(
        cmd,
        sh_quote(self.REMOTE_CMD_OK),
        sh_quote(self.REMOTE_CMD_FAIL),
    )


# The method 'SSHRemoteIO_run' is a patched version of
# 'datalad/distributed/ora-remote.py:SSHRemoteIO._run'
# from datalad@58b8e06317fe1a03290aed80526bff1e2d5b7797
def SSHRemoteIO_run(self, cmd, no_output=True, check=False):
    # TODO: we might want to redirect stderr to stdout here (or have
    #       additional end marker in stderr) otherwise we can't empty stderr
    #       to be ready for next command. We also can't read stderr for
    #       better error messages (RemoteError) without making sure there's
    #       something to read in any case (it's blocking!).
    #       However, if we are sure stderr can only ever happen if we would
    #       raise RemoteError anyway, it might be okay.
    call = self._append_end_markers(cmd)
    self.shell.stdin.write(call.encode())
    self.shell.stdin.flush()

    # PATCH: helper to strip the endmarker newline
    def _strip_endmarker_newline(lines):
        if lines[-1] == '\n':
            lines = lines[:-1]
        else:
            lines[-1] = lines[-1][:-1]
        return lines

    lines = []
    while True:
        line = self.shell.stdout.readline().decode()
        if line == self.REMOTE_CMD_OK + '\n':
            # PATCH remove leading newline that also belongs to the endmarker
            lines = _strip_endmarker_newline(lines)
            # end reading
            break
        elif line == self.REMOTE_CMD_FAIL + '\n':
            # PATCH remove leading newline that also belongs to the endmarker
            lines = _strip_endmarker_newline(lines)
            if check:
                raise RemoteCommandFailedError(
                    "{cmd} failed: {msg}".format(cmd=cmd,
                                                 msg="".join(lines[:-1]))
                )
            else:
                break
        # PATCH add line only here, to skip end markers alltogether
        lines.append(line)
    if no_output and len(lines) > 1:
        raise RIARemoteError("{}: {}".format(call, "".join(lines)))
    return "".join(lines)


# The method 'SSHRemoteIO_run' is a patched version of
# 'datalad/distributed/ora-remote.py:SSHRemoteIO._run'
# from datalad@58b8e06317fe1a03290aed80526bff1e2d5b7797
def SSHRemoteIO_remove_dir(self, path):
    with self.ensure_writeable(path.parent):
        self._run('rmdir {}'.format(sh_quote(str(path))),
                  # THIS IS THE PATCH
                  # we want it to fail, like rmdir() would fail
                  # on non-empty dirs
                  check=True)


for target, patch in (
        ('__init__', SSHRemoteIO__init__),
        ('_append_end_markers', SSHRemoteIO_append_end_markers),
        ('_run', SSHRemoteIO_run),
        ('remove_dir', SSHRemoteIO_remove_dir),
):
    apply_patch('datalad.distributed.ora_remote', 'SSHRemoteIO', target, patch)
