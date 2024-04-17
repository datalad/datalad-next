"""Provide proper arguments for scp-command calls in `SSHConnection`

The original code has errors in the methods ``BaseSSHConnection.put``
``BaseSSHConnection.get``. Both methods use ``self.sshri.hostname`` to
determine the target for an ``scp``-command. They should instead use
``self.sshri.as_str()`` in order to include a user specification into the
target.

The changes in this patch use ``self.sshri.as_str()`` to provide the correct
targets for ``scp``-commands.
"""

import logging

from datalad.support.sshconnector import (
    StdOutErrCapture,
    ensure_list,
)
from datalad_next.patches import apply_patch


# use same logger as -core
lgr = logging.getLogger('datalad.support.sshconnector')


# The method 'BaseSSHConnection_get' is a patched version of
# 'datalad/support/sshconnector.py:BaseSSHConnection.get'
# from datalad@e0b357d9b8ca5f432638c23c0cb7c373028c8e52
def BaseSSHConnection_get(self, source, destination, recursive=False, preserve_attrs=False):
    """Copies source file/folder from remote to a local destination.

    Note: this method performs escaping of filenames to an extent that
    moderately weird ones should work (spaces, quotes, pipes, other
    characters with special shell meaning), but more complicated cases
    might require appropriate external preprocessing of filenames.

    Parameters
    ----------
    source : str or list
      file/folder path(s) to copy from the remote host
    destination : str
      file/folder path to copy to on the local host
    recursive : bool
      flag to enable recursive copying of given sources
    preserve_attrs : bool
      preserve modification times, access times, and modes from the
      original file

    Returns
    -------
    str
      stdout, stderr of the copy operation.
    """
    # make sure we have an open connection, will test if action is needed
    # by itself
    self.open()
    scp_cmd = self._get_scp_command_spec(recursive, preserve_attrs)
    # add source filepath(s) to scp command, prefixed with the remote host
    # PATCH in the line below: replaces `self.sshri.hostname` with `self.sshri.as_str()`
    scp_cmd += ["%s:%s" % (self.sshri.as_str(), self._quote_filename(s))
                for s in ensure_list(source)]
    # add destination path
    scp_cmd += [destination]
    out = self.runner.run(scp_cmd, protocol=StdOutErrCapture)
    return out['stdout'], out['stderr']


# The method 'BaseSSHConnection_put' is a patched version of
# 'datalad/support/sshconnector.py:BaseSSHConnection.put'
# from datalad@e0b357d9b8ca5f432638c23c0cb7c373028c8e52
def BaseSSHConnection_put(self, source, destination, recursive=False, preserve_attrs=False):
    """Copies source file/folder to destination on the remote.

    Note: this method performs escaping of filenames to an extent that
    moderately weird ones should work (spaces, quotes, pipes, other
    characters with special shell meaning), but more complicated cases
    might require appropriate external preprocessing of filenames.

    Parameters
    ----------
    source : str or list
      file/folder path(s) to copy from on local
    destination : str
      file/folder path to copy to on remote
    recursive : bool
      flag to enable recursive copying of given sources
    preserve_attrs : bool
      preserve modification times, access times, and modes from the
      original file

    Returns
    -------
    str
      stdout, stderr of the copy operation.
    """
    # make sure we have an open connection, will test if action is needed
    # by itself
    self.open()
    scp_cmd = self._get_scp_command_spec(recursive, preserve_attrs)
    # add source filepath(s) to scp command
    scp_cmd += ensure_list(source)
    # add destination path
    scp_cmd += ['%s:%s' % (
        # PATCH in the line below: replaces `self.sshri.hostname` with `self.sshri.as_str()`
        self.sshri.as_str(),
        self._quote_filename(destination),
    )]
    out = self.runner.run(scp_cmd, protocol=StdOutErrCapture)
    return out['stdout'], out['stderr']


apply_patch(
    modname='datalad.support.sshconnector',
    objname='BaseSSHConnection',
    attrname='get',
    patch=BaseSSHConnection_get,
)

apply_patch(
    modname='datalad.support.sshconnector',
    objname='BaseSSHConnection',
    attrname='put',
    patch=BaseSSHConnection_put,
)
