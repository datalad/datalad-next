"""Provide a full replacement of `SSHRemoteIO`

First and foremost, this replacement no longer uses the remote shell
implementation of the previous version, but is based on `datalad_next.shell`.

Moreover, the ``cmd``-argument for the shell ssh-process, is not correct, if
``self.ssh`` is an instance of ``NoMultiplexSSHConnection``.

The changes in this patch build the correct ``cmd``-argument by adding
additional arguments to ``cmd``, if `self.ssh` is an instance of
``NoMultiplexSSHConnection``. More precisely, the arguments that are
required to open a "shell" in a ``NoMultiplexSSHConnection`` are stored in
``NoMultiplexSSHConnection._ssh_open_args`` and not in
``NoMultiplexSSHConnection._ssh_args``. This patch therefore provides
arguments from both lists, i.e. from ``_ssh_args`` and ``_ssh_open_args`` in
the call that opens a "shell", if ``self.ssh`` is an instance of
``NoMultiplexSSHConnection``.
"""

from urllib.parse import urlparse
from urllib.request import unquote

from datalad.distributed.ora_remote import (
    DEFAULT_BUFFER_SIZE,
    IOBase,
    RemoteError,
    RIARemoteError,
    contextmanager,
    functools,
    on_osx,
    sh_quote,
    ssh_manager,
    stat,
)
from datalad.support.sshconnector import NoMultiplexSSHConnection

from datalad_next.exceptions import CapturedException
from datalad_next.patches import apply_patch
from datalad_next.runners import CommandError
from datalad_next.shell import shell


class SSHRemoteIO(IOBase):
    """IO operation if the object tree is SSH-accessible

    It doesn't even think about a windows server.
    """
    def __init__(self, ssh_url, buffer_size=DEFAULT_BUFFER_SIZE):
        """
        Parameters
        ----------
        ssh_url : str
          SSH-accessible host(name) to perform remote IO operations
          on.
        buffer_size: int or None
          The buffer size to be used as the `chunk_size` for communication
          with the remote shell.
        """
        parsed_url = urlparse(ssh_url)

        self.url = ssh_url
        # the connection to the remote
        # we don't open it yet, not yet clear if needed
        self.ssh = ssh_manager.get_connection(
            ssh_url,
            use_remote_annex_bundle=False,
        )
        self.ssh.open()

        ssh_args = self.ssh._ssh_args
        if isinstance(self.ssh, NoMultiplexSSHConnection):
            ssh_args.extend(self.ssh._ssh_open_args)
        cmd = ['ssh'] + ssh_args + [self.ssh.sshri.as_str()]

        # we settle on `bash` as a shell. It should be around and then we
        # can count on it
        cmd.append('bash')
        # open the remote shell
        self.servershell_context = shell(
            cmd,
            chunk_size=buffer_size,
        )
        self.servershell = self.servershell_context.__enter__()

        # if the URL had a path, we try to 'cd' into it to make operations on
        # relative paths intelligible
        if parsed_url.path:
            # unquote path
            real_path = unquote(parsed_url.path)
            try:
                self.servershell(
                    f'cd {sh_quote(real_path)}',
                    check=True,
                )
            except Exception as e:
                # it may be a legit use case to point to a directory that is
                # not yet existing. Log and continue
                CapturedException(e)

    def close(self):
        self.servershell_context.__exit__(None, None, None)

    def _get_download_size_from_key(self, key):
        """Get the size of an annex object file from it's key

        Note, that this is not necessarily the size of the annexed file, but
        possibly only a chunk of it.

        Parameter
        ---------
        key: str
          annex key of the filte

        Returns
        -------
        int
          size in bytes
        """
        # TODO: datalad's AnnexRepo.get_size_from_key() is not correct/not
        #       fitting. Incorporate the wisdom there, too.
        #       We prob. don't want to actually move this method there, since
        #       AnnexRepo would be quite an expensive import. Startup time for
        #       special remote matters.
        # TODO: this method can be more compact. we don't need particularly
        #       elaborated error distinction

        # see: https://git-annex.branchable.com/internals/key_format/
        key_parts = key.split('--')
        key_fields = key_parts[0].split('-')

        s = S = C = None

        for field in key_fields[1:]:  # note: first has to be backend -> ignore
            if field.startswith('s'):
                # size of the annexed file content:
                s = int(field[1:]) if field[1:].isdigit() else None
            elif field.startswith('S'):
                # we have a chunk and that's the chunksize:
                S = int(field[1:]) if field[1:].isdigit() else None
            elif field.startswith('C'):
                # we have a chunk, this is it's number:
                C = int(field[1:]) if field[1:].isdigit() else None

        if s is None:
            return None
        elif S is None and C is None:
            return s
        elif S and C:
            if C <= int(s / S):
                return S
            else:
                return s % S
        else:
            raise RIARemoteError("invalid key: {}".format(key))

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

        path = sh_quote(str(path))
        # remember original mode -- better than to prescribe a fixed mode

        if on_osx:
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

        output = self.servershell(
            f"stat {format_option} {path}",
            check=True,
        ).stdout.decode()
        mode = conversion(output)
        if not mode & stat.S_IWRITE:
            new_mode = oct(mode | stat.S_IWRITE)[-3:]
            self.servershell(f"chmod {new_mode} {path}", check=True)
            changed = True
        else:
            changed = False
        try:
            yield
        finally:
            if changed:
                # restore original mode
                self.servershell(
                    f"chmod {oct(mode)[-3:]} {path}",
                    # don't fail if path doesn't exist anymore
                    check=False,
                )

    def mkdir(self, path):
        self.servershell(
            f'mkdir -p {sh_quote(str(path))}',
            check=True,
        )

    def symlink(self, target, link_name):
        self.servershell(
            f'ln -s {sh_quote(str(target))} {sh_quote(str(link_name))}',
            check=True,
        )

    def put(self, src, dst, progress_cb):
        self.ssh.put(str(src), str(dst))

    def get(self, src, dst, progress_cb):

        # Note, that as we are in blocking mode, we can't easily fail on the
        # actual get (that is 'cat').
        # Therefore check beforehand.
        if not self.exists(src):
            raise RIARemoteError("annex object {src} does not exist."
                                 "".format(src=src))

        from os.path import basename
        key = basename(str(src))
        try:
            size = self._get_download_size_from_key(key)
        except RemoteError as e:
            raise RemoteError(f"src: {src}") from e

        if size is None:
            # rely on SCP for now
            self.ssh.get(str(src), str(dst))
            return

        # TODO: see get_from_archive()

        # TODO: Currently we will hang forever if the file isn't readable and
        #       it's supposed size is bigger than whatever cat spits out on
        #       stdout. This is because we don't notice that cat has exited
        #       non-zero. We could have end marker on stderr instead, but then
        #       we need to empty stderr beforehand to not act upon output from
        #       earlier calls. This is a problem with blocking reading, since we
        #       need to make sure there's actually something to read in any
        #       case.
        cmd = 'cat {}'.format(sh_quote(str(src)))
        self.shell.stdin.write(cmd.encode())
        self.shell.stdin.write(b"\n")
        self.shell.stdin.flush()

        with open(dst, 'wb') as target_file:
            bytes_received = 0
            while bytes_received < size:
                # TODO: some additional abortion criteria? check stderr in
                #       addition?
                c = self.shell.stdout.read1(self.buffer_size)
                # no idea yet, whether or not there's sth to gain by a
                # sophisticated determination of how many bytes to read at once
                # (like size - bytes_received)
                if c:
                    bytes_received += len(c)
                    target_file.write(c)
                    progress_cb(bytes_received)

    def rename(self, src, dst):
        with self.ensure_writeable(dst.parent):
            self.servershell(
                f'mv {sh_quote(str(src))} {sh_quote(str(dst))}',
                check=True,
            )

    def remove(self, path):
        try:
            with self.ensure_writeable(path.parent):
                self.servershell(
                    f'rm {sh_quote(str(path))}',
                    check=True,
                )
        except CommandError as e:
            raise RIARemoteError(
                f"Unable to remove {path} "
                "or to obtain write permission in parent directory.") from e

    def remove_dir(self, path):
        with self.ensure_writeable(path.parent):
            self.servershell(
                f'rmdir {sh_quote(str(path))}',
                check=True,
            )

    def exists(self, path):
        try:
            self.servershell(
                f'test -e {sh_quote(str(path))}',
                check=True,
            )
            return True
        except CommandError:
            return False

    def in_archive(self, archive_path, file_path):

        if not self.exists(archive_path):
            return False

        loc = str(file_path)
        # query 7z for the specific object location, keeps the output
        # lean, even for big archives
        cmd = f'7z l {sh_quote(str(archive_path))} {sh_quote(loc)}'

        # Note: Currently relies on file_path not showing up in case of failure
        # including non-existent archive. If need be could be more sophisticated
        # and called with check=True + catch RemoteCommandFailedError
        out = self.servershell(
            cmd,
            check=False,
        ).stdout.decode()

        return loc in out

    def get_from_archive(self, archive, src, dst, progress_cb):

        # Note, that as we are in blocking mode, we can't easily fail on the
        # actual get (that is 'cat'). Therefore check beforehand.
        if not self.exists(archive):
            raise RIARemoteError("archive {arc} does not exist."
                                 "".format(arc=archive))

        # TODO: We probably need to check exitcode on stderr (via marker). If
        #       archive or content is missing we will otherwise hang forever
        #       waiting for stdout to fill `size`.

        cmd = '7z x -so {} {}\n'.format(
            sh_quote(str(archive)),
            sh_quote(str(src)))
        self.shell.stdin.write(cmd.encode())
        self.shell.stdin.flush()

        # TODO: - size needs double-check and some robustness
        #       - can we assume src to be a posixpath?
        #       - RF: Apart from the executed command this should be pretty much
        #         identical to self.get(), so move that code into a common
        #         function

        from os.path import basename
        size = self._get_download_size_from_key(basename(str(src)))

        with open(dst, 'wb') as target_file:
            bytes_received = 0
            while bytes_received < size:
                c = self.shell.stdout.read1(self.buffer_size)
                if c:
                    bytes_received += len(c)
                    target_file.write(c)
                    progress_cb(bytes_received)

    def read_file(self, file_path):

        cmd = f"cat {sh_quote(str(file_path))}"
        try:
            out = self.servershell(
                cmd,
                check=True,
            ).stdout.decode()
        except CommandError as e:
            # Currently we don't read stderr. All we know is, we couldn't read.
            # Try narrowing it down by calling a subsequent exists()
            if not self.exists(file_path):
                raise FileNotFoundError(f"{str(file_path)} not found.") from e
            else:
                raise RuntimeError(f"Could not read {file_path}") from e

        return out

    def write_file(self, file_path, content, mode='w'):

        if mode == 'w':
            mode = ">"
        elif mode == 'a':
            mode = ">>"
        else:
            raise ValueError("Unknown mode '{}'".format(mode))
        if not content.endswith('\n'):
            content += '\n'

        # it really should read from stdin, but MIH cannot make it happen
        stdin = content.encode()
        cmd = f"head -c {len(stdin)} | cat {mode} {sh_quote(str(file_path))}"
        try:
            self.servershell(
                cmd,
                check=True,
                stdin=[stdin],
            )
        except CommandError as e:
            raise RIARemoteError(f"Could not write to {file_path}") from e

    def get_7z(self):
        # TODO: To not rely on availability in PATH we might want to use `which`
        #       (`where` on windows) and get the actual path to 7z to reuse in
        #       in_archive() and get().
        #       Note: `command -v XXX` or `type` might be cross-platform
        #       solution!
        #       However, for availability probing only, it would be sufficient
        #       to just call 7z and see whether it returns zero.

        try:
            self.servershell(
                "7z",
                check=True,
            )
            return True
        except CommandError:
            return False


# replace the whole class
apply_patch('datalad.distributed.ora_remote', None, 'SSHRemoteIO', SSHRemoteIO)
