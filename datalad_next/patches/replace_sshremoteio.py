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

The implementation also no longer assumes that local and remote platform are
identical. This patch introduces an actual remote platform/system
determination.

This patch also adds the method :meth:`url2transport_path`, which is used to
convert abstract paths, which are used in the patched RIA/ORA-code, into paths
that SSHRemoteIO can operate on.
"""
from __future__ import annotations
from pathlib import (
    Path,
    PurePosixPath,
)
from urllib.parse import (
    unquote,
    urlparse,
)

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
from datalad_next.shell import (
    FixedLengthResponseGeneratorPosix,
    shell,
    posix as posix_ops,
)


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
        self._remote_system = None
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
        if self.servershell_context is None:
            return
        self.servershell_context.__exit__(None, None, None)
        self.servershell_context = None

    def url2transport_path(
            self,
            url_path: PurePosixPath
    ) -> Path | PurePosixPath:
        assert isinstance(url_path, PurePosixPath)
        return url_path

    @property
    def remote_system(self):
        if self._remote_system is None:
            self._remote_system = self.servershell(
                "uname -s",
                check=True
            ).stdout.strip().decode().casefold()
        return self._remote_system

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

        if self.remote_system == 'darwin':
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
        posix_ops.upload(
            self.servershell,
            Path(src),
            PurePosixPath(dst),
            # the given callback only takes a single int, but posix.upload
            # gives two (cur, target) -> have an adaptor
            progress_callback=lambda c, m: progress_cb(c),
            check=True,
        )

    def get(self, src, dst, progress_cb):
        posix_ops.download(
            self.servershell,
            PurePosixPath(src),
            Path(dst),
            # the given callback only takes a single int, but posix.download
            # gives two (cur, target) -> have an adaptor
            progress_callback=lambda c, m: progress_cb(c),
            check=True,
        )

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

        # with `7z -slt` we get an info block per file like this
        #
        #    Path = some.txt
        #    Size = 4
        #    Packed Size = 8
        #    Modified = 2024-04-18 14:55:39.2376272
        #    Attributes = A -rw-rw-r--
        #    CRC = 5A82FD08
        #    Encrypted = -
        #    Method = LZMA2:12
        #    Block = 0
        #
        # we use -scsUTF-8 to be able to match an UTF filename properly,
        # and otherwise use basic grep/cut to get the integer byte size of
        # the file to be extracted
        #
        size_cmd = \
            f'7z -slt -scsUTF-8 l "{archive}" | grep -A9 "Path = {src}" ' \
            '| grep "^Size =" | cut -d " " -f 3'
        res = self.servershell(size_cmd, check=True)
        nbytes = res.stdout.strip().decode()
        if not nbytes:
            raise RIARemoteError(
                'Cannot determine archive member size. Invalid name?')
        member_size = int(res.stdout.strip().decode())

        cmd = f'7z x -so -- {sh_quote(str(archive))} {sh_quote(str(src))}'
        resgen = self.servershell.start(
            cmd,
            response_generator=FixedLengthResponseGeneratorPosix(
                self.servershell.stdout,
                member_size,
            ),
        )
        bytes_received = 0
        with open(dst, 'wb') as target_file:
            for chunk in resgen:
                bytes_received += len(chunk)
                target_file.write(chunk)
                progress_cb(bytes_received)
            assert resgen.returncode == 0
        if member_size:
            assert member_size == bytes_received

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


def oraremote_close_io_onclose(self):
    if self._io:
        self._io.close()
        self._io = None
    if self._push_io:
        self._push_io.close()
        self._push_io = None


# replace the whole class
apply_patch('datalad.distributed.ora_remote', None, 'SSHRemoteIO', SSHRemoteIO)
# add close handler that calls the io.close()
apply_patch('datalad.distributed.ora_remote', 'ORARemote', 'close',
            oraremote_close_io_onclose)
