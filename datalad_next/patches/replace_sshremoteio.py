from datalad.distributed.ora_remote import (
    DEFAULT_BUFFER_SIZE,
    IOBase,
    RemoteError,
    RemoteCommandFailedError,
    RIARemoteError,
    contextmanager,
    functools,
    on_osx,
    sh_quote,
    ssh_manager,
    stat,
    subprocess,
)

from datalad_next.patches import apply_patch


class SSHRemoteIO(IOBase):
    """IO operation if the object tree is SSH-accessible

    It doesn't even think about a windows server.
    """

    # output markers to detect possible command failure as well as end of output
    # from a particular command:
    REMOTE_CMD_FAIL = "ora-remote: end - fail"
    REMOTE_CMD_OK = "ora-remote: end - ok"

    def __init__(self, host, buffer_size=DEFAULT_BUFFER_SIZE):
        """
        Parameters
        ----------
        host : str
          SSH-accessible host(name) to perform remote IO operations
          on.
        """
        # the connection to the remote
        # we don't open it yet, not yet clear if needed
        self.ssh = ssh_manager.get_connection(
            host,
            use_remote_annex_bundle=False,
        )
        self.ssh.open()
        # open a remote shell
        cmd = ['ssh'] + self.ssh._ssh_args + [self.ssh.sshri.as_str()]
        self.shell = subprocess.Popen(cmd,
                                      stderr=subprocess.DEVNULL,
                                      stdout=subprocess.PIPE,
                                      stdin=subprocess.PIPE)
        # swallow login message(s):
        self.shell.stdin.write(b"echo RIA-REMOTE-LOGIN-END\n")
        self.shell.stdin.flush()
        while True:
            line = self.shell.stdout.readline()
            if line == b"RIA-REMOTE-LOGIN-END\n":
                break
        # TODO: Same for stderr?

        # make sure default is used when None was passed, too.
        self.buffer_size = buffer_size if buffer_size else DEFAULT_BUFFER_SIZE

    def close(self):
        # try exiting shell clean first
        self.shell.stdin.write(b"exit\n")
        self.shell.stdin.flush()
        exitcode = self.shell.wait(timeout=0.5)
        # be more brutal if it doesn't work
        if exitcode is None:  # timed out
            # TODO: Theoretically terminate() can raise if not successful.
            #       How to deal with that?
            self.shell.terminate()

    def _append_end_markers(self, cmd):
        """Append end markers to remote command"""

        return cmd + " && printf '%s\\n' {} || printf '%s\\n' {}\n".format(
            sh_quote(self.REMOTE_CMD_OK),
            sh_quote(self.REMOTE_CMD_FAIL))

    def _get_download_size_from_key(self, key):
        """Get the size of an annex object file from it's key

        Note, that this is not necessarily the size of the annexed file, but
        possibly only a chunk of it.

        Parameter
        ---------
        key: str
          annex key of the file

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

    def _run(self, cmd, no_output=True, check=False):

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

        lines = []
        while True:
            line = self.shell.stdout.readline().decode()
            lines.append(line)
            if line == self.REMOTE_CMD_OK + '\n':
                # end reading
                break
            elif line == self.REMOTE_CMD_FAIL + '\n':
                if check:
                    raise RemoteCommandFailedError(
                        "{cmd} failed: {msg}".format(cmd=cmd,
                                                     msg="".join(lines[:-1]))
                    )
                else:
                    break
        if no_output and len(lines) > 1:
            raise RIARemoteError("{}: {}".format(call, "".join(lines)))
        return "".join(lines[:-1])

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

    def mkdir(self, path):
        self._run('mkdir -p {}'.format(sh_quote(str(path))))

    def symlink(self, target, link_name):
        self._run('ln -s {} {}'.format(sh_quote(str(target)), sh_quote(str(link_name))))

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
            self._run('mv {} {}'.format(sh_quote(str(src)), sh_quote(str(dst))))

    def remove(self, path):
        try:
            with self.ensure_writeable(path.parent):
                self._run('rm {}'.format(sh_quote(str(path))), check=True)
        except RemoteCommandFailedError as e:
            raise RIARemoteError(f"Unable to remove {path} "
                                 "or to obtain write permission in parent directory.") from e

    def remove_dir(self, path):
        with self.ensure_writeable(path.parent):
            self._run('rmdir {}'.format(sh_quote(str(path))))

    def exists(self, path):
        try:
            self._run('test -e {}'.format(sh_quote(str(path))), check=True)
            return True
        except RemoteCommandFailedError:
            return False

    def in_archive(self, archive_path, file_path):

        if not self.exists(archive_path):
            return False

        loc = str(file_path)
        # query 7z for the specific object location, keeps the output
        # lean, even for big archives
        cmd = '7z l {} {}'.format(
            sh_quote(str(archive_path)),
            sh_quote(loc))

        # Note: Currently relies on file_path not showing up in case of failure
        # including non-existent archive. If need be could be more sophisticated
        # and called with check=True + catch RemoteCommandFailedError
        out = self._run(cmd, no_output=False, check=False)

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

        cmd = "cat  {}".format(sh_quote(str(file_path)))
        try:
            out = self._run(cmd, no_output=False, check=True)
        except RemoteCommandFailedError as e:
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

        cmd = "printf '%s' {} {} {}".format(
            sh_quote(content),
            mode,
            sh_quote(str(file_path)))
        try:
            self._run(cmd, check=True)
        except RemoteCommandFailedError as e:
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
            self._run("7z", check=True, no_output=False)
            return True
        except RemoteCommandFailedError:
            return False

        # try:
        #     out = self._run("which 7z", check=True, no_output=False)
        #     return out
        # except RemoteCommandFailedError:
        #     return None


# replace the whole class
apply_patch('datalad.distributed.ora_remote', None, 'SSHRemoteIO', SSHRemoteIO)
