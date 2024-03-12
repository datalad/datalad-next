from __future__ import annotations

from abc import (
    ABCMeta,
    abstractmethod,
)
from logging import getLogger

from datalad_next.runners.iter_subproc import OutputFrom
from ..response_generators import ShellCommandResponseGenerator


lgr = getLogger('datalad.ext.next.shell.operations')


class DownloadResponseGenerator(ShellCommandResponseGenerator, metaclass=ABCMeta):
    """Response generator interface for efficient download

    This response generator is used to implement download in a single command
    call (instead of using one command to determine the length of a file and
    a subsequent fixed-length command to download the file). It assumes that
    the shell sends ``<length>\\n``, the content of the
    file, and ``<return code>\\n``. The response generator delegates the
    creation of the appropriate final command list to its subclasses.
    """
    def __init__(self,
                 stdout: OutputFrom,
                 ) -> None:
        super().__init__(stdout, stdout.stderr_deque)
        self.length = 0
        self.read = 0
        self.state = 1
        self.returncode_chunk = b''

    def send(self, _) -> bytes:
        chunk = b''
        # Use a while loop to make arbitrary order of state checks possible.
        # This allows us to put the most active state at the top of the loop
        # and increase performance.
        while True:
            if self.state == 2:
                if not chunk:
                    chunk = next(self.stdout_gen)
                self.read += len(chunk)
                if self.read >= self.length:
                    self.state = 3
                    excess = self.read - self.length
                    if excess > 0:
                        chunk, self.returncode_chunk = chunk[:-excess], chunk[-excess:]
                    else:
                        self.returncode_chunk = b''
                    if chunk:
                        return chunk
                else:
                    return chunk

            if self.state == 1:
                self.length, chunk = self.get_number_and_newline(
                    b'',
                    self.stdout_gen,
                )
                # a negative length indicates an error during download length
                # determination or download length-communication.
                if self.length < 0:
                    self.state = 1
                    self.returncode = 23
                    self.check_result()
                    raise StopIteration
                self.state = 2
                continue

            if self.state == 3:
                self.returncode, trailing = self.get_number_and_newline(
                    self.returncode_chunk,
                    self.stdout_gen,
                )
                if trailing:
                    lgr.warning(
                        'unexpected output after return code: %s',
                        repr(trailing))
                self.state = 4

            if self.state == 4:
                self.state = 1
                self.check_result()
                raise StopIteration

            raise RuntimeError(f'unknown state: {self.state}')

    def throw(self, typ, val=..., tb=...):  # pragma: no cover
        return super().throw(typ, val, tb)

    @abstractmethod
    def get_command_list(self, remote_file_name: bytes) -> list[bytes]:
        """Return a command list to download to remote file

        The generated command list will print the length of the file and
        then emit the file content, followed by the exit value if the command
        pipeline. If length printing fails a negative number, i.e. ``-1`` is
        printed and no return code is printed. The negative number serves as
        indicator to :meth:`DownloadResponseGenerator.send` that nothing will
        be sent. In this case :meth:`DownloadResponseGenerator.send` will set
        the return code to `23`.

        This method is usually only called by
        :meth:`ShellCommandExecutor.__call__`.
        """
        raise NotImplementedError
