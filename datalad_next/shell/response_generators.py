from __future__ import annotations

import logging
from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import deque
from collections.abc import Generator
from random import randint

from datalad_next.itertools import align_pattern
from datalad_next.runners.iter_subproc import OutputFrom


__all__ = [
    'FixedLengthResponseGenerator',
    'FixedLengthResponseGeneratorPosix',
    'FixedLengthResponseGeneratorPowerShell',
    'ShellCommandResponseGenerator',
    'VariableLengthResponseGenerator',
    'VariableLengthResponseGeneratorPosix',
    'VariableLengthResponseGeneratorPowerShell',
]


lgr = logging.getLogger('datalad.ext.next.shell.protocol')


class ShellCommandResponseGenerator(Generator, metaclass=ABCMeta):
    """An abstract class the specifies the minimal functionality of a response generator

    Subclasses of this class can be used to implement operation-specific,
    shell-specific or OS-specific details of the command execution and the
    command output parsing.

    The return code is available in the ``returncode``-attribute, the
    stderr-output is available in the ``stderr_deque``-attribute (a
    ``deque``-instance), of instances of this class.
    """
    def __init__(self, stdout_gen: Generator, stderr_deque: deque) -> None:
        self.stdout_gen = stdout_gen
        self.stderr_deque = stderr_deque
        self.state: str | int = 'output'
        self.returncode_chunk = b''
        self.returncode: int | None = None

    @staticmethod
    def _get_number_and_newline(chunk, iterable) -> tuple[int, bytes]:
        """Help that reads a trailing number and a newline from a chunk

        Parameters
        ----------
        chunk : bytes
            An chunk of bytes that should contain the number and the newline.
        iterable : Iterable
            An iterable that will be used to extend ``chunk`` if no
            newline is found in ``chunk``.

        Returns
        -------
        int
            A tuple that contains the number that was found in the chunk and
            the trailing portion of the chunk that was not parsed.
        """
        while b'\n' not in chunk:
            lgr.log(5, 'completing number chunk')
            chunk += next(iterable)
        digits, trailing = chunk.split(b'\n', 1)
        return int(digits), trailing

    @abstractmethod
    def send(self, _) -> bytes:
        """Deliver the next part of generated output

        Whenever the response generator is iterated over, this method is called
        and should deliver the next part of the command output or raise
        ``StopIteration`` if the command has finished.
        """
        raise NotImplementedError

    @abstractmethod
    def get_final_command(self, command: bytes) -> bytes:
        """Return a final command list that executes ``command``

        This method should return a "final" command-pipeline that executes
        ``command`` and generates the output structure that the response
        generator expects. This structure will typically be parsed in the
        implementation of :meth:`send`.

        This method is usually only called by
        :meth:`ShellCommandExecutor.__call__`.
        """
        raise NotImplementedError

    def throw(self, typ, val=..., tb=...):  # pragma: no cover
        return super().throw(typ, val, tb)


class VariableLengthResponseGenerator(ShellCommandResponseGenerator, metaclass=ABCMeta):
    """Response generator that handles outputs of unknown length

    This response generator is used to execute a command that will result in an
    output of unknown length, e.g. ``ls``. The final command list it creates
    will execute the command and print a random end-marker and the return code
    after the output of the command. The :meth:`send`-method of this class uses
    the end-marker to determine then end of the command output.
    """
    def __init__(self,
                 stdout: OutputFrom,
                 ) -> None:
        self.end_marker = _create_end_marker()
        self.stream_marker = self.end_marker + b'\n'
        self.plain_stdout = stdout
        super().__init__(
            align_pattern(stdout, self.stream_marker),
            stdout.stderr_deque
        )

    def send(self, _) -> bytes:
        if self.state == 'output':
            chunk = next(self.stdout_gen)
            if self.stream_marker in chunk:
                self.state = 'returncode'
                chunk, self.returncode_chunk = chunk.split(self.stream_marker)
                if chunk:
                    return chunk
            else:
                return chunk

        if self.state == 'returncode':
            self.returncode, trailing = self._get_number_and_newline(
                self.returncode_chunk,
                self.plain_stdout,
            )
            if trailing:
                lgr.warning(
                    'unexpected output after return code: %s',
                    repr(trailing))
            self.state = 'exhausted'

        if self.state == 'exhausted':
            self.state = 'output'
            raise StopIteration()

        raise RuntimeError(f'unknown state: {self.state}')

    @property
    @abstractmethod
    def zero_command(self) -> bytes:
        """Return a command that functions as "zero command" """
        raise NotImplementedError


class VariableLengthResponseGeneratorPosix(VariableLengthResponseGenerator):
    """A variable length response generator for POSIX shells"""
    def __init__(self, stdout):
        """
        Parameters
        ----------
        stdout : OutputFrom
            A generator that yields output from a shell. Usually the object
            that is returned by :func:`iter_proc`.
        """
        super().__init__(stdout)

    def get_final_command(self, command: bytes) -> bytes:
        """Return a command list that executes ``command`` and prints the end-marker

        The POSIX version for variable length response generators.

        This method is usually only called by
        :meth:`ShellCommandExecutor.__call__`.
        """
        return (
            command + b' ; x=$?; echo -e -n "' + self.end_marker + b'\\n"; echo $x\n'
        )

    @property
    def zero_command(self) -> bytes:
        return b'test 0 -eq 0'


class VariableLengthResponseGeneratorPowerShell(VariableLengthResponseGenerator):
    """A variable length response generator for PowerShell shells"""
    def __init__(self, stdout):
        """
        Parameters
        ----------
        stdout : OutputFrom
            A generator that yields output from a shell. Usually the object
            that is returned by :func:`iter_proc`.
        """
        super().__init__(stdout)

    def get_final_command(self, command: bytes) -> bytes:
        """Return a command list that executes ``command`` and prints the end-marker

        The PowerShell version for variable length response generators.

        This method is usually only called by
        :meth:`ShellCommandExecutor.__call__`.
        """
        # TODO: check whether `command` sets `$LASTEXITCODE` and assign that
        #  to `$x`, iff set.
        return (
            b'$x=0; try {' + command + b'} catch { $x=1 }\n'
            + b'Write-Host -NoNewline ' + self.end_marker + b'`n$x`n\n'
        )

    @property
    def zero_command(self) -> bytes:
        return b'Write-Host hello'


class FixedLengthResponseGenerator(ShellCommandResponseGenerator, metaclass=ABCMeta):
    """Response generator for efficient handling of outputs of known length

    This response generator is used to execute commands that have an output of
    known length. The final command list it creates will execute the command and
    print the return code followed by a newline.

    The :meth:`send`-method of this response generator will read the specified
    number of bytes and a trailing return code. This is more performant than
    scanning the output for an end-marker.
    """
    def __init__(self,
                 stdout: OutputFrom,
                 length: int,
                 ) -> None:
        """
        Parameters
        ----------
        stdout : OutputFrom
            A generator that yields output from a shell. Usually the object
            that is returned by :func:`iter_proc`.
        length : int
            The length (in bytes) of the output that a command will generate.
        """
        super().__init__(stdout, stdout.stderr_deque)
        self.length = length
        self.read = 0

    def send(self, _) -> bytes:
        if self.state == 'output':
            chunk = next(self.stdout_gen)
            self.read += len(chunk)
            if self.read >= self.length:
                self.state = 'returncode'
                excess = self.read - self.length
                if excess > 0:
                    chunk, self.returncode_chunk = chunk[:-excess], chunk[-excess:]
                else:
                    self.returncode_chunk = b''
                if chunk:
                    return chunk
            else:
                return chunk

        if self.state == 'returncode':
            self.returncode, trailing = self._get_number_and_newline(
                self.returncode_chunk,
                self.stdout_gen,
            )
            if trailing:
                lgr.warning(
                    'unexpected output after return code: %s',
                    repr(trailing))
            self.state = 'exhausted'

        if self.state == 'exhausted':
            self.state = 'output'
            raise StopIteration()

        raise RuntimeError(f'unknown state: {self.state}')


class FixedLengthResponseGeneratorPosix(FixedLengthResponseGenerator):
    def get_final_command(self, command: bytes) -> bytes:
        """Return a final command list for a command with a fixed length output

        The POSIX version for fixed length response generators.

        This method is usually only called by
        :meth:`ShellCommandExecutor.__call__`.
        """
        return command + b' ; echo $?\n'


class FixedLengthResponseGeneratorPowerShell(FixedLengthResponseGenerator):
    def get_final_command(self, command: bytes) -> bytes:
        """Return a final command list for a command with a fixed length output

        The PowerShell version for fixed length response generators.

        This method is usually only called by
        :meth:`ShellCommandExecutor.__call__`.
        """
        return (
            b'$x=0; try {' + command + b'} catch { $x=1 }\n'
            + b'Write-Host -NoNewline $x`n\n'
        )


def _create_end_marker() -> bytes:
    """ Create a hopefully unique marker for the shell """
    # The following line is marked with `nosec` because `randint` is only
    # used to diversify markers, not for cryptographic purposes.
    marker_id = f'{randint(1000000000, 9999999999)}'.encode()  # nosec
    fixed_part = b'----datalad-end-marker-'
    return fixed_part + marker_id + fixed_part[::-1]
