"""
-- autoclass:: ShellCommandExecutor
   :special-members: __call__


"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from queue import Queue
from typing import (
    Generator,
    Iterable,
)

from .response_generators import (
    ShellCommandResponseGenerator,
    VariableLengthResponseGenerator,
    VariableLengthResponseGeneratorPosix,
)
from datalad_next.consts import COPY_BUFSIZE
from datalad_next.runners.iter_subproc import (
    OutputFrom,
    iter_subproc,
)


__all__ = [
    'shell',
    'ShellCommandExecutor',
]


lgr = logging.getLogger('datalad.ext.next.shell')


@contextmanager
def shell(shell_cmd: list[str],
          *,
          chunk_size: int = COPY_BUFSIZE,
          zero_command_rg_class: type[VariableLengthResponseGenerator] = VariableLengthResponseGeneratorPosix,
          ) -> Generator[ShellCommandExecutor, None, None]:
    """Context manager that provides an interactive connection to a shell

    This context manager uses the provided argument ``shell_cmd`` to start a
    shell-subprocess. Usually the commands provided in ``shell_cmd`` will
    start a client for a remote shell, e.g. ``ssh``.

    :func:`shell` returns an instance of :class:`ShellCommandExecutor` in the
    ``as``-variable. This instance can be used to interact with the shell. That
    means, it can be used to execute commands to the shell, receive the data
    that the commands write to their ``stdout``, and retrieve the return code
    of the executed commands. All commands that are executed via the returned
    instance of :class:`ShellCommandExecutor` are executed in the same shell.

    Simple example that invokes a single command::

        >>> from datalad_next.shell import shell
        >>> with shell(['ssh', 'localhost']) as ssh:
        ...     result = ssh(b'ls -l /etc/passwd')
        ...     print(b''.join(result))
        ...     print(result.returncode)
        ...
        b'-rw-r--r-- 1 root root 2773 Nov 14 10:05 /etc/passwd\\n'
        0

    Example that invokes two commands, the second of which exits with a non-zero
    return code. The error output is retrieved from ``results.stderr_deque``::

        >>> from datalad_next.shell import shell
        >>> with shell(['ssh', 'localhost']) as ssh:
        ...     result = ssh(b'head -1 /etc/passwd')
        ...     print(b''.join(result))
        ...     result = ssh(b'ls /no-such-file')
        ...     print(b''.join(result))
        ...     print(result.returncode)
        ...     print(b''.join(result.stderr_deque))
        ...
        b'root:x:0:0:root:/root:/bin/bash\\n'
        b''
        2
        b"Pseudo-terminal will not be allocated because stdin is not a terminal.\\r\\nls: cannot access '/no-such-file': No such file or directory\\n"



    Parameters
    ----------
    shell_cmd : list[str]
        The command to execute the shell. It should be a list of strings that
        is given to :func:`iter_subproc` as `args`-parameter. For example:
        ``['ssh', '-p', '2222', 'localhost']``.
    chunk_size : int, optional
        The size of the chunks that are read from the shell's ``stdout`` and
        ``stderr``. This also defines the size of stored ``stderr``-content.
    zero_command_rg_class : type[VariableLengthResponseGenerator], optional, default: 'VariableLengthResponseGeneratorPosix'
        Shell uses an instance of the specified response generator class to
        execute the *zero command* ("zero command" is the command used to skip
        the login messages of the shell). This class will also be used as the
        default response generator for all further commands executed in the
        :class:`ShellCommandExecutor`-instances that is returned by
        :func:`shell`. Currently, the following concrete subclasses of
        :class:`VariableLengthResponseGenerator` exist:

            - :class:`VariableLengthResponseGeneratorPosix`: compatible with
              POSIX-compliant shells, e.g. ``sh`` or ``bash``.

            - :class:`VariableLengthResponseGeneratorPowerShell`: compatible
              with PowerShell.

    Yields
    ------
    :class:`ShellCommandExecutor`
    """

    def train(queue: Queue):
        """Use a queue to allow chaining of iterables at different times"""
        for iterable in iter(queue.get, None):
            yield from iterable

    subprocess_inputs: Queue = Queue()
    with iter_subproc(shell_cmd,
                      input=train(subprocess_inputs),
                      chunk_size=chunk_size,
                      bufsize=0) as shell_output:

        assert issubclass(zero_command_rg_class, VariableLengthResponseGenerator)

        cmd_executor = ShellCommandExecutor(
            subprocess_inputs,
            shell_output,
            shell_cmd,
            zero_command_rg_class
        )
        try:
            cmd_executor.command_zero(zero_command_rg_class(shell_output))
            # Return the now ready connection
            yield cmd_executor
        finally:
            # Ensure that the shell is terminated if an exception is raised by
            # code that uses `shell`. This is necessary because
            # the `terminate`-call that is invoked when leaving the
            # `iterable_subprocess`-context will not end the shell-process. It
            # will only terminate if its stdin is closed, or if it is killed.
            subprocess_inputs.put(None)


class ShellCommandExecutor:
    """Execute a command in a shell and return a generator that yields output

    Instances of :class:`ShellCommandExecutor` allow to execute commands that
    are provided as byte-strings via its :meth:`__call__`-method.

    To execute the command and collect its output,
    return code, and stderr-output, :class:`ShellCommandExecutor` uses
    instances of subclasses of :class:`ShellCommandResponseGenerator`, e.g.
    :class:`VariableLengthResponseGeneratorPosix`.
    """

    def __init__(self,
                 process_inputs: Queue,
                 stdout: OutputFrom,
                 shell_cmd: list[str],
                 default_rg_class: type[VariableLengthResponseGenerator],
                 ) -> None:
        self.process_inputs = process_inputs
        self.stdout = stdout
        self.shell_cmd = shell_cmd
        self.default_rg_class = default_rg_class

    def __call__(self,
                 command: bytes | str,
                 *,
                 stdin: Iterable[bytes] | None = None,
                 response_generator: ShellCommandResponseGenerator | None = None,
                 encoding: str = 'utf-8',
                 ) -> ShellCommandResponseGenerator:
        """Execute a command in the connected shell

        Parameters
        ----------
        command : bytes | str
            The command to execute. If the command is given as a string, it
            will be encoded to bytes using the encoding given in `encoding`.
        stdin : Iterable[byte] | None, optional, default: None
            If given, the bytes are sent to stdin of the command.

            Note: If the command reads its ``stdin`` until EOF, you have to use
            :meth:`self.close` to close ``stdin`` of the command. Otherwise,
            the command will usually not terminate. Once :meth:`self.close` is
            called, no more commands can be executed with this
            :class:`ShellCommandExecutor`-instance. If you want to execute
            further commands in the same :class:`ShellCommandExecutor`-instance,
            you must ensure
            that commands consume a fixed amount of input, for example,
            by using `head -c <byte-count> | <command>`.
        response_generator : ShellCommandResponseGenerator | None, optional, default: None
            If given, the responder generator (usually an instance of a subclass
            of ``ShellCommandResponseGenerator``), that is used to generate the
            command line and to parse the output of the command. This can be
            used to implement, for example, fixed length output processing.
        encoding : str, optional, default: 'utf-8'
            The encoding that is used to encode the command if it is given as a
            string. Note: the encoding should match the decoding the is used in
            the connected shell.

        Yields
        ------
        :class:`ShellCommandResponseGenerator`

            A generator that yields the output of ``stdout`` of the command.
            The generator is exhausted when all output is read. After that,
            the return code of the command execution and the stderr-output
            is available in the ``code``-attribute of the generator. If a
            response generator was passed in via the
            ``response_generator``-parameter, the same instance will be yielded.
        """
        # If no response generator is provided, we use the standard, variable
        # content response generator.
        if response_generator is None:
            response_generator = self.default_rg_class(self.stdout)

        if isinstance(command, str):
            command = command.endswith(encoding)

        command_list = response_generator.get_command_list(command)
        # Store the command list to report it in `CommandError`-exceptions.
        # This is done here to relieve the response generator classes from
        # this task.
        response_generator.current_command_list = command_list
        self.process_inputs.put(command_list)
        if stdin is not None:
            self.process_inputs.put(stdin)
        return response_generator

    def __repr__(self):
        return f'{self.__class__.__name__}({self.shell_cmd!r})'

    def close(self):
        """stop input to the shell

        This method closes stdin of the shell. This will in turn terminate
        the shell, no further commands can be executed in the shell.
        """
        self.process_inputs.put(None)

    def command_zero(self,
                     response_generator: VariableLengthResponseGenerator
                     ) -> None:
        """Execute the zero command

        This method is only used by :func:`shell` to skip any login messages
        """
        result_zero = self(
            response_generator.zero_command,
            response_generator=response_generator
        )
        for line in result_zero:
            lgr.debug('skipped login message line: %s', line)
