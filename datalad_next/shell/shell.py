"""
-- autoclass:: ShellCommandExecutor
   :special-members: __call__


"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
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
from datalad_next.exceptions import CommandError
from datalad_next.runners.iter_subproc import (
    OutputFrom,
    iter_subproc,
)


__all__ = [
    'shell',
    'ExecutionResult',
    'ShellCommandExecutor',
]


lgr = logging.getLogger('datalad.ext.next.shell')


@dataclass
class ExecutionResult:
    stdout: bytes
    stderr: bytes
    returncode: int | None

    def to_exception(self,
                     command: bytes | str | list[str],
                     message: str = ''
                     ):
        if self.returncode != 0:
            raise CommandError(
                cmd=command.decode()
                    if isinstance(command, bytes)
                    else str(command),
                msg=message,
                code=self.returncode,
                stdout=self.stdout,
                stderr=self.stderr,
            )


@contextmanager
def shell(shell_cmd: list[str],
          *,
          credential: str | None = None,
          chunk_size: int = COPY_BUFSIZE,
          zero_command_rg_class: type[VariableLengthResponseGenerator] = VariableLengthResponseGeneratorPosix,
          ) -> Generator[ShellCommandExecutor, None, None]:
    """Context manager that provides an interactive connection to a shell

    This context manager uses the provided argument ``shell_cmd`` to start a
    shell-subprocess. Usually the commands provided in ``shell_cmd`` will
    start a client for a remote shell, e.g. ``ssh``.

    :func:`shell` returns an instance of :class:`ShellCommandExecutor` in the
    ``as``-variable. This instance can be used to interact with the shell. That
    means, it can be used to execute commands in the shell, receive the data
    that the commands write to their ``stdout`` and ``stderr``, and retrieve
    the return code of the executed commands. All commands that are executed
    via the returned instance of :class:`ShellCommandExecutor` are executed in
    the same shell instance.

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

    Examples
    --------

    **Example 1:** a simple example that invokes a single command, prints its
    output and its return code::

        >>> from datalad_next.shell import shell
        >>> with shell(['ssh', 'localhost']) as ssh:
        ...     result = ssh(b'ls -l /etc/passwd')
        ...     print(result.stdout)
        ...     print(result.returncode)
        ...
        b'-rw-r--r-- 1 root root 2773 Nov 14 10:05 /etc/passwd\\n'
        0

    **Example 2:** this example invokes two commands, the second of which exits
    with a non-zero return code. The error output is retrieved from
    ``result.stderr``, which contains all ``stderr`` data that was written
    since the last command was executed::

        >>> from datalad_next.shell import shell
        >>> with shell(['ssh', 'localhost']) as ssh:
        ...     print(ssh(b'head -1 /etc/passwd').stdout)
        ...     result = ssh(b'ls /no-such-file')
        ...     print(result.stdout)
        ...     print(result.returncode)
        ...     print(result.stderr)
        ...
        b'root:x:0:0:root:/root:/bin/bash\\n'
        b''
        2
        b"Pseudo-terminal will not be allocated because stdin is not a terminal.\\r\\nls: cannot access '/no-such-file': No such file or directory\\n"

    **Example 3:** demonstrates how to use the
    ``check``-parameter to raise a :class:`CommandError`-exception if the
    return code of the command is
    not zero. This delegates error handling to the calling code and helps to
    keep the code clean::

        >>> from datalad_next.shell import shell
        >>> with shell(['ssh', 'localhost']) as ssh:
        ...     print(ssh(b'ls /no-such-file', check=True).stdout)
        ...
        Traceback (most recent call last):
          File "<stdin>", line 2, in <module>
          File "/home/cristian/Develop/datalad-next/datalad_next/shell/shell.py", line 279, in __call__
            return create_result(
          File "/home/cristian/Develop/datalad-next/datalad_next/shell/shell.py", line 349, in create_result
            result.to_exception(command, error_message)
          File "/home/cristian/Develop/datalad-next/datalad_next/shell/shell.py", line 52, in to_exception
            raise CommandError(
        datalad.runner.exception.CommandError: CommandError: 'ls /no-such-file' failed with exitcode 2 [err: 'cannot access '/no-such-file': No such file or directory']

    **Example 4:** an example for manual checking of the return code::

        >>> from datalad_next.shell import shell
        >>> def file_exists(file_name):
        ...     with shell(['ssh', 'localhost']) as ssh:
        ...         result = ssh(f'ls {file_name}')
        ...         return result.returncode == 0
        ... print(file_exists('/etc/passwd'))
        True
        >>> print(file_exists('/no-such-file'))
        False

    **Example 5:** an example for result content checking::

        >>> from datalad_next.shell import shell
        >>> with shell(['ssh', 'localhost']) as ssh:
        ...     result = ssh(f'grep root /etc/passwd', check=True).stdout
        ...     if len(result.splitlines()) != 1:
        ...         raise ValueError('Expected exactly one line')

    **Example 6:** how to work with generator-based results.
    For long running commands a generator-based result fetching
    can be used. To use generator-based output the command has to be executed
    with the method
    :meth:`ShellCommandExecutor.start`. This method returns a generator that
    provides command output as soon as it is available::

        >>> import time
        >>> from datalad_next.shell import shell
        >>> with shell(['ssh', 'localhost']) as ssh:
        ...     result_generator = ssh.start(b'c=0; while [ $c -lt 6 ]; do head -2 /etc/passwd; sleep 2; c=$(( $c + 1 )); done')
        ...     for result in result_generator:
        ...         print(time.time(), result)
        ...     assert result_generator.returncode == 0
        1713358098.82588 b'root:x:0:0:root:/root:/bin/bash\\nsystemd-timesync:x:497:497:systemd Time Synchronization:/:/usr/sbin/nologin\\n'
        1713358100.8315682 b'root:x:0:0:root:/root:/bin/bash\\nsystemd-timesync:x:497:497:systemd Time Synchronization:/:/usr/sbin/nologin\\n'
        1713358102.8402972 b'root:x:0:0:root:/root:/bin/bash\\nsystemd-timesync:x:497:497:systemd Time Synchronization:/:/usr/sbin/nologin\\n'
        1713358104.8490314 b'root:x:0:0:root:/root:/bin/bash\\nsystemd-timesync:x:497:497:systemd Time Synchronization:/:/usr/sbin/nologin\\n'
        1713358106.8577306 b'root:x:0:0:root:/root:/bin/bash\\nsystemd-timesync:x:497:497:systemd Time Synchronization:/:/usr/sbin/nologin\\n'
        1713358108.866439 b'root:x:0:0:root:/root:/bin/bash\\nsystemd-timesync:x:497:497:systemd Time Synchronization:/:/usr/sbin/nologin\\n'

    (The exact output of the above example might differ, depending on the
    length of the first two entries in the ``/etc/passwd``-file.)

    **Example 7:** how to use the ``stdin``-parameter to feed data to a command
    that is executed in the persistent shell.
    The methods :meth:`ShellCommandExecutor.__call__` and
    :meth:`ShellCommandExecutor.start` allow to pass an iterable in the
    ``stdin``-argument. The content of this iterable will be sent to ``stdin``
    of the executed command::

        >>> from datalad_next.shell import shell
        >>> with shell(['ssh', 'localhost']) as ssh:
        ...     result = ssh(b'head -c 4', stdin=(b'ab', b'c', b'd'))
        ...     print(result.stdout)
        b'abcd'

    **Example 8:** how to work with commands that consume ``stdin`` completely.
    In the previous example, the command
    ``head -c 4`` was used to consume data from ``stdin``. This command
    terminates after
    reading exactly 4 bytes from ``stdin``. If ``cat`` was used
    instead of ``head -c 4``, the command would have
    continued to run until its ``stdin`` was closed. The ``stdin`` of the
    command that is executed in the persistent shell can be close by calling
    :meth:`ssh.close`. But, in order to be able to call :meth:`ssh.close`,
    any process that consumes ``stdin`` completely should be executed by
    calling the :meth:`ssh.start`-method.
    The reason for this is that :meth:`ssh.start` will return immediately which
    allows to call the :meth:`ssh.close`-method, as shown in the following
    code (:meth:`ssh.__call__` would have waited for ``cat`` to terminate, but
    because :meth:`ssh.close` is not called, ``cat`` would never terminate)::

        >>> from datalad_next.shell import shell
        >>> with shell(['ssh', 'localhost']) as ssh:
        ...     result_generator = ssh.start(b'cat', stdin=(b'12', b'34', b'56'))
        ...     ssh.close()
        ...     print(tuple(result_generator))
        (b'123456',)

    Note that
    the ``ssh``-object cannot be used for further command execution after
    :meth:`ssh.close` was called. Further command execution requires to spin up
    a new persistent shell-object. To prevent this overhead, it is advised to
    limit the number of bytes that a shell-command consumes, either by their
    number, e.g. by using ``head -c``, or by some other means, e.g.
    by interpreting the content or using a command like ``timeout``.

    **Example 9:** upload a file to the persistent shell. The command
    ``head -c`` can be used to implement the upload a file to a remote shell.
    The basic idea
    is to determine the number of bytes that will be uploaded and create a
    command in the remote shell that will consume exactly this amount of bytes.
    The following code implements this idea (without file-name escaping and
    error handling)::

        >>> import os
        >>> import time
        >>> from datalad_next.shell import shell
        >>> def upload(ssh, file_name, remote_file_name):
        ...     size = os.stat(file_name).st_size
        ...     f = open(file_name, 'rb')
        ...     return ssh(f'head -c {size} > {remote_file_name}', stdin=iter(f.read, b''))
        ...
        >>> with shell(['ssh', 'localhost']) as ssh:
        ...     upload(ssh, '/etc/passwd', '/tmp/uploaded-1')

    Note: in this example, ``f`` is not explicitly closed, it is only
    closed when the program exits. The reason for
    this is that the shell uses threads internally for stdin-feeding, and there
    is no simple way to determine whether the thread that reads ``f`` has yet
    read an EOF and exited. If ``f`` is closed before the thread exits, and the
    thread tries to read from ``f``, a ``ValueError`` will be raised (the
    function :func:`datalad_next.shell.posix.upload` contains a solution
    for this problem that has slightly more code. For the sake of simplicity,
    this solution was not implemented in the example above).

    **Example 10:** download a file. This example
    uses a fixed-length response generator
    to download a file from a remote shell. The basic idea is to determine the
    number of bytes that will be downloaded and create a fixed-length response
    generator that reads exactly this number of bytes. The fixed length response
    generator is then passed to :meth:`ssh.start` in the keyword-argument
    ``response_generator``. This instructs :meth:`ssh.start` to use the response
    generator to interpret the output of this command invocation (the example
    code has no file-name escaping or error handling)::

        >>> from datalad_next.shell import shell
        >>> from datalad_next.shell.response_generators import FixedLengthResponseGeneratorPosix
        >>> def download(ssh, remote_file_name, local_file_name):
        ...     size = ssh(f'stat -c %s {remote_file_name}').stdout
        ...     with open(local_file_name, 'wb') as f:
        ...         response_generator = FixedLengthResponseGeneratorPosix(ssh.stdout, int(size))
        ...         results = ssh.start(f'cat {remote_file_name}', response_generator=response_generator)
        ...         for chunk in results:
        ...             f.write(chunk)
        ...
        >>> with shell(['ssh', 'localhost']) as ssh:
        ...     download(ssh, '/etc/passwd', '/tmp/downloaded-1')
        ...

    Note that :meth:`ssh.start` is used to start the download. This allows to
    process downloaded data as soon as it is available.

    **Example 11:**
    This example implements interaction with a *Python* interpreter (which
    can be local or remote). Interaction in the context of this example means,
    executing a
    line of python code, returning the result, i.e. the output on ``stdout``,
    and detect whether an exception was raised or not. To this end
    a Python-specific variable-length response generator is created by
    subclassing the
    generic class :class:`VariableLengthResponseGenerator`. The new response
    generator implements the method :meth:`get_final_command`, which takes a
    python statement and returns a ``try``-``except``-block that executes the
    python statement, prints the end-marker and a return code (which is ``0`` if
    the statement was executed successfully, and ``1`` if an exception was
    raised)::

        >>> from datalad_next.shell import shell
        >>> from datalad_next.shell.response_generators import VariableLengthResponseGenerator
        >>> class PythonResponseGenerator(VariableLengthResponseGenerator):
        ...     def get_final_command(self, command: bytes) -> bytes:
        ...         return f'''try:
        ...     {command.decode()}
        ...     print('{self.end_marker.decode()}')
        ...     print(0)
        ... except:
        ...     print('{self.end_marker.decode()}')
        ...     print(1)
        ... '''.encode()
        ...     @property
        ...     def zero_command(self) -> bytes:
        ...         return b'True'
        ...
        >>> with shell(['python', '-u', '-i']) as py:
        ...     print(py('1 + 1'))
        ...     print(py('1 / 0'))
        ...
        ExecutionResult(stdout=b'2\\n', stderr=b'>>> ... ... ... ... ... ... ... ... ', returncode=0)
        ExecutionResult(stdout=b'', stderr=b'... ... ... ... ... ... ... ... Traceback (most recent call last):\\n  File "<stdin>", line 2, in <module>\\nZeroDivisionError: division by zero', returncode=1)

    The python response generator could be extended to deliver exception
    information in an extended ``ExecutionResult``. This can be achieved by
    *pickling* (see the ``pickle``-module) a caught exception to a byte-string,
    printing this byte-string after the return-code line, and printing another
    end-marker. The :meth:`send`-method of the response generator must then
    be overwritten to unpickle the exception information and store it in an
    extended ``ExecutionResult`` (or raise it in the shell-context, if that is
    preferred).

    **Example 12:** this example shows how to use the shell context handler
    in situations were a ``with``-statement is not suitable, e.g. if a shell
    object should be used in multiple, independently called functions. In this
    case the context manager
    can be manually entered and exited. The following code generates a global
    ``ShellCommandExecutor``-instance in the ``ssh``-variable::

        >>> from datalad_next.shell import shell
        >>> context_manager = shell(['ssh', 'localhost'])
        >>> ssh = context_manager.__enter__()
        >>> print(ssh(b'ls /etc/passwd').stdout)
        b'/etc/passwd\\n'
        >>> context_manager.__exit__(None, None, None)
        False

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
                 check: bool = False
                 ) -> ExecutionResult:
        """Execute a command in the connected shell and return the result

        This method executes the given command in the connected shell. It
        assembles all output on stdout, all output on stderr that was
        written during the execution of the command, and the return
        code of the command.
        (The response generator defines when the command output is considered
        complete. Usually that is done by checking for a random end-of-output
        marker.)

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
        check : bool, optional, default: False
            If True, a :class:`CommandError`-exception is raised if the return
            code of the command is not zero.

        Returns
        -------
        :class:`ExecutionResult`
            An instance of :class:`ExecutionResult` that contains the
            ``stdout``-output, the ``stderr``-output, and the return code of
            the command.

        Raises
        ------
        :class:`CommandError`
            If the return code of the command is not zero and `check` is True.
        """
        response_generator = self.start(
            command,
            stdin=stdin,
            response_generator=response_generator,
            encoding=encoding,
        )
        stdout = b''.join(response_generator)
        stderr = b''.join(self.stdout.stderr_deque)
        self.stdout.stderr_deque.clear()
        return create_result(
            response_generator,
            command,
            stdout,
            stderr,
            check=check
        )

    def start(self,
              command: bytes | str,
              *,
              stdin: Iterable[bytes] | None = None,
              response_generator: ShellCommandResponseGenerator | None = None,
              encoding: str = 'utf-8',
              ) -> ShellCommandResponseGenerator:
        """Execute a command in the connected shell

        Execute a command in the connected shell and return a generator that
        provides the content written to stdout of the command. After the
        generator is exhausted, the return code of the command is available
        in the ``returncode``-attribute of the generator.

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

        Returns
        -------
        :class:`ShellCommandResponseGenerator`

            A generator that yields the output of ``stdout`` of the command.
            The generator is exhausted when all output is read. After that,
            the return code of the command execution
            is available in the ``returncode``-attribute of the generator,
            and the stderr-output is available in the ``stderr_deque``-attribute
            of the response generator.
            If a response generator was passed in via the
            ``response_generator``-parameter, the same instance will be
            returned.
        """
        if response_generator is None:
            response_generator = self.default_rg_class(self.stdout)

        if isinstance(command, str):
            command = command.encode(encoding)

        final_command = response_generator.get_final_command(command)
        # Store the command list to report it in `CommandError`-exceptions.
        # This is done here to relieve the response generator classes from
        # this task.
        self.process_inputs.put([final_command])
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
            response_generator=response_generator,
            check=True,
        )
        lgr.debug('skipped login message: %s', result_zero.stdout)


def create_result(response_generator: ShellCommandResponseGenerator,
                  command: bytes | str | list[str],
                  stdout: bytes,
                  stderr: bytes,
                  error_message: str = '',
                  check: bool = False) -> ExecutionResult:

    result = ExecutionResult(
        stdout=stdout,
        stderr=stderr,
        returncode=response_generator.returncode
    )
    if check is True:
        result.to_exception(command, error_message)
    return result
