"""A persistent shell connection

This module provides a context manager that establishes a connection to a shell
and can be used to execute multiple commands in that shell. Shells are usually
remote shells, e.g. connected via an ``ssh``-client, but local shells like
``zsh``, ``bash`` or ``PowerShell`` can also be used.

The context manager returns an instance of :class:`ShellCommandExecutor` that
can be used to execute commands in the shell via the method
:meth:`ShellCommandExecutor.__call__`. The method will return an instance of
a subclass of :class:`ShellCommandResponseGenerator` that can be used to
retrieve the output of the command, the result code of the command, and the
stderr-output of the command.

Every response generator expects a certain output structure. It is responsible
for ensuring that the output structure is generated. To this end every
response generator provides a method
:meth:`ShellCommandResponseGenerator.get_command_list`. The method
:class:`ShellCommandExecutor.__call__` will pass the user-provided command to
:meth:`ShellCommandResponseGenerator.get_command_list` and receive a list of
final commands that should be executed in the connected shell and that will
generate the expected output structure. Instances of
:class:`ShellCommandResponseGenerator` have therefore four tasks:

    1. Create a final command list that is used to execute the user provided
       command. This could, for example, execute the command, print an
       end marker, and print the return code of the command.

    2. Parse the output of the command, yield it to the user.

    3. Read the return code and provide it to the user.

    4. Provide stderr-output to the user.

A very versatile example of a response generator is the class
:class:`VariableLengthResponseGenerator`. It can be used to execute a command
that will result in an output of unknown length, e.g. ``ls``, and will yield
the output of the command to the user. It does that by using a random
*end marker* to detect the end of the output and read the trailing return code.
This is suitable for almost all commands.

If :class:`VariableLengthResponseGenerator` is so versatile, why not just
implement its functionality in :class:`ShellCommandExecutor`? There are two
major reasons for that:

1. Although the :class:`VariableLengthResponseGenerator` is very versatile,
   it is not the most efficient implementation for commands that produce large
   amounts of output. In addition, there is also a minimal risk that the end
   marker is part of the output of the command, which would trip up the response
   generator. Putting response generation into a separate class allows to
   implement specific operations more efficiently and more safely.
   For example,
   :class:`DownloadResponseGenerator` implements the download of files. It
   takes a remote file name as user "command" and creates a final command list
   that emits the length of the file, a newline, the file content, a return
   code, and a newline. This allows :class:`DownloadResponseGenerator`
   to parse the output without relying on an end marker, thus increasing
   efficiency and safety

2. Factoring out the response generation creates an interface that can be used
   to support the syntax of different shells and the difference in command
   names and options in different operating systems. For example, the response
   generator class :class:`VariableLengthResponseGeneratorPowerShell` supports
   the invocation of commands with variable length output in a ``PowerShell``.

In short, parser generator classes encapsulate details of shell-syntax and
operation implementation. That allows support of different shell syntax, and
the efficient implementation of specific higher level operations, e.g.
``download``. It also allows users to extend the functionality of
:class:`ShellCommandExecutor` by providing their own response generator
classes.

The module :mod:`datalad_next.shell.response_generators` provides two generally
applicable abstract response generator classes:

    - :class:`VariableLengthResponseGenerator`

    - :class:`FixedLengthResponseGenerator`

The functionality of the former is described above. The latter can be used to
execute a command that will result in output of known
length, e.g. ``echo -n 012345``. It reads the specified number of bytes and a
trailing return code. This is more performant than the variable length response
generator (because it does not have to search for the end marker). In addition,
it does not rely on the uniqueness of the end marker. It is most useful for
operation like ``download``, where the length of the output can be known in
advance.

As mentioned above, the classes :class:`VariableLengthResponseGenerator` and
:class:`FixedLengthResponseGenerator` are abstract. The module
:mod:`datalad_next.shell.response_generators` provides the following concrete
implementations for them:

    - :class:`VariableLengthResponseGeneratorPosix`

    - :class:`VariableLengthResponseGeneratorPowerShell`

    - :class:`FixedLengthResponseGeneratorPosix`

    - :class:`FixedLengthResponseGeneratorPowerShell`

When :func:`datalad_next.shell.shell` is executed it will use a
:class:`VariableLengthResponseClass` to skip the login message of the shell.
This is done by executing a *zero command* (a command that will possibly
generate some output, and successfully return) in the shell. The zero command is
provided by the concrete implementation of class
:class:`VariableLengthResponseGenerator`. For example, the zero command for
POSIX shells is ``test 0 -eq 0``, for PowerShell it is ``Write-Host hello``.

Because there is no way for func:`shell` to determine the kind of shell it
connects to, the user can provide an alternative response generator class, in
the ``zero_command_rg_class``-parameter. Instance of that class
will then be used to execute the zero command. Currently, the following two
response generator classes are available:

    - :class:`VariableLengthResponseGeneratorPosix`: works with POSIX-compliant
      shells, e.g. ``sh`` or ``bash``. This is the default.
    - :class:`VariableLengthResponseGeneratorPowerShell`: works with PowerShell.

Whenever a command is executed via :meth:`ShellCommandExecutor.__call__`, the
class identified by ``zero_command_rg_class`` will be used by default to create
the final command list and to parse the result. Users can override this on a
per-call basis by providing a different response generator class in the
``response_generator``-parameter of :meth:`ShellCommandExecutor.__call__`.

Examples
--------

See the documentation of :func:`datalad_next.shell.shell` for examples of how to
use the shell-function and different response generator classes.

API overview
------------
.. currentmodule:: datalad_next.shell

.. autosummary::
   :toctree: generated
   :recursive:

   ShellCommandExecutor
   ShellCommandResponseGenerator
   VariableLengthResponseGenerator
   VariableLengthResponseGeneratorPosix
   VariableLengthResponseGeneratorPowerShell
   FixedLengthResponseGenerator
   FixedLengthResponseGeneratorPosix
   FixedLengthResponseGeneratorPowerShell
   DownloadResponseGenerator
   DownloadResponseGeneratorPosix
   operations.posix.upload
   operations.posix.download
   operations.posix.delete
"""


__all__ = [
    'shell',
    'posix',
]

from .shell import (
    shell,
    ShellCommandExecutor,
)

from .operations import posix
from .operations.posix import (
    DownloadResponseGenerator,
    DownloadResponseGeneratorPosix,
)
from .response_generators import (
    FixedLengthResponseGenerator,
    FixedLengthResponseGeneratorPosix,
    FixedLengthResponseGeneratorPowerShell,
    ShellCommandResponseGenerator,
    VariableLengthResponseGenerator,
    VariableLengthResponseGeneratorPosix,
    VariableLengthResponseGeneratorPowerShell,
)
