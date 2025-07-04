"""Execution of subprocesses

.. deprecated:: 1.6
   This module is deprecated. It has been partially migrated to the
   `datalad-core library <https://pypi.org/project/datalad-core>`__. Imports
   should be adjusted to ``datalad_core.runners``.

This module provides all relevant components for subprocess execution.  The
main work horse is :func:`~datalad_next.runners.iter_subproc`, a context
manager that enables interaction with a subprocess in the form of an iterable
for input/output processing. Execution errors are communicated with the
:class:`~datalad_next.runners.CommandError` exception. In addition, a few
convenience functions are provided to execute Git commands (including
git-annex).

.. currentmodule:: datalad_next.runners
.. autosummary::
   :toctree: generated

   iter_subproc
   call_git
   call_git_lines
   call_git_oneline
   call_git_success
   iter_git_subproc
   CommandError


Low-level tooling from datalad-core
-----------------------------------

.. deprecated:: 1.4
   The functionality described here has been deprecated, and the associated
   imports from datalad-core are scheduled for removal with version 2.0.
   Use the implementations listed above instead.

Few process execution/management utilities are provided, for
generic command execution, and for execution command in the context
of a Git repository.

.. autosummary::
   :toctree: generated

   GitRunner
   Runner

Additional information on the design of the subprocess execution tooling
is available from https://docs.datalad.org/design/threaded_runner.html

A standard exception type is used to communicate any process termination
with a non-zero exit code

.. autosummary::
   :toctree: generated

   CommandError

Command output can be processed via "protocol" implementations that are
inspired by ``asyncio.SubprocessProtocol``.

.. autosummary::
   :toctree: generated

   KillOutput
   NoCapture
   StdOutCapture
   StdErrCapture
   StdOutErrCapture
"""

__all__ = [
    'call_git',
    'call_git_lines',
    'call_git_oneline',
    'call_git_success',
    'iter_git_subproc',
    'iter_subproc',
    'CommandError',
    'GitRunner',
    'KillOutput',
    'NoCapture',
    'Protocol',
    'Runner',
    'StdErrCapture',
    'StdOutCapture',
    'StdOutErrCapture',
    'STDERR_FILENO',
    'STDOUT_FILENO',
    'ThreadedRunner',
    'LineSplitter',
    'GeneratorMixIn',
    'NoCaptureGeneratorProtocol',
    'StdOutCaptureGeneratorProtocol',
    'DEVNULL',
]

import warnings

# TODO: REMOVE FOR V2.0
from subprocess import (
    DEVNULL,
)

# runners
# TODO: REMOVE FOR V2.0
# protocols
# TODO: REMOVE FOR V2.0
from datalad.runner import (
    GitRunner,
    KillOutput,
    NoCapture,
    Protocol,
    Runner,
    StdErrCapture,
    StdOutCapture,
    StdOutErrCapture,
)

# TODO: REMOVE FOR V2.0
# utilities
# TODO: REMOVE FOR V2.0
from datalad.runner.nonasyncrunner import (
    STDERR_FILENO,
    STDOUT_FILENO,
    ThreadedRunner,
)

# TODO: REMOVE FOR V2.0
from datalad.runner.protocol import GeneratorMixIn

# TODO: REMOVE FOR V2.0
from datalad.runner.utils import (
    LineSplitter,
)
from datalad_core.runners import (
    call_git,
    call_git_lines,
    call_git_oneline,
    call_git_success,
    iter_git_subproc,
    iter_subproc,
)

# exceptions
# The following import supports legacy code that uses `CommandError` from this
# module. If you are writing new code, please use `CommandError` from
# `datalad_core.runners`. We intend to remove this import in the future.
from datalad_next.exceptions import CommandError

# TODO: REMOVE FOR V2.0
from .protocols import (
    NoCaptureGeneratorProtocol,
    StdOutCaptureGeneratorProtocol,
)

warnings.warn(
    '`datalad_next.runners` has been partially migrated to the '
    'datalad-core library, '
    'check docs, and adjust imports to `datalad_core.runners`',
    DeprecationWarning,
    stacklevel=1,
)
