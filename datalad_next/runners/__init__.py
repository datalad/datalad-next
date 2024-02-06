"""Execution of subprocesses

This module provides all relevant components for subprocess execution.

.. currentmodule:: datalad_next.runners

Low-level tooling
-----------------

Few process execution/management utilities are provided, for
generic command execution, and for execution command in the context
of a Git repository.

.. autosummary::
   :toctree: generated

   GitRunner
   Runner
   iter_subproc
   iter_git_subproc

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

from .iter_subproc import (
    iter_subproc,
)
from .git import (
    call_git,
    call_git_lines,
    call_git_oneline,
    call_git_success,
    iter_git_subproc,
)

# runners
# TODO REMOVE FOR V2.0
from datalad.runner import (
    GitRunner,
    Runner,
)
# TODO REMOVE FOR V2.0
from datalad.runner.nonasyncrunner import ThreadedRunner
# protocols
# TODO REMOVE FOR V2.0
from datalad.runner import (
    KillOutput,
    NoCapture,
    Protocol,
    StdOutCapture,
    StdErrCapture,
    StdOutErrCapture,
)
# TODO REMOVE FOR V2.0
from datalad.runner.protocol import GeneratorMixIn
# TODO REMOVE FOR V2.0
from .protocols import (
    NoCaptureGeneratorProtocol,
    StdOutCaptureGeneratorProtocol,
)
# exceptions
# The following import supports legacy code that uses `CommandError` from this
# module. If you are writing new code, please use `CommandError` from
# `datalad.support.exceptions`. We intend to remove this import in the future.
from datalad_next.exceptions import CommandError

# utilities
# TODO REMOVE FOR V2.0
from datalad.runner.nonasyncrunner import (
    STDOUT_FILENO,
    STDERR_FILENO,
)
# TODO REMOVE FOR V2.0
from datalad.runner.utils import (
    LineSplitter,
)
# TODO REMOVE FOR V2.0
from subprocess import (
    DEVNULL,
)
