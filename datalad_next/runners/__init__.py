"""Execution of subprocesses

This module provides all relevant components for subprocess execution.

.. currentmodule:: datalad_next.runners

Low-level tooling
-----------------

Two essential process execution/management utilities are provided, for
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

# runners
from datalad.runner import (
    GitRunner,
    Runner,
)
from datalad.runner.nonasyncrunner import ThreadedRunner

# protocols
from datalad.runner import (
    KillOutput,
    NoCapture,
    Protocol,
    StdOutCapture,
    StdErrCapture,
    StdOutErrCapture,
)
from datalad.runner.protocol import GeneratorMixIn
from .protocols import (
    NoCaptureGeneratorProtocol,
    StdOutCaptureGeneratorProtocol,
)
# exceptions
from datalad.runner.exception import (
    CommandError,
)

# utilities
from datalad.runner.nonasyncrunner import (
    STDOUT_FILENO,
    STDERR_FILENO,
)
from datalad.runner.utils import (
    LineSplitter,
)
from subprocess import (
    DEVNULL,
)
