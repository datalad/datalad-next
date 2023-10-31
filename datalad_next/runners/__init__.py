"""Execution of subprocesses

This module provides all relevant components for subprocess execution.

.. currentmodule:: datalad_next.runners


Execution of subprocesses is provided by the context manager
:func:`datalad_next.runners.run.run`. The context manager uses protocols
to process the output of the subprocess. A number of protocols are provided
in this module. New protocols can be implemented by the user of the
``run``-context.

There are two execution modes for subprocesses, a synchronous mode and an
asynchronous mode.

In the synchronous mode the subprocess will be executed and
the result of the ``as``-variable is (usually) a dictionary containing the
exit code, stdout-output, and stderr-output of the subprocess execution (whether
stdout- and stderr-content is actually contained depends on the protocol that
was used).

In asynchronous mode the ``as``-variable will reference a generator that will
yield output from the subprocess as soon as it is available, i.e. as soon as the
protocol "sends" them to the generator. After the process has exited, the
generator will stop the iteration and the exit code of the process will be
available in the ``return_code``-attribute of the generator.

The context manager will only exit the context, if the subprocess has exited.
That means, a subprocess that never exits will prevent the context manager from
exiting the context. The user of the run-context should therefore trigger a
subprocess exit before leaving the context, e.g. by closing stdin of the
subprocess, or by sending it a signal.

To ensure that control flow leaves the run-context, the run-context provides
timeouts that will terminate and, if termination fails, finally kill the
subprocess. Timeouts are supported in synchronous and in asynchronous mode.
In synchronous mode the timeout is measured from the moment when the context
is entered. In asynchronous mode, the timeout is measured when fetching the next
element from the result generator and stopped when the result generator yields
the element. In asynchronous mode the timeout is also measured, when the
control flow leaves the run-context, i.e. when the control flow enters the
exit-handler of the context. (Check the documentation of the context manager
:func:`datalad_next.runners.run.run`. For additional keyword arguments check
also the documentation of
:class:`datalad.runner.nonasyncrunner.ThreadedRunner`.)

.. currentmodule:: datalad_next.runners
.. autosummary::
   :toctree: generated

   run


A standard exception type is used to communicate any process termination
with a non-zero exit code (unless the keyword argument ``exception_on_error`` is
set to ``False``.


.. autosummary::
   :toctree: generated

   CommandError

Command output can be processed via "protocol" implementations that are
inspired by ``asyncio.SubprocessProtocol``. The following synchronous protocols
are provided in ``datalad_next.runners``

.. autosummary::
   :toctree: generated

   KillOutput
   NoCapture
   StdOutCapture
   StdErrCapture
   StdOutErrCapture

In addition ``datalad_next.runners```provides the following asynchronous
protocols:

.. autosummary::
   :toctree: generated

   NoCaptureGeneratorProtocol
   StdOutCaptureGeneratorProtocol


Low-level tooling
-----------------
The ``run``-context uses the class :class:`ThreadedRunner` to execute
subprocesses. Additional information on the design of :class:`ThreadedRunner`
 is available from https://docs.datalad.org/design/threaded_runner.html

"""

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
