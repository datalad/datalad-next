"""
This module provides a run-context manager that executes a subprocess and
can guarantee that the subprocess is terminated when the context is left.
"""
from __future__ import annotations

import subprocess
import sys
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from queue import Queue
from subprocess import DEVNULL
from typing import (
    Any,
    IO,
)

from datalad.runner.nonasyncrunner import ThreadedRunner as _ThreadedRunner

from . import (
    GeneratorMixIn,
    Protocol,
)


def _create_kill_wrapper(cls: type[Protocol]) -> type[Protocol]:
    """ Extend ``cls`` to supports the "kill-interface"

    This function creates a subclass of `cls` that contains the components
    of the "kill-interface".  The two major components are a method called
    `arm`, and logic inside the timeout handler that can trigger a termination
    or kill signal to the subprocess, if the termination time or kill time has
    come.

    Parameters
    ----------
    cls : type[Protocol]
        A protocol class that should be extended by the kill-interface

    Returns
    -------
    KillWrapper
        A protocol class that inherits `cls` and implements the kill logic
        that is used by the run-context-manager to forcefully terminate
        subprocesses.
    """

    class KillWrapper(cls):
        def __init__(self, *args, **kwargs):
            kill_wrapper_kwargs = kwargs.pop('dl_kill_wrapper_kwargs')
            self.armed = kill_wrapper_kwargs.pop('armed')
            self.introduced_timeout = kill_wrapper_kwargs.pop('introduced_timeout')
            self.terminate_time = kill_wrapper_kwargs.pop('terminate_time')
            kill_time = kill_wrapper_kwargs.pop('kill_time')
            self.kill_time = (
                ((self.terminate_time or 0) + kill_time)
                if kill_time is not None
                else kill_time
            )

            self.process: subprocess.Popen | None = None
            self.return_code: int | None = None
            self.kill_counter: int = 0

            super().__init__(*args, **kwargs)

        def arm(self) -> None:
            self.kill_counter = 0
            self.armed = True

        def connection_made(self, process: subprocess.Popen) -> None:
            self.process = process
            super().connection_made(process)

        def timeout(self, fd: int | None) -> bool:
            if self.armed and fd is None:
                self.kill_counter += 1
                if self.kill_time and self.kill_counter >= self.kill_time:
                    self.process.kill()
                    self.kill_time = None
                if self.terminate_time and self.kill_counter >= self.terminate_time:
                    self.process.terminate()
                    self.terminate_time = None

            # If we set the timeout argument due to a not-None kill_time
            # or a not-None terminate_time, and due to a None timeout parameter,
            # we leave the timeout handler here.
            if self.introduced_timeout:
                return False

            # If the timeout was set by the user of the context, we execute
            # the timeout handler of the superclass.
            return super().timeout(fd)

        def process_exited(self) -> None:
            self.return_code = self.process.poll()

    return KillWrapper


class KillingResultGenerator(Generator):
    """ A generator wrapper the arms a kill-protocol while waiting for yield"""
    def __init__(self, result_generator: Generator):
        self.result_generator = result_generator

    def send(self, value):
        self.result_generator.runner.protocol.arm()
        result = self.result_generator.send(value)
        self.result_generator.runner.protocol.disarm()
        return result

    def __getattr__(self, item):
        return getattr(self.result_generator, item)

    def throw(self, exception_type, value=None, trace_back=None):
        return Generator.throw(self, exception_type, value, trace_back)


@contextmanager
def run(
    cmd: list,
    protocol_class: type[Protocol],
    *,
    cwd: Path | None = None,
    stdin: int | IO | bytes | Queue[bytes | None] | None = None,
    timeout: float | None = None,
    terminate_time: int | None = None,
    kill_time: int | None = None,
    protocol_kwargs: dict | None = None,
) -> Any | Generator:
    """ A context manager for subprocesses

    The run-context manager will start a subprocess via ``ThreadedRunner``,
    provide the result of the subprocess invocation, i.e. the result of
    ``ThreadedRunner.run()`` in the ``as``-variable, and
    clean up the subprocess when the context is left.

    The run-context manager supports the guaranteed exit of a subprocess through
    either:

     a) natural exit of the subprocess
     b) termination of the subprocess via SIGTERM, if ``terminate_time`` is
        specified
     c) termination of the subprocess via SIGKILL, if ``kill_time`` is specified

    If the process terminates the run-context manager will ensure that its exit
    status is read, in order to prevent zombie processes.

    If neither ``terminate_time`` nor ``kill_time`` are specified, and the
    subprocess does not exit by itself, for example, because it waits for some
    input, the ``__exit__``-method of the run-context manager will never return.
    In other words the thread will seem to "hang" when leaving the run-context.
    The only way to ensure that the context is eventually left is to provide a
    ``kill_time``. It is a good idea to provide a ``terminate_time`` in
    addition, to allow the subprocess a graceful exit (see ``kill_time``- and
    ``terminate_time``-argument descriptions below).

    Generator- and non-generator-protocols are both supported by the
    context manager. Depending on the type of the provided protocol the
    interpretation of ``terminate_time`` and ``kill_time`` are different.

    If a non-generator-protocol is used, counting of the ``terminate_time`` and
    the ``kill_time`` starts when the subprocess is started.

    If a generator-protocol is used, counting of the ``terminate_time`` and
    the ``kill_time`` starts when the run-context is left.

    Parameters
    ----------
    cmd : list[str]
        The command list that is passed to ``ThreadedRunner.run()``
    protocol_class : type[Protocol]
        The protocol class that should be used to process subprocess events.
    cwd: Path, optional
        If provided, defines the current work directory for the subprocess
    stdin: int | IO | bytes | Queue[bytes | None], optional
        Input source or data for stdin of the subprocess. See the constructor
        of :class:`ThreadedRunner` for a detailed description
    timeout: float, optional
        If provided, defines the time after which the timeout-callback of the
        protocol will be enabled. See the constructor
        of :class:`ThreadedRunner` for a detailed description
    terminate_time: int, optional
        The number of timeouts after which a terminate-signal will be sent to
        the process, if it is still running. If no timeouts were provided in the
        ``timeout``-argument, the timeout is set to ``1.0``.
    kill_time: int, optional
        If ``terminate_time`` is given, a kill-signal will be sent to the
        subprocess after kill-signal after ``terminate_time + kill_time``
        timeouts. If ``terminate_time`` is not set, a kill-signal will be sent
        after ``kill_time`` timeouts.
        It is a good idea to set ``kill_time`` and ``terminate_time`` in order
        to let the process exit gracefully, if it is capable to do so.
    protocol_kwargs : dict
        A dictionary with Keyword arguments that will be used when
        instantiating the protocol class.

    Yields
    -------
    Any | Generator
        The result of the invocation of :meth:`ThreadedRunner.run` is returned.
    """
    introduced_timeout = False
    if timeout is None:
        introduced_timeout = True
        timeout = 1.0

    armed = False if issubclass(protocol_class, GeneratorMixIn) else True

    # Create the wrapping class. This is done mainly to ensure that the
    # termination-related functionality is present in the protocol class that
    # is used, independent of the actual protocol class that the user passes as
    # argument.
    # A side effect of this approach is, that the number of protocol class
    # definitions is reduced, because the user does not need to define
    # terminate-capable protocols for every protocol they want to use.
    kill_protocol_class = _create_kill_wrapper(protocol_class)

    runner = _ThreadedRunner(
        cmd=cmd,
        protocol_class=kill_protocol_class,
        stdin=DEVNULL if stdin is None else stdin,
        protocol_kwargs=dict(
            **(protocol_kwargs or {}),
            dl_kill_wrapper_kwargs=dict(
                introduced_timeout=introduced_timeout,
                terminate_time=terminate_time,
                kill_time=kill_time,
                armed=armed,
            )
        ),
        timeout=timeout,
        exception_on_error=False,
        cwd=cwd,
    )
    result = runner.run()
    # We distinguish between a non-generator run, i,e. a blocking run and
    # a generator run.
    if not issubclass(protocol_class, GeneratorMixIn):
        yield result
    else:
        try:
            yield KillingResultGenerator(result)
        finally:
            # Arm the protocol, that will enable terminate signaling or kill
            # signaling, if terminate_time or kill_time are not None.
            result.runner.protocol.arm()
            # Exhaust the generator. Because we have set a timeout, this will
            # lead to invocation of the timeout method of the wrapper, which
            # will take care of termination or killing. And it will fetch
            # the result code of the terminated process.
            # NOTE: if terminate_time and kill_time are both None, this might
            # loop forever.
            for _ in result:
                pass
