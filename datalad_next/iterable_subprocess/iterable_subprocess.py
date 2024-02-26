from collections import deque
from collections.abc import Generator
from contextlib import contextmanager
from subprocess import PIPE, Popen
from threading import Thread

# Importing from datalad-core to prevent circular imports
from datalad_next.exceptions import CommandError


class OutputFrom(Generator):
    def __init__(self, stdout, stderr_deque, chunk_size=65536):
        self.stdout = stdout
        self.stderr_deque = stderr_deque
        self.chunk_size = chunk_size
        self.returncode = None

    def send(self, _):
        chunk = self.stdout.read(self.chunk_size)
        if not chunk:
            raise StopIteration
        return chunk

    def throw(self, typ, value=None, traceback=None):
        return super().throw(typ, value, traceback)


@contextmanager
def iterable_subprocess(
    program,
    input_chunks,
    chunk_size=65536,
    cwd=None,
    bufsize=-1,
):
    # This context starts a thread that populates the subprocess's standard input. It
    # also starts a threads that reads the process's standard error. Otherwise we risk
    # a deadlock - there is no output because the process is waiting for more input.
    #
    # This itself introduces its own complications and risks, but hopefully mitigated
    # by having a well defined start and stop mechanism that also avoid sending data
    # to the process if it's not running
    #
    # To start, i.e. on entry to the context from client code
    # - The process is started
    # - The thread to read from standard error is started
    # - The thread to populate input is started
    #
    # When running:
    # - The standard input thread iterates over the input, passing chunks to the process
    # - While the standard error thread fetches the error output
    # - And while this thread iterates over the processe's output from client code
    #   in the context
    #
    # To stop, i.e. on exit of the context from client code
    # - This thread closes the process's standard output
    # - Wait for the standard input thread to exit
    # - Wait for the standard error thread to exit
    # - Wait for the process to exit
    #
    # By using context managers internally, this also gives quite strong guarantees that
    # the above order is enforced to make sure the thread doesn't send data to the process
    # whose standard input is closed and so we don't get BrokenPipe errors

    # Writing to the process can result in a BrokenPipeError. If this then results in
    # a non-zero code from the process, the process's standard error probably has useful
    # information on the cause of this. However, the non-zero error code happens after
    # BrokenPipeError, so propagating "what happens first" isn't helpful in this case.
    # So, we re-raise BrokenPipeError as _BrokenPipeError so we can catch it after the
    # process ends to then allow us to branch on its error code:
    # - if it's non-zero raise a CommandError containing its standard error
    # - if it's zero, re-raise the original BrokenPipeError
    class _BrokenPipeError(Exception):
        pass

    @contextmanager
    def thread(target, *args):
        exception = None

        def wrapper():
            nonlocal exception
            try:
                target(*args)
            except BaseException as e:
                exception = e

        t = Thread(target=wrapper)

        def start():
            t.start()

        def join():
            if t.ident:
                t.join()
            return exception

        yield start, join

    def input_to(stdin):
        try:
            for chunk in input_chunks:
                try:
                    stdin.write(chunk)
                except BrokenPipeError:
                    raise _BrokenPipeError()
                except OSError as e:
                    if e.errno != 22:
                        # Errno22 indicates an IO failure with a
                        # file descriptor (maybe process is dead already)
                        raise _BrokenPipeError()
                    else:
                        # no idea what this could be, let it bubble up
                        raise
        finally:
            try:
                stdin.close()
            except BrokenPipeError:
                raise _BrokenPipeError()
            except OSError as e:
                # silently ignore Errno22, which happens on
                # windows when trying to interacted with file descriptors
                # associated with a process that exited already
                if e.errno != 22:
                    raise

    def keep_only_most_recent(stderr, stderr_deque):
        total_length = 0
        while True:
            chunk = stderr.read(chunk_size)
            total_length += len(chunk)
            if not chunk:
                break
            stderr_deque.append(chunk)
            if total_length - len(stderr_deque[0]) >= chunk_size:
                total_length -= len(stderr_deque[0])
                stderr_deque.popleft()

    def raise_if_not_none(exception):
        if exception is not None:
            raise exception from None

    proc = None
    stderr_deque = deque()
    chunk_generator = None
    exception_stdin = None
    exception_stderr = None

    try:

        with \
                Popen(  # nosec - all arguments are controlled by the caller
                    program,
                    stdin=PIPE,
                    stdout=PIPE,
                    stderr=PIPE,
                    cwd=cwd,
                    bufsize=bufsize,
                ) as proc, \
                thread(
                    keep_only_most_recent,
                    proc.stderr,
                    stderr_deque,
                ) as (start_t_stderr, join_t_stderr), \
                thread(
                    input_to,
                    proc.stdin,
                ) as (start_t_stdin, join_t_stdin):

            try:
                start_t_stderr()
                start_t_stdin()
                chunk_generator = OutputFrom(
                    proc.stdout,
                    stderr_deque,
                    chunk_size
                )
                yield chunk_generator
            except BaseException:
                proc.terminate()
                raise
            finally:
                proc.stdout.close()
                exception_stdin = join_t_stdin()
                exception_stderr = join_t_stderr()

            raise_if_not_none(exception_stdin)
            raise_if_not_none(exception_stderr)

    except _BrokenPipeError as e:
        if chunk_generator:
            chunk_generator.returncode = proc.returncode
        if proc.returncode == 0:
            raise e.__context__ from None
    except BaseException:
        if chunk_generator:
            chunk_generator.returncode = proc.returncode
        raise

    chunk_generator.returncode = proc.returncode
    if proc.returncode:
        raise CommandError(
            cmd=program,
            code=proc.returncode,
            stderr=b''.join(stderr_deque)[-chunk_size:],
            cwd=cwd,
        )
