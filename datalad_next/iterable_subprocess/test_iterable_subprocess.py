import io
import sys
import threading
import time
import zipfile

import psutil
import pytest
from threading import Thread

from .iterable_subprocess import (
    iterable_subprocess,
    CommandError,
)


def test_cat_not_necessarily_streamed():
    def yield_small_input():
        yield b'first'
        yield b'second'
        yield b'third'

    with iterable_subprocess(['cat'], yield_small_input()) as output:
        assert b''.join(output) == b'firstsecondthird'


def test_cat_streamed():
    latest_input = None

    def yield_input():
        nonlocal latest_input

        for i in range(0, 10000000):
            yield b'*' * 10
            latest_input = i

    with iterable_subprocess(['cat'], yield_input()) as output:
        latest_input_during_output = [latest_input for _ in output]

        # Make sure the input is progressing during the output. In test, there
        # are about 915 steps, so checking that it's greater than 50 shouldm't
        # make this test too flakey
        num_steps = 0
        prev_i = 0
        for i in latest_input_during_output:
            if i != prev_i:
                num_steps += 1
            prev_i = i

        assert num_steps > 50


def test_process_closed_after():
    # in datalad-next we do not necessarily have no child-processes
    # so determine the number of test incrementally
    #assert len(psutil.Process().children(recursive=True)) == 0
    n_children = len(psutil.Process().children(recursive=True))
    with iterable_subprocess(['cat'], ()) as output:
        assert len(psutil.Process().children(recursive=True)) == (n_children + 1)
    assert len(psutil.Process().children(recursive=True)) == n_children


def test_exception_from_input_before_yield_propagated():
    def yield_input():
        raise Exception('Something went wrong')

    with pytest.raises(Exception, match='Something went wrong'):
        with iterable_subprocess(['cat'], yield_input()) as output:
            pass


def test_exception_from_input_after_yield_propagated():
    def yield_input():
        yield b'*'
        raise Exception('Something went wrong')

    with pytest.raises(Exception, match='Something went wrong'):
        with iterable_subprocess(['cat'], yield_input()) as output:
            pass


def test_exception_from_input_incorrect_type_propagated():
    def yield_input():
        yield 'this-should-be-bytes'


    with pytest.raises(TypeError):
        with iterable_subprocess(['cat'], yield_input()) as output:
            pass


@pytest.mark.parametrize("size", [
    1, 100, 10000, 1000000,
])
def test_exception_from_output_during_input_iterating_propagates_and_does_not_hang(size):
    event = threading.Event()

    def yield_input():
        while True:
            event.set()
            yield b'*' * size

    with pytest.raises(Exception, match='My error'):
        with iterable_subprocess(['cat'], yield_input()) as output:
            event.wait()
            raise Exception('My error')


@pytest.mark.parametrize("chunk_size", [
    1, 100, 10000, 1000000,
])
@pytest.mark.parametrize("at_iteration", [
    0, 1, 100,
])
def test_exception_from_output_iterating_propagates_and_does_not_hang(at_iteration, chunk_size):
    def yield_input():
        while True:
            yield b'*' * chunk_size

    with pytest.raises(Exception, match='My error'):
        with iterable_subprocess(['cat'], yield_input(), chunk_size=chunk_size) as output:
            for i, chunk in enumerate(output):
                if i == at_iteration:
                    raise Exception('My error')


def test_exception_from_not_found_process_propagated():
    with pytest.raises(FileNotFoundError):
        with iterable_subprocess(['does-not-exist'], ()) as output:
            b''.join(output)


def test_exception_from_return_code(monkeypatch):
    monkeypatch.setenv('LANG', 'C')
    with pytest.raises(CommandError, match='No such file or directory') as excinfo:
        with iterable_subprocess(['ls', 'does-not-exist'], ()) as output:
            a = b''.join(output)

    assert excinfo.value.returncode > 0
    assert b'No such file or directory' in excinfo.value.stderr


def test_exception_from_context_even_though_return_code_with_long_standard_error():
    with pytest.raises(Exception, match="Another exception"):
        with iterable_subprocess([sys.executable, '-c', 'import sys; print("Out"); print("Error message" * 100000, file=sys.stderr); sys.exit(1)'], ()) as output:
            for _ in output:
                pass
            raise Exception('Another exception')


def test_exception_from_return_code_with_long_standard_error():
    with pytest.raises(CommandError) as excinfo:
        with iterable_subprocess([sys.executable, '-c', 'import sys; print("Out"); print("Error message" * 100000, file=sys.stderr); sys.exit(2)'], ()) as output:
            for _ in output:
                pass

    assert excinfo.value.returncode == 2
    assert len(excinfo.value.stderr) == 65536


def test_if_process_exits_with_non_zero_error_code_and_inner_exception_it_propagates():
    def yield_input():
        while True:
            yield b'*' * 10

    with pytest.raises(Exception, match='Another exception'):
        with iterable_subprocess([
            sys.executable, '-c', 'import sys; print("The error", file=sys.stderr); print("After output"); sys.exit(1)',
        ], yield_input()) as output:
            all_output = b''.join(output)
            raise Exception('Another exception')

    # rstrip to account for different platform line endings here
    assert all_output.rstrip() == b'After output'



def test_if_process_closes_standard_input_but_exits_with_non_zero_error_code_then_broken_pipe_error():
    def yield_input():
        while True:
            yield b'*' * 10

    with pytest.raises(BrokenPipeError):
        with iterable_subprocess([
            sys.executable, '-c', 'import sys; sys.stdin.close(); print("The error", file=sys.stderr); print("After output"); sys.exit(0)',
        ], yield_input()) as output:
            all_output = b''.join(output)

    # rstrip to account for different platform line endings here
    assert all_output.rstrip() == b'After output'


def test_if_process_closes_standard_input_but_exits_with_non_zero_error_code_then_iterable_subprocess_error():
    def yield_input():
        while True:
            yield b'*' * 10

    with pytest.raises(CommandError) as excinfo:
        with iterable_subprocess([
            sys.executable, '-c', 'import sys; sys.stdin.close(); print("The error", file=sys.stderr); print("After output"); sys.exit(3)',
        ], yield_input()) as output:
            all_output = b''.join(output)

    # rstrip to account for different platform line endings here
    assert all_output.rstrip() == b'After output'
    assert excinfo.value.returncode == 3
    assert excinfo.value.stderr.rstrip()== b'The error'


def test_program_that_outputs_for_a_long_time_is_interrupted_on_context_exit():
    start = time.monotonic()

    with pytest.raises(CommandError) as excinfo:
        with iterable_subprocess([sys.executable, '-c', 'import time; start = time.monotonic()\nwhile (time.monotonic() - start) < 60:\n    print("Output" * 1000)'], ()) as output:
            pass

    end = time.monotonic()

    assert excinfo.value.returncode != 0
    # alternative condition reflects error communication on windows (errno22)
    assert b'BrokenPipeError' in excinfo.value.stderr or b'Errno 22' in excinfo.value.stderr
    assert end - start < 10


def test_program_that_sleeps_exits_quickly_if_exception():
    start = time.monotonic()

    with pytest.raises(Exception, match='From context'):
        with iterable_subprocess([sys.executable, '-c', 'import time; time.sleep(60)'], ()) as output:
            raise Exception('From context')

    end = time.monotonic()

    assert end - start < 10


def test_program_that_sleeps_exits_quickly_if_keyboard_interrupt():
    start = time.monotonic()

    with pytest.raises(KeyboardInterrupt, match='From context'):
        with iterable_subprocess([sys.executable, '-c', 'import time; time.sleep(60)'], ()) as output:
            raise KeyboardInterrupt('From context')

    end = time.monotonic()

    assert end - start < 10


def test_program_that_sleeps_exits_quickly_if_keyboard_interrupt_just_before_thread_starts(monkeypatch):
    start = time.monotonic()

    def start_that_raises_keyboard_interrupt(self):
        raise KeyboardInterrupt('Just before starting thread')
    monkeypatch.setattr(Thread, 'start', start_that_raises_keyboard_interrupt)

    with pytest.raises(KeyboardInterrupt, match='Just before starting thread'):
        iterable_subprocess([sys.executable, '-c', 'import time; time.sleep(60)'], ()).__enter__()

    end = time.monotonic()

    assert end - start < 10


def test_program_that_sleeps_exits_quickly_if_keyboard_interrupt_just_after_thread_starts(monkeypatch):
    start = time.monotonic()

    original_start = Thread.start
    def start_that_raises_keyboard_interrupt(self):
        original_start(self)
        raise KeyboardInterrupt('Just after starting thread')
    monkeypatch.setattr(Thread, 'start', start_that_raises_keyboard_interrupt)

    with pytest.raises(KeyboardInterrupt, match='Just after starting thread'):
        iterable_subprocess([sys.executable, '-c', 'import time; time.sleep(60)'], ()).__enter__()

    end = time.monotonic()

    assert end - start < 10


def test_program_that_sleeps_not_quickly_if_no_exception():
    start = time.monotonic()

    with iterable_subprocess([sys.executable, '-c', 'import time; time.sleep(2)'], ()) as output:
        pass

    end = time.monotonic()

    assert end - start > 2


def test_funzip_no_compression():
    contents = b'*' * 100000

    def yield_input():
        file = io.BytesIO()
        with zipfile.ZipFile(file, 'w', zipfile.ZIP_STORED) as zf:
            zf.writestr('any.txt', contents)

        yield file.getvalue()

    with iterable_subprocess(['funzip'], yield_input()) as output:
        assert b''.join(output) == contents


def test_funzip_deflate():
    contents = b'*' * 100000

    def yield_input():
        file = io.BytesIO()
        with zipfile.ZipFile(file, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr('any.txt', contents)

        yield file.getvalue()

    with iterable_subprocess(['funzip'], yield_input()) as output:
        assert b''.join(output) == contents


def test_error_returncode_available_from_generator():
    with pytest.raises(CommandError):
        with iterable_subprocess(['ls', 'does-not-exist'], ()) as ls:
            tuple(ls)
    assert ls.returncode != 0


def test_error_returncode_available_from_generator_with_exception():
    with pytest.raises(StopIteration):
        with iterable_subprocess(['ls', 'does-not-exist'], ()) as ls:
            while True:
                next(ls)
    assert ls.returncode != 0


def test_returncode_available_from_generator_with_exception():
    with pytest.raises(StopIteration):
        with iterable_subprocess(['echo', 'a'], ()) as echo:
            while True:
                next(echo)
    # On a Linux system, all exceptions that are raised before the subprocess
    # exited will lead to a -15 return code. If StopIteration is raised, the
    # subprocess will either have terminated which results in a 0-return code,
    # or the subprocess is still running and will therefore be terminated which
    # results in a -15 return code. Any other exception than StopIteration,
    # e.g. a CommandError because echo could not be found, would lead to an
    # early test-exit and not proceed to the assign-statement.
    assert echo.returncode in (0, -15)
