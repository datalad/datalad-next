import sys

from ..data_processors import (
    decode_processor,
    splitlines_processor,
)
from ..protocols import (
    StdOutCaptureProcessingGeneratorProtocol,
    StdOutErrCaptureProcessingGeneratorProtocol,
)
from ..run import run


def test_stdout_pipeline_protocols_simple():
    # verify that the pipeline is used and finalized
    processors = [splitlines_processor()]
    protocol = StdOutCaptureProcessingGeneratorProtocol(processors=processors)

    data = b'abc\ndef\nghi'
    protocol.pipe_data_received(1, data)
    protocol.pipe_connection_lost(1, None)

    assert tuple(protocol.result_queue) == (b'abc\n', b'def\n', b'ghi')


def test_stdout_pipeline_protocol():
    with run(
        [sys.executable, '-u', '-c', 'print("abc\\ndef\\nghi", end="")'],
        protocol_class=StdOutCaptureProcessingGeneratorProtocol,
        protocol_kwargs=dict(
            processors=[decode_processor(), splitlines_processor()]
        )
    ) as r:
        # There is no way to get un-decoded byte content with the non-generator
        # protocols.
        assert tuple(r) == ('abc\n', 'def\n', 'ghi')


def test_stdout_stderr_pipeline_protocol_simple():
    protocol = StdOutErrCaptureProcessingGeneratorProtocol(
        stdout_processors=[decode_processor(), splitlines_processor()],
        stderr_processors=[splitlines_processor()]
    )

    protocol.pipe_data_received(1, b'abc\ndef\nghi')
    assert tuple(protocol.result_queue) == ((1, 'abc\n'), (1, 'def\n'))
    protocol.result_queue.clear()

    # Check that the processing pipeline is finalized
    protocol.pipe_connection_lost(1, None)
    assert tuple(protocol.result_queue) == ((1, 'ghi'),)
    protocol.result_queue.clear()

    protocol.pipe_data_received(2, b'rst\nuvw\nxyz')
    assert tuple(protocol.result_queue) == ((2, b'rst\n'), (2, b'uvw\n'))
    protocol.result_queue.clear()

    # Check that the processing pipeline is finalized
    protocol.pipe_connection_lost(2, None)
    assert tuple(protocol.result_queue) == ((2, b'xyz'),)


def test_stdout_stderr_pipeline_protocol():
    with run(
        [
            sys.executable, '-u', '-c',
            'import sys\n'
            'print("abc\\ndef\\nghi", end="")\n'
            'print("rst\\nuvw\\nxyz", end="", file=sys.stderr)\n'
        ],
        protocol_class=StdOutErrCaptureProcessingGeneratorProtocol,
        protocol_kwargs=dict(
            stdout_processors=[decode_processor(), splitlines_processor()],
            stderr_processors=[splitlines_processor()]
        )
    ) as r:
        result = tuple(r)

    assert len(result) == 6
    assert ''.join(x[1] for x in result if x[0] == 1) == 'abc\ndef\nghi'
    assert b''.join(x[1] for x in result if x[0] == 2) == b'rst\nuvw\nxyz'
