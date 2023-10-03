"""Additional runner protocol implementations

.. currentmodule:: datalad_next.runners.protocols
"""
from . import (
    GeneratorMixIn,
    NoCapture,
    StdOutCapture,
)


#
# Below are generic generator protocols that should be provided
# upstream
#
class NoCaptureGeneratorProtocol(NoCapture, GeneratorMixIn):
    """Generator-variant of the :class:`~datalad_next.runners.NoCapture`"""
    def __init__(self, done_future=None, encoding=None):
        NoCapture.__init__(self, done_future, encoding)
        GeneratorMixIn.__init__(self)

    def timeout(self, fd):
        raise TimeoutError(f"Runner timeout: process has not terminated yet")


class StdOutCaptureGeneratorProtocol(StdOutCapture, GeneratorMixIn):
    """Generator-variant of the :class:`~datalad_next.runners.StdOutCapture`"""
    def __init__(self, done_future=None, encoding=None):
        StdOutCapture.__init__(self, done_future, encoding)
        GeneratorMixIn.__init__(self)

    def pipe_data_received(self, fd: int, data: bytes):
        assert fd == 1
        self.send_result(data)

    def timeout(self, fd):
        raise TimeoutError(f"Runner timeout {fd}")
