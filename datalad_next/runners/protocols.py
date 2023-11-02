from typing import Optional

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
    def __init__(self, done_future=None, encoding=None):
        NoCapture.__init__(self, done_future, encoding)
        GeneratorMixIn.__init__(self)

    def timeout(self, fd):
        raise TimeoutError(f"Runner timeout: process has not terminated yet")


class StdOutCaptureGeneratorProtocol(StdOutCapture, GeneratorMixIn):
    def __init__(self, done_future=None, encoding=None):
        StdOutCapture.__init__(self, done_future, encoding)
        GeneratorMixIn.__init__(self)

    def pipe_data_received(self, fd: int, data: bytes):
        assert fd == 1
        self.send_result(data)

    def timeout(self, fd):
        raise TimeoutError(f"Runner timeout {fd}")


class StdOutLineCaptureGeneratorProtocol(StdOutCapture, GeneratorMixIn):
    def __init__(self,
                 done_future=None,
                 encoding=None,
                 separator: Optional[str] = None,
                 keep_ends: bool = False
                 ):
        from . import LineSplitter

        StdOutCapture.__init__(self, done_future, encoding)
        GeneratorMixIn.__init__(self)
        self.encoding = encoding or 'utf-8'
        self.line_splitter = LineSplitter(separator, keep_ends)

    def pipe_data_received(self, fd: int, data: bytes):
        assert fd == 1
        # This naive decode call might fail, if encodings are crossing data
        # chunk borders. Because this protocol is part of a POC of the
        # run-context-manager and the batch-context-manager, we leave it for
        # now. The proper way to do that is IMHO to use the
        # `StdOutCaptureProcessingGeneratorProtocol` from PR
        # https://github.com/datalad/datalad-next/pull/484
        # and the processing pipeline:
        # `[decode_processor('utf-8'), splitlines_processor()]`
        #
        for line in self.line_splitter.process(data.decode()):
            self.send_result(line)

    def pipe_connection_lost(self, fd: int, exc: Optional[BaseException]) -> None:
        if fd == 1:
            remaining_string = self.line_splitter.finish_processing()
            if remaining_string:
                self.send_result(remaining_string)

    def timeout(self, fd):
        raise TimeoutError(f"Runner timeout {fd}")
