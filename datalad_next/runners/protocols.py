from __future__ import annotations

from typing import Optional

from . import (
    GeneratorMixIn,
    NoCapture,
    StdOutCapture,
    StdOutErrCapture,
)
from .data_processor_pipeline import DataProcessorPipeline


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


class StdOutCaptureProcessingGeneratorProtocol(StdOutCaptureGeneratorProtocol):
    """ A generator protocol that applies a processor pipeline to stdout data

    This protocol can be initialized with a list of processors. Data that is
    read from stdout will be processed by the processors and the result of the
    last processor will be sent to the result generator, which will then
    yield it.
    """
    def __init__(self,
                 done_future=None,
                 processors: list | None = None
                 ) -> None:
        StdOutCaptureGeneratorProtocol.__init__(self, done_future, None)
        self.processor_pipeline = (
            DataProcessorPipeline(processors)
            if processors
            else None
        )

    def pipe_data_received(self, fd: int, data: bytes):
        assert fd == 1
        if self.processor_pipeline:
            for processed_data in self.processor_pipeline.process(data):
                self.send_result(processed_data)
            return
        self.send_result(data)

    def pipe_connection_lost(self, fd: int, exc: Optional[BaseException]) -> None:
        assert fd == 1
        if self.processor_pipeline:
            for processed_data in self.processor_pipeline.finalize():
                self.send_result(processed_data)


class StdOutErrCaptureProcessingGeneratorProtocol(StdOutErrCapture, GeneratorMixIn):
    """ A generator protocol that applies processor-pipeline to stdout- and stderr-data

    This protocol can be initialized with a list of processors for stdout-data,
    and with a list of processors for stderr-data. Data that is read from stdout
    or stderr will be processed by the respective processors. The protocol will
    send 2-tuples to the result generator. Each tuple consists of the file
    descriptor on which data arrived and the output of the last processor of the
    respective pipeline. The result generator. which will then yield the
    results.
    """
    def __init__(self,
                 done_future=None,
                 stdout_processors: list | None = None,
                 stderr_processors: list | None = None,
                 ) -> None:
        StdOutErrCapture.__init__(self, done_future, None)
        GeneratorMixIn.__init__(self)
        self.processor_pipelines = {
            fd: DataProcessorPipeline(processors)
            for fd, processors in ((1, stdout_processors), (2, stderr_processors))
            if processors is not None
        }

    def pipe_data_received(self, fd: int, data: bytes):
        assert fd in (1, 2)
        if fd in self.processor_pipelines:
            for processed_data in self.processor_pipelines[fd].process(data):
                self.send_result((fd, processed_data))
            return
        self.send_result((fd, data))

    def pipe_connection_lost(self, fd: int, exc: Optional[BaseException]) -> None:
        assert fd in (1, 2)
        if fd in self.processor_pipelines:
            for processed_data in self.processor_pipelines[fd].finalize():
                self.send_result((fd, processed_data))

    def timeout(self, fd):
        raise TimeoutError(f"Runner timeout {fd}")
