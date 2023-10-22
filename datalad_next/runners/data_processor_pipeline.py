"""
This module contains the implementation of a data processing pipeline driver.
The data processing pipeline takes chunks of bytes as input and feeds them
into a list of data processors, i.e. the data processing pipeline.

Data processing can be performed via calls to
:meth:`ProcessorPipeline.process` and :meth:`ProcessorPipeline.finalize`.
Alternatively, it can be performed over data chunks that are yielded by a
generator via the method :meth:`ProcessorPipeline.process_from`, which
creates a new generator that will yield the results of the data processing
pipeline.

Typical data processors would be:

- decode a stream of bytes
- split a stream of characters at line-ends
- convert a line of text into JSON

Data processors have a common interface and can be chained. For example,
one can pass data chunks, where each chunk is a byte-string, into a chain
of two data processors: a decode-processor that converts bytes into strings,
and a linesplit-processor that converts character-streams into lines. The result
of the chain would be lines of text.

Data processors are callables that have the following signature::

    def process(data: list[T], final: bool) -> tuple[list[N] | None, list[T]]:
        ...

where N is the type that is returned by processor. The return value is a
consisting of optional results, i.e. list[N] | None, and a number of input
elements that were not processed and should be presented again, when more
data arrives from the "preceding" element.

Data processors might need to buffer some data before yielding their result. The
"driver" of the data processing chains supports the buffering of input data for
individual processors. Therefore, data processors do not need to store
state themselves and can be quite simple.

The module currently supports the following data processors:

 - ``jsonline_processor``
 - ``decode_processor``
 - ``splitlines_processor``
 - ``pattern_processor`


"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Generator
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Union,
)


StrList = List[str]
BytesList = List[bytes]
StrOrBytes = Union[str, bytes]
StrOrBytesList = List[StrOrBytes]


class DataProcessorPipeline:
    """
    Hold a list of data processors and pushes data through them.

    Calls the processors in the specified order and feeds the output
    of a preceding processor into the following processor. If a processor
    has unprocessed data, either because it did not have enough data to
    successfully process it, or because not all data was processed, it returns
    the unprocessed data to the `process`-method and will receive it together
    with newly arriving data in the "next round".
    """
    def __init__(self,
                 processors: list[Callable]
                 ) -> None:
        self.processors = processors
        self.waiting_data: dict[Callable, list] = defaultdict(list)
        self.remaining = None
        self.finalized = False

    def process(self, data: bytes) -> list[Any]:
        output = [data]
        for processor in self.processors:
            if self.waiting_data[processor]:
                output = self.waiting_data[processor] + output
            output, self.waiting_data[processor] = processor(output)
            if not output:
                # If this processor does not output anything then the next
                # one has only the input that he already has buffered. We can
                # therefore end here.
                break
        return output

    def finalize(self) -> list[Any]:
        assert self.finalized is False, f'finalize() called repeatedly on {self}'
        self.finalized = True
        output = []
        for processor in self.processors:
            if self.waiting_data[processor]:
                output = self.waiting_data[processor] + output
            # We used to do the following
            if not output:
                continue
            # This cannot be done anymore because some processors store internal
            # data, e.g. SplitLinesProcessor. Those would not require an input
            # to generate an output on the final round.
            output, self.waiting_data[processor] = processor(output, True)
        return output

    def process_from(self, data_source: Iterable) -> Generator:
        """ pass output from a generator through this pipeline and yield output

        This method takes an existing byte-yielding generator, uses it as input
        and executes the specified processors over it. The result of the first
        processor is fed into the second processor and so on. The result of the
        last processor is yielded by the function.

        Parameters
        ----------
        data_source : Iterable
            An iterable object or generator that will deliver a byte stream in a
            number of chunks

        Yields
        -------
        Any
           Individual responses that were created by the last processor
        """
        for data_chunk in data_source:
            result = self.process(data_chunk)
            if result:
                yield from result
        result = self.finalize()
        if result:
            yield from result


def process_from(data_source: Iterable,
                 processors: list[Callable]
                 ) -> Generator:
    """ A convenience wrapper around the ProcessorPipeline.process_from-method

    Parameters
    ----------
    data_source : Iterable
        An iterable object or generator that will deliver a byte stream in a
        number of chunks

    processors : List[Callable]
        The list of processors that process the incoming data. The processors are
        receiving the data in the order `processors[0], processor[1], ...,
        processor[-1]`

    Yields
    -------
    Any
       Individual responses that were created by the last processor
    """
    processor_pipeline = DataProcessorPipeline(processors)
    yield from processor_pipeline.process_from(data_source=data_source)
