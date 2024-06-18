"""Context manager to communicate with a subprocess using iterables

.. deprecated:: 1.6

   This code has been moved to the datasalad library.
   Use it from ``datasalad.iterable_subprocess`` instead.

This offers a higher level interface to subprocesses than Python's built-in
subprocess module, and is particularly helpful when data won't fit in memory
and has to be streamed.

This also allows an external subprocess to be naturally placed in a chain of
iterables as part of a data processing pipeline.

This code has been taken from https://pypi.org/project/iterable-subprocess/
and was subsequently adjusted for cross-platform compatibility and performance,
as well as tighter integration with DataLad.

The original code was made available under the terms of the MIT License,
and was written by Michal Charemza.
"""

__all__ = ['iterable_subprocess']

from datasalad.iterable_subprocess import iterable_subprocess
