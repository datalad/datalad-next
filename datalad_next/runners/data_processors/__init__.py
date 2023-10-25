""" This module contains data processors for the data pipeline processor

Available data processors:

.. currentmodule:: datalad_next.runners.data_processors
.. autosummary::
   :toctree: generated

   decode
   jsonline
   pattern
   splitlines

"""

from .decode import decode_processor
from .jsonline import jsonline_processor
from .pattern import pattern_processor
from .splitlines import splitlines_processor
