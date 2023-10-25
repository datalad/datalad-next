""" Data processor that generates JSON objects from lines of bytes or strings """

from __future__ import annotations

import json
from typing import Any

from ..data_processor_pipeline import StrOrBytesList


def jsonline_processor(lines: StrOrBytesList,
                       _: bool = False
                       ) -> tuple[list[tuple[bool, Any]], StrOrBytesList]:
    """ A data processor that converts lines into JSON objects, if possible.

    Parameters
    ----------
    lines: StrOrBytesList
        A list containing strings or byte-strings that that hold JSON-serialized
        data.

    _: bool
        The ``final`` parameter is ignored because lines are assumed to be
        complete and the conversion takes place for every line. Consequently,
        no remaining input data exists, and there is no need for "flushing" in
        a final round.

    Returns
    -------
    tuple[list[Tuple[bool, StrOrBytes]], StrOrByteList]
        The result, i.e. the first element of the result tuple, is a list that
        contains one tuple for each element of ``lines``. The first element of the
        tuple is a bool that indicates whether the line could be converted. If it
        was successfully converted the value is ``True``. The second element is the
        Python structure that resulted from the conversion if the first element
        was ``True``. If the first element is ``False``, the second element contains
        the input that could not be converted.
    """
    result = []
    for line in lines:
        assert len(line.splitlines()) == 1
        try:
            result.append((True, json.loads(line)))
        except json.decoder.JSONDecodeError:
            result.append((False, lines))
    return result, []
