# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 et:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Helper for parameter validation, documentation and conversion"""

__docformat__ = 'restructuredtext'

import re


def _strip_typerepr(s):
    """Strip away <class '...'> and <type '...'> decorations for docstrings
    """
    return re.sub(r"<(class|type) '(\S+)'>", r'\2', s)


def _type_str(t):
    """Get string human-readable representation of a data type

    If type (t) is given as a tuple, assume ability to choose any of the
    listed types, so those types listing get joined with |
    """
    if isinstance(t, tuple):
        s = ' or '.join(map(_type_str, t))
        return ("(%s)" % s) if len(t) > 1 else s
    return _strip_typerepr(str(t))
