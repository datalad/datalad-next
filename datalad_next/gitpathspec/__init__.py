"""Handling of Git's pathspecs with subdirectory mangling support

.. deprecated:: 1.6
   This module is deprecated. It has been migrated to the `datasalad library
   <https://pypi.org/project/datasalad>`__. Imports should be adjusted to
   ``datasalad.gitpathspec``.
"""

__all__ = ['GitPathSpec', 'GitPathSpecs']

import warnings

from datasalad.gitpathspec import (
    GitPathSpec,
    GitPathSpecs,
)

warnings.warn(
    '`datalad_next.gitpathspec` has been migrated to the datasalad library, '
    'adjust imports to `datasalad.gitpathspec`',
    DeprecationWarning,
    stacklevel=1,
)
