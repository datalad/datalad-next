"""UI abstractions for user communication

.. currentmodule:: datalad_next.uis
.. autosummary::
   :toctree: generated

   ansi_colors
   ui_switcher
"""

# make more obvious that this is a frontend that behaves
# differently depending on many conditions
from datalad.ui import ui as ui_switcher

from datalad.support import ansi_colors
