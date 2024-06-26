.. _pyutils:

Python tooling
**************

``datalad-next`` comprises a number of more-or-less self-contained
mini-packages providing particular functionality. These implementations
are candidates for a migration into the DataLad core package, and are
provided here for immediate use. If and when components are migrated,
transition modules will be kept to prevent API breakage in dependent
packages.


.. currentmodule:: datalad_next
.. autosummary::
   :toctree: generated

   archive_operations
   commands
   config
   constraints
   consts
   credman
   datasets
   exceptions
   gitpathspec
   iterable_subprocess
   itertools
   iter_collections
   repo_utils
   runners
   shell
   tests
   tests.fixtures
   types
   uis
   url_operations
   utils
