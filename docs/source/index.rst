DataLad NEXT extension
**********************

This `DataLad <http://datalad.org>`__ extension can be thought of as a
staging area for additional functionality, or for improved performance
and user experience. Unlike other topical or more experimental
extensions, the focus here is on functionality with broad
applicability. This extension is a suitable dependency for other
software packages that intend to build on this improved set of
functionality.


API
===

High-level API commands
-----------------------

.. currentmodule:: datalad.api
.. autosummary::
   :toctree: generated

   create_sibling_webdav
   credentials
   download_file
   tree


Command line reference
----------------------

.. toctree::
   :maxdepth: 1

   generated/man/datalad-create-sibling-webdav
   generated/man/datalad-credentials
   generated/man/datalad-download-file
   generated/man/datalad-tree


Python utilities
----------------

.. currentmodule:: datalad_next
.. autosummary::
   :toctree: generated

   credman
   constraints
   requests_auth
   url_operations
   http_url_operations
   file_url_operations
   ssh_url_operations
   http_helpers
   utils


Git remote helpers
------------------

.. currentmodule:: datalad_next.gitremote
.. autosummary::
   :toctree: generated

   datalad_annex


Git-annex backends
------------------

.. currentmodule:: datalad_next.backend
.. autosummary::
   :toctree: generated

   base
   xdlra


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. |---| unicode:: U+02014 .. em dash
