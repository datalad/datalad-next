DataLad NEXT extension
**********************

This `DataLad <http://datalad.org>`__ extension can be thought of as a
staging area for additional functionality, or for improved performance
and user experience. Unlike other topical or more experimental
extensions, the focus here is on functionality with broad
applicability. This extension is a suitable dependency for other
software packages that intend to build on this improved set of
functionality.

Installation and usage
======================

Install from PyPi or Github like any other Python package::

    # create and enter a new virtual environment (optional)
    $ virtualenv --python=python3 ~/env/dl-next
    $ . ~/env/dl-next/bin/activate
    # install from PyPi
    $ python -m pip install datalad-next

Once installed, additional commands provided by this extension are immediately
available. However, in order to fully benefit from all improvements, the
extension has to be enabled for auto-loading by executing::

    git config --global --add datalad.extensions.load next

Doing so will enable the extension to also alter the behavior the core DataLad
package and its commands.

API
===

High-level API commands
-----------------------

.. currentmodule:: datalad.api
.. autosummary::
   :toctree: generated

   create_sibling_webdav
   credentials
   download
   tree


Command line reference
----------------------

.. toctree::
   :maxdepth: 1

   generated/man/datalad-create-sibling-webdav
   generated/man/datalad-credentials
   generated/man/datalad-download
   generated/man/datalad-tree


Python utilities
----------------

.. currentmodule:: datalad_next
.. autosummary::
   :toctree: generated

   commands.ValidatedInterface
   constraints
   exceptions
   url_operations
   url_operations.any
   url_operations.file
   url_operations.http
   url_operations.ssh
   utils
   utils.credman
   utils.http_helpers
   utils.requests_auth


Git remote helpers
------------------

.. currentmodule:: datalad_next.gitremotes
.. autosummary::
   :toctree: generated

   datalad_annex


Git-annex backends
------------------

.. currentmodule:: datalad_next.annexbackends
.. autosummary::
   :toctree: generated

   base
   xdlra


Git-annex special remotes
-------------------------

.. currentmodule:: datalad_next.annexremotes
.. autosummary::
   :toctree: generated

   uncurl


DataLad patches
---------------

Patches that are automatically applied to DataLad when loading the
``datalad-next`` extension package.

.. currentmodule:: datalad_next.patches
.. autosummary::
   :toctree: generated

   annexrepo
   common_cfg
   configuration
   create_sibling_ghlike
   distribution_dataset
   interface_utils
   push_optimize
   push_to_export_remote
   siblings


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. |---| unicode:: U+02014 .. em dash
