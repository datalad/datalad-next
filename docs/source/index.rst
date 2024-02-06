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


Functionality provided by DataLad NEXT
======================================

The following table of contents offers entry points to the main components
provided by this extension. The `project README
<https://github.com/datalad/datalad-next/blob/main/README.md#summary-of-functionality-provided-by-this-extension>`__
offers a more detailed summary in a different format.

.. toctree::
   :maxdepth: 1

   api.rst
   cmd.rst
   Infrastructure classes and utilities <pyutils.rst>
   git-remote-helpers.rst
   annex-backends.rst
   annex-specialremotes.rst
   patches.rst


Developing with DataLad NEXT
============================

This extension package moves fast in comparison to the DataLad core package.
Nevertheless, attention is paid to API stability, adequate semantic versioning,
and informative changelogs.

Besides the DataLad commands shipped with this extension package, a number of
Python utilities are provided that facilitate the implementation of workflows
and additional functionality. An overview is available in the
:ref:`reference manual <pyutils>`.

Public vs internal Python API
-----------------------------

Anything that can be imported directly from any of the top-level sub-packages in
`datalad_next` is considered to be part of the public API. Changes to this API
determine the versioning, and development is done with the aim to keep this API
as stable as possible. This includes signatures and return value behavior.

As an example::

    from datalad_next.runners import iter_git_subproc

imports a part of the public API, but::

    from datalad_next.runners.git import iter_git_subproc

does not.

Use of the internal API
-----------------------

Developers can obviously use parts of the non-public API. However, this should
only be done with the understanding that these components may change from one
release to another, with no guarantee of transition periods, deprecation
warnings, etc.

Developers are advised to never reuse any components with names starting with
`_` (underscore). Their use should be limited to their individual sub-package.


Contributor information
=======================

.. toctree::
   :maxdepth: 2

   developer_guide/index.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. |---| unicode:: U+02014 .. em dash
