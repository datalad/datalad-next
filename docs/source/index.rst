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


Provided functionality
======================

.. toctree::
   :maxdepth: 1

   api.rst
   cmd.rst
   Infrastructure classes and utilities <pyutils.rst>
   git-remote-helpers.rst
   annex-backends.rst
   annex-specialremotes.rst
   patches.rst


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
