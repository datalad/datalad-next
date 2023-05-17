"""Iterators for particular types of collections

Most importantly this includes different collections (or containers) for files,
such as a file system directory, or an archive (also see the
``ls_file_collection`` command). However, this module is not per-se limited
to file collections.

Most, if not all, implementation come in the form of a function that takes
a collection identifier or a collection location (e.g., a file system path),
and possibly some additional options. When called, an iterator is returned
that produces collection items in the form of data class instances of
a given type. The particular type can be different across different
collections.


.. currentmodule:: datalad_next.iter_collections
.. autosummary::
   :toctree: generated

   directory
   gitworktree
   tarfile
   zipfile
   utils
"""
