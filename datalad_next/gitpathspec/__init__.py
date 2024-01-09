"""Data class for Git's pathspecs with subdirectory mangling support

The main purpose of this functionality is to be able to take a pathspecs that
is valid in the context of a top-level repository, and translate it such that
the set of pathspecs given to the same command running on/in a
submodule/subdirectory gives the same results, as if the initial top-level
invocation reported them (if it even could).

This functionality can be used to add support for pathspecs to implementation
that rely on Git commands that do not support submodule recursion directly.
"""

from .pathspec import GitPathSpec
