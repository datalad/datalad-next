"""Handling of Git's pathspecs with subdirectory mangling support

This functionality can be used to add support for pathspecs to implementations
that rely on Git commands that do not support submodule recursion directly.

"""

__all__ = ['GitPathSpec', 'GitPathSpecs']

from .pathspec import GitPathSpec
