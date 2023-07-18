"""Type ENUMs"""

from enum import Enum


class ArchiveType(Enum):
    """Enumeration of archive types

    Each one should have an associated ArchiveOperations handler.
    """
    # TODO the values could also be handler classes ...
    tar = 'tar'
    zip = 'zip'
