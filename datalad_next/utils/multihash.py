"""Compute more than one hash for the same data in one go"""

from __future__ import annotations

import hashlib
from typing import (
    ByteString,
    Dict,
)


class NoOpHash:
    """Companion of :class:`MultiHash` that computes no hash at all

    This can be used wherever ``MultiHash`` would be used, because
    it implements its API. However, no hash is computed and no
    hexdigest is reported.
    """
    def __init__(self, algorithms: None = None):
        pass

    def update(self, data):
        pass

    def get_hexdigest(self):
        return {}


class MultiHash:
    """Compute any number of hashes as if computing just one

    Supports any hash algorithm supported by the ``hashlib`` module
    of the standard library.
    """
    def __init__(self, algorithms: list[str]):
        """
        Parameters
        ----------
        algorithms: list
          Hash names, must match the name of the algorithms in the
          ``hashlib`` module (case insensitive).
        """
        # yes, this will crash, if an invalid hash algorithm name
        # is given
        _hasher = []
        for h in algorithms:
            hr = getattr(hashlib, h.lower(), None)
            if hr is None:
                raise ValueError(f'unsupported hash algorithm {h}')
            _hasher.append(hr())
        self._hasher = dict(zip(algorithms, _hasher))

    def update(self, data: ByteString) -> None:
        """Updates all configured digests"""
        for h in self._hasher.values():
            h.update(data)

    def get_hexdigest(self) -> Dict[str, str]:
        """Returns a mapping of algorithm name to hexdigest for all algorithms
        """
        return {a: h.hexdigest() for a, h in self._hasher.items()}
