"""git-annex key representation"""

from __future__ import annotations

from dataclasses import dataclass
import re


# BACKEND[-sNNNN][-mNNNN][-SNNNN-CNNNN]--NAME
_annexkey_regex = re.compile(
    '(?P<backend>[A-Z0-9]+)'
    '(|-s(?P<size>[0-9]+))'
    '(|-m(?P<mtime>[0-9]+))'
    '(|-S(?P<chunksize>[0-9]+)-C(?P<chunknumber>[0-9]+))'
    '--(?P<name>.*)$'
)


@dataclass(frozen=True)
class AnnexKey:
    """Representation of a git-annex key

    https://git-annex.branchable.com/internals/key_format/
    """
    name: str
    backend: str
    size: int | None = None
    mtime: int | None = None
    chunksize: int | None = None
    chunknumber: int | None = None

    @classmethod
    def from_str(cls, key: str):
        """Return an ``AnnexKey`` instance from a key string"""
        key_matched = _annexkey_regex.match(key)
        if not key_matched:
            # without a sensible key there is no hope
            raise ValueError(f'{key!r} is not a valid git-annex key')
        return cls(**key_matched.groupdict())

    def __str__(self) -> str:
        return '{backend}{size}{mtime}{chunk}--{name}'.format(
            name=self.name,
            backend=self.backend,
            size=f'-s{self.size}' if self.size else '',
            mtime=f'-m{self.mtime}' if self.mtime else '',
            # if me reading of the spec is correct, the two chunk props
            # can only occur together
            chunk=f'-S{self.chunksize}-C{self.chunknumber}'
            if self.chunknumber else '',
        )
