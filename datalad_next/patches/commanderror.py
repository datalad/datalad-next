"""Improve ``CommandError`` rendering

Without this patch that overwrites ``__repr__``, it would use
``RuntimeError``'s variant and ignore all additional structured information
except for ``.msg`` -- which is frequently empty and confuses with a
`CommandError('')` display.
"""

from datalad.runner.exception import CommandError


def commanderror_repr(self) -> str:
    return self.to_str()


CommandError.__repr__ = commanderror_repr
