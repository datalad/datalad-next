"""Improve ``CommandError`` rendering and add ``returncode`` alias for ``code``


This patch does two things:

It overwrites ``__repr__``, otherwise ``CommandError` would use
``RuntimeError``'s variant and ignore all additional structured information
except for ``.msg`` -- which is frequently empty and confuses with a
`CommandError('')` display.

It adds a ``returncode`` alias for ``code``. This unifies return code access
between ``CommandError`` and `Popen``-like objects, which usually have a
``returncode`` attribute.
"""

from datalad.runner.exception import CommandError


def commanderror_repr(self) -> str:
    return self.to_str()


CommandError.__repr__ = commanderror_repr


# Basic alias idea taken from here:
# <https://stackoverflow.com/questions/4017572/how-can-i-make-an-alias-to-a-non-function-member-attribute-in-a-python-class>
_commanderror_aliases = {
    'returncode': 'code',
}

def commanderror_getattr(self, item):
    return object.__getattribute__(self, _commanderror_aliases.get(item, item))

def commanderror_setattr(self, key, value):
    if key == '_aliases':
        raise AttributeError('Cannot set `_aliases`')
    return object.__setattr__(self, _commanderror_aliases.get(key, key), value)


CommandError.__getattr__ = commanderror_getattr
CommandError.__setattr__ = commanderror_setattr
