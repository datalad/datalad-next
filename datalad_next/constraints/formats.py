from json import loads
from urllib.parse import (
    urlparse,
    ParseResult,
)

from .base import Constraint


class EnsureJSON(Constraint):
    """Ensures that string is JSON formated and can be deserialized.
    """
    def __init__(self):
        super().__init__()

    def __call__(self, value: str):
        return loads(value)

    def short_description(self):
        return 'JSON'


class EnsureURL(Constraint):
    """Ensures that a string is a valid URL with a select set of components

    and/or does not contain certain components.

    Given that a large variety of strings are also a valid URL, a typical use
    of this contraint would involve using a `required=['scheme']` setting.

    All URL attribute names supported by `urllib.parse.urlparse()` are also
    supported here: scheme, netloc, path, params, query, fragment, username,
    password, hostname, port.

    .. seealso::
      https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urlparse
    """
    def __init__(self, required: list = None, forbidden: list = None):
        self._required = required
        self._forbidden = forbidden
        super().__init__()

    def __call__(self, value: str) -> str:
        self._validate_parsed(value)
        return value

    def _validate_parsed(self, value: str) -> ParseResult:
        parsed = urlparse(value, scheme='', allow_fragments=True)
        for r in (self._required or []):
            if not getattr(parsed, r, None):
                raise ValueError(f'URL is missing {r!r} component')
        for f in (self._forbidden or []):
            if getattr(parsed, f, None):
                raise ValueError(f'URL has forbidden {f!r} component')
        return parsed

    def short_description(self):
        return 'URL{}{}{}{}'.format(
            f' with required {self._required}' if self._required else '',
            ' and' if self._required and self._forbidden else '',
            f' with no {self._forbidden}' if self._forbidden else '',
            ' component(s)' if self._required or self._forbidden else '',
        )


class EnsureParsedURL(EnsureURL):
    """Like `EnsureURL`, but returns a parsed URL"""
    def __call__(self, value: str) -> ParseResult:
        return self._validate_parsed(value)
