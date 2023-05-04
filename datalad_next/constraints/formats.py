"""Constraints for particular formats or protocols"""

# allow for |-type UnionType declarations
from __future__ import annotations

from json import loads
import re
from urllib.parse import (
    urlparse,
    ParseResult,
)

from .base import Constraint


class EnsureJSON(Constraint):
    """Ensures that string is JSON formatted and can be deserialized.
    """
    def __init__(self):
        super().__init__()

    def __call__(self, value: str):
        try:
            return loads(value)
        except Exception as e:
            self.raise_for(
                value,
                str(e),
            )

    def short_description(self):
        return 'JSON'


class EnsureURL(Constraint):
    """Ensures that a string is a valid URL with a select set of components

    and/or:

    - does not contain certain components
    - matches a particular regular expression

    Given that a large variety of strings are also a valid URL, a typical use
    of this constraint would involve using a `required=['scheme']` setting.

    All URL attribute names supported by `urllib.parse.urlparse()` are also
    supported here: scheme, netloc, path, params, query, fragment, username,
    password, hostname, port.

    .. seealso::
      https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urlparse
    """
    def __init__(
        self,
        required: list | None = None,
        forbidden: list | None = None,
        match: str | None = None,
    ):
        """
        Parameters
        ----------
        required: list, optional
          List of any URL component names as recognized by ``urlparse()``,
          such as ``scheme``, ``netloc``, ``path``, ``params``, ``query``,
          ``fragment``, ``username``, ``password``, ``hostname``, ``port``
        forbidden: list, optional
          Like ``required`` but names URL components that must not be present
        match: str, optional
          Regular expression that the URL must match
        """
        self._required = required
        self._forbidden = forbidden
        self._match_exp = re.compile(match) if match else None
        super().__init__()

    def __call__(self, value: str) -> str:
        self._validate_parsed(value)
        # return the str here, see EnsureParsedURL for an alternative
        return value

    def _validate_parsed(self, value: str) -> ParseResult:
        if not isinstance(value, str):
            self.raise_for(value, 'not a string')
        if self._match_exp and not self._match_exp.match(value):
            self.raise_for(
                value,
                'does not match expression {match_expression!r}',
                match_expression=self._match_exp.pattern,
            )
        parsed = urlparse(value, scheme='', allow_fragments=True)
        for r in (self._required or []):
            if not getattr(parsed, r, None):
                self.raise_for(
                    value,
                    'URL is missing {component!r} component',
                    component=r,
                )
        for f in (self._forbidden or []):
            if getattr(parsed, f, None):
                self.raise_for(
                    value,
                    'URL has forbidden {component!r} component',
                    component=f,
                )
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
