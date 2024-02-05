"""Exceptions to be used by all handlers"""

from __future__ import annotations

from typing import (
    Any,
)


class UrlOperationsRemoteError(Exception):
    def __init__(self, url, message=None, status_code: Any = None):
        # use base exception feature to store all arguments in a tuple
        # and have named properties to access them
        super().__init__(
            url,
            message,
            status_code,
        )

    def __str__(self):
        url, message, status_code = self.args
        if message:
            return message

        if status_code:
            return f"error {status_code} for {url!r}"

        return f"{self.__class__.__name__} for {url!r}"

    def __repr__(self) -> str:
        url, message, status_code = self.args
        return f"{self.__class__.__name__}(" \
               f"{url!r}, {message!r}, {status_code!r})"

    @property
    def url(self):
        return self.args[0]

    @property
    def message(self):
        return self.args[1]

    @property
    def status_code(self):
        return self.args[2]


class UrlOperationsResourceUnknown(UrlOperationsRemoteError):
    """A connection request succeeded in principle, but target was not found

    Equivalent of an HTTP404 response.
    """
    pass


class UrlOperationsInteractionError(UrlOperationsRemoteError):
    pass


class UrlOperationsAuthenticationError(UrlOperationsInteractionError):
    def __init__(self,
                 url: str,
                 credential: dict | None = None,
                 message: str | None = None,
                 status_code: Any = None):
        super().__init__(url, message=message, status_code=status_code)
        self.credential = credential


class UrlOperationsAuthorizationError(UrlOperationsRemoteError):
    def __init__(self,
                 url: str,
                 credential: dict | None = None,
                 message: str | None = None,
                 status_code: Any | None = None):
        super().__init__(url, message=message, status_code=status_code)
        self.credential = credential
