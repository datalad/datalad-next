"""Exceptions for credential management and manipulation workflows"""

from __future__ import annotations

from typing import (
    Dict,
    List,
)


class InvalidCredential(ValueError):
    """Raised when a provisioned credential is not fit for a target operation

    Use cases:
    - unknown user
    - expired token
    - known user does not have required permissions for a particular operation

    The exception is **not** to be raised for operational failures unrelated
    to the provisioned credential. Examples for when this exception is not to
    be raised:

    - service unreachable
    - operation fails due to technical issues after successful authentication,
      such as a not-found-error when accessing resources
    """
    def __init__(
        self,
        reason: str,
        target: str | None = None,
        cred: Dict | str | None = None,
    ):
        """
        Parameters
        ----------
        reason: str
          Reason or explanation why a particular credential is invalid in or
          for a certain context. This should be a brief statement that helps
          a user provide or select an adequate credential.
        target: str, optional
          Some identifier or description of the entity the credential was tried
          on/for, for example a URL.
        cred: dict or str, optional
          The credential that was tried, either a dict of credential properties,
          or the name of a credential.
        """
        super().__init__(reason, target, cred)

    @property
    def reason(self):
        return self.args[0]

    @property
    def target(self):
        return self.args[1]

    @property
    def credential(self):
        return self.args[2]

    def __str__(self):
        if not self.credential:
            cred_str = ''
        elif isinstance(self.credential, dict):
            # we do not want to spill the secret into logs and backtraces
            cred_str = {
                k: v for k, v in self.args[2].items()
                if k != 'secret' and not k.startswith('_')
            }
        elif isinstance(self.credential, str):
            cred_str = f'credential {self.credential!r}'

        return '{reason}{target_spacer}{target}{cred_spacer}{cred}'.format(
            # make sure we have at least a generic "reason" if there happens
            # to be no real one
            reason=self.reason if self.reason else 'failed',
            target_spacer=' for ' if self.target else '',
            target=self.target or '',
            cred_spacer=' with ' if cred_str else '',
            cred=cred_str
        )


class StopCredentialEntry(StopIteration):
    """Exception raised when a credential entry (loop) is to be discontinued

    Use cases:

    - user indicates no further credential entry is desired
    - configuration dictates that some threshold for credential retry has been
      reached
    """
    pass


class NoSuitableCredentialAvailable(ValueError):
    """Exception raised when no matching credential could be obtained

    Use cases:

    - no matching credential is on record, and no manual entry of a new
      credential is possible or enabled
    - initially matching credential(s) was rejected and no further attempt
      is possible or desired
    """
    def __init__(self, purpose: str | None = None, failures: List[InvalidCredential] | None = None):
        """
        Parameters
        ----------
        failures: list or None
          If any credentials were tried, but failed, the associated
          ``InvalidCredential`` exceptions can be passed here as a list,
          to enable standardized reporting.
        """
        super().__init__(purpose, failures)

    @property
    def purpose(self):
        return self.args[0]

    @property
    def failures(self):
        return self.args[1]

    def __str__(self):
        return (
            "No suitable credential available{purpose_spacer}"
            "{purpose}{fail_spacer}{fails}").format(
                purpose_spacer=' for ' if self.purpose else '',
                purpose=self.purpose or '',
                fail_spacer='; failed with: ' if self.failures else '',
                fails=', '.join(str(f) for f in self.failures)
                if self.failures else '',
        )
