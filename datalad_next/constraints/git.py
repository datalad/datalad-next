"""Constraints for Git-related concepts and parameters"""
from __future__ import annotations

import subprocess

from .base import (
    Constraint,
    DatasetParameter,
)


class EnsureGitRefName(Constraint):
    """Ensures that a reference name is well formed

    Validation is performed by calling `git check-ref-format`.
    """
    def __init__(self,
                 allow_onelevel: bool = True,
                 normalize: bool = True,
                 refspec_pattern: bool = False):
        """
        Parameters
        ----------
        allow_onelevel:
          Flag whether one-level refnames are accepted, e.g. just 'main'
          instead of 'refs/heads/main'.
        normalize:
          Flag whether a normalized refname is validated and return.
          This includes removing any leading slash (/) characters and
          collapsing runs of adjacent slashes between name components
          into a single slash.
        refspec_pattern:
          Flag whether to interpret a value as a reference name pattern
          for a refspec (allowed to contain a single '*').
        """
        super().__init__()
        self._allow_onelevel = allow_onelevel
        self._normalize = normalize
        self._refspec_pattern = refspec_pattern

    def __call__(self, value: str) -> str:
        if not value:
            # simple, do here
            self.raise_for(value, 'refname must not be empty')

        cmd = ['git', 'check-ref-format']
        cmd.append('--allow-onelevel'
                   if self._allow_onelevel
                   else '--no-allow-onelevel')
        if self._refspec_pattern:
            cmd.append('--refspec-pattern')
        if self._normalize:
            cmd.append('--normalize')

        cmd.append(value)

        try:
            out = subprocess.run(
                cmd,
                text=True,
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            self.raise_for(
                value,
                'is not a valid refname',
                __caused_by__=e,
            )

        if self._normalize:
            return out.stdout.strip()
        else:
            return value

    def short_description(self):
        return '{}Git refname{}'.format(
            '(single-level) ' if self._allow_onelevel else '',
            ' or refspec pattern' if self._refspec_pattern else '',
        )


class EnsureRemoteName(EnsureGitRefName):
    """Ensures a remote name is given, and optionally if such a remote is known
    """

    def __init__(self,
                 known: bool | None = None,
                 dsarg: DatasetParameter | None = None):
        """
        Parameters
        ----------
        known: bool, optional
           By default, a given value is only checked if it is a syntactically
           correct remote name.
           If ``True``, also checks that the given name corresponds to a
           known remote in the dataset given by ``dsarg``. If ``False``,
           checks that the given remote does not match any known remote
           in that dataset.
        dsarg: DatasetParameter, optional
           Identifies a dataset for testing remote existence, if requested.
        """
        self._known = known
        self._dsarg = dsarg
        super().__init__(allow_onelevel=True,
                         refspec_pattern=False)

    def __call__(self, value: str) -> str:
        if not value:
            # simple, do here
            raise ValueError('must state a sibling name')
        super().__call__(value)

        if self._known is None:
            # we only need to know that something was provided,
            # no further check
            return value

        assert self._dsarg, \
            "Existence check for remote requires dataset specification"

        remotes = list(self._dsarg.ds.repo.call_git_items_(['remote']))
        if self._known and value not in remotes:
            self.raise_for(
                value,
                'is not one of the known remote(s) {remotes!r}',
                remotes=remotes,
            )
        elif self._known is False and value in remotes:
            self.raise_for(
                value,
                'name conflicts with a known remote',
                remotes=remotes,
            )
        else:
            return value

    def short_description(self):
        return "Name of a{desc} remote".format(
            desc=' known' if self._known
            else ' not-yet-known' if self._known is False else ''
        )

    def for_dataset(self, dataset: DatasetParameter) -> Constraint:
        """Return an similarly parametrized variant that checks remote names
        against a given dataset (argument)"""
        return self.__class__(
            known=self._known,
            dsarg=dataset,
        )
