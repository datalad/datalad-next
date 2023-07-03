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
    """Ensures a remote name is provided, and if it fulfills optional
    requirements"""

    def __init__(self,
                 preexists: bool | None = None,
                 dsarg: DatasetParameter | None = None):
        """
        Parameters
        ----------
        preexists: bool
           If true, validates that the remote exists, fails otherwise.
           If false, validates that the remote doesn't exist, fails otherwise.
           If None, just checks that a sibling name was provided.

        """
        self._preexists = preexists
        self._dsarg = dsarg
        super().__init__(allow_onelevel=True,
                         refspec_pattern=False)

    def __call__(self, value: str) -> str:
        if not value:
            # simple, do here
            raise ValueError('must state a sibling name')
        super().__call__(value)

        if self._preexists is None:
            # we only need to know that something was provided, no further check
            return value

        from datalad.runner import GitRunner, StdOutCapture
        runner = GitRunner()
        cmd = ['git', 'remote'] if not self._dsarg else \
              ['git', '-C', f'{self._dsarg.ds.path}', 'remote']
        remotes = runner.run(cmd, protocol=StdOutCapture)['stdout'].split()
        if self._preexists and value not in remotes:
            raise ValueError(
                f'Sibling {value} is not among available remotes {remotes}'
            )
        elif self._preexists is False and value in remotes:
            raise ValueError(
                f'Sibling {value} is already among available remotes {remotes}'
            )
        else:
            return value

    def short_description(self):
        if self._preexists is not None:
            desc = ' that exists' if self._preexists else ' that does not yet exist'
        else:
            desc = ''
        return "Sibling name{}".format(desc)

    def for_dataset(self, dataset: DatasetParameter) -> Constraint:
        """Return an similarly parametrized variant that resolves
        paths against a given dataset (argument)


        """
        return self.__class__(
            preexists=self._preexists,
            dsarg=dataset,
        )
