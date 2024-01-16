"""Constraints for Git-related concepts and parameters"""
from __future__ import annotations

from datalad_next.runners import (
    CommandError,
    call_git,
    call_git_oneline,
)
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

        cmd = ['check-ref-format']
        cmd.append('--allow-onelevel'
                   if self._allow_onelevel
                   else '--no-allow-onelevel')
        if self._refspec_pattern:
            cmd.append('--refspec-pattern')
        if self._normalize:
            cmd.append('--normalize')

        cmd.append(value)

        try:
            res = (call_git_oneline
                   if self._normalize else call_git)(cmd)
        except CommandError as e:
            self.raise_for(
                value,
                'is not a valid refname',
                __caused_by__=e,
            )

        if self._normalize:
            return res
        else:
            return value

    def short_description(self):
        return '{}Git refname{}'.format(
            '(single-level) ' if self._allow_onelevel else '',
            ' or refspec pattern' if self._refspec_pattern else '',
        )


class EnsureRemoteName(Constraint):
    """Ensures a valid remote name, and optionally if such a remote is known
    """
    _label = 'remote'

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
        self._label = 'remote'
        self._known = known
        self._dsarg = dsarg

    def __call__(self, value: str) -> str:
        if not value:
            # simple, do here
            self.raise_for(
                value,
                f'missing {self._label} name',
            )

        if self._known is not None:
            assert self._dsarg, \
                f"Existence check for {self._label} requires dataset " \
                "specification"

        if self._known:
            # we don't need to check much, only if a remote of this name
            # already exists -- no need to check for syntax compliance
            # again
            if not any(
                k.startswith(f"remote.{value}.")
                for k in self._dsarg.ds.config.keys()
            ):
                self.raise_for(
                    value,
                    f'is not a known {self._label}',
                )
        else:
            # whether or not the remote must not exist, or we would not care,
            # in all cases we need to check for syntax compliance
            EnsureGitRefName(
                allow_onelevel=True,
                refspec_pattern=False,
            )(value)

        if self._known is None:
            # we only need to know that something was provided,
            # no further check
            return value

        if self._known is False and any(
            k.startswith(f"remote.{value}.")
            for k in self._dsarg.ds.config.keys()
        ):
            self.raise_for(
                value,
                f'name conflicts with a known {self._label}',
            )

        return value

    def short_description(self):
        return f"Name of a{{desc}} {self._label}".format(
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


class EnsureSiblingName(EnsureRemoteName):
    """Identical to ``EnsureRemoteName``, but used the term "sibling"

    Only error messages and documentation differ, with "remote" being
    replaced with "sibling".
    """
    _label = 'sibling'
