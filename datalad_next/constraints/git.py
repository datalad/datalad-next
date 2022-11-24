from .base import Constraint


class EnsureGitRefName(Constraint):
    """Ensures that a reference name is well formed

    Validation is peformed by calling `git check-ref-format`.
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
            raise ValueError('refname must not be empty')

        from datalad.runner import GitRunner, StdOutCapture
        from datalad_next.exceptions import CommandError
        runner = GitRunner()
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
            out = runner.run(cmd, protocol=StdOutCapture)
        except CommandError as e:
            raise ValueError(f'{value} is not a valid refname') from e

        if self._normalize:
            return out['stdout'].strip()
        else:
            return value

    def short_description(self):
        return '{}Git refname{}'.format(
            '(single-level) ' if self._allow_onelevel else '',
            ' or refspec pattern' if self._refspec_pattern else '',
        )
