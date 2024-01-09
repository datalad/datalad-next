#
# Intentionally written without importing datalad code
#
from __future__ import annotations

from dataclasses import dataclass
import fnmatch
from itertools import chain


@dataclass(frozen=True)
class GitPathSpec:
    # TODO think about adding support for another magic that represents
    # the root of a repository hierarchy (amending 'top', which is
    # the root of the working tree -- but presumably for a single repository
    spectypes: tuple[str]
    dirprefix: str
    pattern: str

    def __str__(self) -> str:
        """Generate normalized (long-form) pathspec"""
        if not self.spectypes and not self.dirprefix and not self.pattern:
            return ':'
        ps = ''
        if self.spectypes:
            ps += ':('
            ps += ','.join(self.spectypes)
            ps += ')'
        ps += self._get_joined_pattern()
        return ps

    def _get_joined_pattern(self):
        return f'{self.dirprefix if self.dirprefix else ""}' \
            f'{"/" if self.dirprefix else ""}' \
            f'{self.pattern if self.pattern else ""}'

    def for_subdir(self, subdir: str) -> list[GitPathSpec]:
        """
        The following rules apply to particular magic pathspecs:

        - 'top' are not modified. This makes them essentially
          relative to the root of the respective repository

        Parameters
        ----------
        subdir: str
          Relative path in POSIX notation

        split into sd_parts
        split into pattern_parts

        """
        if not subdir:
            return [self]
        elif 'top' in self.spectypes:
            # no need to mangle, these are treated as absolute to
            # any repo they are evaluated one -- this means that
            # they are OK to change their reference when moving
            # into submodules
            return [self]
        elif 'literal' in self.spectypes:
            testpattern = self._get_joined_pattern()
            testsubdir = subdir if subdir.endswith('/') else f'{subdir}/'
            # TODO icase
            if (
                ('icase' in self.spectypes
                 and testpattern.casefold().startswith(testsubdir.casefold()))
                or testpattern.startswith(testsubdir)
            ):
                return [GitPathSpec(
                    self.spectypes,
                    *GitPathSpec._split_prefix_pattern(
                        testpattern[len(testsubdir):])
                )]
        elif str(self) == ':':
            return []
        elif 'glob' in self.spectypes:
            raise NotImplementedError
        else:
            # "ordinary" pathspec with fnmatch
            # we find the shortest and the longest match for the subdir
            # and report the list of unique results.
            # we do that, because we only have partial knowledge of a
            # would-be matching path. Any '*' might potentially match
            # more than just the subdir, thereby consuming more of the
            # pathspec than we can know here.
            #
            # We report the shortest and longest detectable match -- not
            # because this is a complete or correct solution, but because it
            # adds support for relatively common usecases, e.g. `*item*.jpg`
            # (match any .jpg file with "item" somewhere is its path.
            # The shortest match for a translation to a "moreitem/" would cause
            # a translated pathspec of `*item*.jpg` (already the initial '*'
            # matches the full subdir, hence any FILEname inside that subdir
            # would now be required to contain "item". The longest match,
            # however, translates to `*.jpg`, which is more fitting for the
            # underlying intentions.
            return list(set(chain(
                self._find_shortest_subspec_fnmatch(subdir, 'shortest'),
                self._find_longest_subspec_fnmatch(subdir, 'longest'),
            )))

    def _find_longest_subspec_fnmatch(self, subdir: str, mode: str):
        testpattern = self._get_joined_pattern()
        testsubdir = subdir
        tp = testpattern
        while tp:
            if fnmatch.fnmatch(testsubdir, tp):
                # tp is a match for subdir, subtract it from the full pattern.
                # we do not want to strip a '*', it has the potential to
                # match more than just the present subdir
                final = testpattern[len(tp) - (1 if tp.endswith('*') else 0):]
                # strip initial directory separators, make no sense when
                # porting to a subdir
                final = final.lstrip('/')
                if final:
                    yield GitPathSpec(
                        self.spectypes,
                        *GitPathSpec._split_prefix_pattern(final)
                    )
                return
            # get the next chunk
            idx = index_any(tp.rindex, ['/', '*'], 0, len(tp))
            if idx is None:
                # we scanned the whole pattern and nothing matched
                return
            tp = tp[:idx]

    def _find_shortest_subspec_fnmatch(self, subdir: str, mode: str):
        testpattern = self._get_joined_pattern()
        # add a trailing directory separator to prevent undesired
        # matches of partial directory names
        testsubdir = subdir if subdir.endswith('/') else f'{subdir}/'
        tp = None
        while tp is None or (len(tp) + 1 < len(testpattern)):
            # get the next chunk
            idx = index_any(
                testpattern.index,
                ['/', '*'],
                0 if tp is None else (len(tp) + 1),
                len(testpattern),
            )
            if idx is None:
                # we scanned the whole pattern and nothing matched, use the
                # full pattern with a trailing / (because we added that to
                # testsubdir)
                tp = f'{testpattern}/'
            else:
                tp = testpattern[:idx + 1]
            if fnmatch.fnmatch(testsubdir, tp):
                # tp is a match for subdir, subtract it from the full pattern.
                # we do not want to strip a '*', it has the potential to
                # match more than just the present subdir
                final = testpattern[len(tp) - (1 if tp.endswith('*') else 0):]
                if final:
                    # do not emit a non-pathspec to avoid any downstream
                    # processing giving a non-pattern is the same as not
                    # given one
                    yield GitPathSpec(
                        self.spectypes,
                        *GitPathSpec._split_prefix_pattern(final)
                    )
                return

    @classmethod
    def from_pathspec_str(
        cls,
        pathspec: str,
    ) -> GitPathSpec:
        spectypes = []
        dirprefix = None
        pattern = None

        if pathspec.startswith(':('):
            # long-form magic
            magic, pattern = pathspec[2:].split(')', maxsplit=1)
            spectypes = magic.split(',')
        elif pathspec.startswith(':'):
            # short-form magic
            magic_signatures = {
                '/': 'top',
                '!': 'exclude',
                '^': 'exclude',
                ':': None,
            }
            pattern = pathspec[1:]
            spectypes = []
            for i in range(1, len(pathspec)):
                sig = magic_signatures.get(pathspec[i])
                if sig is None:
                    pattern = pathspec[i:]
                    break
                spectypes.append(sig)
        else:
            pattern = pathspec

        # TODO raise when glob and literal magic markers are present
        # simultaneously
        if 'glob' in spectypes and 'literal' in spectypes:
            raise ValueError('glob magic is incompatible with literal magic')

        # split off dirprefix
        dirprefix, pattern = GitPathSpec._split_prefix_pattern(pattern)

        return cls(
            spectypes=tuple(spectypes),
            dirprefix=dirprefix,
            pattern=pattern,
        )

    @staticmethod
    def _split_prefix_pattern(pathspec):
        # > the pathspec up to the last slash represents a directory prefix.
        # > The scope of that pathspec is limited to that subtree.
        try:
            last_slash_idx = pathspec[::-1].index('/')
        except ValueError:
            # everything is the pattern
            dirprefix = None
            pattern = pathspec
        else:
            dirprefix = pathspec[:-last_slash_idx - 1]
            pattern = pathspec[-last_slash_idx:] \
                if last_slash_idx > 0 else None
        return dirprefix, pattern


def index_any(fx, subs: list[str], start: int, end: int) -> int | None:
    idx = []
    for sub in subs:
        try:
            idx.append(fx(sub, start, end))
        except ValueError:
            pass
    if not idx:
        return None
    else:
        return min(idx)
