#
# Intentionally written without importing datalad code
#
from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatch
import posixpath
from typing import Generator


@dataclass(frozen=True)
class GitPathSpec:
    # TODO think about adding support for another magic that represents
    # the root of a repository hierarchy (amending 'top', which is
    # the root of the working tree -- but presumably for a single repository
    spectypes: tuple[str, ...]
    dirprefix: str
    pattern: str | None

    @property
    def is_nopathspecs(self):
        """Whether this pathspec represent "no pathspecs", AKA ':'"""
        return not self.spectypes and not self.dirprefix and not self.pattern

    def __str__(self) -> str:
        """Generate normalized (long-form) pathspec"""
        if self.is_nopathspecs:
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

        Returns
        -------
        list
          When an empty list is return, this indicates that the pathsspec
          cannot be translated to the given ``subdir``, because it does
          not match the ``subdir`` itself. If a pathspec translates to
          "no pathspecs", a list with a dedicated ':' pathspec is returned.
        """
        # special case of a non-translation (pretty much only here to
        # make some test implementations simpler
        if not subdir:
            return [self]

        return list(yield_subdir_match_remainder_pathspecs(subdir, self))

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
            for i in range(1,len(pathspec)):
                sig = magic_signatures.get(pathspec[i])
                if sig is None:
                    pattern = pathspec[i:]
                    break
                spectypes.append(sig)
        else:
            pattern = pathspec

        # raise when glob and literal magic markers are present
        # simultaneously
        if 'glob' in spectypes and 'literal' in spectypes:
            raise ValueError("'glob' magic is incompatible with 'literal' magic")

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


def yield_subdir_match_remainder_pathspecs(
    subdir: str,
    pathspec: GitPathSpec,
) -> Generator[GitPathSpec, None, None]:
    """Translate a pathspec into a set of possible subdirectory pathspecs

    The processing implemented here is purely lexical. This means that it
    works without matching against actual file system (or Git tree) content.
    This means that it yield, to some degree, overly broad results, but also
    that it works in cases where there is nothing (yet) to match against.
    For example, a not-yet-cloned submodule.

    This function does not perform any validatity checking of pathspecs. Only
    valid pathspecs and well-formed paths are supported.

    A pathspec with the ``top`` magic is returned immediately and as-is. These
    pathspecs have an absolute reference and do not require a translation into
    a subdirectory namespace.

    Parameters
    ----------
    subdir: str
      POSIX-notation relative path of a subdirectory. The reference directory
      match be the same as that of the pathspec to be translated.
    pathspec: GitPathSpec
      To-be-translated pathspec

    Yields
    ------
    GitPathSpec
      Any number of pathspecs that an input pathspec decomposed into upon
      translation into the namespace of a subdirectory.
    """
    if 'top' in pathspec.spectypes or pathspec.is_nopathspecs:
        # pathspec with an absolute reference, or "no pathspecs"
        # no translation needed
        yield pathspec
        return

    # add a trailing directory separator to prevent undesired
    # matches of partial directory names
    subdir = subdir \
        if subdir.endswith('/') \
        else f'{subdir}/'
    tp = pathspec._get_joined_pattern()

    if 'icase' in pathspec.spectypes:
        subdir = subdir.casefold()
        tp = tp.casefold()

    # literal pathspecs
    if 'literal' in pathspec.spectypes:
        # append a trailing slash to allow for full matches
        tp_endslash = f'{tp}/'
        if not tp_endslash.startswith(subdir):
            # no match
            # BUT
            # we might have a multi-level subdir, and we might match an
            # intermediate subdir and could still yield a 'no pathspec'
            # result
            while subdir := posixpath.split(subdir)[0]:
                if tp_endslash.startswith(subdir):
                    ps = GitPathSpec(tuple(), '', None)
                    yield ps
                    return
            return

        remainder = tp[len(subdir):]
        if not remainder:
            # full match
            yield GitPathSpec(tuple(), '', None)
        else:
            yield GitPathSpec(
                pathspec.spectypes,
                *GitPathSpec._split_prefix_pattern(remainder)
            )
        return

    # tokenize the testpattern using the wildcard that also matches
    # directories
    token_delim = '**' if 'glob' in pathspec.spectypes else '*'
    tp_chunks = tp.split(token_delim)
    prefix_match = ''
    yielded = set()
    for i, chunk in enumerate(tp_chunks):
        last_chunk = i + 1 == len(tp_chunks)
        if last_chunk:
            trymatch = \
                f'{prefix_match}{chunk}{"" if chunk.endswith("/") else "/"}'
        else:
            trymatch = f'{prefix_match}{chunk}*'
        if not fnmatch(subdir, f'{trymatch}'):
            # each chunk needs match in order, first non-match ends the
            # algorithm
            # BUT
            # we have an (initial) chunk that points already
            # inside the target subdir
            submatch = trymatch
            while submatch := posixpath.split(submatch)[0]:
                if fnmatch(f'{subdir}', f'{submatch}/'):
                    ps = GitPathSpec(
                        pathspec.spectypes,
                        *GitPathSpec._split_prefix_pattern(
                            # +1 for trailing slash
                            tp[len(submatch) + 1:])
                    )
                    if ps not in yielded:
                        yield ps
                    return
            # OR
            # we might have a multi-level subdir, and we might match an
            # intermediate subdir and could still yield a 'no pathspec'
            # result
            while subdir := posixpath.split(subdir)[0]:
                if fnmatch(f'{subdir}/', trymatch):
                    ps = GitPathSpec(tuple(), '', None)
                    yield ps
                    return
            return

        remainder = tp_chunks[i + 1:]
        if all(not c for c in remainder):
            # direct hit, no pathspecs after translation
            ps = GitPathSpec(tuple(), '', None)
            yield ps
            return
        else:
            ps = GitPathSpec(
                pathspec.spectypes,
                *GitPathSpec._split_prefix_pattern(
                    f'{token_delim}{token_delim.join(remainder)}',
                )
            )
            yield ps
            yielded.add(ps)
        # extend prefix for the next round
        prefix_match = trymatch
