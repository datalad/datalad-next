
from __future__ import annotations

import logging
import re
from abc import abstractmethod
from pathlib import Path
from typing import (
    TYPE_CHECKING,
)

if TYPE_CHECKING:  # pragma: nocover
    from os import PathLike

from datasalad.itertools import (
    decode_bytes,
    itemize,
)

from datalad_next.config.item import ConfigurationItem
from datalad_next.config.source import ConfigurationSource
from datalad_next.runners import (
    call_git,
    iter_git_subproc,
)

lgr = logging.getLogger('datalad.config')


class GitConfig(ConfigurationSource):
    """Configuration source using git-config to read and write"""
    is_writable = True

    @abstractmethod
    def _get_git_config_cmd(self) -> list[str]:
        """Return the git-config command suitable for a particular config"""

    @abstractmethod
    def _get_git_config_cwd(self) -> Path:
        """Return path the git-config command should run in"""

    def reset(self) -> None:
        super().reset()
        self._sources = {}

    def load(self) -> None:
        cwd = self._get_git_config_cwd()
        dct = {}
        fileset = set()

        with iter_git_subproc(
            [*self._get_git_config_cmd(), '--show-origin', '--list', '-z'],
            input=None,
            cwd=cwd,
        ) as gitcfg:
            for line in itemize(
                decode_bytes(gitcfg),
                sep='\0',
                keep_ends=False,
            ):
                _proc_dump_line(line, fileset, dct)

        # take blobs with verbatim markup
        origin_blobs = {f for f in fileset if f.startswith('blob:')}
        # convert file specifications to Path objects with absolute paths
        origin_paths = {Path(f[5:]) for f in fileset if f.startswith('file:')}
        origin_paths = {f if f.is_absolute() else cwd / f for f in origin_paths}
        # TODO: add "version" tracking. The legacy config manager used mtimes
        # and we will too. but we also need to ensure that the version for
        # the "blobs" is known
        self._sources = origin_paths.union(origin_blobs)

        for k, v in dct.items():
            super().__setitem__(k, ConfigurationItem(
                value=v,
                store_target=self.__class__,
            ))

    def __setitem__(self, key: str, value: ConfigurationItem) -> None:
        call_git(
            [*self._get_git_config_cmd(), '--replace-all', key, value.value],
        )
        super().__setitem__(key, value)


class SystemGitConfig(GitConfig):
    def _get_git_config_cmd(self) -> list[str]:
        return ['config', '--system']

    def _get_git_config_cwd(self) -> Path:
        return Path.cwd()


class GlobalGitConfig(GitConfig):
    def _get_git_config_cmd(self) -> list[str]:
        return ['config', '--global']

    def _get_git_config_cwd(self) -> Path:
        return Path.cwd()


class LocalGitConfig(GitConfig):
    def __init__(self, path: PathLike):
        super().__init__()
        self._path = str(path)

    def _get_git_config_cmd(self) -> list[str]:
        return ['config', '--show-origin', '--local', '-z']

    def _get_git_config_cwd(self) -> Path:
        return Path.cwd()


def _proc_dump_line(line: str, fileset: set[str], dct: dict[str, str]) -> None:
    # line is a null-delimited chunk
    k = None
    # in anticipation of output contamination, process within a loop
    # where we can reject non syntax compliant pieces
    while line:
        if line.startswith(('file:', 'blob:')):
            fileset.add(line)
            break
        if line.startswith('command line:'):
            # no origin that we could as a pathobj
            break
        # try getting key/value pair from the present chunk
        k, v = _gitcfg_rec_to_keyvalue(line)
        if k is not None:
            # we are done with this chunk when there is a good key
            break
        # discard the first line and start over
        ignore, line = line.split('\n', maxsplit=1)  # noqa: PLW2901
        lgr.debug('Non-standard git-config output, ignoring: %s', ignore)
    if not k:
        # nothing else to log, all ignored dump was reported before
        return
    # multi-value reporting
    present_v = dct.get(k)
    if present_v is None:
        dct[k] = v
    elif isinstance(present_v, tuple):
        dct[k] = (*present_v, v)
    else:
        dct[k] = (present_v, v)


# git-config key syntax with a section and a subsection
# see git-config(1) for syntax details
cfg_k_regex = re.compile(r'([a-zA-Z0-9-.]+\.[^\0\n]+)$', flags=re.MULTILINE)
# identical to the key regex, but with an additional group for a
# value in a null-delimited git-config dump
cfg_kv_regex = re.compile(
    r'([a-zA-Z0-9-.]+\.[^\0\n]+)\n(.*)$',
    flags=re.MULTILINE | re.DOTALL
)


def _gitcfg_rec_to_keyvalue(rec: str) -> tuple[str, str]:
    """Helper for parse_gitconfig_dump()

    Parameters
    ----------
    rec: str
      Key/value specification string

    Returns
    -------
    str, str
      Parsed key and value. Key and/or value could be None
      if not syntax-compliant (former) or absent (latter).
    """
    kv_match = cfg_kv_regex.match(rec)
    if kv_match:
        k, v = kv_match.groups()
    elif cfg_k_regex.match(rec):
        # could be just a key without = value, which git treats as True
        # if asked for a bool
        k, v = rec, None
    else:
        # no value, no good key
        k = v = None
    return k, v
