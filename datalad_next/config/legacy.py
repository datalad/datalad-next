"""`MultiConfiguration` adaptor for `ConfigManager` drop-in replacement"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
)

if TYPE_CHECKING:
    from datalad.distribution.dataset import Dataset
    from datalad.support.gitrepo import GitRepo

    from datalad_next.config import MultiConfiguration


class ConfigManager:
    def __init__(
        self,
        _mngr: MultiConfiguration,
        dataset: GitRepo | Dataset | None = None,
        overrides=None,
        source='any',
    ):
        # TODO: assemble new MultiConfiguration and only uses the source
        # instances of the incoming one. We also need to inject a
        # LegacyOverrides source
        self._mngr = _mngr
        # TODO: actually, these need really complex handling, because that
        # container is manipulated directly in client code...
        self.overrides = overrides

        # TODO: make obsolete
        self._repo_dot_git = None
        self._repo_pathobj = None
        if dataset:
            if hasattr(dataset, 'dot_git'):
                # `dataset` is actually a Repo instance
                self._repo_dot_git = dataset.dot_git
                self._repo_pathobj = dataset.pathobj
            elif dataset.repo:
                self._repo_dot_git = dataset.repo.dot_git
                self._repo_pathobj = dataset.repo.pathobj

    def reload(self, force: bool = False) -> None:
        for s in self._mngr.sources.values():
            s.load()

    def obtain(self, var, default=None, dialog_type=None, valtype=None,
               store=False, scope=None, reload=True, **kwargs):
        raise NotImplementedError

    def __repr__(self):
        # give full list of all tracked config sources, plus overrides
        return "ConfigManager({}{})".format(
            [str(s) for s in self._mngr.sources.values()],
            f', overrides={self.overrides!r}'
            if self.overrides else '',
        )

    def __str__(self):
        # give path of dataset, if there is any, plus overrides
        return "ConfigManager({}{})".format(
            self._repo_pathobj if self._repo_pathobj else '',
            'with overrides' if self.overrides else '',
        )

    def __len__(self) -> int:
        return len(self._mngr)

    def __getitem__(self, key: str) -> Any:
        # use a custom default to discover unset values
        val = self._mngr.getvalue(key, _Unset)
        if val is _Unset:
            # we do not actually have it
            raise KeyError
        return val

    def __contains__(self, key) -> bool:
        return key in self._mngr

    def keys(self):
        return self._mngr.keys()

    def get(self, key, default=None, get_all=False):
        val = self._mngr.getvalue(key, default)
        if not get_all and isinstance(val, tuple):
            return val[-1]
        return val

    def get_from_source(self, source, key, default=None):
        raise NotImplementedError

    def sections(self):
        raise NotImplementedError

    def options(self, section):
        raise NotImplementedError

    def has_section(self, section):
        raise NotImplementedError

    def has_option(self, section, option):
        raise NotImplementedError

    def getint(self, section, option):
        raise NotImplementedError

    def getfloat(self, section, option):
        raise NotImplementedError

    def getbool(self, section, option, default=None):
        raise NotImplementedError

    def items(self, section=None):
        raise NotImplementedError

    def get_value(self, section, option, default=None):
        raise NotImplementedError

    def add(self, var, value, scope='branch', reload=True):
        raise NotImplementedError

    def set(self, var, value, scope='branch', reload=True, force=False):
        raise NotImplementedError

    def rename_section(self, old, new, scope='branch', reload=True):
        raise NotImplementedError

    def remove_section(self, sec, scope='branch', reload=True):
        raise NotImplementedError

    def unset(self, var, scope='branch', reload=True):
        raise NotImplementedError


class _Unset:
    pass
