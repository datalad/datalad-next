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

from datalad_next.config.defaults import ImplementationDefault


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
        self._defaults = [
            s for s in self._mngr.sources.values()
            if isinstance(ImplementationDefault)
        ][-1]
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
        # do local import, as this module is import prominently and the
        # could theoretically import all kind of weird things for type
        # conversion
        from datalad.interface.common_cfg import definitions as cfg_defs

        # fetch what we know about this variable
        cdef = cfg_defs.get(var, {})
        # type conversion setup
        if valtype is None and 'type' in cdef:
            valtype = cdef['type']
        if valtype is None:
            valtype = lambda x: x

        # any default?
        if default is None and 'default' in cdef:
            default = cdef['default']

        _value = None
        if var in self:
            # nothing needs to be obtained, it is all here already
            _value = self[var]
        elif store is False and default is not None:
            # nothing will be stored, and we have a default -> no user confirmation
            # we cannot use logging, because we want to use the config to configure
            # the logging
            #lgr.debug('using default {} for config setting {}'.format(default, var))
            _value = default

        if _value is not None:
            # we got everything we need and can exit early
            try:
                return valtype(_value)
            except Exception as e:
                raise ValueError(
                    "value '{}' of existing configuration for '{}' cannot be "
                    "converted to the desired type '{}' ({})".format(
                        _value, var, valtype, e)) from e

        # now we need to try to obtain something from the user
        from datalad.ui import ui

        # configure UI
        dialog_opts = kwargs
        if dialog_type is None:  # no override
            # check for common knowledge on how to obtain a value
            if 'ui' in cdef:
                dialog_type = cdef['ui'][0]
                # pull standard dialog settings
                dialog_opts = cdef['ui'][1]
                # update with input
                dialog_opts.update(kwargs)

        if (not ui.is_interactive or dialog_type is None) and default is None:
            raise RuntimeError(
                "cannot obtain value for configuration item '{}', "
                "not preconfigured, no default, no UI available".format(var))

        if not hasattr(ui, dialog_type):
            raise ValueError("UI '{}' does not support dialog type '{}'".format(
                ui, dialog_type))

        # configure storage destination, if needed
        if store:
            if scope is None and 'destination' in cdef:
                scope = cdef['destination']
            if scope is None:
                raise ValueError(
                    "request to store configuration item '{}', but no "
                    "storage destination specified".format(var))

        # obtain via UI
        dialog = getattr(ui, dialog_type)
        _value = dialog(default=default, **dialog_opts)

        if _value is None:
            # we got nothing
            if default is None:
                raise RuntimeError(
                    "could not obtain value for configuration item '{}', "
                    "not preconfigured, no default".format(var))
            # XXX maybe we should return default here, even it was returned
            # from the UI -- if that is even possible

        # execute type conversion before storing to check that we got
        # something that looks like what we want
        try:
            value = valtype(_value)
        except Exception as e:
            raise ValueError(
                "cannot convert user input `{}` to desired type ({})".format(
                    _value, e)) from e
            # XXX we could consider "looping" until we have a value of proper
            # type in case of a user typo...

        if store:
            # store value as it was before any conversion, needs to be str
            # anyway
            # needs string conversion nevertheless, because default could come
            # in as something else
            self.add(var, '{}'.format(_value), scope=scope, reload=reload)
        return value

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
