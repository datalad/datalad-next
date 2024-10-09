"""`MultiConfiguration` adaptor for `ConfigManager` drop-in replacement"""

from __future__ import annotations

import logging
import warnings
from copy import copy
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
)

if TYPE_CHECKING:
    from datalad.distribution.dataset import Dataset  # type: ignore
    from datalad.support.gitrepo import GitRepo  # type: ignore
    from datasalad.settings import Source

from datasalad.settings import (
    InMemory,
    Settings,
)
from datasalad.settings.setting import UnsetValue as SaladUnsetValue

from datalad_next.config import dialog
from datalad_next.config.git import (
    DataladBranchConfig,
    LocalGitConfig,
)
from datalad_next.config.item import (
    ConfigurationItem,
    UnsetValue,
)
from datalad_next.runners import (
    call_git,
)

lgr = logging.getLogger('datalad.config')


def _where_to_scope(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'where' in kwargs:
            if 'scope' in kwargs:
                raise ValueError("Do not specify both 'scope' and DEPRECATED 'where'")
            kwargs = kwargs.copy()
            where = kwargs.pop('where')
            if where == 'dataset':
                warnings.warn("'where=\"dataset\"' is deprecated, use 'scope=\"branch\"' instead",
                              DeprecationWarning)
                where = 'branch'
            else:
                warnings.warn("'where' is deprecated, use 'scope' instead",
                              DeprecationWarning)
            kwargs['scope'] = where
        return func(*args, **kwargs)
    return wrapper


class LegacyOverridesProxy:
    """Proxy class to wrap the legacy ConfigManager overrides

    There were handed out for direct manipulation of their holding
    dict. This allowed for arbitrary modification. This class is
    supposed to give us a fighting change to keep supporting this
    interface, while being able to issue deprecation warnings
    and continue to integrate with the new setup.

    For now this wraps the legacy-override source, but it could
    eventually migrate to read from and write to the git-command
    source.
    """
    def __init__(self, overrides: InMemory):
        self._ov = overrides

    def items(self):
        for k, v in self._ov._items.items():
            yield k, v.value if not isinstance(v, tuple) \
                else (i.value for i in v)

    def update(self, other):
        for k, v in other.items():
            self._ov._items[k] = self._ov.item_type(v) \
                if not isinstance(v, tuple) \
                else tuple(self._ov.item_type(i) for i in v)

    def copy(self):
        return dict(self.items())


class ConfigManager:
    def __init__(
        self,
        dataset: GitRepo | Dataset | None = None,
        overrides=None,
        source='any',
    ):
        # to new code, while new code already uses the new interface
        from datalad_next.config import manager

        self._mngr = Settings(get_sources(
            manager,
            dataset=dataset,
            overrides=overrides,
            source=source,
        ))
        self._defaults = manager.sources['defaults']
        self.reload()

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

    @property
    def overrides(self):
        # this is a big hassle. the original class hands out the real dict to
        # do any manipulation with it. for a transition we want to keep some
        # control, and hand out a proxy only
        return LegacyOverridesProxy(self._mngr.sources['legacy-overrides'])

    @property
    def _stores(self):
        # this beast only exists to satisfy a test that reaches into the
        # internals (that no longer exists) and verifies that the right
        # source files are read
        files = set()
        # only for tests
        for label in ['git-system', 'git-global', 'git-local']:
            src = self._mngr.sources.get(label)
            if src is None:
                continue
            src.load()
            files.update(src._sources)
        return {'git': {'files': files}}

    def reload(self, force: bool = False) -> None:
        for n, s in self._mngr.sources.items():
            if n in ('legacy-overrides', 'defaults'):
                continue
            s.reinit()
            s.load()

    def obtain(self, var, default=None, dialog_type=None, valtype=None,
               store=False, scope=None, reload=True, **kwargs):
        # maybe we have a default
        item = copy(self._defaults.get(var, ConfigurationItem(UnsetValue)))
        if valtype is not None:
            item._coercer = valtype
        item.update(self._mngr.get(var, ConfigurationItem(UnsetValue)))

        # we need to check for the salad value if reaching into the guts
        if item._value is not SaladUnsetValue:
            # might crash here, if not valid, but we want that
            return item.value

        # configure storage destination, if needed
        #if store:
        #    if scope is None and 'destination' in cdef:
        #        scope = cdef['destination']
        #    if scope is None:
        #        raise ValueError(
        #            "request to store configuration item '{}', but no "
        #            "storage destination specified".format(var))

        if dialog_type:
            item._dialog = dialog.get_dialog_class_from_legacy_ui_label(
                dialog_type)(**kwargs)

        if item._dialog is None:
            if default is None:
                msg = f"cannot obtain value for configuration item '{var}', " \
                    "not preconfigured, no default, no UI specified"
                raise RuntimeError(msg)
            return default

        if store and item._store_target is None:
            msg = (
                f"request to store configuration item {var!r}, but no "
                "storage destination specified"
            )
            raise ValueError(msg)

        # `default` here is different from what one would think. It is the
        # default to present to the user when asking for a value.
        val = self._obtain_from_user(
            var,
            item,
            default=default,
        )

        item._value = val

        # TODO: should loop if something invalid was entered. Do better
        # in reimplementation
        validated = item.value

        if store:
            src = self.get_src(item._store_target)
            src.add(var, item)
            if reload:
                src.load()
        return validated

    def _obtain_from_user(
        self,
        var,
        default_item,
        default=None,
        valtype=None,
        **kwargs,
    ):
        # now we need to try to obtain something from the user
        from datalad.ui import ui  # type: ignore

        if (not ui.is_interactive or default_item.dialog is None) and default is None:
            raise RuntimeError(
                f"cannot obtain value for configuration item '{var}', "
                "not preconfigured, no default, no UI available")

        # obtain via UI
        try:
            dialog_cls = getattr(
                ui,
                {
                    dialog.Question: 'question',
                    dialog.YesNo: 'yesno',
                }[type(default_item.dialog)],
            )
        except KeyError:
            msg = f"UI {ui!r} does not support dialog {default_item.dialog!r}"
            raise ValueError(msg)

        _value = dialog_cls(
            default=default,
            title=default_item.dialog.title,
            text=default_item.dialog.text,
        )

        if _value is None:
            # we got nothing
            if default is None:
                raise RuntimeError(
                    f"could not obtain value for configuration item '{var}', "
                    "not preconfigured, no default")
            # XXX maybe we should return default here, even it was returned
            # from the UI -- if that is even possible

        return _value


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
        # the legacy implementation returned all values here
        val = self.get(
            key,
            default=UnsetValue,
            get_all=True,
        )
        if not isinstance(val, tuple) and val is UnsetValue:
            raise KeyError
        if len(val) == 1:
            val = val[0]
        return val

    def __contains__(self, key) -> bool:
        return key in self._mngr

    def keys(self):
        return self._mngr.keys()

    def get(self, key, default=None, get_all=False):
        if key not in self:
            return default
        val = self._mngr.getall(key)
        if not get_all and isinstance(val, tuple):
            return val[-1].value
        return tuple(v.value for v in val)

    def get_from_source(self, source, key, default=None):
        src = self.get_src(source)
        return src.get(key, default).value

    def sections(self):
        """Returns a list of the sections available"""
        return list(set([
            '.'.join(k.split('.')[:-1]) for k in self._mngr.keys()
        ]))

    def options(self, section):
        return [
            k.split('.')[-1] for k in self._mngr.keys()
            if k.startswith(f'{section}.')
        ]

    def has_section(self, section):
        """Indicates whether a section is present in the configuration"""
        # TODO: next one is the proper implementation, but core tests
        # force us to do it wrong
        #return any(k.startswith(f'{section}.') for k in self._mngr.keys())
        return any(k.startswith(section) for k in self._mngr.keys())

    def has_option(self, section, option):
        return f'{section}.{option}' in self._mngr

    def getint(self, section, option):
        return int(self._mngr[f'{section}.{option}'].value)

    def getfloat(self, section, option):
        return float(self._mngr[f'{section}.{option}'].value)

    def getbool(self, section, option, default=None):
        return anything2bool(self._mngr.get(
            f'{section}.{option}',
            default=default).value)

    def items(self, section=None):
        prefix = f'{section}.' if section else ''
        return [
            (k, self[k]) for k in self._mngr.keys()
            if k.startswith(prefix)
        ]

    def get_value(self, section, option, default=None):
        key = f'{section}.{option}'
        if key not in self._mngr.keys() and default is None:
            # this strange dance is needed because gitpython did
            # it this way
            raise KeyError
        return self._mngr.get(
            f'{section}.{option}',
            default=default).value

    def add(self, var, value, scope='branch', reload=True):
        src = self.get_src(scope)
        # there would be no need for a reload, but the core tests
        # enforce no direct updating of the available knowledge
        src.add(var, ConfigurationItem(value))

    @_where_to_scope
    def set(self, var, value, scope='branch', reload=True, force=False):
        src = self.get_src(scope)
        if scope == 'override':
            src._items[var] = ConfigurationItem(value)
            return
        cmd = [*src._get_git_config_cmd()]
        if force:
            cmd.append('--replace-all')
        call_git(
            [*cmd, var, value],
            capture_output=True,

        )
        if reload:
            src.reinit()
            src.load()

    def rename_section(self, old, new, scope='branch', reload=True):
        src = self.get_src(scope)
        if scope == 'override':
            for k in list(src._items.keys()):
                if k.startswith(f'{old}.'):
                    src._items[f'{new}.{k.split(".")[-1]}'] = src._items[k]
                    del src._items[k]
            return
        call_git(
            [*src._get_git_config_cmd(), '--rename-section', old, new],
            capture_output=True,

        )
        if reload:
            src.reinit()
            src.load()

    def remove_section(self, sec, scope='branch', reload=True):
        src = self.get_src(scope)
        if scope == 'override':
            for k in list(src._items.keys()):
                if k.startswith(f'{sec}.'):
                    del src._items[k]

    def unset(self, var, scope='branch', reload=True):
        src = self.get_src(scope)
        if scope == 'override':
            del src[var]
            return
        call_git(
            [*src._get_git_config_cmd(), '--unset-all', var],
            capture_output=True,

        )
        if reload:
            src.reinit()
            src.load()

    def get_src(self, scope):
        if scope is None:
            scope = 'local'
        name = scope_label_to_source_label_map.get(scope)
        if name is None:
            raise ValueError(f'unknown scope {scope!r}')
        return self._mngr.sources[name]


scope_label_to_source_label_map = {
    'branch': 'datalad-branch',
    'local': 'git-local',
    'global': 'git-global',
    'override': 'legacy-overrides',
    # old names
    'dataset': 'datalad-branch',
}


def get_sources(
    manager: Settings,
    dataset: GitRepo | Dataset | None = None,
    overrides=None,
    source='any',
) -> dict[str, Source]:
    """Implement the legacy ruleset of what to read from

    Parameters
    ----------
    source : {'any', 'local', 'branch', 'branch-local'}, optional
      Which sources of configuration setting to consider. If 'branch',
      configuration items are only read from a dataset's persistent
      configuration file in current branch, if any is present
      (the one in ``.datalad/config``, not
      ``.git/config``); if 'local', any non-committed source is considered
      (local and global configuration in Git config's terminology);
      if 'branch-local', persistent configuration in current dataset branch
      and local, but not global or system configuration are considered; if 'any'
      all possible sources of configuration are considered.
      Note: 'dataset' and 'dataset-local' are deprecated in favor of 'branch'
      and 'branch-local'.
    """
    nodataset_errmsg = (
        'ConfigManager configured to read from (a branch of) a dataset, '
        'but no dataset given'
    )
    # if applicable, we want to reuse the exact same source instances as the
    # global non-legacy manager to get a somewhat smooth transition of old code
    global_sources = manager.sources

    ovsrc = InMemory()
    if overrides is not None:
        for k, v in overrides.items():
            ovsrc[k] = ConfigurationItem(v)
    #
    # No scenario can return Defaults(), the legacy manager did not
    # have that
    #
    if source == 'branch':
        if dataset is None:
            raise ValueError(nodataset_errmsg)
        return {
            'legacy-overrides': ovsrc,
            'datalad-branch': DataladBranchConfig(dataset.pathobj),
        }
    if source == 'local':
        if dataset is None:
            return {
                'legacy-environment': global_sources['legacy-environment'],
                'legacy-overrides': ovsrc,
                'git-global': global_sources['git-global'],
                'git-system': global_sources['git-system'],
            }
        return {
            'legacy-environment': global_sources['legacy-environment'],
            'legacy-overrides': ovsrc,
            'git-local': LocalGitConfig(dataset.pathobj),
            'git-global': global_sources['git-global'],
            'git-system': global_sources['git-system'],
        }
    if source == 'branch-local':
        if dataset is None:
            raise ValueError(nodataset_errmsg)
        return {
            'legacy-overrides': ovsrc,
            'git-local': LocalGitConfig(dataset.pathobj),
            'datalad-branch': DataladBranchConfig(dataset.pathobj),
        }
    if source == 'any':
        # the full stack
        if not dataset:
            return {
                'legacy-environment': global_sources['legacy-environment'],
                'legacy-overrides': ovsrc,
                'git-global': global_sources['git-global'],
                'git-system': global_sources['git-system'],
            }
        return {
            'legacy-environment': global_sources['legacy-environment'],
            'legacy-overrides': ovsrc,
            'git-local': LocalGitConfig(dataset.pathobj),
            'git-global': global_sources['git-global'],
            'git-system': global_sources['git-system'],
            'datalad-branch': DataladBranchConfig(dataset.pathobj),
        }

    raise ValueError(f'unknown configuration source {source!r}')


def anything2bool(val):
    if val == '':
        return False
    if hasattr(val, 'lower'):
        val = val.lower()
    if val in {"off", "no", "false", "0"} or not bool(val):
        return False
    elif val in {"on", "yes", "true", True} \
            or (hasattr(val, 'isdigit') and val.isdigit() and int(val)) \
            or isinstance(val, int) and val:
        return True
    else:
        raise TypeError(
            "Got value %s which could not be interpreted as a boolean"
            % repr(val))


def rewrite_url(cfg, url):
    """Any matching 'url.<base>.insteadOf' configuration is applied

    Any URL that starts with such a configuration will be rewritten
    to start, instead, with <base>. When more than one insteadOf
    strings match a given URL, the longest match is used.

    Parameters
    ----------
    cfg : ConfigManager or dict
      dict-like with configuration variable name/value-pairs.
    url : str
      URL to be rewritten, if matching configuration is found.

    Returns
    -------
    str
      Rewritten or unmodified URL.
    """
    insteadof = {
        # only leave the base url
        k[4:-10]: v
        for k, v in cfg.items()
        if k.startswith('url.') and k.endswith('.insteadof')
    }

    # all config that applies
    matches = {
        key: v
        for key, val in insteadof.items()
        for v in (val if isinstance(val, tuple) else (val,))
        if url.startswith(v)
    }
    # find longest match, like Git does
    if matches:
        rewrite_base, match = sorted(
            matches.items(),
            key=lambda x: len(x[1]),
            reverse=True,
        )[0]
        if sum(match == v for v in matches.values()) > 1:
            lgr.warning(
                "Ignoring URL rewrite configuration for '%s', "
                "multiple conflicting definitions exists: %s",
                match,
                [f'url.{k}.insteadof'
                 for k, v in matches.items()
                 if v == match]
            )
        else:
            url = f'{rewrite_base}{url[len(match):]}'
    return url


# for convenience, bind to class too
ConfigManager.rewrite_url = rewrite_url  # type: ignore
