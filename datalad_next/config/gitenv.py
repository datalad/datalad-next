from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Hashable,
)

if TYPE_CHECKING:
    from collections.abc import Collection

from datasalad.settings import (
    Setting,
    WritableSource,
)

from datalad_next.config.item import ConfigurationItem
from datalad_next.config.utils import (
    get_gitconfig_items_from_env,
    set_gitconfig_items_in_env,
)


class GitEnvironment(WritableSource):
    # this implementation is intentionally stateless to ease providing a
    # context manager for temporary manipulations
    item_type = ConfigurationItem

    def reinit(self):
        """Does nothing"""

    def load(self) -> None:
        """Does nothing

        All accessors inspect the process environment directly.
        """

    def __getitem__(self, key: Hashable) -> Setting:
        val = get_gitconfig_items_from_env()[str(key)]
        if isinstance(val, tuple):
            return self.item_type(val[-1])
        return self.item_type(val)

    def __setitem__(self, key: Hashable, value: Setting) -> None:
        env = get_gitconfig_items_from_env()
        env[str(key)] = str(value.value)
        set_gitconfig_items_in_env(env)

    def __delitem__(self, key: Hashable) -> None:
        env = get_gitconfig_items_from_env()
        del env[str(key)]
        set_gitconfig_items_in_env(env)

    def keys(self) -> Collection:
        return get_gitconfig_items_from_env().keys()

    def getall(
        self,
        key: Hashable,
        default: Any = None,
    ) -> tuple[Setting, ...]:
        try:
            val = get_gitconfig_items_from_env()[str(key)]
        except KeyError:
            return (self._get_default_setting(default),)
        vals = val if isinstance(val, tuple) else (val,)
        return tuple(self.item_type(v) for v in vals)

    def add(self, key: Hashable, value: Setting) -> None:
        key_str = str(key)
        value_str = str(value.value)
        env = get_gitconfig_items_from_env()
        val = env.get(key_str)
        if val is None:
            env[key_str] = value_str
        elif isinstance(val, tuple):
            env[key_str] = (*val, value_str)
        else:
            env[key_str] = (val, value_str)
        set_gitconfig_items_in_env(env)
