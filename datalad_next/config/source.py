from __future__ import annotations

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    KeysView,
)

from datalad_next.config.item import ConfigurationItem


class ConfigurationSource(ABC):
    def __init__(self):
        self.__items: dict[str, ConfigurationItem] | None = None

    @property
    @abstractmethod
    def is_writable(self) -> bool:
        """Flag whether configuration item values can be set at the source"""

    @abstractmethod
    def load(self) -> None:
        """Implements loading items from the configuration source.

        It is expected that after calling this method, an instance of
        this source reports on configuration items according to the
        latest/current state of the source.

        No side-effects are implied. Particular implementations may
        even choose to have this method be a no-op.

        Importantly, calling this method does not imply a `reset()``.
        If a from-scratch reload is desired, ``reset()`` must be called
        explicitly.
        """

    @property
    def _items(self) -> dict[str, ConfigurationItem]:
        if self.__items is None:
            self.reset()
            self.load()
        return self.__items

    def reset(self) -> None:
        # particular implementations may not use this facility,
        # but it is provided as a convenience. Maybe factor
        # it out into a dedicated subclass even.
        self.__items = {}

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, key: str) -> ConfigurationItem:
        return self._items[key]

    def __setitem__(self, key: str, value: ConfigurationItem) -> None:
        if not self.is_writable:
            raise NotImplementedError
        self._items[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._items

    def keys(self) -> KeysView:
        return self._items.keys()

    def get(self, key, default: Any = None) -> ConfigurationItem:
        try:
            return self._items[key]
        except KeyError:
            if isinstance(default, ConfigurationItem):
                return default
            return ConfigurationItem(value=default)

    def getvalue(self, key, default: Any = None) -> Any:
        if key not in self:
            return default
        item = self[key]
        # there are two ways to do validation and type conversion.
        # on-access, or on-load. Doing it on-load would allow to reject
        # invalid configuration immediately. But it might spend time
        # on items that never get accessed. On-access might waste
        # cycles on repeated checks, and possible complain later than
        # useful. Here we nevertheless run a validator on-access in
        # the default implementation. Particular sources may want to
        # override this, or ensure that the stored value that is passed
        # to a validator is already in the best possible form to
        # make re-validation the cheapest.
        return item.validator(item.value) if item.validator else item.value

    def __repr__(self) -> str:
        return self._items.__repr__()

    def __str__(self) -> str:
        return self._items.__str__()
