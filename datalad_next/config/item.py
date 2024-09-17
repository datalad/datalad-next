from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
)

from datasalad.settings import Setting
from datasalad.settings.setting import UnsetValue as SaladUnsetValue
from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from datasalad.settings import Source

    from datalad_next.config import (
        Dialog,
        dialog as dialog_collection,
    )
    from datalad_next.constraints import Constraint

from datalad.interface.common_cfg import _NotGiven  # type: ignore

# make a type alias with a subjectively more self-explaining name
# we reuse the core type to keep checking code here simple, and
# easy to migrate later
UnsetValue: TypeAlias = _NotGiven
#UnsetValue: TypeAlias = SaladUnsetValue


class ConfigurationItem(Setting):
    def __init__(
        self,
        value: Any | UnsetValue = UnsetValue,
        *,
        validator: Constraint | Callable | None = None,
        lazy: bool = False,
        dialog: dialog_collection.Dialog | None = None,
        store_target: type[Source] | str | None = None,
    ):
        """
        - Value of a configuration item
        - Type or validator of the configuration value
        - Hint how a UI should gather a value for this item
        - Hint with which configuration source this item should be stored

        Any hint should be a type.

        If a string label is given, it will be interpreted as a class name.
        This functionality is deprecated and is only supported, for the time
        being, to support legacy implementations. It should not be used for any
        new implementations.
        """
        super().__init__(
            value=SaladUnsetValue if value is UnsetValue else value,
            coercer=validator,
            lazy=lazy,
        )
        self._dialog = dialog
        self._store_target = store_target

    @property
    def dialog(self) -> Dialog | None:
        return self._dialog

    @property
    def value(self) -> Any:
        val = super().value
        if val is SaladUnsetValue:
            return UnsetValue
        return val

    @property
    def validator(self) -> Callable | None:
        return self.coercer

    def update(self, item: Setting) -> None:
        super().update(item)
        for attr in ('_dialog', '_store_target'):
            val = getattr(item, attr)
            if val is not None:
                setattr(self, attr, val)
