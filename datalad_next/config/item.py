from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
)

if TYPE_CHECKING:
    from datalad_next.config import (
        ConfigurationSource,
    )
    from datalad_next.config import (
        dialog as dialog_collection,
    )
    from datalad_next.constraints import Constraint


from datalad.interface.common_cfg import _NotGiven


# make a type alias with a subjectively more self-explaining name
# we reuse the core type to keep checking code here simple, and
# easy to migrate later
UnsetValue = _NotGiven


@dataclass
class ConfigurationItem:
    value: Any | UnsetValue
    """Value of a configuration item"""
    validator: Constraint | Callable | None = None
    """Type or validator of the configuration value"""
    dialog: dialog_collection.Dialog | None = None
    """Hint how a UI should gather a value for this item"""
    store_target: type[ConfigurationSource] | str | None = None
    """Hint with which configuration source this item should be stored

    Any hint should be a type.

    If a string label is given, it will be interpreted as a class name.  This
    functionality is deprecated and is only supported, for the time being, to
    support legacy implementations. It should not be used for any new
    implementations.
    """

    # TODO: `default_fn` would be a validator that returns
    # the final value from a possibly pointless value like
    # None -- in an ImplementationDefault(ConfigurationSource)
