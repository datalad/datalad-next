from __future__ import annotations

import logging
from typing import (
    Any,
    Callable,
)


from datalad_next.config.dialog import get_dialog_class_from_legacy_ui_label
from datalad_next.config.item import ConfigurationItem
from datalad_next.config.source import ConfigurationSource
from datalad_next.constraints import (
    Constraint,
    DatasetParameter,
    NoConstraint,
)

lgr = logging.getLogger('datalad.config.default')


class ImplementationDefault(ConfigurationSource):
    is_writable = True

    def load(self) -> None:
        # there is no loading. clients have to set any items they want to
        # see a default known for. There is typically only one instance of
        # this class, and it is the true source of the information by itself.
        pass

    def __setitem__(self, key: str, value: ConfigurationItem) -> None:
        if key in self:
            # resetting is something that is an unusual event.
            # __setitem__ does not allow for a dedicated "force" flag,
            # so we leave a message at least
            lgr.debug('Resetting %r default', key)
        super().__setitem__(key, value)

    def __str__(self):
        return 'ImplementationDefaults'
