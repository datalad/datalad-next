from __future__ import annotations

import logging
from typing import (
    Any,
    Callable,
)

# momentarily needed for the legacy_register_config()
# implementation.
from datalad.interface.common_cfg import (
    _NotGiven,
    definitions,
)
from datalad.support.extensions import (
    register_config as _legacy_register_config,
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


def load_legacy_defaults(source: ImplementationDefault) -> None:
    for name, cfg in definitions.items():
        if 'default' not in cfg:
            lgr.debug(
                'Configuration %r has no default(_fn), not registering',
                name
            )
            continue

        ui = cfg.get('ui', None)
        if ui is not None:
            dialog = get_dialog_class_from_legacy_ui_label(ui[0])(
                title=ui[1]['title'],
                text=ui[1].get('text', ''),
            )
        else:
            dialog = None
        source[name] = ConfigurationItem(
            value=cfg['default'],
            validator=cfg.get('type'),
            dialog=dialog,
            store_target=get_store_target_from_destination_label(
                cfg.get('destination'),
            ),
        )


def legacy_register_config(
    source: ImplementationDefault,
    name: str,
    title: str,
    *,
    default: Any = _NotGiven,
    default_fn: Callable | type[_NotGiven] = _NotGiven,
    description: str | None = None,
    type: Constraint | None = None,  # noqa: A002
    dialog: str | None = None,
    scope: str | type[_NotGiven] = _NotGiven,
):
    validator = type
    # compose the non-legacy configuration item.
    # keeping in mind that this is for the ImplementationDefault
    # source, so there is no extra default, the default is the
    # value
    value = default
    if value is _NotGiven and default_fn is not _NotGiven:
        validator = DynamicDefaultConstraint(
            default_fn,
            validator if validator is not _NotGiven else NoConstraint,
        )

    item = ConfigurationItem(
        value=value,
        validator=validator,
        dialog=get_dialog_class_from_legacy_ui_label(dialog)(
            title=title,
            text=description or '',
        ),
        store_target=get_store_target_from_destination_label(scope),
    )
    source[name] = item

    # lastly trigger legacy registration
    _legacy_register_config(
        name=name,
        title=title,
        default=default,
        default_fn=default_fn,
        description=description,
        type=type,
        dialog=dialog,
        scope=scope,
    )


class DynamicDefaultConstraint(Constraint):
    def __init__(
        self,
        default_fn: Callable,
        constraint: Constraint,
    ):
        super().__init__()
        self._default_fn = default_fn
        self._constraint = constraint

    def for_dataset(self, dataset: DatasetParameter) -> Constraint:
        # blind implementation for now
        return self.__class__(
            self._default_fn,
            self._constraint.for_dataset(dataset),
        )

    def __call__(self, value=_NotGiven):
        if value is _NotGiven:
            value = self._default_fn()
        return self._constraint(value)


def get_store_target_from_destination_label(label: str | None) -> str | None:
    if label in (None, _NotGiven):
        return None
    if label == 'global':
        return 'GlobalGitConfig'
    if label == 'local':
        return 'LocalGitConfig'
    if label == 'dataset':
        return 'DatasetBranchConfig'
    msg = f'unsupported configuration destination label {label!r}'
    raise ValueError(msg)
