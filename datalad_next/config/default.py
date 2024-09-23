from __future__ import annotations

import logging
from typing import (
    Any,
    Callable,
)

# momentarily needed for the legacy_register_config()
# implementation.
from datalad.interface.common_cfg import definitions
from datalad.support.extensions import (
    register_config as _legacy_register_config,
)

from datalad_next.config.dialog import get_dialog_class_from_legacy_ui_label
from datalad_next.config.item import (
    ConfigurationItem,
    UnsetValue,
)
from datalad_next.config.source import ConfigurationSource
from datalad_next.constraints import (
    Constraint,
    DatasetParameter,
    EnsureNone,
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

    def getvalue(self, key, default: Any = None) -> Any:
        """Get value of an implementation default configuration

        Implementation default values are *not* validated, they are assumed to
        be valid. If validation is nevertheless desired, the ``get()``, or
        ``__getitem__()`` methods can be used, which return a
        ``ConfigurationItem`` instance that carries any configured validator.

        There is one exception to this rule: If an implementation default
        value is the type ``UnsetValue`` the validator is executed. This can
        be used to implement dynamic defaults. The assigned validator must be
        able to process the type ``UnsetValue`` as an input value, and yield
        the desired dynamic default value in return.
        """
        if key not in self:
            return default
        item = self[key]
        return item.validator(item.value) \
            if item.validator and item.value is UnsetValue \
            else item.value

    def __str__(self):
        return 'ImplementationDefaults'


#
# legacy support tooling from here.
# non of this is executed by the code above. It has to be triggered manually
# and pointed to an instance of ImplementationDefaults
#

def load_legacy_defaults(source: ImplementationDefault) -> None:
    for name, cfg in definitions.items():
        if 'default' not in cfg:
            lgr.debug(
                'Configuration %r has no default(_fn), not registering',
                name
            )
            continue

        cfg_props = cfg._props
        ui = cfg_props.get('ui', None)
        if ui is not None:
            dialog = get_dialog_class_from_legacy_ui_label(ui[0])(
                title=ui[1]['title'],
                text=ui[1].get('text', ''),
            )
        else:
            dialog = None

        type_ = cfg_props.get('type', UnsetValue)
        if name == 'datalad.tests.temp.dir':
            # https://github.com/datalad/datalad/issues/7662
            type_ = type_ | EnsureNone()

        # we want to pass any default_fn on, unevaluated, to keep the
        # point of evaluation on the access by any client, not this
        # conversion
        validator = get_validator_from_legacy_spec(
            type_=type_,
            default=cfg_props.get('default', UnsetValue),
            default_fn=cfg_props.get('default_fn', UnsetValue),
        )
        source[name] = ConfigurationItem(
            value=cfg_props.get('default', UnsetValue),
            validator=validator,
            dialog=dialog,
            store_target=get_store_target_from_destination_label(
                cfg_props.get('destination'),
            ),
        )


def get_validator_from_legacy_spec(
    type_: Constraint | None = None,
    default: Any = UnsetValue,
    default_fn: Callable | type[UnsetValue] = UnsetValue,
):
    validator = type_
    if default is UnsetValue and default_fn is not UnsetValue:
        validator = DynamicDefaultConstraint(
            default_fn,
            type_ if type_ is not UnsetValue else NoConstraint(),
        )
    return validator


def legacy_register_config(
    source: ImplementationDefault,
    name: str,
    title: str,
    *,
    default: Any = UnsetValue,
    default_fn: Callable | type[UnsetValue] = UnsetValue,
    description: str | None = None,
    type: Constraint | None = None,  # noqa: A002
    dialog: str | None = None,
    scope: str | type[UnsetValue] = UnsetValue,
):
    validator = get_validator_from_legacy_spec(
        type_=type,
        default=default,
        default_fn=default_fn,
    )
    # compose the non-legacy configuration item.
    # keeping in mind that this is for the ImplementationDefault
    # source, so there is no extra default, the default is the
    # value
    item = ConfigurationItem(
        value=default,
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

    def __call__(self, value=UnsetValue):
        if value is UnsetValue:
            value = self._default_fn()
        return self._constraint(value)


def get_store_target_from_destination_label(label: str | None) -> str | None:
    if label in (None, UnsetValue):
        return None
    if label == 'global':
        return 'GlobalGitConfig'
    if label == 'local':
        return 'LocalGitConfig'
    if label == 'dataset':
        return 'DatasetBranchConfig'
    msg = f'unsupported configuration destination label {label!r}'
    raise ValueError(msg)
