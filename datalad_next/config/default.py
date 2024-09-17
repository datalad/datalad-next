from __future__ import annotations

import logging
from typing import (
    Any,
    Callable,
)

# momentarily needed for the legacy_register_config()
# implementation.
from datalad.interface.common_cfg import definitions  # type: ignore
from datalad.support.extensions import (  # type: ignore
    register_config as _legacy_register_config,
)
from datasalad.settings import Defaults

from datalad_next.config.dialog import get_dialog_class_from_legacy_ui_label
from datalad_next.config.item import (
    ConfigurationItem,
    UnsetValue,
)
from datalad_next.constraints import (
    Constraint,
    EnsureNone,
)

lgr = logging.getLogger('datalad.config')


class ImplementationDefault(Defaults):
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

        coercer = cfg_props.get('type')
        if name == 'datalad.tests.temp.dir':
            # https://github.com/datalad/datalad/issues/7662
            coercer = coercer | EnsureNone()

        default = cfg_props.get('default', UnsetValue)
        default_fn = cfg_props.get('default_fn')

        source[name] = ConfigurationItem(
            default_fn if default_fn else default,
            validator=coercer,
            lazy=default_fn is not None,
            dialog=dialog,
            store_target=get_store_target_from_destination_label(
                cfg_props.get('destination'),
            ),
        )


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
    source[name] = ConfigurationItem(
        default_fn if default_fn else default,
        validator=type,
        lazy=default_fn is not None,
        dialog=None if dialog is None
        else get_dialog_class_from_legacy_ui_label(dialog)(
            title=title,
            text=description or '',
        ),
        store_target=get_store_target_from_destination_label(scope),
    )

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


def get_store_target_from_destination_label(
    label: str | UnsetValue | None,
) -> str | None:
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
