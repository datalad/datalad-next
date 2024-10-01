from __future__ import annotations

from os import environ
from typing import (
    Mapping,
    Tuple,
)


def get_gitconfig_items_from_env() -> dict[str, str | Tuple[str, ...]]:
    """Parse git-config ENV (``GIT_CONFIG_COUNT|KEY|VALUE``) and return as dict

    This implementation does not use ``git-config`` directly, but aims to
    mimic its behavior with respect to parsing the environment as much
    as possible.

    Raises
    ------
    ValueError
      Whenever ``git-config`` would also error out, and includes an
      message in the respective exception that resembles ``git-config``'s
      for that specific case.

    Returns
    -------
    dict
      Configuration key-value mappings. When a key is declared multiple
      times, the respective values are aggregated in reported as a tuple
      for that specific key.
    """
    items: dict[str, str | Tuple[str, ...]] = {}
    for k, v in ((_get_gitconfig_var_from_env(i, 'key'),
                  _get_gitconfig_var_from_env(i, 'value'))
                 for i in range(_get_gitconfig_itemcount())):
        val = items.get(k)
        if val is None:
            items[k] = v
        elif isinstance(val, tuple):
            items[k] = val + (v,)
        else:
            items[k] = (val, v)
    return items


def _get_gitconfig_itemcount() -> int:
    try:
        return int(environ.get('GIT_CONFIG_COUNT', '0'))
    except (TypeError, ValueError) as e:
        raise ValueError("bogus count in GIT_CONFIG_COUNT") from e


def _get_gitconfig_var_from_env(nid: int, kind: str) -> str:
    envname = f'GIT_CONFIG_{kind.upper()}_{nid}'
    var = environ.get(envname)
    if var is None:
        raise ValueError(f"missing config {kind} {envname}")
    if kind != 'key':
        return var
    if not var:
        raise ValueError(f"empty config key {envname}")
    if '.' not in var:
        raise ValueError(f"key {envname} does not contain a section: {var}")
    return var


def set_gitconfig_items_in_env(items: Mapping[str, str | Tuple[str, ...]]):
    """Set git-config ENV (``GIT_CONFIG_COUNT|KEY|VALUE``) from a mapping

    Any existing declaration of configuration items in the environment is
    replaced. Any ENV variable of a *valid* existing declaration is removed,
    before the set configuration items are posted in the ENV.

    Multi-value configuration keys are supported (values provided as a tuple).

    Any item with a value of ``None`` will be posted into the ENV with an
    empty string as value, i.e. the corresponding ``GIT_CONFIG_VALUE_{count}``
    variable will be an empty string. ``None`` item values indicate that the
    configuration key was unset on the command line, via the global option
    ``-c``.

    No verification (e.g., of syntax compliance) is performed.
    """
    _clean_env_from_gitconfig_items()

    count = 0
    for key, value in items.items():
        # homogeneous processing of multiple value items, and single values
        values = value if isinstance(value, tuple) else (value,)
        for v in values:
            environ[f'GIT_CONFIG_KEY_{count}'] = key
            # we support None even though not an allowed input type, because
            # of https://github.com/datalad/datalad/issues/7589
            # this can be removed, when that issue is resolved.
            environ[f'GIT_CONFIG_VALUE_{count}'] = '' if v is None else str(v)
            count += 1
    if count:
        environ['GIT_CONFIG_COUNT'] = str(count)


def _clean_env_from_gitconfig_items():
    # we only care about intact specifications here, if there was cruft
    # to start with, we have no responsibilities
    try:
        count = _get_gitconfig_itemcount()
    except ValueError:
        return

    for i in range(count):
        environ.pop(f'GIT_CONFIG_KEY_{i}', None)
        environ.pop(f'GIT_CONFIG_VALUE_{i}', None)

    environ.pop('GIT_CONFIG_COUNT', None)
