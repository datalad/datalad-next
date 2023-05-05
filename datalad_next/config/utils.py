from __future__ import annotations

from os import environ
from typing import (
    Dict,
    Mapping,
    Tuple,
)


def get_gitconfig_items_from_env() -> Mapping[str, str | Tuple[str, ...]]:
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
    try:
        count = int(environ.get('GIT_CONFIG_COUNT', '0'))
    except (TypeError, ValueError) as e:
        raise ValueError("bogus count in GIT_CONFIG_COUNT") from e

    items: Dict[str, str | Tuple[str, ...]] = {}
    for k, v in ((_get_gitconfig_var_from_env(i, 'key'),
                  _get_gitconfig_var_from_env(i, 'value'))
                 for i in range(count)):
        val = items.get(k)
        if val is None:
            items[k] = v
        elif isinstance(val, tuple):
            items[k] = val + (v,)
        else:
            items[k] = (val, v)
    return items


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
