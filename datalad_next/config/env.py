from __future__ import annotations

import json
import logging
from os import environ
from typing import (
    Any,
)

from datasalad.settings import (
    CachingSource,
)

from datalad_next.config.item import ConfigurationItem

lgr = logging.getLogger('datalad.config')


class LegacyEnvironment(CachingSource):
    """
    All loaded items have a ``store_target`` of ``Environment``, assuming
    that if they are loaded from the environment, a modification can
    also target the environment again.
    """

    def load(self) -> None:
        # not resetting here, incremental load
        for k, v in self._load_legacy_overrides().items():
            self._items[k] = ConfigurationItem(value=v)
        for k in environ:
            if not k.startswith('DATALAD_'):
                continue
            # translate variable name to config item key
            item_key = k.replace('__', '-').replace('_', '.').lower()
            self._items[item_key] = ConfigurationItem(value=environ[k])

    def _load_legacy_overrides(self) -> dict[str, Any]:
        try:
            return {
                str(k): v
                for k, v in json.loads(
                    environ.get("DATALAD_CONFIG_OVERRIDES_JSON", '{}')
                ).items()
            }
        except json.decoder.JSONDecodeError as exc:
            lgr.warning(
                "Failed to load DATALAD_CONFIG_OVERRIDES_JSON: %s",
                exc,
            )
            return {}

    def __str__(self):
        return 'LegacyEnvironment'

    def __repr__(self):
        return 'LegacyEnvironment()'
