import warnings
warnings.warn(
    "datalad_next.utils.credman was replaced by datalad_next.credman in "
    "datalad_next 1.0. This transition helper module will be removed in "
    "datalad_next 2.0.",
    DeprecationWarning,
)

from datalad_next.credman.manager import (
    CredentialManager,
    verify_property_names,
)
