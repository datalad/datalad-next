from datalad.conftest import setup_package

# fixture setup
from datalad_next.tests.fixtures import (
    # no test can leave global config modifications behind
    check_gitconfig_global,
    # no test can leave secrets behind
    check_plaintext_keyring,
    # function-scope credential manager
    credman,
    # function-scope config manager
    datalad_cfg,
    # function-scope temporary keyring
    tmp_keyring,
)
