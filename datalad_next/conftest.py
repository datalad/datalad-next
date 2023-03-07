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
    # function-scope, Dataset instance
    dataset,
    #function-scope, Dataset instance with underlying repository
    existing_dataset,
    #function-scope, Dataset instance with underlying Git-only repository
    existing_noannex_dataset,
    # session-scope, standard http credential (full dict)
    http_credential,
    # function-scope, auth-less HTTP server
    http_server,
    # function-scope, HTTP server with required authentication
    http_server_with_basicauth,
    # session-scope, standard webdav credential (full dict)
    webdav_credential,
    # function-scope, serve a local temp-path via WebDAV
    webdav_server,
)
