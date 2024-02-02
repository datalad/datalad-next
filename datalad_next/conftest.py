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
    # function-scope UI wrapper that can provide staged responses
    datalad_interactive_ui,
    # function-scope UI wrapper that can will raise when asked for responses
    datalad_noninteractive_ui,
    # function-scope temporary keyring
    tmp_keyring,
    # function-scope, Dataset instance
    dataset,
    # function-scope, Dataset instance with underlying repository
    existing_dataset,
    # function-scope, Dataset instance with underlying Git-only repository
    existing_noannex_dataset,
    # session-scope, Dataset instance with various modifications,
    # to-be-treated read-only
    modified_dataset,
    # session-scope, standard http credential (full dict)
    http_credential,
    # function-scope, auth-less HTTP server
    http_server,
    # function-scope, HTTP server with required authentication
    http_server_with_basicauth,
    # function-scope relay httpbin_service, unless undesired and skips instead
    httpbin,
    # session-scope HTTPBIN instance startup and URLs
    httpbin_service,
    # function-scope, disabled datalad command result rendering for all
    # command calls
    no_result_rendering,
    # session-scope redirection of log messages
    reduce_logging,
    # session-scope determine setup of an SSH server to use for testing
    sshserver_setup,
    # function-scope SSH server base url and local path
    sshserver,
    # session-scope, standard webdav credential (full dict)
    webdav_credential,
    # function-scope, serve a local temp-path via WebDAV
    webdav_server,
)
from datalad_next.iter_collections.tests.test_itertar import (
    # session-scope, downloads a tarball with a set of standard
    # file/dir/link types
    sample_tar_xz,
)
from datalad_next.iter_collections.tests.test_iterzip import (
    # session-scope, create a sample zip file
    sample_zip,
)
