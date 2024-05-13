"""This file collects all patches for ORA/RIA-related code.

The patches have to goals:

1. Improve stability and consolidate code by using persistent shell support in
   class :class:`SSHRemoteIO`.

2. Improve ORA/RIA-related code so that it also works on Windows.
"""

from . import (
    add_method_url2transport_path,
    # this replaces SSHRemoteIO entirely
    replace_sshremoteio,
    # The following patches add Windows-support to ORA/RIA code
    ria_utils,
    replace_ora_remote,
    fix_ria_ora_tests,
    # `replace_create_sibling_ria` be imported after `replace_sshremoteio`
    # and `ria_utils`.
    replace_create_sibling_ria,
)
