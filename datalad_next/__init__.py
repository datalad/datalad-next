"""DataLad NEXT extension"""

from __future__ import annotations

__docformat__ = 'restructuredtext'


import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datalad_next.commands import ResultHandler


lgr = logging.getLogger('datalad.next')


_command_result_handler_class: type[ResultHandler] | None = None


def set_command_result_handler_class(handler_cls: type[ResultHandler] | None):
    """Set a global result handler class for use by `eval_results`

    This must be a class implementing the `ResultHandler` interface.

    If set to `None`, a default implementation will be used.
    """
    global _command_result_handler_class  # noqa: PLW0603
    _command_result_handler_class = handler_cls


def get_command_result_handler_class() -> type[ResultHandler] | None:
    """Get the set a global result handler class for use by `eval_results`

    See `set_command_result_handler_class()` for more information.
    """
    return _command_result_handler_class


# Defines a datalad command suite.
# This variable must be bound as a setuptools entrypoint
# to be found by datalad
command_suite = (
    # description of the command suite, displayed in cmdline help
    "What is next in DataLad",
    [
        # specification of a command, any number of commands can be defined
        (
            # importable module that contains the command implementation
            'datalad_next.commands.credentials',
            # name of the command class implementation in above module
            'Credentials',
        ),
        (
            # importable module that contains the command implementation
            'datalad_next.commands.create_sibling_webdav',
            # name of the command class implementation in above module
            'CreateSiblingWebDAV',
            # we gotta make this explicit, or the build_support code will
            # not pick it up, due to the dashes in the name
            'create-sibling-webdav',
        ),
        (
            # importable module that contains the command implementation
            'datalad_next.commands.tree',
            # name of the command class implementation in above module
            'TreeCommand',
            # command name (differs from lowercase command class name)
            'tree'
        ),
        (
            'datalad_next.commands.download', 'Download', 'download',
        ),
        (
            'datalad_next.commands.ls_file_collection', 'LsFileCollection',
            'ls-file-collection',
        ),
        (
            'datalad_next.commands.status', 'Status',
            'next-status', 'next_status',
        ),
    ]
)


# patch datalad-core
# register additional configuration items in datalad-core
from datalad.support.extensions import register_config

import datalad_next.patches.enabled
from datalad_next.constraints import (
    EnsureBool,
    EnsureChoice,
)

register_config(
    'datalad.credentials.repeat-secret-entry',
    'Require entering secrets twice for interactive specification?',
    type=EnsureBool(),
    default=True,
    dialog='yesno')
register_config(
    'datalad.credentials.hidden-secret-entry',
    'Hide secret in interactive entry?',
    type=EnsureBool(),
    default=True,
    dialog='yesno')
register_config(
    'datalad.clone.url-substitute.webdav',
    'webdav(s):// clone URL substitution',
    description="Convenience conversion of custom WebDAV URLs to "
    "git-cloneable 'datalad-annex::'-type URLs. The 'webdav://' "
    "prefix implies a remote sibling in 'filetree' or 'export' mode "
    "See https://docs.datalad.org/design/url_substitution.html for details",
    dialog='question',
    scope='global',
    default=(
        r',^webdav([s]*)://([^?]+)$,datalad-annex::http\1://\2?type=webdav&encryption=none&exporttree=yes&url={noquery}',
    ),
)
register_config(
    'datalad.runtime.parameter-violation',
    'Perform exhaustive command parameter validation, or fail on first error?',
    type=EnsureChoice('raise-early', 'raise-at-end'),
    default='raise-early',
    dialog='question',
)
register_config(
    'datalad.archivist.legacy-mode',
    'Fall back on legacy ``datalad-archives`` special remote implementation?',
    description='If enabled, all `archivist` special remote operations '
    'fall back onto the legacy ``datalad-archives`` special remote '
    'implementation. This mode is only provided for backward-compatibility. '
    'This legacy implementation unconditionally downloads archive files '
    'completely, and keeps an internal cache of the full extracted archive '
    'around. The implied 200% storage cost overhead for obtaining a complete '
    'dataset can be prohibitive for datasets tracking large amount of data '
    '(in archive files).',
    type=EnsureBool(),
    default=False,
    dialog='yesno',
)


try:
    from ._version import version as __version__
except ImportError:
    pass
