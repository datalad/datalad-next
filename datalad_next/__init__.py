"""DataLad NEXT extension"""

__docformat__ = 'restructuredtext'

import logging
lgr = logging.getLogger('datalad.next')

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
            'datalad_next.credentials',
            # name of the command class implementation in above module
            'Credentials',
        ),
        (
            # importable module that contains the command implementation
            'datalad_next.create_sibling_webdav',
            # name of the command class implementation in above module
            'CreateSiblingWebDAV',
            # we gotta make this explicit, or the build_support code will
            # not pick it up, due to the dashes in the name
            'create-sibling-webdav',
        ),
    ]
)


# patch datalad-core
import datalad_next.patches

# register additional configuration items in datalad-core
from datalad.support.extensions import register_config
from datalad.support.constraints import EnsureBool
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


from datalad import setup_package
from datalad import teardown_package

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
