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
    ]
)

from datalad import setup_package
from datalad import teardown_package

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
