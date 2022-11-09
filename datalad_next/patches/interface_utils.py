import logging
from typing import Dict

from datalad.interface import utils as mod_interface_utils
from datalad.interface.utils import anInterface

# use same logger as -core
lgr = logging.getLogger('datalad.interface.utils')


# This function interface is taken from
# datalad-core@e94a49e3076b3c4cd340c8190e668a66f81a88ad
def _validate_cmd_call(interface: anInterface, kwargs: Dict) -> None:
    """Validate a parameterization of a command call

    This is called by `_execute_command_()` before a command call, with
    the respective Interface sub-type of the command, and all its
    arguments in keyword argument dict style. This dict also includes
    the default values for any parameter that was not explicitly included
    in the command call.

    This expected behavior is to raise an exception whenever an invalid
    parameterization is encountered.
    """
    pass


# TODO disabled for until a decision is made re usage of pydantic vs a
# constraint-based approach
## apply patch
#lgr.debug('Apply datalad-next patch to interface.utils.py:_validate_cmd_call')
#mod_interface_utils._validate_cmd_call = _validate_cmd_call
