# import all the pieces one would need for an implementation
# in a single place
from annexremote import UnsupportedRequest

from datalad.customremotes import (
    # this is an enhanced RemoteError that self-documents its cause
    RemoteError,
    SpecialRemote as _SpecialRemote,
)
from datalad.customremotes.main import main as super_main


class SpecialRemote(_SpecialRemote):
    pass
