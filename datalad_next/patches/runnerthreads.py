from __future__ import annotations

import logging

from datalad.runner import runnerthreads as mod_runnerthreads

from datalad_next.consts import COPY_BUFSIZE

# use same logger as -core, looks weird but is correct
lgr = logging.getLogger('datalad.runner.runnerthreads')

# we need to preserve it as the workhorse, this patch only wraps around it
orig_ReadThread__init__ = mod_runnerthreads.ReadThread.__init__


def ReadThread__init__(
    self,
    identifier: str,
    signal_queues: list[mod_runnerthreads.Queue],
    user_info: mod_runnerthreads.Any,
    source: mod_runnerthreads.IO,
    destination_queue: mod_runnerthreads.Queue,
    # This is the key aspect of the patch, use a platform-tailored
    # default, not the small-ish 1024 bytes
    length: int = COPY_BUFSIZE,
):
    orig_ReadThread__init__(
        self,
        identifier=identifier,
        signal_queues=signal_queues,
        user_info=user_info,
        source=source,
        destination_queue=destination_queue,
        length=length,
    )


# apply patch
lgr.debug(
    'Apply datalad-next patch to runner.runnerthreads.ReadThread.__init__')
mod_runnerthreads.ReadThread.__init__ = ReadThread__init__
