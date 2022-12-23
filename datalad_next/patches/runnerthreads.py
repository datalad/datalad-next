"""Make ``runnerthreads.ReadThread`` use shutil's buffer size default

This has a dramatic effect on the throughput, i.e. when using
`ThreadedRunner()` for SSH-based downloads. It can take the throughput
from a few dozen MB/s to several hundreds.
"""

from __future__ import annotations

import logging

from datalad.runner import runnerthreads as mod_runnerthreads

from datalad_next.utils.consts import COPY_BUFSIZE
from datalad_next.utils.patch import apply_patch


# use same logger as -core, looks weird but is correct
lgr = logging.getLogger('datalad.runner.runnerthreads')


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


# we need to preserve it as the workhorse, this patch only wraps around it
orig_ReadThread__init__ = apply_patch(
    'datalad.runner.runnerthreads', 'ReadThread', '__init__',
    ReadThread__init__)
