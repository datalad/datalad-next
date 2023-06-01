"""Connect ``log_progress``-style progress reporting to git-annex

This patch introduces a dedicated progress log handler as a proxy between
standard datalad progress logging and a git-annex special remote as
an approach to report (data transfer) progress to a git-annex parent process.

This functionality is only (to be) used in dedicated special remote processes.
"""

import logging
from typing import (
    Dict,
    Type,
)

from . import apply_patch
from datalad_next.annexremotes import SpecialRemote


def only_progress_logrecords(record: logging.LogRecord) -> bool:
    """Log filter to ignore any non-progress log message"""
    return hasattr(record, 'dlm_progress')


class AnnexProgressLogHandler(logging.Handler):
    """Log handler to funnel progress logs to git-annex

    For this purpose the handler wraps
    :class:`datalad_next.annexremotes.SpecialRemote` instance.
    When it receives progress log messages, it converts any
    increment reports to absolute values, and then calls
    the special remote's ``send_progress()`` method, which will
    cause the respective progress update protocol message to
    be issued.

    .. note::

       Git-annex only supports "context-free" progress reporting.  When a
       progress report is send, it is assumed to be on a currently running
       transfer. Only a single integer value can be reported, and it
       corresponds to the number of bytes transferred.

       This approach implemented here cannot distinguish progress reports
       that corresponding to git-annex triggered data transfers and other
       (potentially co-occurring) operations. The likelihood of unrelated
       operations reporting progress is relatively low, because this
       handler is only supposed to be used in dedicated special remote
       processes, but remains possible.

       This implementation is set up to support tracking multiple
       processes, and could report one of them selectively. However, at
       present any progress update is relayed to git-annex directly.
       This could lead to confusing and non-linear progress reporting.
    """
    def __init__(self, annexremote: SpecialRemote):
        super().__init__()
        self.annexremote = annexremote
        self._ptrackers: Dict[str, int] = {}

    def emit(self, record: logging.LogRecord):
        """Process a log record

        Any incoming log record, compliant with
        http://docs.datalad.org/design/progress_reporting.html
        is processed. Increment reports are converted to absolute
        values, and each update is eventually passed on to special remote,
        which issues a progress report to git-annex.
        """
        if not hasattr(record, 'dlm_progress'):
            # a filter should have been used to prevent this call
            return

        maint = getattr(record, 'dlm_progress_maint', None)
        if maint in ('clear', 'refresh'):
            return
        pid = getattr(record, 'dlm_progress')
        update = getattr(record, 'dlm_progress_update', None)
        if pid not in self._ptrackers:
            # this is new
            prg = getattr(record, 'dlm_progress_initial', 0)
            self._ptrackers[pid] = prg
            self.annexremote.send_progress(prg)
        elif update is None:
            # not an update -> done
            self._ptrackers.pop(pid)
        else:
            prg = self._ptrackers[pid]
            if getattr(record, 'dlm_progress_increment', False):
                prg += update
            else:
                prg = update
            self._ptrackers[pid] = prg
            self.annexremote.send_progress(prg)


def patched_underscore_main(args: list, cls: Type[SpecialRemote]):
    """Full replacement for datalad.customremotes.main._main()

    Its only purpose is to create a running instance of a SpecialRemote.
    The only difference to the original in datalad-core is that once this
    instance exists, it is linked to a log handler that converts incoming
    progress log messages to the equivalent annex protocol progress reports.

    This additional log handler is a strict addition to the log handling
    setup established at this point. There should be no interference with
    any other log message processing.

    .. seealso::

       :class:`AnnexProgressLogHandler`
    """
    assert cls is not None
    from annexremote import Master
    master = Master()
    remote = cls(master)
    master.LinkRemote(remote)

    # we add an additional handler to the logger to deal with
    # progress reports
    dlroot_lgr = logging.getLogger('datalad')
    phandler = AnnexProgressLogHandler(remote)
    phandler.addFilter(only_progress_logrecords)
    dlroot_lgr.addHandler(phandler)

    # run the remote
    master.Listen()
    # cleanup
    if hasattr(remote, 'stop'):
        remote.stop()


apply_patch(
    'datalad.customremotes.main', None, '_main',
    patched_underscore_main,
    msg='Replace special remote _main() '
    "with datalad-next's progress logging enabled variant")
