"""Add the method :meth:`url2transport_path` to RIA IO-abstraction classes

This patch adds the method :meth:`url2transport_path` to the IO-abstraction
classes: :class:`datalad.distributed.ora_remote.LocalIO`, and to the class
:class:`datalad.distributed.ora_remote.HTTPRemoteIO`.

This method is required by the patches that add Windows-client support to
RIA-code. It converts internally used abstract paths to concrete paths that are
platform- andIO-abstraction specific and on which IO-operations cam be
performed.
"""
from __future__ import annotations

import logging
from re import compile
from pathlib import (
    Path,
    PurePosixPath,
)

from datalad_next.consts import on_windows
from . import apply_patch


# The methods are patched into the ora_remote/ria_remote. Use the same logger.
lgr = logging.getLogger('datalad.customremotes.ria_remote')


drive_letter_matcher = compile('^/[A-Z]:')


def str2windows_path(url_path: PurePosixPath):
    path_str = str(url_path)
    match = drive_letter_matcher.match(path_str)
    if match:
        if path_str[3] == '/':
            return Path(*([f'{path_str[1]}:', '/'] + path_str[4:].split('/')))
        else:
            lgr.warning(f'Non-absolute Windows-path detected: {path_str}')
            return Path(*([f'{path_str[1]}:'] + path_str[3:].split('/')))
    else:
        return Path(path_str)


def local_io_url2transport_path(
        self,
        url_path: PurePosixPath
) -> Path | PurePosixPath:
    assert url_path.__class__ is PurePosixPath
    if on_windows:
        return str2windows_path(url_path)
    else:
        return Path(url_path)


def http_remote_io_url2transport_path(
        self,
        url_path: PurePosixPath
) -> Path | PurePosixPath:
    assert url_path.__class__ is PurePosixPath
    return url_path


# Add a `url2transport_path`-method to `ora_remote.LocalIO`
apply_patch(
    'datalad.distributed.ora_remote',
    'LocalIO',
    'url2transport_path',
    local_io_url2transport_path,
    expect_attr_present=False,
)


# Add a `url2transport_path`-method to `ora_remote.HTTPRemoteIO`
apply_patch(
    'datalad.distributed.ora_remote',
    'HTTPRemoteIO',
    'url2transport_path',
    http_remote_io_url2transport_path,
    expect_attr_present=False,
)
